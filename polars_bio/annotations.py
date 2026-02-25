"""Annotation operations for VEP cache lookups and cache conversion.

Provides ``pb.annotations.lookup_variants()`` for annotating VCF variants
against native Ensembl, parquet, or fjall VEP caches, and
``pb.annotations.create_vep_cache()`` for converting native caches to
parquet or fjall format.
"""

import os
from typing import List, Optional, Union

import polars as pl

from .context import _resolve_zero_based, ctx
from .logging import logger

try:
    import pandas as pd
except ImportError:
    pd = None


def _resolve_cache_format(cache_path: str, cache_format: str) -> str:
    """Resolve the cache format from an explicit value or auto-detect from path.

    Args:
        cache_path: Filesystem path to the cache.
        cache_format: One of ``"auto"``, ``"native"``, ``"parquet"``, ``"fjall"``.

    Returns:
        Resolved format string: ``"native"``, ``"parquet"``, or ``"fjall"``.

    Raises:
        ValueError: If auto-detection fails or format is invalid.
    """
    _VALID_FORMATS = {"auto", "native", "parquet", "fjall"}
    if cache_format not in _VALID_FORMATS:
        raise ValueError(
            f"Invalid cache_format: {cache_format!r}. "
            f"Must be one of {sorted(_VALID_FORMATS)}"
        )

    if cache_format != "auto":
        return cache_format

    # Auto-detect
    if cache_path.endswith(".parquet") or cache_path.endswith(".parquet.snappy"):
        return "parquet"

    if os.path.isdir(cache_path):
        # Ensembl VEP native caches contain an info.txt sentinel file at the root
        if os.path.isfile(os.path.join(cache_path, "info.txt")):
            return "native"
        # Directory without info.txt -> assume fjall KV store
        return "fjall"

    raise ValueError(
        f"Cannot auto-detect cache format for path: {cache_path!r}. "
        "Pass cache_format='native', 'parquet', or 'fjall' explicitly."
    )


class AnnotationOperations:
    """Annotation operations: VEP cache lookups and cache conversion."""

    @staticmethod
    def annotate_variants(
        vcf_path: str,
        cache_path: str,
        cache_format: str = "auto",
        cache_entity: str = "variation",
        columns: Optional[List[str]] = None,
        prune_nulls: bool = False,
        match_mode: str = "exact",
        vcf_info_fields: Optional[List[str]] = None,
        use_zero_based: Optional[bool] = None,
        output_type: str = "polars.LazyFrame",
    ) -> Union[pl.LazyFrame, pl.DataFrame, "pd.DataFrame"]:
        """Annotate VCF variants against a VEP cache.

        Supports native Ensembl VEP caches, parquet caches, and fjall
        KV caches. The cache format is auto-detected by default.

        Args:
            vcf_path: Path to the VCF file (.vcf, .vcf.gz, .vcf.bgz).
            cache_path: Path to the VEP cache.
            cache_format: ``"auto"`` (default), ``"native"``, ``"parquet"``,
                or ``"fjall"``.
            cache_entity: Cache entity type. Default ``"variation"``.
            columns: Cache column names to include in output.
            prune_nulls: Remove rows with all-null cache columns.
            match_mode: Allele matching: ``"exact"``,
                ``"exact_or_colocated_ids"``, or ``"exact_or_vep_existing"``.
            vcf_info_fields: VCF INFO fields to parse.
            use_zero_based: Coordinate system override.
            output_type: ``"polars.LazyFrame"``, ``"polars.DataFrame"``,
                or ``"pandas.DataFrame"``.

        Returns:
            Annotated DataFrame in the requested format.
        """
        from polars_bio.polars_bio import InputFormat, ReadOptions, VepCacheReadOptions

        from .io import _parse_vep_cache_entity
        from .vep_op import _vep_annotate_impl

        resolved = _resolve_cache_format(cache_path, cache_format)
        zero_based = _resolve_zero_based(use_zero_based)

        if resolved == "parquet":
            cache_input_format = InputFormat.Parquet
            cache_ro = ReadOptions()
        elif resolved == "native":
            parsed_entity = _parse_vep_cache_entity(cache_entity)
            vep_cache_read_options = VepCacheReadOptions(
                entity=parsed_entity,
                zero_based=zero_based,
            )
            cache_input_format = InputFormat.VepCache
            cache_ro = ReadOptions(vep_cache_read_options=vep_cache_read_options)
        elif resolved == "fjall":
            cache_input_format = InputFormat.VepKvCache
            cache_ro = ReadOptions()
        else:
            raise ValueError(f"Unexpected resolved format: {resolved!r}")

        return _vep_annotate_impl(
            vcf_path=vcf_path,
            cache_path=cache_path,
            cache_input_format=cache_input_format,
            cache_read_options=cache_ro,
            cache_entity=cache_entity,
            columns=columns,
            prune_nulls=prune_nulls,
            match_mode=match_mode,
            vcf_info_fields=vcf_info_fields,
            zero_based=zero_based,
            output_type=output_type,
        )

    @staticmethod
    def create_vep_cache(
        source_path: str,
        output_path: str,
        output_format: str = "parquet",
        entity: str = "variation",
        use_zero_based: Optional[bool] = None,
    ) -> str:
        """Convert a native Ensembl VEP cache to parquet or fjall format.

        Args:
            source_path: Path to the native Ensembl VEP cache directory.
            output_path: Destination path for the converted cache.
            output_format: ``"parquet"`` or ``"fjall"``.
            entity: Cache entity type. Default ``"variation"``.
            use_zero_based: Coordinate system override.

        Returns:
            The output path.

        Raises:
            ValueError: If output_format is invalid.
        """
        from polars_bio.polars_bio import (
            InputFormat,
            ReadOptions,
            VepCacheReadOptions,
            py_create_vep_kv_cache,
        )

        from .io import IOOperations, _parse_vep_cache_entity

        _VALID_OUTPUT_FORMATS = {"parquet", "fjall"}
        if output_format not in _VALID_OUTPUT_FORMATS:
            raise ValueError(
                f"Invalid output_format: {output_format!r}. "
                f"Must be one of {sorted(_VALID_OUTPUT_FORMATS)}"
            )

        zero_based = _resolve_zero_based(use_zero_based)

        if output_format == "parquet":
            lf = IOOperations.scan_vep_cache(
                source_path, entity=entity, use_zero_based=use_zero_based
            )
            lf.sink_parquet(output_path)
            logger.info("VEP parquet cache written to %s", output_path)
            return output_path

        # fjall
        parsed_entity = _parse_vep_cache_entity(entity)
        vep_cache_read_options = VepCacheReadOptions(
            entity=parsed_entity,
            zero_based=zero_based,
        )
        cache_ro = ReadOptions(vep_cache_read_options=vep_cache_read_options)

        result_path = py_create_vep_kv_cache(
            ctx,
            source_path,
            output_path,
            InputFormat.VepCache,
            cache_ro,
        )
        logger.info("VEP fjall cache created at %s", result_path)
        return result_path
