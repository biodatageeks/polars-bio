from typing import Dict, Iterator, List, Optional, Union

import polars as pl
import pyarrow as pa
from polars.io.plugins import register_io_source
from tqdm.auto import tqdm

from ._metadata import set_coordinate_system, set_source_metadata
from .context import _resolve_zero_based, ctx
from .logging import logger

try:
    import pandas as pd
except ImportError:
    pd = None


def _extract_column_names_from_expr(with_columns) -> List[str]:
    """Extract column names from Polars expressions (same logic as io.py)."""
    if with_columns is None:
        return []
    if hasattr(with_columns, "__iter__") and not isinstance(with_columns, str):
        column_names = []
        for item in with_columns:
            if isinstance(item, str):
                column_names.append(item)
            elif hasattr(item, "meta") and hasattr(item.meta, "output_name"):
                try:
                    column_names.append(item.meta.output_name())
                except Exception:
                    pass
        return column_names
    elif isinstance(with_columns, str):
        return [with_columns]
    elif hasattr(with_columns, "meta") and hasattr(with_columns.meta, "output_name"):
        try:
            return [with_columns.meta.output_name()]
        except Exception:
            pass
    return []


class VepOperations:
    """VEP (Variant Effect Predictor) annotation operations.

    Annotates VCF variants against an Ensembl VEP variation cache using
    interval join with allele matching via the ``lookup_variants()`` UDTF.
    """

    @staticmethod
    def vep_annotate(
        vcf_path: str,
        cache_path: str,
        cache_entity: str = "variation",
        columns: Optional[List[str]] = None,
        prune_nulls: bool = False,
        vcf_info_fields: Optional[List[str]] = None,
        use_zero_based: Optional[bool] = None,
        output_type: str = "polars.LazyFrame",
    ) -> Union[pl.LazyFrame, pl.DataFrame, "pd.DataFrame"]:
        """Annotate VCF variants against an Ensembl VEP variation cache.

        Performs an interval join between VCF variants and the VEP cache,
        matching by genomic position and allele. Returns VCF columns plus
        cache annotation columns.

        Args:
            vcf_path: Path to the VCF file (.vcf, .vcf.gz, .vcf.bgz).
            cache_path: Path to the VEP cache. Accepts either a Parquet file
                (created via ``scan_vep_cache(...).sink_parquet()``) or the
                original Ensembl VEP cache root directory.
            cache_entity: Cache entity type. Default ``"variation"``.
            columns: List of cache column names to include in output.
                If None, uses upstream defaults (``variation_name``,
                ``allele_string``, ``clin_sig``).
            prune_nulls: If True, remove rows where all cache columns
                are null (no annotation match). Default False.
            vcf_info_fields: List of VCF INFO field names to parse.
                If None, all INFO fields are auto-detected from header.
            use_zero_based: Coordinate system for both VCF and cache.

                - ``None`` (default) -- use global config (``pb.options``),
                  which defaults to 1-based.
                - ``True`` -- 0-based half-open coordinates.
                - ``False`` -- 1-based closed coordinates.
            output_type: One of ``"polars.LazyFrame"``,
                ``"polars.DataFrame"``, or ``"pandas.DataFrame"``.

        Returns:
            DataFrame with VCF columns and cache annotation columns.

        Example:
            ```python
            import polars_bio as pb

            # Basic annotation
            df = pb.vep_annotate("variants.vcf", "/path/to/vep_cache").collect()

            # Select specific cache columns
            df = pb.vep_annotate(
                "variants.vcf",
                "/path/to/vep_cache",
                columns=["variation_name", "clin_sig"],
            ).collect()

            # As pandas DataFrame
            pdf = pb.vep_annotate(
                "variants.vcf",
                "/path/to/vep_cache",
                output_type="pandas.DataFrame",
            )
            ```
        """
        from polars_bio.polars_bio import (
            InputFormat,
            ReadOptions,
            VcfReadOptions,
            VepCacheReadOptions,
            py_get_table_schema,
            py_read_sql,
            py_register_table,
        )

        from .io import _parse_vep_cache_entity

        zero_based = _resolve_zero_based(use_zero_based)

        # 1. Register VCF table (pass same zero_based to match cache coords)
        vcf_read_options = VcfReadOptions(
            info_fields=vcf_info_fields,
            zero_based=zero_based,
        )
        vcf_ro = ReadOptions(vcf_read_options=vcf_read_options)
        vcf_table = py_register_table(ctx, vcf_path, None, InputFormat.Vcf, vcf_ro)
        vcf_table_name = vcf_table.name

        # 2. Register VEP cache table (parquet or original format)
        is_parquet_cache = cache_path.endswith(".parquet") or cache_path.endswith(
            ".parquet.snappy"
        )
        if is_parquet_cache:
            cache_ro = ReadOptions()
            cache_table = py_register_table(
                ctx, cache_path, None, InputFormat.Parquet, cache_ro
            )
        else:
            parsed_entity = _parse_vep_cache_entity(cache_entity)
            vep_cache_read_options = VepCacheReadOptions(
                entity=parsed_entity,
                zero_based=zero_based,
            )
            cache_ro = ReadOptions(vep_cache_read_options=vep_cache_read_options)
            cache_table = py_register_table(
                ctx, cache_path, None, InputFormat.VepCache, cache_ro
            )
        cache_table_name = cache_table.name
        cache_input_format = (
            InputFormat.Parquet if is_parquet_cache else InputFormat.VepCache
        )

        # 3. Build SQL for lookup_variants() UDTF
        sql_args = [f"'{vcf_table_name}'", f"'{cache_table_name}'"]
        if columns is not None:
            cols_str = ",".join(columns)
            sql_args.append(f"'{cols_str}'")
            sql_args.append("true" if prune_nulls else "false")
        elif prune_nulls:
            # Need to pass default columns arg to reach prune_nulls
            sql_args.append("''")
            sql_args.append("true")

        sql_text = f"SELECT * FROM lookup_variants({', '.join(sql_args)})"
        logger.debug("VEP annotate SQL: %s", sql_text)

        # 4. Extract VCF metadata from the registered VCF table schema
        vcf_schema = py_get_table_schema(ctx, vcf_table_name)
        from .metadata_extractors import extract_all_schema_metadata

        full_metadata = extract_all_schema_metadata(vcf_schema)
        format_specific = full_metadata.get("format_specific", {})
        vcf_meta = format_specific.get("vcf", {})
        header_metadata = {
            "info_fields": vcf_meta.get("info_fields") or {},
            "format_fields": vcf_meta.get("format_fields"),
            "sample_names": vcf_meta.get("sample_names"),
            "version": vcf_meta.get("version"),
            "contigs": vcf_meta.get("contigs"),
            "filters": vcf_meta.get("filters"),
            "alt_definitions": vcf_meta.get("alt_definitions"),
        }

        # 5. Get result schema from a lazy query (no data read)
        query_df = py_read_sql(ctx, sql_text)
        arrow_schema = query_df.schema()  # returns PyArrow Schema

        # Build rename mapping: strip "cache_" prefix from annotation columns
        cache_rename = {}  # {original_name: unprefixed_name}
        for field in arrow_schema:
            if field.name.startswith("cache_"):
                cache_rename[field.name] = field.name[len("cache_") :]

        empty_table = pa.table(
            {
                cache_rename.get(field.name, field.name): pa.array([], type=field.type)
                for field in arrow_schema
            }
        )
        polars_schema = dict(pl.from_arrow(empty_table).schema)

        # Add VEP annotation columns as INFO fields in VCF metadata
        # (using unprefixed names so sink_vcf writes clean field names)
        _ARROW_TYPE_TO_VCF = {
            "int64": "Integer",
            "int32": "Integer",
            "uint32": "Integer",
            "float": "Float",
            "double": "Float",
            "string": "String",
            "large_string": "String",
            "utf8": "String",
            "large_utf8": "String",
            "bool": "Flag",
        }
        info_fields = header_metadata.get("info_fields") or {}
        for field in arrow_schema:
            if field.name.startswith("cache_"):
                unprefixed = cache_rename[field.name]
                vcf_type = _ARROW_TYPE_TO_VCF.get(str(field.type), "String")
                info_fields[unprefixed] = {
                    "number": ".",
                    "type": vcf_type,
                    "description": f"VEP cache annotation: {unprefixed}",
                }
        header_metadata["info_fields"] = info_fields

        # Reverse mapping: unprefixed -> original cache_ name (for projection pushdown)
        reverse_rename = {v: k for k, v in cache_rename.items()}

        # 6. Define streaming callback (executed on every .collect() / sink)
        def _vep_source(
            with_columns: Union[pl.Expr, None],
            predicate: Union[pl.Expr, None],
            n_rows: Union[int, None],
            _batch_size: Union[int, None],
        ) -> Iterator[pl.DataFrame]:
            from polars_bio.polars_bio import py_read_sql as _py_read_sql
            from polars_bio.polars_bio import py_register_table as _py_register_table

            from .context import ctx as _ctx

            # Re-register tables each time the callback fires so the
            # LazyFrame stays valid across multiple .collect() / sink calls.
            _py_register_table(_ctx, vcf_path, vcf_table_name, InputFormat.Vcf, vcf_ro)
            _py_register_table(
                _ctx, cache_path, cache_table_name, cache_input_format, cache_ro
            )

            inner_df = _py_read_sql(_ctx, sql_text)

            # Projection pushdown — map unprefixed names back to cache_ names
            # for the DataFusion query, then rename after streaming.
            projection_applied = False
            if with_columns is not None:
                requested_cols = _extract_column_names_from_expr(with_columns)
                if requested_cols:
                    try:
                        df_cols = [reverse_rename.get(c, c) for c in requested_cols]
                        select_exprs = [
                            inner_df.parse_sql_expr(f'"{c}"') for c in df_cols
                        ]
                        inner_df = inner_df.select(*select_exprs)
                        projection_applied = True
                    except Exception as e:
                        logger.debug("VEP projection pushdown failed: %s", e)

            # Predicate pushdown
            predicate_pushed_down = False
            if predicate is not None:
                try:
                    from .predicate_translator import (
                        datafusion_expr_to_sql,
                        translate_predicate,
                    )

                    df_expr = translate_predicate(
                        predicate,
                        string_cols={
                            "chrom",
                            "ref",
                            "alt",
                            "id",
                            "filter",
                            "variation_name",
                            "allele_string",
                            "clin_sig",
                        },
                        uint32_cols={"start", "end", "pos", "qual"},
                    )
                    sql_predicate = datafusion_expr_to_sql(df_expr)
                    native_expr = inner_df.parse_sql_expr(sql_predicate)
                    inner_df = inner_df.filter(native_expr)
                    predicate_pushed_down = True
                except Exception as e:
                    logger.debug("VEP predicate pushdown failed: %s", e)

            # Limit pushdown
            if n_rows and n_rows > 0:
                inner_df = inner_df.limit(int(n_rows))

            # Stream batches
            df_stream = inner_df.execute_stream()
            progress_bar = tqdm(unit="rows")
            remaining = int(n_rows) if n_rows is not None else None

            for batch in df_stream:
                out = pl.DataFrame(batch.to_pyarrow())

                # Strip cache_ prefix from column names
                if cache_rename:
                    out = out.rename(
                        {k: v for k, v in cache_rename.items() if k in out.columns}
                    )

                # Client-side predicate filtering (fallback)
                if predicate is not None and not predicate_pushed_down:
                    out = out.filter(predicate)

                # Client-side projection fallback
                if with_columns is not None and not projection_applied:
                    out = out.select(with_columns)

                if remaining is not None:
                    if remaining <= 0:
                        break
                    if len(out) > remaining:
                        out = out.head(remaining)
                    remaining -= len(out)

                progress_bar.update(len(out))
                yield out

                if remaining is not None and remaining <= 0:
                    return

            # Clean up registered tables to free memory
            for tname in (vcf_table_name, cache_table_name):
                try:
                    _ctx.deregister_table(tname)
                except Exception:
                    pass

        # 7. Create lazy frame and set VCF metadata
        lf = register_io_source(_vep_source, schema=polars_schema)
        set_coordinate_system(lf, zero_based)
        set_source_metadata(lf, format="vcf", path=vcf_path, header=header_metadata)

        # 8. Handle output_type
        if output_type == "polars.LazyFrame":
            return lf
        elif output_type == "polars.DataFrame":
            return lf.collect()
        elif output_type == "pandas.DataFrame":
            if pd is None:
                raise ImportError(
                    "pandas is not installed. Please run `pip install pandas` "
                    "or `pip install polars-bio[pandas]`."
                )
            return lf.collect().to_pandas()
        else:
            raise ValueError(f"Invalid output_type: {output_type!r}")
