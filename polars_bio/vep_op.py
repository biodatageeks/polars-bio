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


def _normalize_lookup_columns(columns: Optional[List[str]]) -> Optional[List[str]]:
    """Normalize user-provided annotation columns for lookup_variants()."""
    if columns is None:
        return None

    normalized: List[str] = []
    seen = set()
    for col in columns:
        candidate = str(col).strip()
        if not candidate:
            continue
        if "," in candidate:
            raise ValueError(
                f"Invalid column name {candidate!r}: commas are not allowed in column names."
            )
        if "'" in candidate:
            raise ValueError(
                f"Invalid column name {candidate!r}: single quotes are not allowed."
            )
        if candidate not in seen:
            normalized.append(candidate)
            seen.add(candidate)

    return normalized or None


def _default_lookup_columns(cache_schema) -> List[str]:
    """Default cache columns used when lookup args must be fully explicit."""
    coord_cols = {"chrom", "start", "end"}
    return [
        field.name
        for field in cache_schema
        if field.name not in coord_cols and not field.name.startswith("source_")
    ]


def _vep_annotate_impl(
    vcf_path: str,
    cache_path: str,
    cache_input_format,
    cache_read_options,
    cache_entity: str = "variation",
    columns: Optional[List[str]] = None,
    prune_nulls: bool = False,
    match_mode: str = "exact",
    vcf_info_fields: Optional[List[str]] = None,
    zero_based: bool = False,
    output_type: str = "polars.LazyFrame",
) -> Union[pl.LazyFrame, pl.DataFrame, "pd.DataFrame"]:
    """Core VEP annotation pipeline shared by VepOperations and AnnotationOperations.

    Registers the VCF and cache tables, builds the lookup_variants() SQL,
    creates a streaming Polars LazyFrame with metadata, and returns the result.

    Args:
        vcf_path: Path to the VCF file.
        cache_path: Path to the VEP cache (native, parquet, or fjall).
        cache_input_format: InputFormat enum value for the cache.
        cache_read_options: ReadOptions for the cache table registration.
        cache_entity: Cache entity type (only used for display/logging).
        columns: Cache column names to include.
        prune_nulls: Remove rows with all-null cache columns.
        match_mode: Allele matching strategy.
        vcf_info_fields: VCF INFO fields to parse.
        zero_based: Coordinate system flag.
        output_type: Output format string.

    Returns:
        LazyFrame, DataFrame, or pandas DataFrame.
    """
    from polars_bio.polars_bio import (
        InputFormat,
        ReadOptions,
        VcfReadOptions,
        py_get_table_schema,
        py_read_sql,
        py_register_table,
    )

    # 1. Register VCF table
    vcf_read_options = VcfReadOptions(
        info_fields=vcf_info_fields,
        zero_based=zero_based,
    )
    vcf_ro = ReadOptions(vcf_read_options=vcf_read_options)
    vcf_table = py_register_table(ctx, vcf_path, None, InputFormat.Vcf, vcf_ro)
    vcf_table_name = vcf_table.name

    # 2. Register cache table
    cache_table = py_register_table(
        ctx, cache_path, None, cache_input_format, cache_read_options
    )
    cache_table_name = cache_table.name

    # 3. Build SQL for lookup_variants() UDTF
    _VALID_MATCH_MODES = {"exact", "exact_or_colocated_ids", "exact_or_vep_existing"}
    normalized_match_mode = match_mode.strip().lower()
    if normalized_match_mode not in _VALID_MATCH_MODES:
        raise ValueError(
            f"Invalid match_mode: {match_mode!r}. "
            f"Must be one of {sorted(_VALID_MATCH_MODES)}"
        )
    normalized_columns = _normalize_lookup_columns(columns)

    sql_args = [f"'{vcf_table_name}'", f"'{cache_table_name}'"]
    if normalized_columns is not None:
        cols_str = ",".join(normalized_columns)
        sql_args.append(f"'{cols_str}'")
        sql_args.append("true" if prune_nulls else "false")
        sql_args.append(f"'{normalized_match_mode}'")
    elif prune_nulls or normalized_match_mode != "exact":
        cache_schema = py_get_table_schema(ctx, cache_table_name)
        default_cols = _default_lookup_columns(cache_schema)
        sql_args.append(f"'{','.join(default_cols)}'")
        sql_args.append("true" if prune_nulls else "false")
        sql_args.append(f"'{normalized_match_mode}'")

    sql_text = f"SELECT * FROM lookup_variants({', '.join(sql_args)})"
    logger.debug("VEP annotate SQL: %s", sql_text)

    # 4. Extract VCF metadata
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

    # 5. Get result schema
    query_df = py_read_sql(ctx, sql_text)
    arrow_schema = query_df.schema()

    cache_rename = {}
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

    reverse_rename = {v: k for k, v in cache_rename.items()}

    # 6. Streaming callback
    def _vep_source(
        with_columns: Union[pl.Expr, None],
        predicate: Union[pl.Expr, None],
        n_rows: Union[int, None],
        _batch_size: Union[int, None],
    ) -> Iterator[pl.DataFrame]:
        from polars_bio.polars_bio import py_read_sql as _py_read_sql
        from polars_bio.polars_bio import py_register_table as _py_register_table

        from .context import ctx as _ctx

        _py_register_table(_ctx, vcf_path, vcf_table_name, InputFormat.Vcf, vcf_ro)
        _py_register_table(
            _ctx, cache_path, cache_table_name, cache_input_format, cache_read_options
        )

        inner_df = _py_read_sql(_ctx, sql_text)

        projection_applied = False
        if with_columns is not None:
            requested_cols = _extract_column_names_from_expr(with_columns)
            if requested_cols:
                try:
                    df_cols = [reverse_rename.get(c, c) for c in requested_cols]
                    select_exprs = [inner_df.parse_sql_expr(f'"{c}"') for c in df_cols]
                    inner_df = inner_df.select(*select_exprs)
                    projection_applied = True
                except Exception as e:
                    logger.debug("VEP projection pushdown failed: %s", e)

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

        if n_rows and n_rows > 0:
            inner_df = inner_df.limit(int(n_rows))

        df_stream = inner_df.execute_stream()
        progress_bar = tqdm(unit="rows")
        remaining = int(n_rows) if n_rows is not None else None

        for batch in df_stream:
            out = pl.DataFrame(batch.to_pyarrow())

            if cache_rename:
                out = out.rename(
                    {k: v for k, v in cache_rename.items() if k in out.columns}
                )

            if predicate is not None and not predicate_pushed_down:
                out = out.filter(predicate)

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

        for tname in (vcf_table_name, cache_table_name):
            try:
                _ctx.deregister_table(tname)
            except Exception:
                pass

    # 7. Create lazy frame with metadata
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
