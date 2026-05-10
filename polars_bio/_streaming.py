from dataclasses import dataclass
from typing import Callable, Iterator, List, Optional, Set, Union

import polars as pl
import pyarrow as pa
from tqdm.auto import tqdm

from polars_bio.logging import logger


@dataclass(frozen=True)
class PredicatePushdownConfig:
    string_cols: Optional[Set[str]]
    uint32_cols: Optional[Set[str]]
    float32_cols: Optional[Set[str]]


@dataclass(frozen=True)
class StreamingConfig:
    use_sql_expr_projection: bool = True   # False for range_op (uses direct select)
    predicate_config: Optional[PredicatePushdownConfig] = None  # None = disabled
    pre_query_hook: Optional[Callable] = None  # e.g. GFF re-registration
    apply_limit_pushdown: bool = True      # False for range_op (limit in Rust)
    projection_pushdown: bool = True       # False to disable projection entirely


def extract_column_names(with_columns: Union[pl.Expr, list]) -> List[str]:
    """Extract column names from Polars expressions."""
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


def pyarrow_schema_to_polars_dict(schema: pa.Schema) -> dict[str, pl.DataType]:
    empty_table = pa.Table.from_arrays(
        [pa.array([], type=field.type) for field in schema],
        schema=schema,
    )
    df = pl.from_arrow(empty_table)
    return dict(df.schema)


def make_streaming_source(
    df_factory: Callable[[Optional[str], Optional[int]], object],
    table_name: Optional[str],
    config: StreamingConfig,
) -> Callable:
    """Return a register_io_source-compatible callback.

    Args:
        df_factory: Called on every collect(). Receives (table_name, n_rows) and
            returns a DataFusion DataFrame.  Range ops use n_rows to pass the limit
            into Rust; scan/pileup ignore it and apply the limit here instead.
        table_name: Logical table name passed through to df_factory and the
            pre-query hook.  May be None for range ops.
        config: Controls which pushdowns are active and how they are applied.
    """

    def _source(
        with_columns: Union[pl.Expr, None],
        predicate: Union[pl.Expr, None],
        n_rows: Union[int, None],
        _batch_size: Union[int, None],
    ) -> Iterator[pl.DataFrame]:
        # --- pre-query hook (e.g. GFF re-registration) ---
        effective_table = table_name
        if config.pre_query_hook is not None:
            result = config.pre_query_hook(table_name, with_columns)
            if result is not None:
                effective_table = result

        # --- acquire DataFusion DataFrame ---
        query_df = df_factory(
            effective_table,
            n_rows if not config.apply_limit_pushdown else None,
        )

        # --- predicate pushdown ---
        predicate_pushed_down = False
        if config.predicate_config is not None and predicate is not None:
            try:
                from polars_bio.predicate_translator import (
                    datafusion_expr_to_sql,
                    translate_predicate,
                )

                cfg = config.predicate_config
                df_expr = translate_predicate(
                    predicate,
                    string_cols=cfg.string_cols,
                    uint32_cols=cfg.uint32_cols,
                    float32_cols=cfg.float32_cols,
                )
                sql_predicate = datafusion_expr_to_sql(df_expr)
                native_expr = query_df.parse_sql_expr(sql_predicate)
                query_df = query_df.filter(native_expr)
                predicate_pushed_down = True
            except Exception as e:
                logger.debug("Predicate pushdown failed: %s", e)

        # --- projection pushdown ---
        projection_applied = False
        if config.projection_pushdown and with_columns is not None:
            requested_cols = extract_column_names(with_columns)
            if requested_cols:
                try:
                    if config.use_sql_expr_projection:
                        select_exprs = [
                            query_df.parse_sql_expr(f'"{c}"') for c in requested_cols
                        ]
                        query_df = query_df.select(*select_exprs)
                    else:
                        query_df = query_df.select(requested_cols)
                    projection_applied = True
                except Exception as e:
                    logger.debug("Projection pushdown failed: %s", e)

        # --- limit pushdown (scan/pileup only; range ops handle limit in Rust) ---
        if config.apply_limit_pushdown and n_rows and n_rows > 0:
            query_df = query_df.limit(int(n_rows))

        # --- stream batches ---
        df_stream = query_df.execute_stream()
        progress_bar = tqdm(unit="rows")
        remaining = int(n_rows) if n_rows is not None else None

        for batch in df_stream:
            out = pl.DataFrame(batch.to_pyarrow())

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

    return _source
