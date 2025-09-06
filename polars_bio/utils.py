from typing import Iterator, Union

import polars as pl
from datafusion import DataFrame
from polars.io.plugins import register_io_source
from tqdm.auto import tqdm


def _cleanse_fields(t: Union[list[str], None]) -> Union[list[str], None]:
    if t is None:
        return None
    return [x.strip() for x in t]


def _lazy_scan(
    df: Union[pl.DataFrame, pl.LazyFrame],
    projection_pushdown: bool = False,
    predicate_pushdown: bool = False,
    table_name: str = None,
    input_format=None,
    file_path: str = None,
) -> pl.LazyFrame:
    df_lazy: DataFrame = df
    original_schema = df_lazy.schema()

    def _overlap_source(
        with_columns: Union[pl.Expr, None],
        predicate: Union[pl.Expr, None],
        n_rows: Union[int, None],
        _batch_size: Union[int, None],
    ) -> Iterator[pl.DataFrame]:
        # Extract column names from with_columns if projection pushdown is enabled
        projected_columns = None
        if projection_pushdown and with_columns is not None:
            projected_columns = _extract_column_names_from_expr(with_columns)

        # Apply column projection and predicate pushdown to DataFusion query if enabled
        query_df = df_lazy
        datafusion_projection_applied = False
        datafusion_predicate_applied = False

        # Handle predicate pushdown first
        if predicate_pushdown and predicate is not None:
            try:
                from .predicate_translator import (
                    translate_polars_predicate_to_datafusion,
                )

                datafusion_predicate = translate_polars_predicate_to_datafusion(
                    predicate
                )
                query_df = query_df.filter(datafusion_predicate)
                datafusion_predicate_applied = True
            except Exception as e:
                # Fallback to Python-level filtering if predicate pushdown fails
                datafusion_predicate_applied = False
                # Note: error handling for debugging could be added here if needed
        if projection_pushdown and projected_columns:
            try:
                query_df = df_lazy.select(projected_columns)
                datafusion_projection_applied = True

                # For testing: allow inspection of the execution plan
                if hasattr(df_lazy, "_test_projection_capture"):
                    df_lazy._test_projection_capture = {
                        "original_plan": str(df_lazy.optimized_logical_plan()),
                        "projected_plan": str(query_df.optimized_logical_plan()),
                        "projected_columns": projected_columns,
                        "datafusion_projection_applied": True,
                    }

            except Exception as e:
                # Fallback to original behavior if projection fails
                query_df = df_lazy
                projected_columns = None
                datafusion_projection_applied = False

                # For testing: capture the failure
                if hasattr(df_lazy, "_test_projection_capture"):
                    df_lazy._test_projection_capture = {
                        "original_plan": str(df_lazy.optimized_logical_plan()),
                        "projected_plan": None,
                        "projected_columns": projected_columns,
                        "datafusion_projection_applied": False,
                        "error": str(e),
                    }

        if n_rows and n_rows < 8192:  # 8192 is the default batch size in datafusion
            df = query_df.limit(n_rows).execute_stream().next().to_pyarrow()
            df = pl.DataFrame(df).limit(n_rows)
            # Apply Python-level predicate only if DataFusion predicate pushdown failed
            if predicate is not None and not datafusion_predicate_applied:
                df = df.filter(predicate)
            # Apply Python-level projection if DataFusion projection failed or projection pushdown is disabled
            if with_columns is not None and (
                not projection_pushdown or not datafusion_projection_applied
            ):
                df = df.select(with_columns)
            yield df
            return

        df_stream = query_df.execute_stream()
        progress_bar = tqdm(unit="rows")
        for r in df_stream:
            py_df = r.to_pyarrow()
            df = pl.DataFrame(py_df)
            # Apply Python-level predicate only if DataFusion predicate pushdown failed
            if predicate is not None and not datafusion_predicate_applied:
                df = df.filter(predicate)
            # Apply Python-level projection if DataFusion projection failed or projection pushdown is disabled
            if with_columns is not None and (
                not projection_pushdown or not datafusion_projection_applied
            ):
                df = df.select(with_columns)
            progress_bar.update(len(df))
            yield df

    return register_io_source(_overlap_source, schema=original_schema)


def _extract_column_names_from_expr(with_columns: Union[pl.Expr, list]) -> list[str]:
    """Extract column names from Polars expressions."""
    if with_columns is None:
        return []

    # Handle different types of with_columns input
    if hasattr(with_columns, "__iter__") and not isinstance(with_columns, str):
        # It's a list of expressions or strings
        column_names = []
        for item in with_columns:
            if isinstance(item, str):
                column_names.append(item)
            elif hasattr(item, "meta") and hasattr(item.meta, "output_name"):
                # Polars expression with output name
                try:
                    column_names.append(item.meta.output_name())
                except Exception:
                    pass
        return column_names
    elif isinstance(with_columns, str):
        return [with_columns]
    elif hasattr(with_columns, "meta") and hasattr(with_columns.meta, "output_name"):
        # Single Polars expression
        try:
            return [with_columns.meta.output_name()]
        except Exception:
            pass

    return []
