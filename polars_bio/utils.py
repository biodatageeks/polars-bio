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
    df: Union[pl.DataFrame, pl.LazyFrame], projection_pushdown: bool = False
) -> pl.LazyFrame:
    df_lazy: DataFrame = df
    arrow_schema = df_lazy.schema()

    def _overlap_source(
        with_columns: Union[pl.Expr, None],
        predicate: Union[pl.Expr, None],
        n_rows: Union[int, None],
        _batch_size: Union[int, None],
    ) -> Iterator[pl.DataFrame]:
        if n_rows and n_rows < 8192:  # 8192 is the default batch size in datafusion
            df = df_lazy.limit(n_rows).execute_stream().next().to_pyarrow()
            df = pl.DataFrame(df).limit(n_rows)
            if predicate is not None:
                df = df.filter(predicate)
            if with_columns is not None:
                if projection_pushdown:
                    # Column projection will be handled by DataFusion when implemented
                    pass
                else:
                    df = df.select(with_columns)
            yield df
            return
        df_stream = df_lazy.execute_stream()
        progress_bar = tqdm(unit="rows")
        for r in df_stream:
            py_df = r.to_pyarrow()
            df = pl.DataFrame(py_df)
            if predicate is not None:
                df = df.filter(predicate)
            if with_columns is not None:
                if projection_pushdown:
                    # Column projection will be handled by DataFusion when implemented
                    pass
                else:
                    df = df.select(with_columns)
            progress_bar.update(len(df))
            yield df

    return register_io_source(_overlap_source, schema=arrow_schema)
