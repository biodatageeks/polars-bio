from typing import Union

import polars as pl

import polars_bio as pb
from polars_bio.polars_bio import FilterOp


@pl.api.register_lazyframe_namespace("pb")
class PolarsRangesOperations:
    def __init__(self, ldf: pl.LazyFrame) -> None:
        self._ldf = ldf

    def overlap(
        self,
        other_df: pl.LazyFrame,
        suffixes: tuple[str, str] = ("_1", "_2"),
        how="inner",
        overlap_filter=FilterOp.Strict,
        cols1=["chrom", "start", "end"],
        cols2=["chrom", "start", "end"],
    ) -> pl.LazyFrame:
        """
        !!! note
            Alias for [overlap](api.md#polars_bio.overlap)
        """
        return pb.overlap(
            self._ldf,
            other_df,
            how=how,
            overlap_filter=overlap_filter,
            suffixes=suffixes,
            cols1=cols1,
            cols2=cols2,
        )

    def nearest(
        self,
        other_df: pl.LazyFrame,
        suffixes: tuple[str, str] = ("_1", "_2"),
        overlap_filter=FilterOp.Strict,
        cols1=["chrom", "start", "end"],
        cols2=["chrom", "start", "end"],
    ) -> pl.LazyFrame:
        """
        !!! note
            Alias for [nearest](api.md#polars_bio.nearest)
        """
        return pb.nearest(
            self._ldf,
            other_df,
            overlap_filter=overlap_filter,
            suffixes=suffixes,
            cols1=cols1,
            cols2=cols2,
        )

    def count_overlaps(
        self,
        other_df: pl.LazyFrame,
        overlap_filter=FilterOp.Strict,
        suffixes: tuple[str, str] = ("", "_"),
        cols1=["chrom", "start", "end"],
        cols2=["chrom", "start", "end"],
        on_cols: Union[list[str], None] = None,
        naive_query: bool = True,
    ) -> pl.LazyFrame:
        """
        !!! note
            Alias for [count_overlaps](api.md#polars_bio.count_overlaps)
        """
        return pb.count_overlaps(
            self._ldf,
            other_df,
            overlap_filter=overlap_filter,
            suffixes=suffixes,
            cols1=cols1,
            cols2=cols2,
            on_cols=on_cols,
            naive_query=naive_query,
        )

    def merge(
        self,
        overlap_filter: FilterOp = FilterOp.Strict,
        min_dist: float = 0,
        cols: Union[list[str], None] = None,
    ) -> pl.LazyFrame:
        """
        !!! note
            Alias for [merge](api.md#polars_bio.merge)
        """
        return pb.merge(
            self._ldf, overlap_filter=overlap_filter, min_dist=min_dist, cols=cols
        )

    def sort(
        self, cols: Union[tuple[str], None] = ["chrom", "start", "end"]
    ) -> pl.LazyFrame:
        """
        Sort a bedframe.
        !!! note
            Adapted to Polars API from [bioframe.sort_bedframe](https://github.com/open2c/bioframe/blob/2b685eebef393c2c9e6220dcf550b3630d87518e/bioframe/ops.py#L1698)

        Parameters:
            cols: The names of columns containing the chromosome, start and end of the genomic intervals.


        !!! Example
              ```python
              import polars_bio as pb
              df = pb.read_table("https://www.encodeproject.org/files/ENCFF001XKR/@@download/ENCFF001XKR.bed.gz",schema="bed9")
              df.pb.sort().limit(5).collect()
              ```
                ```plaintext
                <class 'builtins.PyExpr'>
                shape: (5, 9)
                ┌───────┬─────────┬─────────┬──────┬───┬────────┬────────────┬──────────┬──────────┐
                │ chrom ┆ start   ┆ end     ┆ name ┆ … ┆ strand ┆ thickStart ┆ thickEnd ┆ itemRgb  │
                │ ---   ┆ ---     ┆ ---     ┆ ---  ┆   ┆ ---    ┆ ---        ┆ ---      ┆ ---      │
                │ str   ┆ i64     ┆ i64     ┆ str  ┆   ┆ str    ┆ str        ┆ str      ┆ str      │
                ╞═══════╪═════════╪═════════╪══════╪═══╪════════╪════════════╪══════════╪══════════╡
                │ chr1  ┆ 193500  ┆ 194500  ┆ .    ┆ … ┆ +      ┆ .          ┆ .        ┆ 179,45,0 │
                │ chr1  ┆ 618500  ┆ 619500  ┆ .    ┆ … ┆ +      ┆ .          ┆ .        ┆ 179,45,0 │
                │ chr1  ┆ 974500  ┆ 975500  ┆ .    ┆ … ┆ +      ┆ .          ┆ .        ┆ 179,45,0 │
                │ chr1  ┆ 1301500 ┆ 1302500 ┆ .    ┆ … ┆ +      ┆ .          ┆ .        ┆ 179,45,0 │
                │ chr1  ┆ 1479500 ┆ 1480500 ┆ .    ┆ … ┆ +      ┆ .          ┆ .        ┆ 179,45,0 │
                └───────┴─────────┴─────────┴──────┴───┴────────┴────────────┴──────────┴──────────┘

                ```

        """
        return self._ldf.sort(by=cols)

    def expand(
        self,
        pad: Union[int, None] = None,
        scale: Union[float, None] = None,
        side: str = "both",
        cols: Union[list[str], None] = ["chrom", "start", "end"],
    ) -> pl.LazyFrame:
        """
        Expand each interval by an amount specified with `pad`.
        !!! Note
            Adapted to Polars API from [bioframe.expand](https://github.com/open2c/bioframe/blob/2b685eebef393c2c9e6220dcf550b3630d87518e/bioframe/ops.py#L150)

        Negative values for pad shrink the interval, up to the midpoint.
        Multiplicative rescaling of intervals enabled with scale. Only one of pad
        or scale can be provided.

        Parameters:
            pad :
                The amount by which the intervals are additively expanded *on each side*.
                Negative values for pad shrink intervals, but not beyond the interval
                midpoint. Either `pad` or `scale` must be supplied.

            scale :
                The factor by which to scale intervals multiplicatively on each side, e.g
                ``scale=2`` doubles each interval, ``scale=0`` returns midpoints, and
                ``scale=1`` returns original intervals. Default False.
                Either `pad` or `scale` must be supplied.

            side :
                Which side to expand, possible values are 'left', 'right' and 'both'.
                Default 'both'.

            cols :
                The names of columns containing the chromosome, start and end of the
                genomic intervals. Default values are 'chrom', 'start', 'end'.


        """
        df = self._ldf
        ck, sk, ek = ["chrom", "start", "end"] if cols is None else cols
        padsk = "pads"
        midsk = "mids"

        if scale is not None and pad is not None:
            raise ValueError("only one of pad or scale can be supplied")
        elif scale is not None:
            if scale < 0:
                raise ValueError("multiplicative scale must be >=0")
            df = df.with_columns(
                [(0.5 * (scale - 1) * (pl.col(ek) - pl.col(sk))).alias(padsk)]
            )
        elif pad is not None:
            if not isinstance(pad, int):
                raise ValueError("additive pad must be integer")
            df = df.with_columns([pl.lit(pad).alias(padsk)])
        else:
            raise ValueError("either pad or scale must be supplied")
        if side == "both" or side == "left":
            df = df.with_columns([(pl.col(sk) - pl.col(padsk)).alias(sk)])
        if side == "both" or side == "right":
            df = df.with_columns([(pl.col(ek) + pl.col(padsk)).alias(ek)])

        if pad is not None:
            if pad < 0:
                df = df.with_columns(
                    [(pl.col(sk) + 0.5 * (pl.col(ek) - pl.col(sk))).alias(midsk)]
                )
                df = df.with_columns(
                    [
                        pl.min_horizontal(pl.col(sk), pl.col(midsk))
                        .cast(pl.Int64)
                        .alias(sk),
                        pl.max_horizontal(pl.col(ek), pl.col(midsk))
                        .cast(pl.Int64)
                        .alias(ek),
                    ]
                )
        if scale is not None:
            df = df.with_columns(
                [
                    pl.col(sk).round(0).cast(pl.Int64).alias(sk),
                    pl.col(ek).round(0).cast(pl.Int64).alias(ek),
                ]
            )
        schema = df.collect_schema().names()
        if padsk in schema:
            df = df.drop(padsk)
        if midsk in schema:
            df = df.drop(midsk)
        return df

    def coverage(
        self,
        other_df: pl.LazyFrame,
        cols1=["chrom", "start", "end"],
        cols2=["chrom", "start", "end"],
        suffixes: tuple[str, str] = ("_1", "_2"),
    ) -> pl.LazyFrame:
        """
        !!! note
            Alias for [coverage](api.md#polars_bio.coverage)
        """
        return pb.coverage(
            self._ldf, other_df, cols1=cols1, cols2=cols2, suffixes=suffixes
        )
