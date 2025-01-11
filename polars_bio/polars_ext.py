from typing import Union

import polars as pl


@pl.api.register_lazyframe_namespace("pb")
class PolarsRangesOperations:
    def __init__(self, ldf: pl.LazyFrame) -> None:
        self._ldf = ldf

    def sort_bedframe(
        self, cols: Union[tuple[str], None] = ["chrom", "start", "end"]
    ) -> pl.LazyFrame:
        """
        Sort a bedframe

        Parameters:
            cols: The names of columns containing the chromosome, start and end of the genomic intervals.


        !!! Example
              ```python
              import polars_bio as pb
              df = pb.read_table("https://www.encodeproject.org/files/ENCFF001XKR/@@download/ENCFF001XKR.bed.gz",schema="bed9")
              df.pb.sort_bedframe().limit(5).collect()
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
