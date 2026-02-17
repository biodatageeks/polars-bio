from typing import Optional, Union

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
        cols1=["chrom", "start", "end"],
        cols2=["chrom", "start", "end"],
        algorithm: str = "Coitrees",
    ) -> pl.LazyFrame:
        """
        !!! note
            Alias for [overlap](api.md#polars_bio.overlap)

        Note:
            Coordinate system is determined from DataFrame metadata.
            Set metadata using: `df.config_meta.set(coordinate_system_zero_based=True)`
        """
        return pb.overlap(
            self._ldf,
            other_df,
            suffixes=suffixes,
            cols1=cols1,
            cols2=cols2,
            algorithm=algorithm,
        )

    def nearest(
        self,
        other_df: pl.LazyFrame,
        suffixes: tuple[str, str] = ("_1", "_2"),
        cols1=["chrom", "start", "end"],
        cols2=["chrom", "start", "end"],
        k: int = 1,
        overlap: bool = True,
        distance: bool = True,
    ) -> pl.LazyFrame:
        """
        !!! note
            Alias for [nearest](api.md#polars_bio.nearest)

        Note:
            Coordinate system is determined from DataFrame metadata.
            Set metadata using: `df.config_meta.set(coordinate_system_zero_based=True)`
        """
        return pb.nearest(
            self._ldf,
            other_df,
            suffixes=suffixes,
            cols1=cols1,
            cols2=cols2,
            k=k,
            overlap=overlap,
            distance=distance,
        )

    def count_overlaps(
        self,
        other_df: pl.LazyFrame,
        suffixes: tuple[str, str] = ("", "_"),
        cols1=["chrom", "start", "end"],
        cols2=["chrom", "start", "end"],
        on_cols: Union[list[str], None] = None,
        naive_query: bool = True,
    ) -> pl.LazyFrame:
        """
        !!! note
            Alias for [count_overlaps](api.md#polars_bio.count_overlaps)

        Note:
            Coordinate system is determined from DataFrame metadata.
            Set metadata using: `df.config_meta.set(coordinate_system_zero_based=True)`
        """
        return pb.count_overlaps(
            self._ldf,
            other_df,
            suffixes=suffixes,
            cols1=cols1,
            cols2=cols2,
            on_cols=on_cols,
            naive_query=naive_query,
        )

    def merge(
        self,
        min_dist: float = 0,
        cols: Union[list[str], None] = None,
    ) -> pl.LazyFrame:
        """
        !!! note
            Alias for [merge](api.md#polars_bio.merge)

        Note:
            Coordinate system is determined from DataFrame metadata.
            Set metadata using: `df.config_meta.set(coordinate_system_zero_based=True)`
        """
        return pb.merge(
            self._ldf,
            min_dist=min_dist,
            cols=cols,
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

        Note:
            Coordinate system is determined from DataFrame metadata.
            Set metadata using: `df.config_meta.set(coordinate_system_zero_based=True)`
        """
        return pb.coverage(
            self._ldf, other_df, cols1=cols1, cols2=cols2, suffixes=suffixes
        )

    def sink_vcf(self, path: str) -> None:
        """
        Streaming write LazyFrame to VCF format.

        Coordinate system is automatically read from LazyFrame metadata.
        Compression is auto-detected from file extension.

        Parameters:
            path: Output file path. Compression detected from extension
                  (.vcf.bgz for BGZF, .vcf.gz for GZIP, .vcf for uncompressed).

        !!! Example
            ```python
            import polars_bio as pb

            lf = pb.scan_vcf("input.vcf").filter(pl.col("qual") > 30)
            lf.pb.sink_vcf("filtered.vcf.bgz")
            ```
        """
        pb.sink_vcf(self._ldf, path)

    def sink_fastq(self, path: str) -> None:
        """
        Streaming write LazyFrame to FASTQ format.

        Compression is auto-detected from file extension.

        Parameters:
            path: Output file path. Compression detected from extension
                  (.fastq.bgz for BGZF, .fastq.gz for GZIP, .fastq for uncompressed).

        !!! Example
            ```python
            import polars_bio as pb

            lf = pb.scan_fastq("input.fastq.gz").limit(1000)
            lf.pb.sink_fastq("sample.fastq")
            ```
        """
        pb.sink_fastq(self._ldf, path)

    def sink_bam(self, path: str, sort_on_write: bool = False) -> None:
        """
        Streaming write LazyFrame to BAM/SAM format.

        For CRAM format, use `sink_cram()` instead.

        Parameters:
            path: Output file path (.bam or .sam)
            sort_on_write: If True, sort records by (chrom, start) and set header SO:coordinate.
                If False (default), set header SO:unsorted.

        !!! Example
            ```python
            import polars_bio as pb

            lf = pb.scan_bam("input.bam").filter(pl.col("mapping_quality") > 20)
            lf.pb.sink_bam("filtered.bam")
            ```
        """
        pb.sink_bam(self._ldf, path, sort_on_write=sort_on_write)

    def sink_sam(self, path: str, sort_on_write: bool = False) -> None:
        """
        Streaming write LazyFrame to SAM format (plain text).

        Parameters:
            path: Output file path (.sam)
            sort_on_write: If True, sort records by (chrom, start) and set header SO:coordinate.
                If False (default), set header SO:unsorted.

        !!! Example
            ```python
            import polars_bio as pb

            lf = pb.scan_bam("input.bam").filter(pl.col("mapping_quality") > 20)
            lf.pb.sink_sam("filtered.sam")
            ```
        """
        pb.sink_sam(self._ldf, path, sort_on_write=sort_on_write)

    def sink_cram(
        self,
        path: str,
        reference_path: str,
        sort_on_write: bool = False,
    ) -> None:
        """
        Streaming write LazyFrame to CRAM format.

        CRAM uses reference-based compression for optimal file sizes.

        Parameters:
            path: Output CRAM file path
            reference_path: Path to reference FASTA file (required). The reference must
                contain all sequences referenced by the alignment data.
            sort_on_write: If True, sort records by (chrom, start) and set header SO:coordinate.
                If False (default), set header SO:unsorted.

        !!! Example
            ```python
            import polars_bio as pb
            import polars as pl

            lf = pb.scan_bam("input.bam").filter(pl.col("mapping_quality") > 20)

            # Write CRAM with reference (required)
            lf.pb.sink_cram("filtered.cram", reference_path="reference.fasta")

            # For sorted output
            lf.pb.sink_cram("filtered.cram", reference_path="reference.fasta", sort_on_write=True)
            ```
        """
        pb.sink_cram(self._ldf, path, reference_path, sort_on_write=sort_on_write)


@pl.api.register_dataframe_namespace("pb")
class PolarsDataFrameOperations:
    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def write_vcf(self, path: str) -> int:
        """
        Write DataFrame to VCF format.

        Coordinate system is automatically read from DataFrame metadata.
        Compression is auto-detected from file extension.

        Parameters:
            path: Output file path. Compression detected from extension
                  (.vcf.bgz for BGZF, .vcf.gz for GZIP, .vcf for uncompressed).

        Returns:
            The number of rows written.

        !!! Example
            ```python
            import polars_bio as pb

            df = pb.read_vcf("input.vcf")
            df.pb.write_vcf("output.vcf.gz")
            ```
        """
        return pb.write_vcf(self._df, path)

    def write_fastq(self, path: str) -> int:
        """
        Write DataFrame to FASTQ format.

        Compression is auto-detected from file extension.

        Parameters:
            path: Output file path. Compression detected from extension
                  (.fastq.bgz for BGZF, .fastq.gz for GZIP, .fastq for uncompressed).

        Returns:
            The number of rows written.

        !!! Example
            ```python
            import polars_bio as pb

            df = pb.read_fastq("input.fastq")
            df.pb.write_fastq("output.fastq.gz")
            ```
        """
        return pb.write_fastq(self._df, path)

    def write_bam(self, path: str, sort_on_write: bool = False) -> int:
        """
        Write DataFrame to BAM/SAM format.

        Compression is auto-detected from file extension.
        For CRAM format, use `write_cram()` instead.

        Parameters:
            path: Output file path (.bam or .sam)
            sort_on_write: If True, sort records by (chrom, start) and set header SO:coordinate.
                If False (default), set header SO:unsorted.

        Returns:
            The number of rows written.

        !!! Example
            ```python
            import polars_bio as pb

            df = pb.read_bam("input.bam", tag_fields=["NM", "AS"])
            df.pb.write_bam("output.bam")
            ```
        """
        return pb.write_bam(self._df, path, sort_on_write=sort_on_write)

    def write_sam(self, path: str, sort_on_write: bool = False) -> int:
        """
        Write DataFrame to SAM format (plain text).

        Parameters:
            path: Output file path (.sam)
            sort_on_write: If True, sort records by (chrom, start) and set header SO:coordinate.
                If False (default), set header SO:unsorted.

        Returns:
            The number of rows written.

        !!! Example
            ```python
            import polars_bio as pb

            df = pb.read_bam("input.bam", tag_fields=["NM", "AS"])
            df.pb.write_sam("output.sam")
            ```
        """
        return pb.write_sam(self._df, path, sort_on_write=sort_on_write)

    def write_cram(
        self,
        path: str,
        reference_path: str,
        sort_on_write: bool = False,
    ) -> int:
        """
        Write DataFrame to CRAM format.

        CRAM uses reference-based compression for optimal file sizes.

        Parameters:
            path: Output CRAM file path
            reference_path: Path to reference FASTA file (required). The reference must
                contain all sequences referenced by the alignment data.
            sort_on_write: If True, sort records by (chrom, start) and set header SO:coordinate.
                If False (default), set header SO:unsorted.

        Returns:
            The number of rows written.

        !!! Example
            ```python
            import polars_bio as pb

            df = pb.read_bam("input.bam", tag_fields=["NM", "AS"])

            # Write CRAM with reference (required)
            df.pb.write_cram("output.cram", reference_path="reference.fasta")

            # For sorted output
            df.pb.write_cram("output.cram", reference_path="reference.fasta", sort_on_write=True)
            ```
        """
        return pb.write_cram(
            self._df, path, reference_path, sort_on_write=sort_on_write
        )
