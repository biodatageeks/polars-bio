from typing import Optional, Union

import polars as pl
from polars.io.plugins import register_io_source

from ._metadata import set_coordinate_system
from .context import _resolve_zero_based, ctx
from polars_bio._streaming import (
    PredicatePushdownConfig,
    StreamingConfig,
    make_streaming_source,
    pyarrow_schema_to_polars_dict,
)

try:
    import pandas as pd
except ImportError:
    pd = None


class PileupOperations:
    """Per-base read depth (pileup) operations on alignment files.

    Computes per-position depth from BAM/SAM/CRAM files by walking CIGAR
    operations, producing mosdepth-compatible coverage blocks.
    """

    @staticmethod
    def depth(
        path: str,
        filter_flag: int = 1796,
        min_mapping_quality: int = 0,
        binary_cigar: bool = True,
        dense_mode: str = "auto",
        use_zero_based: Optional[bool] = None,
        per_base: bool = False,
        output_type: str = "polars.LazyFrame",
    ) -> Union[pl.LazyFrame, pl.DataFrame, "pd.DataFrame"]:
        """Compute per-base read depth (pileup) from a BAM/SAM/CRAM file.

        Walks CIGAR operations to produce coverage blocks -- similar to
        mosdepth / samtools depth.

        Args:
            path: Path to alignment file (.bam, .sam, or .cram).
                Index files (BAI/CSI/CRAI) are auto-discovered.
            filter_flag: SAM flag mask -- reads with any of these flags are
                excluded. Default 1796 (unmapped, secondary, failed QC,
                duplicate).
            min_mapping_quality: Minimum MAPQ threshold. Default 0
                (no filter).
            binary_cigar: Use binary CIGAR parsing (faster). Default True.
            dense_mode: Accumulation strategy:

                - ``"auto"`` -- use dense accumulation when contig lengths are
                  available in schema metadata (default).
                - ``"force"`` -- always use dense accumulation.
                - ``"disable"`` -- always use sparse (event-list) accumulation.
            use_zero_based: Coordinate system for output positions.

                - ``None`` (default) -- use global config (``pb.options``),
                  which defaults to 1-based.
                - ``True`` -- 0-based half-open coordinates.
                - ``False`` -- 1-based closed coordinates.
            per_base: If True, emit one row per genomic position (like
                ``samtools depth -a``) instead of RLE coverage blocks.
                Requires dense mode (BAM header with contig lengths).
                Default False.
            output_type: One of ``"polars.LazyFrame"``,
                ``"polars.DataFrame"``, or ``"pandas.DataFrame"``.

        Returns:
            DataFrame with columns depending on ``per_base``:

            - Block mode (default): ``contig`` (Utf8), ``pos_start`` (Int32),
              ``pos_end`` (Int32), ``coverage`` (Int16).
            - Per-base mode: ``contig`` (Utf8), ``pos`` (Int32),
              ``coverage`` (Int16).

        Example:
            ```python
            import polars_bio as pb

            # Basic depth computation (RLE blocks)
            df = pb.depth("alignments.bam").collect()

            # Per-base output (one row per position)
            df = pb.depth("alignments.bam", per_base=True).collect()

            # With MAPQ filter
            df = pb.depth("alignments.bam", min_mapping_quality=20).collect()

            # As pandas DataFrame
            pdf = pb.depth("alignments.bam", output_type="pandas.DataFrame")
            ```
        """
        from polars_bio.polars_bio import (
            PileupOptions,
            py_get_table_schema,
            py_register_pileup_table,
        )

        zero_based = _resolve_zero_based(use_zero_based)

        opts = PileupOptions(
            filter_flag=filter_flag,
            min_mapping_quality=min_mapping_quality,
            binary_cigar=binary_cigar,
            dense_mode=dense_mode,
            zero_based=zero_based,
            per_base=per_base,
        )

        # 1. Register table (no execution)
        table_name = py_register_pileup_table(ctx, path, opts)

        # 2. Get schema without materializing data
        schema = py_get_table_schema(ctx, table_name)

        polars_schema = pyarrow_schema_to_polars_dict(schema)

        # 3. Define streaming callback (executed only on .collect())
        def _df_factory(tn, *_):
            from polars_bio.polars_bio import py_read_table
            from .context import ctx as _ctx
            return py_read_table(_ctx, tn)

        _config = StreamingConfig(
            predicate_config=PredicatePushdownConfig(
                string_cols={"contig"},
                uint32_cols={"pos", "pos_start", "pos_end", "coverage"},
                float32_cols=None,
            ),
        )

        # 4. Create lazy frame
        lf = register_io_source(
            make_streaming_source(_df_factory, table_name, _config),
            schema=polars_schema,
        )
        set_coordinate_system(lf, zero_based)

        # 5. Handle output_type
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
