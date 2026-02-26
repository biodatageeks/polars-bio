from typing import Iterator, List, Optional, Union

import polars as pl
import pyarrow as pa
from polars.io.plugins import register_io_source
from tqdm.auto import tqdm

from ._metadata import set_coordinate_system
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
        empty_table = pa.table(
            {field.name: pa.array([], type=field.type) for field in schema}
        )
        polars_schema = dict(pl.from_arrow(empty_table).schema)

        # 3. Define streaming callback (executed only on .collect())
        def _pileup_source(
            with_columns: Union[pl.Expr, None],
            predicate: Union[pl.Expr, None],
            n_rows: Union[int, None],
            _batch_size: Union[int, None],
        ) -> Iterator[pl.DataFrame]:
            from polars_bio.polars_bio import py_read_table

            from .context import ctx as _ctx

            query_df = py_read_table(_ctx, table_name)

            # Projection pushdown
            projection_applied = False
            if with_columns is not None:
                requested_cols = _extract_column_names_from_expr(with_columns)
                if requested_cols:
                    try:
                        select_exprs = [
                            query_df.parse_sql_expr(f'"{c}"') for c in requested_cols
                        ]
                        query_df = query_df.select(*select_exprs)
                        projection_applied = True
                    except Exception as e:
                        logger.debug("Projection pushdown failed: %s", e)

            # Predicate pushdown (reuses pattern from _overlap_source in io.py)
            predicate_pushed_down = False
            if predicate is not None:
                try:
                    from .predicate_translator import (
                        datafusion_expr_to_sql,
                        translate_predicate,
                    )

                    df_expr = translate_predicate(
                        predicate,
                        string_cols={"contig"},
                        uint32_cols={"pos", "pos_start", "pos_end", "coverage"},
                    )
                    sql_predicate = datafusion_expr_to_sql(df_expr)
                    native_expr = query_df.parse_sql_expr(sql_predicate)
                    query_df = query_df.filter(native_expr)
                    predicate_pushed_down = True
                except Exception as e:
                    logger.debug("Pileup predicate pushdown failed: %s", e)

            # Limit pushdown
            if n_rows and n_rows > 0:
                query_df = query_df.limit(int(n_rows))

            # Stream batches
            df_stream = query_df.execute_stream()
            progress_bar = tqdm(unit="rows")
            remaining = int(n_rows) if n_rows is not None else None

            for batch in df_stream:
                out = pl.DataFrame(batch.to_pyarrow())

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

            # Clean up registered table to free memory
            try:
                _ctx.deregister_table(table_name)
            except Exception:
                pass

        # 4. Create lazy frame
        lf = register_io_source(_pileup_source, schema=polars_schema)
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
