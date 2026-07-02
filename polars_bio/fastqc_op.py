from typing import List, Optional

import polars as pl
import pyarrow as pa

from .context import ctx
from .logging import logger

ALL_MODULES = ["basic_stats", "per_base_quality", "per_seq_gc", "dup_levels"]


def _run_tidy(path: str, modules: Optional[List[str]]) -> pl.DataFrame:
    """Execute the single streaming fastqc pass and materialize the tidy result.

    The FASTQ is streamed out-of-core in Rust; only the tiny *aggregated* tidy
    output (a few hundred rows regardless of input size) is materialized here,
    so every per-module view is a cheap in-memory operation over one pass.
    """
    from polars_bio.polars_bio import (
        py_get_table_schema,
        py_read_table,
        py_register_fastqc_table,
    )

    table_name = py_register_fastqc_table(ctx, path, modules)
    try:
        query_df = py_read_table(ctx, table_name)
        frames = [
            pl.DataFrame(batch.to_pyarrow()) for batch in query_df.execute_stream()
        ]
        if frames:
            return pl.concat(frames) if len(frames) > 1 else frames[0]
        # No rows (e.g. empty FASTQ): return an empty frame with the right schema.
        schema = py_get_table_schema(ctx, table_name)
        empty = pa.table({f.name: pa.array([], type=f.type) for f in schema})
        return pl.from_arrow(empty)
    finally:
        try:
            ctx.deregister_table(table_name)
        except Exception:
            pass


class FastQCResult:
    """Result of a single streaming ``fastqc`` pass.

    Each per-module property is an Arrow-backed LazyFrame pivoted from the tidy
    stream. Accessing a module that was not computed raises ``KeyError``.
    """

    def __init__(self, tidy: pl.DataFrame, computed: List[str]):
        self._tidy_df = tidy
        self.computed = set(computed)

    @property
    def tidy(self) -> pl.LazyFrame:
        """The raw tidy result as a LazyFrame (single pass already computed)."""
        return self._tidy_df.lazy()

    def _require(self, module: str) -> None:
        if module not in self.computed:
            raise KeyError(
                f"module '{module}' was not computed "
                f"(requested: {sorted(self.computed)}); "
                f"call fastqc(..., modules=[..., '{module}'])"
            )

    def _module_rows(self, module: str) -> pl.LazyFrame:
        return self.tidy.filter(pl.col("module") == module)

    @property
    def basic_stats(self) -> pl.LazyFrame:
        self._require("basic_stats")
        return (
            self._module_rows("basic_stats")
            .filter(pl.col("metric") != "status")
            .select("metric", "value")
        )

    @property
    def per_base_quality(self) -> pl.LazyFrame:
        self._require("per_base_quality")
        return (
            self._module_rows("per_base_quality")
            .filter(pl.col("position").is_not_null())
            .collect()
            .pivot(values="value", index="position", on="metric")
            .lazy()
            .sort("position")
        )

    @property
    def per_seq_gc(self) -> pl.LazyFrame:
        self._require("per_seq_gc")
        return (
            self._module_rows("per_seq_gc")
            .filter(pl.col("metric") == "count")
            .select(pl.col("position").alias("gc_pct"), pl.col("value").alias("count"))
            .sort("gc_pct")
        )

    @property
    def dup_levels(self) -> pl.LazyFrame:
        self._require("dup_levels")
        return (
            self._module_rows("dup_levels")
            .filter(pl.col("metric") == "pct")
            .select(pl.col("label").alias("dup_level"), pl.col("value").alias("pct"))
        )

    def summary(self) -> pl.LazyFrame:
        return self.tidy.filter(pl.col("metric") == "status").select(
            pl.col("module"), pl.col("value_str").alias("status")
        )


class FastQCOperations:
    """FastQC quality-control operations on FASTQ files."""

    @staticmethod
    def fastqc(
        path: str,
        modules: Optional[List[str]] = None,
        group: bool = True,
    ) -> FastQCResult:
        """Compute FastQC modules over a FASTQ file in one streaming pass.

        Args:
            path: Path to a FASTQ file (plain, .gz, or .bgz).
            modules: Module names to compute; ``None`` computes all
                (``basic_stats``, ``per_base_quality``, ``per_seq_gc``,
                ``dup_levels``). Accessing a non-computed module on the result
                raises ``KeyError``.
            group: Reserved for FastQC-style position binning of long reads
                (``group=False`` == FastQC ``--nogroup``). No-op for Phase 1
                modules.

        Returns:
            FastQCResult with ``.tidy``, per-module LazyFrames, and
            ``.summary()``.

        Example:
            ```python
            import polars_bio as pb

            qc = pb.fastqc("reads_R1.fastq.gz")
            qc.per_base_quality.collect()
            qc.summary().collect()
            ```
        """
        if modules is not None:
            unknown = [m for m in modules if m not in ALL_MODULES]
            if unknown:
                raise ValueError(
                    f"unknown fastqc modules {unknown}; valid: {ALL_MODULES}"
                )
        computed = list(modules) if modules is not None else list(ALL_MODULES)
        tidy = _run_tidy(path, modules)
        if not group:
            logger.debug("group=False has no effect for Phase 1 modules")
        return FastQCResult(tidy, computed)
