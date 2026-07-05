"""Regression test for issue #395.

An eager ``pb.overlap(DataFrame, DataFrame)`` followed by a lazy
``pb.overlap(LazyFrame, LazyFrame).collect()`` in the same process used to
segfault non-deterministically. Root cause: the eager path registered Arrow
batches whose buffers were still owned by Python (imported over the Arrow C
Data Interface from ``polars`` / ``pyarrow``). Those tables live in the shared,
long-lived session context, and the *next* range operation dropped them while
re-registering the ``s1``/``s2`` tables -- on a GIL-free worker thread. The
Arrow FFI release callback then ran ``_PyObject_Free`` without the GIL, which
corrupts memory. The fix drops those tables under the GIL before the
``py.detach`` block. See ``src/lib.rs::deregister_range_tables``.

Only the ``eager -> lazy`` direction is exercised: it is the dangerous one. A
lazy overlap registers Rust-owned Arrow streams (no Python FFI release
callbacks), so ``lazy -> eager`` is safe even on the buggy build; the crash
needs a *prior eager* table (Python-owned batches) to be dropped off-GIL by the
next op.

The crash is timing dependent, so we drive the eager->lazy pattern many times
in a *subprocess* and assert it exits cleanly. A regression re-introduces the
segfault, which shows up as a non-zero (139) exit code instead of taking down
the whole test session.
"""

import subprocess
import sys
import textwrap

# On the buggy build this pattern segfaults well within a few hundred
# iterations (observed 8/8 crashes at 400). Use a healthy margin.
_ITERATIONS = 400

_WORKER = textwrap.dedent(
    """
    import sys
    import numpy as np
    import polars as pl
    import polars_bio as pb

    pb.set_option("datafusion.bio.coordinate_system_check", "false")
    pb.set_option("datafusion.bio.coordinate_system_zero_based", True)

    def eager(contig):
        queries = pl.DataFrame({
            "chrom": [contig] * 3,
            "start": np.array([0, 100, 200], dtype=np.int64),
            "end":   np.array([50, 150, 250], dtype=np.int64),
        })
        table = pl.DataFrame({
            "chrom": [contig] * 3,
            "start": np.array([10, 110, 210], dtype=np.int64),
            "end":   np.array([20, 130, 230], dtype=np.int64),
            "sample_id": ["s1", "s2", "s3"],
            "value": np.array([1.0, 2.0, 3.0], dtype=np.float32),
        })
        return pb.overlap(
            queries, table,
            cols1=["chrom", "start", "end"], cols2=["chrom", "start", "end"],
            output_type="polars.DataFrame",
        ).height

    def lazy(contig):
        lf_table = pl.DataFrame({
            "index": np.arange(3, dtype=np.int32),
            "chrom": [contig] * 3,
            "start": np.array([14, 104, 204], dtype=np.int64),
            "end":   np.array([16, 106, 206], dtype=np.int64),
        }).lazy()
        lf_queries = pl.DataFrame({
            "chrom": [contig] * 3,
            "start": np.array([0, 100, 200], dtype=np.int64),
            "end":   np.array([50, 150, 250], dtype=np.int64),
        }).lazy().with_row_index("query")
        return pb.overlap(
            lf_queries, lf_table,
            cols1=["chrom", "start", "end"], cols2=["chrom", "start", "end"],
            projection_pushdown=True,
        ).collect().height

    contigs = ["chr1", "chr19", "chr20"]
    n = int(sys.argv[1])
    for i in range(n):
        c = contigs[i % len(contigs)]
        eager(c)   # eager DataFrame overlap registers Python-owned batches
        lazy(c)    # lazy overlap re-registers s1/s2 -> used to drop them off-GIL
    print("OK")
    """
)


def test_eager_then_lazy_overlap_does_not_segfault():
    result = subprocess.run(
        [sys.executable, "-c", _WORKER, str(_ITERATIONS)],
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, (
        f"eager->lazy overlap loop exited with {result.returncode} "
        f"(negative/139 => segfault regression of issue #395)\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "OK" in result.stdout
