"""Streaming / out-of-core and pushdown integration tests for the BBI readers.

These run everywhere (no external reference library needed); they use a larger
committed fixture (``large_signal.bw``: 20,000 intervals on chr1 + 5,000 on
chr2) so limit/streaming behaviour is actually exercised across batch
boundaries (the BBI batch size is 8,192 rows).
"""

from pathlib import Path

import polars as pl

import polars_bio as pb

DATA = Path(__file__).parent / "data" / "io" / "bbi"
LARGE_BW = str(DATA / "large_signal.bw")

CHR1_N = 20_000
CHR2_N = 5_000
TOTAL = CHR1_N + CHR2_N


def test_scan_bigwig_returns_lazyframe():
    # Building the scan must not materialise data — it returns a LazyFrame.
    lf = pb.scan_bigwig(LARGE_BW, use_zero_based=True)
    assert isinstance(lf, pl.LazyFrame)


def test_bigwig_full_read_counts():
    df = pb.scan_bigwig(LARGE_BW, use_zero_based=True).collect()
    assert df.height == TOTAL
    assert df.filter(pl.col("chrom") == "chr1").height == CHR1_N
    assert df.filter(pl.col("chrom") == "chr2").height == CHR2_N


def test_bigwig_limit_pushdown():
    # `limit` is pushed to DataFusion; spans the 8,192-row batch boundary.
    for k in (1, 10, 8192, 8193, 25_000, 30_000):
        df = pb.scan_bigwig(LARGE_BW, use_zero_based=True).limit(k).collect()
        assert df.height == min(k, TOTAL), f"limit({k})"


def test_bigwig_streaming_matches_in_memory():
    lf = pb.scan_bigwig(LARGE_BW, use_zero_based=True)
    eager = lf.collect().sort(["chrom", "start"])
    streamed = lf.collect(engine="streaming").sort(["chrom", "start"])
    assert streamed.height == eager.height == TOTAL
    assert streamed.equals(eager)


def test_bigwig_streaming_aggregation_matches_eager():
    lf = pb.scan_bigwig(LARGE_BW, use_zero_based=True)
    agg = lf.group_by("chrom").agg(
        pl.len().alias("n"),
        pl.col("value").cast(pl.Float64).sum().alias("s"),
    )
    streamed = agg.collect(engine="streaming").sort("chrom")
    eager = agg.collect().sort("chrom")
    # Row counts are exact and must match between engines.
    assert dict(zip(streamed["chrom"], streamed["n"])) == {"chr1": CHR1_N, "chr2": CHR2_N}
    assert streamed["n"].to_list() == eager["n"].to_list()
    # Float sums agree within tolerance; the streaming and in-memory engines sum
    # in different orders, and floating-point addition is non-associative.
    for a, b in zip(streamed["s"].to_list(), eager["s"].to_list()):
        assert abs(a - b) < 1e-2


def test_bigwig_pushdown_region_equals_clientside_and_is_unclipped():
    # Region whose upper bound (2003) falls inside the [2000, 2005) bin, so the
    # straddling interval must come back with its true end (2005), not clipped.
    pred = (
        (pl.col("chrom") == "chr1")
        & (pl.col("start") >= 1000)
        & (pl.col("start") < 2003)
    )
    pushed = (
        pb.scan_bigwig(LARGE_BW, predicate_pushdown=True, use_zero_based=True)
        .filter(pred)
        .sort("start")
        .collect()
    )
    client = (
        pb.scan_bigwig(LARGE_BW, predicate_pushdown=False, use_zero_based=True)
        .filter(pred)
        .sort("start")
        .collect()
    )
    assert pushed.equals(client)
    # [2000, 2005): start=2000 < 2003 matches; end must be the unclipped 2005.
    assert pushed.filter(pl.col("start") == 2000)["end"].to_list() == [2005]
