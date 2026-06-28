"""Parity tests: polars-bio BBI readers vs pyBigWig (the de-facto reference).

Skipped automatically where pyBigWig is not installed. Validates that the
polars-bio output matches libBigWig row-for-row on the committed fixtures, and
that predicate pushdown returns the same rows as a client-side filter.
"""

from pathlib import Path

import numpy as np
import polars as pl
import pytest

pyBigWig = pytest.importorskip("pyBigWig")

import polars_bio as pb  # noqa: E402

DATA = Path(__file__).parent / "data" / "io" / "bbi"
LARGE_BW = str(DATA / "large_signal.bw")
SIGNAL_BW = str(DATA / "signal.bw")
ANNOT_BB = str(DATA / "annotations.bb")


def _pybigwig_intervals_df(path: str) -> pl.DataFrame:
    bw = pyBigWig.open(path)
    rows = []
    for chrom in bw.chroms():
        for start, end, value in bw.intervals(chrom) or []:
            rows.append((chrom, start, end, np.float32(value)))
    bw.close()
    return (
        pl.DataFrame(rows, schema=["chrom", "start", "end", "value"], orient="row")
        .with_columns(
            pl.col("start").cast(pl.UInt32),
            pl.col("end").cast(pl.UInt32),
            pl.col("value").cast(pl.Float32),
        )
        .sort(["chrom", "start", "end"])
    )


@pytest.mark.parametrize("path", [LARGE_BW, SIGNAL_BW])
def test_bigwig_full_read_matches_pybigwig(path):
    ref = _pybigwig_intervals_df(path)
    got = (
        pb.scan_bigwig(path, use_zero_based=True)
        .collect()
        .sort(["chrom", "start", "end"])
    )
    assert got.height == ref.height
    assert got.equals(ref)


def test_bigbed_rest_matches_pybigwig():
    bb = pyBigWig.open(ANNOT_BB)
    rows = []
    for chrom, length in bb.chroms().items():
        for start, end, rest in bb.entries(chrom, 0, length) or []:
            rows.append((chrom, start, end, rest))
    bb.close()
    ref = (
        pl.DataFrame(rows, schema=["chrom", "start", "end", "rest"], orient="row")
        .with_columns(pl.col("start").cast(pl.UInt32), pl.col("end").cast(pl.UInt32))
        .sort(["chrom", "start", "end", "rest"])
    )
    got = (
        pb.scan_bigbed(ANNOT_BB, use_zero_based=True, schema="rest")
        .collect()
        .sort(["chrom", "start", "end", "rest"])
    )
    assert got.height == ref.height
    assert got.equals(ref)


def test_bigwig_pushdown_region_matches_pybigwig():
    # pyBigWig returns intervals overlapping the window; polars-bio filters on
    # `start` in [1000, 5000). With bin edges aligned to multiples of 5 the two
    # sets coincide, so the rows (and coordinates) must match exactly.
    bw = pyBigWig.open(LARGE_BW)
    overlap = bw.intervals("chr1", 1000, 5000) or []
    bw.close()
    ref = (
        pl.DataFrame(
            [(s, e, np.float32(v)) for (s, e, v) in overlap if 1000 <= s < 5000],
            schema=["start", "end", "value"],
            orient="row",
        )
        .with_columns(
            pl.col("start").cast(pl.UInt32),
            pl.col("end").cast(pl.UInt32),
            pl.col("value").cast(pl.Float32),
        )
        .sort("start")
    )
    got = (
        pb.scan_bigwig(LARGE_BW, predicate_pushdown=True, use_zero_based=True)
        .filter(
            (pl.col("chrom") == "chr1")
            & (pl.col("start") >= 1000)
            & (pl.col("start") < 5000)
        )
        .select(["start", "end", "value"])
        .sort("start")
        .collect()
    )
    assert got.height == ref.height
    assert got.equals(ref)
