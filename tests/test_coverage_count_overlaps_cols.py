import bioframe as bf
import pandas as pd
import polars as pl
import pytest

import polars_bio as pb
from polars_bio._metadata import get_coordinate_system


def _oracle_count_overlaps(
    rows1: list[tuple[str, int, int]],
    rows2: list[tuple[str, int, int]],
    zero_based: bool,
) -> list[int]:
    counts: list[int] = []
    for c1, s1, e1 in rows1:
        cnt = 0
        for c2, s2, e2 in rows2:
            if c1 != c2:
                continue
            overlaps = (s1 < e2 and s2 < e1) if zero_based else (s1 <= e2 and s2 <= e1)
            if overlaps:
                cnt += 1
        counts.append(cnt)
    return counts


def _oracle_coverage(
    rows1: list[tuple[str, int, int]],
    rows2: list[tuple[str, int, int]],
    zero_based: bool,
) -> list[int]:
    coverages: list[int] = []
    for c1, s1, e1 in rows1:
        segs: list[tuple[int, int]] = []
        for c2, s2, e2 in rows2:
            if c1 != c2:
                continue
            lo = max(s1, s2)
            hi = min(e1, e2)
            if (zero_based and lo < hi) or (not zero_based and lo <= hi):
                segs.append((lo, hi))
        if not segs:
            coverages.append(0)
            continue
        segs.sort()
        merged = [segs[0]]
        for lo, hi in segs[1:]:
            prev_lo, prev_hi = merged[-1]
            if (zero_based and lo <= prev_hi) or (not zero_based and lo <= prev_hi + 1):
                merged[-1] = (prev_lo, max(prev_hi, hi))
            else:
                merged.append((lo, hi))
        coverages.append(
            sum((hi - lo) if zero_based else (hi - lo + 1) for lo, hi in merged)
        )
    return coverages


def _bed12_frames(zero_based: bool) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Two BED12-like tables containing both (start, end) and (thickStart, thickEnd)."""
    genes = pl.DataFrame(
        {
            "chrom": ["chr1", "chr1", "chr1", "chr2"],
            "start": [1000, 5000, 9000, 100],
            "end": [2000, 6000, 9500, 800],
            "thickStart": [1200, 5100, 9100, 300],
            "thickEnd": [1800, 5400, 9200, 600],
            "gene": ["G1", "G2", "G3", "G4"],
        }
    )
    peaks = pl.DataFrame(
        {
            "chrom": ["chr1", "chr1", "chr1", "chr1", "chr2"],
            "start": [1100, 1500, 5200, 9300, 250],
            "end": [1300, 1900, 5300, 9450, 450],
            "thickStart": [1010, 1950, 5800, 9050, 120],
            "thickEnd": [1050, 1990, 5900, 9080, 180],
            "peak": ["P1", "P2", "P3", "P4", "P5"],
        }
    )
    genes.config_meta.set(coordinate_system_zero_based=zero_based)
    peaks.config_meta.set(coordinate_system_zero_based=zero_based)
    return genes, peaks


def _disjoint_frames(zero_based: bool) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Two tables whose interval column names do not overlap at all."""
    df1 = pl.DataFrame(
        {
            "chrom_a": ["chr1", "chr1", "chr2"],
            "cds_start": [1200, 9100, 300],
            "cds_end": [1800, 9200, 600],
            "gene": ["G1", "G3", "G4"],
        }
    )
    df2 = pl.DataFrame(
        {
            "chrom_b": ["chr1", "chr1", "chr1", "chr2"],
            "peak_start": [1100, 1500, 9300, 250],
            "peak_end": [1300, 1900, 9450, 450],
            "peak": ["P1", "P2", "P4", "P5"],
        }
    )
    df1.config_meta.set(coordinate_system_zero_based=zero_based)
    df2.config_meta.set(coordinate_system_zero_based=zero_based)
    return df1, df2


@pytest.mark.parametrize("zero_based", [True, False])
@pytest.mark.parametrize(
    "output_type", ["polars.DataFrame", "polars.LazyFrame", "pandas.DataFrame"]
)
def test_coverage_asymmetric_cols_shared_names(zero_based: bool, output_type: str):
    """coverage() must apply cols1 to df1 and cols2 to df2 even when both frames have both column sets."""
    genes, peaks = _bed12_frames(zero_based)
    cols1 = ["chrom", "thickStart", "thickEnd"]
    cols2 = ["chrom", "start", "end"]

    expected = _oracle_coverage(
        list(zip(genes["chrom"], genes["thickStart"], genes["thickEnd"])),
        list(zip(peaks["chrom"], peaks["start"], peaks["end"])),
        zero_based,
    )
    if zero_based:
        bf_cov = bf.coverage(
            genes.to_pandas(), peaks.to_pandas(), cols1=cols1, cols2=cols2
        )
        assert bf_cov["coverage"].tolist() == expected

    res = pb.coverage(genes, peaks, cols1=cols1, cols2=cols2, output_type=output_type)
    assert get_coordinate_system(res) is zero_based
    if isinstance(res, pl.LazyFrame):
        res = res.collect()
    elif isinstance(res, pd.DataFrame):
        res = pl.from_pandas(res)

    res = res.sort("gene")
    assert res["gene"].to_list() == ["G1", "G2", "G3", "G4"]
    assert res["coverage"].to_list() == expected


@pytest.mark.parametrize("zero_based", [True, False])
@pytest.mark.parametrize("naive_query", [True, False])
@pytest.mark.parametrize(
    "output_type", ["polars.DataFrame", "polars.LazyFrame", "pandas.DataFrame"]
)
def test_count_overlaps_asymmetric_cols_shared_names(
    zero_based: bool, naive_query: bool, output_type: str
):
    """count_overlaps() must use cols1 for df1 and cols2 for df2 in both naive and sweep-line modes."""
    genes, peaks = _bed12_frames(zero_based)
    cols1 = ["chrom", "thickStart", "thickEnd"]
    cols2 = ["chrom", "start", "end"]

    expected_counts = _oracle_count_overlaps(
        list(zip(genes["chrom"], genes["thickStart"], genes["thickEnd"])),
        list(zip(peaks["chrom"], peaks["start"], peaks["end"])),
        zero_based,
    )
    expected_rows = sorted(
        zip(
            genes["chrom"].to_list(),
            genes["thickStart"].to_list(),
            genes["thickEnd"].to_list(),
            expected_counts,
        )
    )
    if zero_based:
        bf_cnt = bf.count_overlaps(
            genes.to_pandas(), peaks.to_pandas(), cols1=cols1, cols2=cols2
        )
        assert bf_cnt["count"].tolist() == expected_counts

    res = pb.count_overlaps(
        genes,
        peaks,
        cols1=cols1,
        cols2=cols2,
        naive_query=naive_query,
        output_type=output_type,
    )
    assert get_coordinate_system(res) is zero_based
    if isinstance(res, pl.LazyFrame):
        res = res.collect()
    elif isinstance(res, pd.DataFrame):
        res = pl.from_pandas(res)

    got_rows = sorted(
        zip(
            res["chrom"].to_list(),
            res["thickStart"].to_list(),
            res["thickEnd"].to_list(),
            res["count"].to_list(),
        )
    )
    assert got_rows == expected_rows


@pytest.mark.parametrize("input_kind", ["polars_df", "polars_lf", "pandas_df"])
@pytest.mark.parametrize("output_type", ["polars.DataFrame", "polars.LazyFrame"])
def test_disjoint_column_names_across_frame_types(input_kind: str, output_type: str):
    """coverage() and count_overlaps() must work when df1 and df2 have completely disjoint interval column names."""
    df1, df2 = _disjoint_frames(zero_based=True)
    cols1 = ["chrom_a", "cds_start", "cds_end"]
    cols2 = ["chrom_b", "peak_start", "peak_end"]

    if input_kind == "polars_lf":
        in1, in2 = df1.lazy(), df2.lazy()
    elif input_kind == "pandas_df":
        in1, in2 = df1.to_pandas(), df2.to_pandas()
        in1.attrs["coordinate_system_zero_based"] = True
        in2.attrs["coordinate_system_zero_based"] = True
    else:
        in1, in2 = df1, df2

    cov = pb.coverage(in1, in2, cols1=cols1, cols2=cols2, output_type=output_type)
    if isinstance(cov, pl.LazyFrame):
        cov = cov.collect()
    assert cov.sort("gene")["coverage"].to_list() == [400, 0, 150]

    for naive_query in (True, False):
        cnt = pb.count_overlaps(
            in1,
            in2,
            cols1=cols1,
            cols2=cols2,
            naive_query=naive_query,
            output_type=output_type,
        )
        if isinstance(cnt, pl.LazyFrame):
            cnt = cnt.collect()
        got = sorted(
            zip(
                cnt["chrom_a"].to_list(),
                cnt["cds_start"].to_list(),
                cnt["cds_end"].to_list(),
                cnt["count"].to_list(),
            )
        )
        assert got == [
            ("chr1", 1200, 1800, 2),
            ("chr1", 9100, 9200, 0),
            ("chr2", 300, 600, 1),
        ]


def test_disjoint_column_names_parquet_paths_and_polars_ext(tmp_path):
    """File-path inputs and LazyFrame .pb namespace methods respect asymmetric cols1/cols2."""
    df1, df2 = _disjoint_frames(zero_based=True)
    cols1 = ["chrom_a", "cds_start", "cds_end"]
    cols2 = ["chrom_b", "peak_start", "peak_end"]

    p1 = str(tmp_path / "genes.parquet")
    p2 = str(tmp_path / "peaks.parquet")
    df1.write_parquet(p1)
    df2.write_parquet(p2)

    orig_zb = pb.get_option("datafusion.bio.coordinate_system_zero_based")
    pb.set_option("datafusion.bio.coordinate_system_zero_based", "true")
    try:
        cov_path = pb.coverage(
            p1, p2, cols1=cols1, cols2=cols2, output_type="polars.DataFrame"
        ).sort("gene")
        assert cov_path["coverage"].to_list() == [400, 0, 150]

        cnt_naive = pb.count_overlaps(
            p1,
            p2,
            cols1=cols1,
            cols2=cols2,
            naive_query=True,
            output_type="polars.DataFrame",
            projection_pushdown=False,
        ).sort("gene")
        assert cnt_naive["count"].to_list() == [2, 0, 1]

        cnt_opt = pb.count_overlaps(
            p1,
            p2,
            cols1=cols1,
            cols2=cols2,
            naive_query=False,
            output_type="polars.DataFrame",
        ).sort(["chrom_a", "cds_start"])
        assert cnt_opt["count"].to_list() == [2, 0, 1]
    finally:
        pb.set_option(
            "datafusion.bio.coordinate_system_zero_based",
            orig_zb if orig_zb is not None else "false",
        )

    lf1, lf2 = df1.lazy(), df2.lazy()
    assert lf1.pb.coverage(lf2, cols1=cols1, cols2=cols2).collect().sort("gene")[
        "coverage"
    ].to_list() == [400, 0, 150]
    assert lf1.pb.count_overlaps(
        lf2, cols1=cols1, cols2=cols2, naive_query=False
    ).collect().sort(["chrom_a", "cds_start"])["count"].to_list() == [2, 0, 1]
