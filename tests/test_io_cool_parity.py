"""Row-for-row parity of cooler scans against the reference implementation.

The Python `cooler` package (https://github.com/open2c/cooler) is the format's
reference implementation and the oracle for these tests (openspec change
add-cool-mcool-support, section 6). Fixtures were generated with cooler —
see tests/data/io/cool/generate_fixtures.py.
"""

from pathlib import Path

import numpy as np
import polars as pl
import pytest

import polars_bio as pb

cooler = pytest.importorskip("cooler")

DATA_DIR = Path(__file__).parent / "data" / "io" / "cool"
COOL = str(DATA_DIR / "test.cool")
MCOOL = str(DATA_DIR / "test.mcool")
FLOAT_COOL = str(DATA_DIR / "test_float.cool")

JOINED = ["chrom1", "start1", "end1", "chrom2", "start2", "end2", "count"]
MCOOL_RESOLUTIONS = [1000, 2000, 5000]


def _normalize(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to comparison-stable dtypes (cooler uses pandas int64/object)."""
    casts = []
    for name, dtype in df.schema.items():
        # cooler's pandas frames use Categorical chrom columns
        if dtype in (pl.Utf8, pl.String, pl.Categorical) or isinstance(
            dtype, (pl.Categorical, pl.Enum)
        ):
            casts.append(pl.col(name).cast(pl.Utf8))
        elif dtype.is_float():
            casts.append(pl.col(name).cast(pl.Float64))
        else:
            casts.append(pl.col(name).cast(pl.Int64))
    return df.select(casts)


def _assert_frames_match(actual: pl.DataFrame, expected: pl.DataFrame, context: str):
    assert actual.height == expected.height, f"{context}: row count mismatch"
    assert actual.columns == expected.columns, f"{context}: column mismatch"
    actual = _normalize(actual)
    expected = _normalize(expected)
    for column in actual.columns:
        ours = actual[column].to_numpy()
        theirs = expected[column].to_numpy()
        if np.issubdtype(ours.dtype, np.floating):
            np.testing.assert_allclose(
                ours, theirs, equal_nan=True, err_msg=f"{context}: column {column}"
            )
        else:
            assert (ours == theirs).all(), f"{context}: column {column} diverges"


class TestFullScanParity:
    """Task 6.1: full joined scan, raw COO, weights, per-resolution .mcool."""

    @pytest.mark.parametrize(
        "uri",
        [COOL, FLOAT_COOL]
        + [f"{MCOOL}::/resolutions/{res}" for res in MCOOL_RESOLUTIONS],
        ids=["cool", "float_cool"] + [f"mcool_{res}" for res in MCOOL_RESOLUTIONS],
    )
    def test_joined_pixels(self, uri):
        expected = pl.from_pandas(cooler.Cooler(uri).pixels(join=True)[:])
        actual = pb.scan_cool(uri, use_zero_based=True).collect()
        _assert_frames_match(actual, expected.select(JOINED), f"joined {uri}")

    @pytest.mark.parametrize("uri", [COOL, f"{MCOOL}::/resolutions/2000"])
    def test_raw_coo(self, uri):
        expected = pl.from_pandas(cooler.Cooler(uri).pixels(join=False)[:])
        actual = pb.scan_cool(uri, join_bins=False).collect()
        _assert_frames_match(
            actual, expected.select(["bin1_id", "bin2_id", "count"]), f"raw {uri}"
        )

    @pytest.mark.parametrize("resolution", MCOOL_RESOLUTIONS)
    def test_weights_match_cooler_bins(self, resolution):
        clr = cooler.Cooler(f"{MCOOL}::/resolutions/{resolution}")
        bins = clr.bins()[:]
        raw = clr.pixels(join=False)[:]
        weights = bins["weight"].to_numpy()
        expected = pl.from_pandas(raw).with_columns(
            pl.Series("weight1", weights[raw["bin1_id"].to_numpy()]),
            pl.Series("weight2", weights[raw["bin2_id"].to_numpy()]),
        )
        actual = pb.scan_cool(
            MCOOL, resolution=resolution, include_weights=True, use_zero_based=True
        ).collect()
        _assert_frames_match(
            actual.select(["weight1", "weight2"]),
            expected.select(["weight1", "weight2"]),
            f"weights res={resolution}",
        )
        # balancing must have produced at least one finite weight to be a real test
        assert np.isfinite(actual["weight1"].to_numpy()).any()


class TestRegionQueryParity:
    """Task 6.2: pushed first-axis filters vs Cooler.matrix(...).fetch."""

    # bin-aligned 0-based half-open regions on the 1 kb fixture
    REGIONS = [
        ("chr1", 20000, 40000),
        ("chr1", 0, 5000),
        ("chr2", 10000, 30000),
    ]

    @staticmethod
    def _polars_bio_box_query(uri: str, region, predicate_pushdown: bool):
        chrom, start, end = region
        # Cooler's fetch(region) returns stored (upper-triangle) pixels whose
        # BOTH bins fall in the region box. The first-axis conjuncts are
        # pushdown-eligible; the second-axis conjuncts stay client-side.
        return (
            pb.scan_cool(
                uri, use_zero_based=True, predicate_pushdown=predicate_pushdown
            )
            .filter(
                (pl.col("chrom1") == chrom)
                & (pl.col("start1") >= start)
                & (pl.col("end1") <= end)
                & (pl.col("chrom2") == chrom)
                & (pl.col("start2") >= start)
                & (pl.col("end2") <= end)
            )
            .collect()
        )

    @pytest.mark.parametrize("region", REGIONS, ids=lambda r: f"{r[0]}:{r[1]}-{r[2]}")
    @pytest.mark.parametrize("uri", [COOL, f"{MCOOL}::/resolutions/1000"])
    def test_region_box_matches_cooler_fetch(self, uri, region):
        clr = cooler.Cooler(uri)
        expected = pl.from_pandas(
            clr.matrix(balance=False, as_pixels=True, join=True).fetch(region)
        ).select(JOINED)
        pushed = self._polars_bio_box_query(uri, region, predicate_pushdown=True)
        plain = self._polars_bio_box_query(uri, region, predicate_pushdown=False)
        assert expected.height > 0, "region matched no pixels — weak test"
        sort_cols = ["start1", "start2"]
        _assert_frames_match(
            pushed.sort(sort_cols), expected.sort(sort_cols), f"pushed {region}"
        )
        _assert_frames_match(
            plain.sort(sort_cols), expected.sort(sort_cols), f"plain {region}"
        )

    def test_first_axis_only_slice_matches_cooler_pixels(self):
        # A pure first-axis filter (no chrom2 constraint) equals a pandas
        # filter over cooler's full joined pixels table.
        full = pl.from_pandas(cooler.Cooler(COOL).pixels(join=True)[:]).select(JOINED)
        expected = full.filter(
            (pl.col("chrom1") == "chr1")
            & (pl.col("start1") >= 20000)
            & (pl.col("end1") <= 40000)
        )
        actual = (
            pb.scan_cool(COOL, use_zero_based=True)
            .filter(
                (pl.col("chrom1") == "chr1")
                & (pl.col("start1") >= 20000)
                & (pl.col("end1") <= 40000)
            )
            .collect()
        )
        sort_cols = ["start1", "start2"]
        _assert_frames_match(
            actual.sort(sort_cols), expected.sort(sort_cols), "first-axis slice"
        )


class TestDescribeParity:
    """Task 6.3: describe_cool vs cooler.fileops.list_coolers + Cooler.info."""

    def test_mcool_collections_match_list_coolers(self):
        described = pb.describe_cool(MCOOL)
        assert sorted(described["group_path"].to_list()) == sorted(
            cooler.fileops.list_coolers(MCOOL)
        )

    @pytest.mark.parametrize(
        "uri",
        [COOL, FLOAT_COOL] + [f"{MCOOL}::/resolutions/{r}" for r in MCOOL_RESOLUTIONS],
    )
    def test_collection_info_matches_cooler_info(self, uri):
        info = cooler.Cooler(uri).info
        row = pb.describe_cool(uri).row(0, named=True)
        assert row["resolution"] == info["bin-size"]
        assert row["nbins"] == info["nbins"]
        assert row["nnz"] == info["nnz"]
        assert row["nchroms"] == info["nchroms"]
        assert row["sum"] == info["sum"]
        assert row["format_version"] == info["format-version"]
        if info.get("genome-assembly"):
            assert row["assembly"] == info["genome-assembly"]
