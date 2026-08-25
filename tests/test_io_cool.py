from pathlib import Path

import polars as pl
import pytest

import polars_bio as pb

DATA_DIR = Path(__file__).parent / "data" / "io" / "cool"
COOL = str(DATA_DIR / "test.cool")
MCOOL = str(DATA_DIR / "test.mcool")
FLOAT_COOL = str(DATA_DIR / "test_float.cool")

JOINED_COLUMNS = ["chrom1", "start1", "end1", "chrom2", "start2", "end2", "count"]


class TestCoolScan:
    def test_full_scan_shape_and_schema(self):
        df = pb.scan_cool(COOL).collect()
        assert df.shape == (4210, 7)
        assert df.columns == JOINED_COLUMNS
        assert df["chrom1"].dtype == pl.Utf8
        assert df["start1"].dtype == pl.UInt32
        assert df["count"].dtype == pl.Int32

    def test_first_pixel_one_based_default(self):
        row = pb.scan_cool(COOL).head(1).collect().row(0, named=True)
        # bins (0,0) -> chr1:0-1000 x chr1:0-1000, count 72; 1-based start = 1
        assert row["chrom1"] == "chr1"
        assert row["start1"] == 1
        assert row["end1"] == 1000
        assert row["chrom2"] == "chr1"
        assert row["count"] == 72

    def test_zero_based_coordinates(self):
        row = (
            pb.scan_cool(COOL, use_zero_based=True).head(1).collect().row(0, named=True)
        )
        assert row["start1"] == 0
        assert row["end1"] == 1000

    def test_one_based_explicit(self):
        row = (
            pb.scan_cool(COOL, use_zero_based=False)
            .head(1)
            .collect()
            .row(0, named=True)
        )
        assert row["start1"] == 1
        assert row["end1"] == 1000

    def test_raw_coo_mode(self):
        df = pb.scan_cool(COOL, join_bins=False).collect()
        assert df.columns == ["bin1_id", "bin2_id", "count"]
        assert df["bin1_id"].dtype == pl.Int64
        assert df.row(0) == (0, 0, 72)

    def test_read_cool_eager(self):
        df = pb.read_cool(COOL)
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (4210, 7)

    def test_float_count_dtype(self):
        df = pb.scan_cool(FLOAT_COOL).collect()
        assert df["count"].dtype == pl.Float64
        assert df["count"][0] == 36.0

    def test_mcool_resolution_argument(self):
        df = pb.scan_cool(MCOOL, resolution=2000).collect()
        assert df.height == 2560
        assert df["end1"][0] == 2000

    def test_mcool_uri_syntax(self):
        df = pb.scan_cool(f"{MCOOL}::/resolutions/2000").collect()
        assert df.height == 2560

    def test_mcool_without_resolution_errors(self):
        with pytest.raises(Exception, match="1000.*2000.*5000"):
            pb.scan_cool(MCOOL)

    def test_mcool_missing_resolution_errors(self):
        with pytest.raises(Exception, match="1234"):
            pb.scan_cool(MCOOL, resolution=1234)

    def test_weights_on_balanced_mcool(self):
        df = pb.scan_cool(MCOOL, resolution=1000, include_weights=True).collect()
        assert "weight1" in df.columns and "weight2" in df.columns
        assert df["weight1"].dtype == pl.Float64

    def test_weights_missing_errors(self):
        with pytest.raises(Exception, match="weight"):
            pb.scan_cool(COOL, include_weights=True)

    def test_remote_path_rejected(self):
        with pytest.raises(Exception, match="local filesystem"):
            pb.scan_cool("s3://bucket/contacts.cool")


class TestCoolPushdown:
    def test_projection(self):
        df = pb.scan_cool(COOL).select(["chrom1", "count"]).collect()
        assert df.columns == ["chrom1", "count"]
        assert df.height == 4210

    def test_count_star(self):
        assert pb.scan_cool(COOL).select(pl.len()).collect().item() == 4210

    @pytest.mark.parametrize(
        "predicate",
        [
            pl.col("chrom1") == "chr2",
            (pl.col("chrom1") == "chr1")
            & (pl.col("start1") >= 20001)
            & (pl.col("end1") <= 40000),
            pl.col("chrom1").is_in(["chr2"]),
        ],
    )
    def test_predicate_pushdown_matches_no_pushdown(self, predicate):
        pushed = pb.scan_cool(COOL, predicate_pushdown=True).filter(predicate).collect()
        plain = pb.scan_cool(COOL, predicate_pushdown=False).filter(predicate).collect()
        assert pushed.height > 0
        assert pushed.sort(JOINED_COLUMNS).equals(plain.sort(JOINED_COLUMNS))

    def test_unknown_chrom_empty(self):
        df = pb.scan_cool(COOL).filter(pl.col("chrom1") == "chrX").collect()
        assert df.height == 0


class TestCoolDescribe:
    def test_describe_mcool(self):
        df = pb.describe_cool(MCOOL)
        assert df.height == 3
        assert sorted(df["resolution"].to_list()) == [1000, 2000, 5000]
        assert set(df["nchroms"].to_list()) == {2}

    def test_describe_cool_file(self):
        df = pb.describe_cool(COOL)
        assert df.height == 1
        row = df.row(0, named=True)
        assert row["resolution"] == 1000
        assert row["nnz"] == 4210
        assert row["assembly"] == "toyGenome"


class TestCoolSql:
    def test_register_and_query(self):
        pb.register_cool(MCOOL, "cool_sql_test", resolution=2000)
        df = pb.sql(
            "SELECT chrom1, count FROM cool_sql_test ORDER BY count DESC LIMIT 3"
        ).collect()
        assert df.height == 3
        assert df.columns == ["chrom1", "count"]

    def test_sql_where_first_axis(self):
        pb.register_cool(COOL, "cool_sql_where")
        n = pb.sql(
            "SELECT count(*) AS n FROM cool_sql_where WHERE chrom1 = 'chr2'"
        ).collect()["n"][0]
        plain = pb.scan_cool(COOL).filter(pl.col("chrom1") == "chr2").collect().height
        assert n == plain > 0


class TestCoolerOracleParity:
    """Row-for-row parity against the reference cooler implementation."""

    def test_joined_pixels_match_cooler(self):
        cooler = pytest.importorskip("cooler")
        clr = cooler.Cooler(f"{MCOOL}::/resolutions/2000")
        expected = pl.from_pandas(clr.pixels(join=True)[:])
        actual = pb.scan_cool(MCOOL, resolution=2000, use_zero_based=True).collect()
        assert actual.height == expected.height
        for ours, theirs in [
            ("chrom1", "chrom1"),
            ("start1", "start1"),
            ("end1", "end1"),
            ("chrom2", "chrom2"),
            ("start2", "start2"),
            ("end2", "end2"),
            ("count", "count"),
        ]:
            assert (
                actual[ours].cast(pl.Utf8).to_list()
                == expected[theirs].cast(pl.Utf8).to_list()
            ), f"column {ours} diverges from cooler"


class TestCoolMetadata:
    def test_source_and_collection_metadata(self):
        import json

        lf = pb.scan_cool(MCOOL, resolution=2000)
        meta = lf.config_meta.get_metadata()
        assert meta.get("source_format") == "cool"
        assert meta.get("source_path") == MCOOL
        header = json.loads(meta.get("source_header") or "{}")
        assert header.get("resolution") == 2000
        assert header.get("group_path") == "/resolutions/2000"

    def test_coordinate_system_metadata(self):
        lf = pb.scan_cool(COOL, use_zero_based=True)
        assert lf.config_meta.get_metadata().get("coordinate_system_zero_based") is True
