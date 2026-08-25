from decimal import Decimal
from pathlib import Path
from shutil import copyfile

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


class TestCoolPlanAndParallel:
    """Tasks 6.5/6.6: plan display shows projection; parallel == serial."""

    @staticmethod
    def _execution_plan(name: str, partitions: int):
        from polars_bio.context import ctx
        from polars_bio.polars_bio import (
            CoolReadOptions,
            InputFormat,
            ReadOptions,
            py_read_table,
            py_register_table,
        )

        read_options = ReadOptions(cool_read_options=CoolReadOptions(zero_based=True))
        table = py_register_table(
            ctx, COOL, f"{name}_{partitions}", InputFormat.Cool, read_options
        )
        return py_read_table(ctx, table.name)

    def test_execution_plan_shows_projection(self):
        df = self._execution_plan("cool_plan_proj", 1)
        plan = str(df.select_exprs("chrom1", "count").execution_plan())
        assert "CoolerExec" in plan
        assert "projection=[chrom1, count]" in plan

    def test_execution_plan_full_rows(self):
        df = self._execution_plan("cool_plan_rows", 1)
        plan = str(df.execution_plan())
        assert "rows=4210" in plan

    @pytest.mark.parametrize("partitions", [1, 2, 4, 8])
    def test_parallel_scan_matches_serial(self, partitions, cool_serial_frame):
        from contextlib import contextmanager

        target_key = "datafusion.execution.target_partitions"
        original = pb.get_option(target_key)
        pb.set_option(target_key, str(partitions))
        try:
            plan = self._execution_plan("cool_parallel", partitions).execution_plan()
            actual = pb.read_cool(COOL, use_zero_based=True)
        finally:
            pb.set_option(target_key, original)
        assert plan.partition_count == partitions
        assert actual.sort(JOINED_COLUMNS).equals(cool_serial_frame)


@pytest.fixture(scope="module")
def cool_serial_frame():
    target_key = "datafusion.execution.target_partitions"
    original = pb.get_option(target_key)
    pb.set_option(target_key, "1")
    try:
        return pb.read_cool(COOL, use_zero_based=True).sort(JOINED_COLUMNS)
    finally:
        pb.set_option(target_key, original)


class TestCoolInt64Count:
    def test_int64_count_not_truncated(self):
        df = pb.scan_cool(str(DATA_DIR / "test_int64.cool")).collect()
        assert df["count"].dtype == pl.Int64
        # first pixel's count exceeds i32::MAX; truncation would corrupt it
        assert df["count"][0] == 5_000_000_000


class TestCoolWideValues:
    def test_unsigned_counts_keep_their_full_ranges(self):
        uint32 = pb.scan_cool(str(DATA_DIR / "test_uint32.cool")).collect()
        assert uint32["count"].dtype == pl.UInt32
        assert uint32["count"][0] == 3_000_000_000

        uint64 = pb.scan_cool(str(DATA_DIR / "test_uint64.cool")).collect()
        assert uint64["count"].dtype == pl.UInt64
        assert uint64["count"][0] == 10_000_000_000_000_000_000

    def test_coordinates_above_int32_are_preserved(self):
        df = pb.scan_cool(
            str(DATA_DIR / "test_wide_coords.cool"), use_zero_based=True
        ).collect()
        assert df["start1"][0] == 3_000_000_000
        assert df["end1"][0] == 4_000_000_000

    def test_exact_integer_sum_above_float_range(self):
        df = pb.describe_cool(str(DATA_DIR / "test_exact_sum.cool"))
        assert df["sum"].dtype == pl.Int64
        assert df["sum"][0] == 9_007_199_254_740_993

    def test_mixed_resolution_sums_remain_exact(self):
        df = pb.describe_cool(str(DATA_DIR / "test_mixed_sums.mcool")).sort(
            "resolution"
        )
        assert df["sum"].dtype == pl.Decimal(precision=38, scale=1)
        assert df["sum"].to_list() == [
            Decimal("124193.5"),
            Decimal("9007199254740993.0"),
        ]


class TestCoolSqlDefaultName:
    def test_register_cool_uri_derives_name_from_file(self):
        # contacts.mcool::/resolutions/N must not register as table "N"
        pb.register_cool(f"{MCOOL}::/resolutions/2000")
        n = pb.sql("SELECT count(*) AS n FROM test").collect()["n"][0]
        assert n == 2560

    def test_default_name_is_a_valid_unquoted_identifier(self, tmp_path):
        path = tmp_path / "123 contacts! (final).cool"
        copyfile(COOL, path)
        pb.register_cool(str(path))
        n = pb.sql("SELECT count(*) AS n FROM cool_123_contacts_final").collect()["n"][
            0
        ]
        assert n == 4210
