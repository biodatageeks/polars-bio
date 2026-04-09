import json
import shutil
from pathlib import Path

import polars as pl
import pysam
import pytest
from _expected import DATA_DIR

import polars_bio as pb

CORE_BAM_COLUMNS = [
    "name",
    "chrom",
    "start",
    "end",
    "flags",
    "cigar",
    "mapping_quality",
    "mate_chrom",
    "mate_start",
    "sequence",
    "quality_scores",
    "template_length",
]


def _first_record_tag_data(path: str, mode: str, tags: list[str]) -> dict[str, tuple]:
    with pysam.AlignmentFile(path, mode) as alignment:
        record = next(alignment)
        return {tag: record.get_tag(tag, with_value_type=True) for tag in tags}


class TestIOBAM:
    df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")

    def test_count(self):
        assert len(self.df) == 2333

    def test_fields(self):
        assert self.df["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert self.df["flags"][3] == 1123
        assert self.df["cigar"][4] == "101M"
        assert (
            self.df["sequence"][4]
            == "TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACC"
        )
        assert (
            self.df["quality_scores"][4]
            == "CCDACCDCDABBDCDABBDCDABBDCDABBDCD?BBCCDABBCCDABBACDA?BDCAABBDBDA.=?><;CBB2@:;??:D>?5BAC??=DC;=5=?8:76"
        )

    def test_register(self):
        pb.register_bam(f"{DATA_DIR}/io/bam/test.bam", "test_bam")
        count = pb.sql("select count(*) as cnt from test_bam").collect()
        assert count["cnt"][0] == 2333

        projection = pb.sql("select name, flags from test_bam").collect()
        assert projection["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert projection["flags"][3] == 1123

    def test_bam_no_tags_default(self):
        """Test backward compatibility - no tags by default"""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        assert len(df.columns) == 12  # Original columns only
        assert "NM" not in df.columns
        assert "AS" not in df.columns
        assert "MD" not in df.columns

    def test_bam_single_tag(self):
        """Test reading a single BAM tag"""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam", tag_fields=["NM"])
        assert "NM" in df.columns
        assert len(df.columns) == 13  # 12 original + 1 tag

    def test_bam_multiple_tags(self):
        """Test reading multiple BAM tags"""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam", tag_fields=["NM", "AS", "MD"])
        assert "NM" in df.columns
        assert "AS" in df.columns
        assert "MD" in df.columns
        assert len(df.columns) == 15  # 12 original + 3 tags

    def test_bam_scan_with_tags(self):
        """Test lazy scan with tags and filtering"""
        lf = pb.scan_bam(f"{DATA_DIR}/io/bam/test.bam", tag_fields=["NM", "AS"])
        df = lf.select(["name", "chrom", "NM", "AS"]).collect()
        assert "NM" in df.columns
        assert "AS" in df.columns
        assert len(df.columns) == 4

    def test_bam_scan_with_numeric_prefixed_read_group(self):
        """Regression for issue #319: filenames/read groups starting with digits must not break SQL parsing."""
        bam_path = f"{DATA_DIR}/io/bam/10x_pbmc_tags.bam"

        df = pb.scan_bam(bam_path, tag_fields=["CB", "CR"]).limit(3).collect()
        assert len(df) == 3
        assert "CB" in df.columns
        assert "CR" in df.columns
        assert df["CB"].null_count() == 0
        assert df["CR"].null_count() == 0

        # Ensure RG values with "10k_" prefix are readable.
        rg_df = (
            pb.scan_bam(bam_path, tag_fields=["RG"]).select(["RG"]).limit(1).collect()
        )
        assert len(rg_df) == 1
        assert rg_df["RG"][0].startswith("10k_")

    def test_bam_scan_indexed_no_coor_only_records(self, tmp_path):
        """Regression for issue #330/#86: indexed scans must return no-coordinate records."""
        header = {
            "HD": {"VN": "1.6", "SO": "coordinate"},
            "SQ": [{"SN": "chr1", "LN": 1000}],
        }
        unsorted_path = str(tmp_path / "no_coor.unsorted.bam")
        sorted_path = str(tmp_path / "no_coor.sorted.bam")

        with pysam.AlignmentFile(unsorted_path, "wb", header=header) as out:
            for name, cb, cr, seq in [
                ("r1", "CELL1", "RAW1", "ACGT"),
                ("r2", "CELL2", "RAW2", "TGCA"),
            ]:
                record = pysam.AlignedSegment()
                record.query_name = name
                record.query_sequence = seq
                record.flag = 4
                record.reference_id = -1
                record.reference_start = -1
                record.mapping_quality = 0
                record.cigarstring = None
                record.next_reference_id = -1
                record.next_reference_start = -1
                record.template_length = 0
                record.query_qualities = pysam.qualitystring_to_array("FFFF")
                record.set_tag("CB", cb)
                record.set_tag("CR", cr)
                out.write(record)

        pysam.sort("-o", sorted_path, unsorted_path)
        pysam.index(sorted_path)

        df = pb.scan_bam(sorted_path, tag_fields=["CB", "CR"]).collect()
        assert len(df) == 2
        assert df["chrom"].null_count() == 2
        assert df["start"].null_count() == 2
        assert set(df["CB"].to_list()) == {"CELL1", "CELL2"}
        assert set(df["CR"].to_list()) == {"RAW1", "RAW2"}

    def test_bam_sql_with_tags(self):
        """Test SQL queries with tags"""
        pb.register_bam(
            f"{DATA_DIR}/io/bam/test.bam", "test_tags", tag_fields=["NM", "AS"]
        )
        result = pb.sql('SELECT name, "NM", "AS" FROM test_tags LIMIT 5').collect()
        assert "NM" in result.columns
        assert "AS" in result.columns
        assert len(result) == 5

    def test_describe_bam_no_tags(self):
        """Test describe_bam with auto-discovery (sample_size=0 to skip tags)"""
        schema = pb.describe_bam(f"{DATA_DIR}/io/bam/test.bam", sample_size=0)
        assert "column_name" in schema.columns
        assert "data_type" in schema.columns
        assert "category" in schema.columns
        assert len(schema) == 12  # 12 core columns
        columns = schema["column_name"].to_list()
        assert "name" in columns
        assert "chrom" in columns
        # All should be core columns
        assert all(schema["category"] == "core")

    def test_describe_bam_with_tags(self):
        """Test describe_bam with automatic tag discovery"""
        schema = pb.describe_bam(f"{DATA_DIR}/io/bam/test.bam", sample_size=100)
        assert "column_name" in schema.columns
        assert "data_type" in schema.columns
        assert "category" in schema.columns
        assert "sam_type" in schema.columns
        assert "description" in schema.columns

        # Should have core + discovered tag columns
        assert len(schema) > 12

        columns = schema["column_name"].to_list()
        # Check core columns present
        assert "name" in columns
        assert "chrom" in columns

        # Check some expected tags discovered
        tags = schema.filter(schema["category"] == "tag")
        tag_names = tags["column_name"].to_list()
        assert "NM" in tag_names  # Edit distance
        assert "MD" in tag_names  # Mismatch string

        # Verify tag data types
        nm_row = schema.filter(schema["column_name"] == "NM")
        md_row = schema.filter(schema["column_name"] == "MD")
        assert len(nm_row) == 1
        assert nm_row["data_type"][0] == "Int32"
        assert len(md_row) == 1
        assert md_row["data_type"][0] == "Utf8"


class TestIOSAM:
    df = pb.read_sam(f"{DATA_DIR}/io/sam/test.sam")

    def test_count(self):
        assert len(self.df) == 2333

    def test_fields(self):
        assert self.df["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert self.df["flags"][3] == 1123
        assert self.df["cigar"][4] == "101M"
        assert (
            self.df["sequence"][4]
            == "TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACC"
        )
        assert (
            self.df["quality_scores"][4]
            == "CCDACCDCDABBDCDABBDCDABBDCDABBDCD?BBCCDABBCCDABBACDA?BDCAABBDBDA.=?><;CBB2@:;??:D>?5BAC??=DC;=5=?8:76"
        )

    def test_register(self):
        pb.register_sam(f"{DATA_DIR}/io/sam/test.sam", "test_sam")
        count = pb.sql("select count(*) as cnt from test_sam").collect()
        assert count["cnt"][0] == 2333

        projection = pb.sql("select name, flags from test_sam").collect()
        assert projection["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert projection["flags"][3] == 1123

    def test_sam_no_tags_default(self):
        """Test backward compatibility - no tags by default"""
        df = pb.read_sam(f"{DATA_DIR}/io/sam/test.sam")
        assert len(df.columns) == 12
        assert "NM" not in df.columns

    def test_sam_single_tag(self):
        """Test reading a single SAM tag"""
        df = pb.read_sam(f"{DATA_DIR}/io/sam/test.sam", tag_fields=["NM"])
        assert "NM" in df.columns
        assert len(df.columns) == 13

    def test_sam_scan(self):
        """Test lazy scanning"""
        lf = pb.scan_sam(f"{DATA_DIR}/io/sam/test.sam", tag_fields=["NM", "AS"])
        df = lf.select(["name", "chrom", "NM", "AS"]).collect()
        assert "NM" in df.columns
        assert "AS" in df.columns
        assert len(df.columns) == 4

    def test_describe_sam(self):
        """Test schema discovery with tags"""
        schema = pb.describe_sam(f"{DATA_DIR}/io/sam/test.sam", sample_size=100)
        assert "column_name" in schema.columns
        assert "data_type" in schema.columns
        assert "category" in schema.columns
        assert len(schema) > 12

        columns = schema["column_name"].to_list()
        assert "name" in columns
        assert "chrom" in columns

        tags = schema.filter(schema["category"] == "tag")
        tag_names = tags["column_name"].to_list()
        assert "NM" in tag_names
        assert "MD" in tag_names


class TestBAMWrite:
    """Tests for BAM write functionality."""

    def test_write_bam_roundtrip(self, tmp_path):
        """BAM -> BAM roundtrip: read, write, read back."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        out_path = str(tmp_path / "roundtrip.bam")
        rows_written = pb.write_bam(df, out_path)
        assert rows_written == 2333

        df_back = pb.read_bam(out_path)
        assert len(df_back) == 2333
        assert df_back["name"][2] == df["name"][2]
        assert df_back["flags"][3] == df["flags"][3]

    def test_sink_bam_roundtrip(self, tmp_path):
        """Streaming BAM write roundtrip: scan, sink, read back."""
        lf = pb.scan_bam(f"{DATA_DIR}/io/bam/test.bam")
        out_path = str(tmp_path / "sink.bam")
        pb.sink_bam(lf, out_path)

        df_back = pb.read_bam(out_path)
        assert len(df_back) == 2333

    def test_write_bam_with_tags(self, tmp_path):
        """BAM roundtrip with tag fields."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam", tag_fields=["NM", "AS"])
        out_path = str(tmp_path / "tags.bam")
        pb.write_bam(df, out_path)

        df_back = pb.read_bam(out_path, tag_fields=["NM", "AS"])
        assert "NM" in df_back.columns
        assert "AS" in df_back.columns
        assert len(df_back) == 2333

    def test_sink_bam_with_tags(self, tmp_path):
        """Streaming BAM write with tag fields."""
        lf = pb.scan_bam(f"{DATA_DIR}/io/bam/test.bam", tag_fields=["NM", "AS"])
        out_path = str(tmp_path / "sink_tags.bam")
        pb.sink_bam(lf, out_path)

        df_back = pb.read_bam(out_path, tag_fields=["NM", "AS"])
        assert "NM" in df_back.columns
        assert "AS" in df_back.columns
        assert len(df_back) == 2333

    def test_write_bam_numeric_prefixed_input_output_paths(self, tmp_path):
        """Numeric-leading input/output names should work for eager BAM writes."""
        src = Path(f"{DATA_DIR}/io/bam/test.bam")
        numeric_input = tmp_path / "10_input.bam"
        shutil.copy2(src, numeric_input)

        df = pb.scan_bam(str(numeric_input)).limit(25).collect()
        numeric_output = tmp_path / "20_output.bam"
        rows_written = pb.write_bam(df, str(numeric_output))

        assert rows_written == 25
        df_back = pb.read_bam(str(numeric_output))
        assert len(df_back) == 25

    def test_sink_bam_numeric_prefixed_input_output_paths(self, tmp_path):
        """Numeric-leading input/output names should work for streaming BAM writes."""
        src = Path(f"{DATA_DIR}/io/bam/test.bam")
        numeric_input = tmp_path / "30_input.bam"
        shutil.copy2(src, numeric_input)

        numeric_output = tmp_path / "40_output.bam"
        pb.sink_bam(pb.scan_bam(str(numeric_input)).limit(25), str(numeric_output))

        df_back = pb.read_bam(str(numeric_output))
        assert len(df_back) == 25


class TestTypedTagMetadataAndRoundtrip:
    EXPECTED_TAG_TYPES = {
        "XA": "A",
        "XH": "H",
        "ML": "B:C",
        "FZ": "B:S",
    }

    def _assert_expected_tag_types(self, meta):
        header = meta.get("header", {})
        assert "tag_types" in header
        for tag, expected in self.EXPECTED_TAG_TYPES.items():
            assert header["tag_types"][tag] == expected

    def test_bam_read_exposes_exact_tag_types_in_metadata(
        self, typed_tag_alignment_paths
    ):
        df = pb.read_bam(
            typed_tag_alignment_paths["bam"],
            tag_fields=["XA", "XH", "ML", "FZ"],
        )

        self._assert_expected_tag_types(pb.get_metadata(df))

    def test_bam_tag_types_survive_eager_transformations(
        self, typed_tag_alignment_paths
    ):
        df = pb.read_bam(
            typed_tag_alignment_paths["bam"],
            tag_fields=["XA", "XH", "ML", "FZ"],
        )

        transformed = (
            df.with_columns(
                pl.col("XA").alias("XA"),
                pl.col("ML").alias("ML"),
            )
            .filter(pl.col("mapping_quality") > 0)
            .select(CORE_BAM_COLUMNS + ["XA", "XH", "ML", "FZ"])
        )

        self._assert_expected_tag_types(pb.get_metadata(transformed))

    def test_bam_tag_types_survive_lazy_transformations(
        self, typed_tag_alignment_paths
    ):
        lf = pb.scan_bam(
            typed_tag_alignment_paths["bam"],
            tag_fields=["XA", "XH", "ML", "FZ"],
        )

        transformed = (
            lf.with_columns(
                pl.col("XA").alias("XA"),
                pl.col("ML").alias("ML"),
            )
            .filter(pl.col("mapping_quality") > 0)
            .select(CORE_BAM_COLUMNS + ["XA", "XH", "ML", "FZ"])
        )

        self._assert_expected_tag_types(pb.get_metadata(transformed))

    def test_write_bam_preserves_existing_exact_tag_types(
        self, typed_tag_alignment_paths, tmp_path
    ):
        df = (
            pb.read_bam(
                typed_tag_alignment_paths["bam"],
                tag_fields=["XA", "XH", "ML", "FZ"],
            )
            .filter(pl.col("mapping_quality") > 0)
            .select(CORE_BAM_COLUMNS + ["XA", "XH", "ML", "FZ"])
        )

        out_path = str(tmp_path / "typed_roundtrip.bam")
        pb.write_bam(df, out_path)

        tag_data = _first_record_tag_data(out_path, "rb", ["XA", "XH", "ML", "FZ"])
        assert tag_data["XA"] == ("A", "A")
        assert tag_data["XH"][1] == "H"
        assert tag_data["ML"][1] == "BC"
        assert tag_data["FZ"][1] == "BS"

    def test_write_sam_preserves_existing_exact_tag_types(
        self, typed_tag_alignment_paths, tmp_path
    ):
        df = (
            pb.read_sam(
                typed_tag_alignment_paths["sam"],
                tag_fields=["XA", "XH", "ML", "FZ"],
            )
            .filter(pl.col("mapping_quality") > 0)
            .select(CORE_BAM_COLUMNS + ["XA", "XH", "ML", "FZ"])
        )

        out_path = str(tmp_path / "typed_roundtrip.sam")
        pb.write_sam(df, out_path)

        tag_data = _first_record_tag_data(out_path, "r", ["XA", "XH", "ML", "FZ"])
        assert tag_data["XA"] == ("A", "A")
        assert tag_data["XH"][1] == "H"
        assert tag_data["ML"][1] == "BC"
        assert tag_data["FZ"][1] == "BS"

    def test_write_bam_with_explicit_a_and_h_overrides(
        self, typed_tag_alignment_paths, tmp_path
    ):
        base = pb.read_bam(typed_tag_alignment_paths["bam"]).select(CORE_BAM_COLUMNS)
        df = base.with_columns(
            pl.Series("YA", ["Q", "R"], dtype=pl.Utf8),
            pl.Series("YH", ["0A0B", "C0FFEE"], dtype=pl.Utf8),
        )

        out_path = str(tmp_path / "override_tags.bam")
        pb.write_bam(
            df,
            out_path,
            tag_type_overrides={"YA": "A", "YH": "H"},
        )

        tag_data = _first_record_tag_data(out_path, "rb", ["YA", "YH"])
        assert tag_data["YA"] == ("Q", "A")
        assert tag_data["YH"][1] == "H"

    def test_write_bam_overrides_take_precedence_over_preserved_metadata(
        self, typed_tag_alignment_paths, tmp_path
    ):
        df = pb.read_bam(
            typed_tag_alignment_paths["bam"],
            tag_fields=["XA"],
        ).select(CORE_BAM_COLUMNS + ["XA"])

        out_path = str(tmp_path / "override_precedence.bam")
        pb.write_bam(
            df,
            out_path,
            tag_type_overrides={"XA": "Z"},
        )

        tag_data = _first_record_tag_data(out_path, "rb", ["XA"])
        assert tag_data["XA"] == ("A", "Z")

    def test_write_bam_roundtrip_with_new_scalar_and_array_tags(
        self, typed_tag_alignment_paths, tmp_path
    ):
        base = pb.read_bam(typed_tag_alignment_paths["bam"]).select(CORE_BAM_COLUMNS)
        df = base.with_columns(
            pl.Series("YI", [10, 20], dtype=pl.Int32),
            pl.Series("YF", [1.25, 2.5], dtype=pl.Float32),
            pl.Series("YZ", ["alpha", "beta"], dtype=pl.Utf8),
            pl.Series("YL", [[1, 2], [3, 4]], dtype=pl.List(pl.UInt8)),
        )

        out_path = str(tmp_path / "new_tags.bam")
        pb.write_bam(df, out_path)

        df_back = pb.read_bam(out_path, tag_fields=["YI", "YF", "YZ", "YL"])
        assert df_back["YI"].to_list() == [10, 20]
        assert df_back["YF"].to_list() == pytest.approx([1.25, 2.5])
        assert df_back["YZ"].to_list() == ["alpha", "beta"]
        assert df_back["YL"].dtype == pl.List(pl.UInt8)
        assert df_back["YL"].to_list() == [[1, 2], [3, 4]]

        tag_data = _first_record_tag_data(out_path, "rb", ["YI", "YF", "YZ", "YL"])
        assert tag_data["YI"][1] in {"c", "C", "s", "S", "i", "I"}
        assert tag_data["YF"][1] == "f"
        assert tag_data["YZ"][1] == "Z"
        assert tag_data["YL"][1] == "BC"

    def test_sink_bam_roundtrip_preserves_existing_and_new_typed_tags(
        self, typed_tag_alignment_paths, tmp_path
    ):
        lf = (
            pb.scan_bam(
                typed_tag_alignment_paths["bam"],
                tag_fields=["ML", "FZ"],
            )
            .with_columns(
                pl.lit(42).cast(pl.Int32).alias("YI"),
                pl.lit(2.5).cast(pl.Float64).alias("YF"),
                pl.col("ML").alias("YL"),
            )
            .select(CORE_BAM_COLUMNS + ["ML", "FZ", "YI", "YF", "YL"])
        )

        out_path = str(tmp_path / "lazy_typed_tags.bam")
        pb.sink_bam(lf, out_path)

        df_back = pb.read_bam(out_path, tag_fields=["ML", "FZ", "YI", "YF", "YL"])
        assert df_back["ML"].dtype == pl.List(pl.UInt8)
        assert df_back["FZ"].dtype == pl.List(pl.UInt16)
        assert df_back["YI"].to_list() == [42, 42]
        assert df_back["YF"].to_list() == pytest.approx([2.5, 2.5])
        assert df_back["YL"].dtype == pl.List(pl.UInt8)
        assert df_back["YL"].to_list() == [[1, 2, 3], [2, 3, 4]]

        tag_data = _first_record_tag_data(
            out_path, "rb", ["ML", "FZ", "YI", "YF", "YL"]
        )
        assert tag_data["ML"][1] == "BC"
        assert tag_data["FZ"][1] == "BS"
        assert tag_data["YI"][1] in {"c", "C", "s", "S", "i", "I"}
        assert tag_data["YF"][1] == "f"
        assert tag_data["YL"][1] == "BC"


@pytest.mark.parametrize(
    ("tag_name", "dtype", "value", "expected_exact_type"),
    [
        ("C8", pl.Int8, -5, None),
        ("U8", pl.UInt8, 250, None),
        ("C6", pl.Int16, -300, None),
        ("U6", pl.UInt16, 60_000, None),
        ("I3", pl.Int32, 123_456, None),
        ("I6", pl.Int64, 123_456_789, None),
        ("N3", pl.UInt32, 3_000_000_000, None),
        ("N6", pl.UInt64, 3_000_000_000, None),
        ("F3", pl.Float32, 1.25, "f"),
        ("F6", pl.Float64, 2.5, "f"),
    ],
)
def test_write_bam_accepts_wide_numeric_widths(
    typed_tag_alignment_paths,
    tmp_path,
    tag_name,
    dtype,
    value,
    expected_exact_type,
):
    df = pb.read_bam(typed_tag_alignment_paths["bam"]).head(1).select(CORE_BAM_COLUMNS)
    df = df.with_columns(pl.Series(tag_name, [value], dtype=dtype))

    out_path = str(tmp_path / f"wide_{tag_name}.bam")
    pb.write_bam(df, out_path)

    roundtrip_value, actual_type = _first_record_tag_data(out_path, "rb", [tag_name])[
        tag_name
    ]
    if isinstance(value, float):
        assert roundtrip_value == pytest.approx(value)
        assert actual_type == expected_exact_type
    else:
        assert roundtrip_value == value
        # Integer tags can roundtrip as any valid SAM integer type because the
        # upstream writer canonicalizes widths based on the value being encoded.
        assert actual_type in {"c", "C", "s", "S", "i", "I"}


class TestBAMWritePositionRoundtrip:
    """Regression tests for BAM write position off-by-one.

    See: https://github.com/biodatageeks/polars-bio/issues/356
         https://github.com/biodatageeks/datafusion-bio-formats/issues/163

    When writing BAM files, positions (start, mate_start) must survive a
    read -> write -> read roundtrip without drift.  The upstream
    BamTableProvider defaults to zero_based=true when schema metadata is
    missing, which caused a +1 on every roundtrip for 1-based data.

    The polars-bio fix injects coordinate_system metadata into the schema
    before calling the upstream writer.  A full upstream fix (using the
    self.coordinate_system_zero_based field as fallback) is tracked in
    datafusion-bio-formats#163.
    """

    BAM_PATH = f"{DATA_DIR}/io/bam/test.bam"

    def test_roundtrip_positions_one_based(self, tmp_path):
        """1-based read -> write -> read must preserve start, end, and mate_start."""
        df_orig = pb.read_bam(self.BAM_PATH, use_zero_based=False)
        out = str(tmp_path / "rt_1based.bam")
        pb.write_bam(df_orig, out)
        df_back = pb.read_bam(out, use_zero_based=False)

        assert (
            df_back["start"].to_list() == df_orig["start"].to_list()
        ), "start positions drifted after 1-based roundtrip"
        assert (
            df_back["end"].to_list() == df_orig["end"].to_list()
        ), "end positions drifted after 1-based roundtrip"
        assert (
            df_back["mate_start"].to_list() == df_orig["mate_start"].to_list()
        ), "mate_start positions drifted after 1-based roundtrip"

    def test_roundtrip_positions_zero_based(self, tmp_path):
        """0-based read -> write -> read must preserve start, end, and mate_start."""
        df_orig = pb.read_bam(self.BAM_PATH, use_zero_based=True)
        out = str(tmp_path / "rt_0based.bam")
        pb.write_bam(df_orig, out)
        df_back = pb.read_bam(out, use_zero_based=True)

        assert (
            df_back["start"].to_list() == df_orig["start"].to_list()
        ), "start positions drifted after 0-based roundtrip"
        assert (
            df_back["end"].to_list() == df_orig["end"].to_list()
        ), "end positions drifted after 0-based roundtrip"
        assert (
            df_back["mate_start"].to_list() == df_orig["mate_start"].to_list()
        ), "mate_start positions drifted after 0-based roundtrip"

    def test_roundtrip_positions_sink_one_based(self, tmp_path):
        """1-based scan -> sink -> read must preserve positions."""
        lf = pb.scan_bam(self.BAM_PATH, use_zero_based=False)
        out = str(tmp_path / "sink_1based.bam")
        pb.sink_bam(lf, out)

        df_orig = pb.read_bam(self.BAM_PATH, use_zero_based=False)
        df_back = pb.read_bam(out, use_zero_based=False)

        assert (
            df_back["start"].to_list() == df_orig["start"].to_list()
        ), "start positions drifted after 1-based sink roundtrip"
        assert (
            df_back["end"].to_list() == df_orig["end"].to_list()
        ), "end positions drifted after 1-based sink roundtrip"
        assert (
            df_back["mate_start"].to_list() == df_orig["mate_start"].to_list()
        ), "mate_start positions drifted after 1-based sink roundtrip"

    def test_roundtrip_positions_sink_zero_based(self, tmp_path):
        """0-based scan -> sink -> read must preserve positions."""
        lf = pb.scan_bam(self.BAM_PATH, use_zero_based=True)
        out = str(tmp_path / "sink_0based.bam")
        pb.sink_bam(lf, out)

        df_orig = pb.read_bam(self.BAM_PATH, use_zero_based=True)
        df_back = pb.read_bam(out, use_zero_based=True)

        assert (
            df_back["start"].to_list() == df_orig["start"].to_list()
        ), "start positions drifted after 0-based sink roundtrip"
        assert (
            df_back["end"].to_list() == df_orig["end"].to_list()
        ), "end positions drifted after 0-based sink roundtrip"
        assert (
            df_back["mate_start"].to_list() == df_orig["mate_start"].to_list()
        ), "mate_start positions drifted after 0-based sink roundtrip"


class TestSAMWritePositionRoundtrip:
    """Regression tests for SAM write position off-by-one.

    SAM shares the same write path (execute_bam_streaming_write) as BAM,
    so the same coordinate metadata fix applies.
    """

    SAM_PATH = f"{DATA_DIR}/io/sam/test.sam"

    def test_roundtrip_positions_one_based(self, tmp_path):
        """1-based read -> write -> read must preserve start, end, and mate_start."""
        df_orig = pb.read_sam(self.SAM_PATH, use_zero_based=False)
        out = str(tmp_path / "rt_1based.sam")
        pb.write_sam(df_orig, out)
        df_back = pb.read_sam(out, use_zero_based=False)

        assert (
            df_back["start"].to_list() == df_orig["start"].to_list()
        ), "start positions drifted after 1-based SAM roundtrip"
        assert (
            df_back["end"].to_list() == df_orig["end"].to_list()
        ), "end positions drifted after 1-based SAM roundtrip"
        assert (
            df_back["mate_start"].to_list() == df_orig["mate_start"].to_list()
        ), "mate_start positions drifted after 1-based SAM roundtrip"

    def test_roundtrip_positions_zero_based(self, tmp_path):
        """0-based read -> write -> read must preserve start, end, and mate_start."""
        df_orig = pb.read_sam(self.SAM_PATH, use_zero_based=True)
        out = str(tmp_path / "rt_0based.sam")
        pb.write_sam(df_orig, out)
        df_back = pb.read_sam(out, use_zero_based=True)

        assert (
            df_back["start"].to_list() == df_orig["start"].to_list()
        ), "start positions drifted after 0-based SAM roundtrip"
        assert (
            df_back["end"].to_list() == df_orig["end"].to_list()
        ), "end positions drifted after 0-based SAM roundtrip"
        assert (
            df_back["mate_start"].to_list() == df_orig["mate_start"].to_list()
        ), "mate_start positions drifted after 0-based SAM roundtrip"

    def test_roundtrip_positions_sink_one_based(self, tmp_path):
        """1-based scan -> sink -> read must preserve positions."""
        lf = pb.scan_sam(self.SAM_PATH, use_zero_based=False)
        out = str(tmp_path / "sink_1based.sam")
        pb.sink_sam(lf, out)

        df_orig = pb.read_sam(self.SAM_PATH, use_zero_based=False)
        df_back = pb.read_sam(out, use_zero_based=False)

        assert (
            df_back["start"].to_list() == df_orig["start"].to_list()
        ), "start positions drifted after 1-based SAM sink roundtrip"
        assert (
            df_back["end"].to_list() == df_orig["end"].to_list()
        ), "end positions drifted after 1-based SAM sink roundtrip"
        assert (
            df_back["mate_start"].to_list() == df_orig["mate_start"].to_list()
        ), "mate_start positions drifted after 1-based SAM sink roundtrip"

    def test_roundtrip_positions_sink_zero_based(self, tmp_path):
        """0-based scan -> sink -> read must preserve positions."""
        lf = pb.scan_sam(self.SAM_PATH, use_zero_based=True)
        out = str(tmp_path / "sink_0based.sam")
        pb.sink_sam(lf, out)

        df_orig = pb.read_sam(self.SAM_PATH, use_zero_based=True)
        df_back = pb.read_sam(out, use_zero_based=True)

        assert (
            df_back["start"].to_list() == df_orig["start"].to_list()
        ), "start positions drifted after 0-based SAM sink roundtrip"
        assert (
            df_back["end"].to_list() == df_orig["end"].to_list()
        ), "end positions drifted after 0-based SAM sink roundtrip"
        assert (
            df_back["mate_start"].to_list() == df_orig["mate_start"].to_list()
        ), "mate_start positions drifted after 0-based SAM sink roundtrip"


class TestSAMWrite:
    """Tests for SAM write functionality."""

    def test_write_sam_roundtrip(self, tmp_path):
        """SAM -> SAM roundtrip: read, write, read back."""
        df = pb.read_sam(f"{DATA_DIR}/io/sam/test.sam")
        out_path = str(tmp_path / "roundtrip.sam")
        rows_written = pb.write_sam(df, out_path)
        assert rows_written == 2333

        df_back = pb.read_sam(out_path)
        assert len(df_back) == 2333
        assert df_back["name"][2] == df["name"][2]

    def test_sink_sam_roundtrip(self, tmp_path):
        """Streaming SAM write roundtrip: scan, sink, read back."""
        lf = pb.scan_sam(f"{DATA_DIR}/io/sam/test.sam")
        out_path = str(tmp_path / "sink.sam")
        pb.sink_sam(lf, out_path)

        df_back = pb.read_sam(out_path)
        assert len(df_back) == 2333

    def test_write_sam_with_tags(self, tmp_path):
        """SAM roundtrip with tag fields."""
        df = pb.read_sam(f"{DATA_DIR}/io/sam/test.sam", tag_fields=["NM", "AS"])
        out_path = str(tmp_path / "tags.sam")
        pb.write_sam(df, out_path)

        df_back = pb.read_sam(out_path, tag_fields=["NM", "AS"])
        assert "NM" in df_back.columns
        assert "AS" in df_back.columns
        assert len(df_back) == 2333

    def test_sink_sam_with_tags(self, tmp_path):
        """Streaming SAM write with tag fields."""
        lf = pb.scan_sam(f"{DATA_DIR}/io/sam/test.sam", tag_fields=["NM", "AS"])
        out_path = str(tmp_path / "sink_tags.sam")
        pb.sink_sam(lf, out_path)

        df_back = pb.read_sam(out_path, tag_fields=["NM", "AS"])
        assert "NM" in df_back.columns
        assert "AS" in df_back.columns
        assert len(df_back) == 2333

    def test_bam_to_sam_conversion(self, tmp_path):
        """Read BAM then write SAM, verify content."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        out_path = str(tmp_path / "converted.sam")
        pb.write_sam(df, out_path)

        df_sam = pb.read_sam(out_path)
        assert len(df_sam) == 2333
        assert df_sam["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert df_sam["flags"][3] == 1123


class TestHeaderPreservation:
    """Tests that BAM/SAM/CRAM round-trips preserve full header metadata."""

    def _get_sam_header_counts(self, path):
        """Get header section counts from a SAM file using pysam."""
        with pysam.AlignmentFile(path, "r") as f:
            header_dict = f.header.to_dict()
        return {
            "SQ": len(header_dict.get("SQ", [])),
            "RG": len(header_dict.get("RG", [])),
            "PG": len(header_dict.get("PG", [])),
            "HD": 1 if "HD" in header_dict else 0,
        }

    def _get_header_metadata(self, df):
        """Extract header metadata from a DataFrame."""
        from polars_bio._metadata import get_metadata

        meta = get_metadata(df)
        return meta.get("header", {})

    def test_bam_read_has_header_metadata(self):
        """BAM read should populate header metadata with @SQ, @RG, @PG info."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        header = self._get_header_metadata(df)
        assert "reference_sequences" in header
        assert "read_groups" in header
        assert "program_info" in header
        assert "file_format_version" in header
        assert "sort_order" in header

        ref_seqs = json.loads(header["reference_sequences"])
        assert len(ref_seqs) == 45
        assert ref_seqs[0]["name"] == "chrM"
        assert ref_seqs[0]["length"] == 16571

        read_groups = json.loads(header["read_groups"])
        assert len(read_groups) == 16
        assert read_groups[0]["sample"] == "NA12878"

    def test_sam_read_has_header_metadata(self):
        """SAM read should populate header metadata."""
        df = pb.read_sam(f"{DATA_DIR}/io/sam/test.sam")
        header = self._get_header_metadata(df)
        assert "reference_sequences" in header
        assert "read_groups" in header

        ref_seqs = json.loads(header["reference_sequences"])
        assert len(ref_seqs) == 45

    def test_bam_to_sam_header_roundtrip(self, tmp_path):
        """BAM -> SAM write should preserve full header."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        out_path = str(tmp_path / "header_test.sam")
        pb.write_sam(df, out_path)

        counts = self._get_sam_header_counts(out_path)
        assert counts["SQ"] == 45
        assert counts["RG"] == 16
        assert counts["PG"] > 0
        assert counts["HD"] == 1

    def test_sink_sam_header_roundtrip(self, tmp_path):
        """sink_sam (streaming write) should preserve full header."""
        lf = pb.scan_bam(f"{DATA_DIR}/io/bam/test.bam")
        out_path = str(tmp_path / "sink_header.sam")
        pb.sink_sam(lf, out_path)

        counts = self._get_sam_header_counts(out_path)
        assert counts["SQ"] == 45
        assert counts["RG"] == 16
        assert counts["PG"] > 0

    def test_sam_to_sam_header_roundtrip(self, tmp_path):
        """SAM -> SAM round-trip should preserve header."""
        df = pb.read_sam(f"{DATA_DIR}/io/sam/test.sam")
        out_path = str(tmp_path / "sam_roundtrip.sam")
        pb.write_sam(df, out_path)

        counts = self._get_sam_header_counts(out_path)
        assert counts["SQ"] == 45
        assert counts["RG"] == 16

        # Read back and verify metadata still present
        df_back = pb.read_sam(out_path)
        header = self._get_header_metadata(df_back)

        ref_seqs = json.loads(header["reference_sequences"])
        assert len(ref_seqs) == 45


class TestSortOnWrite:
    """Tests for sort_on_write parameter in BAM/SAM/CRAM write functions."""

    def _is_coordinate_sorted(self, df):
        """Check if a DataFrame is sorted by (chrom, start)."""
        chroms = df["chrom"].to_list()
        starts = df["start"].to_list()
        for i in range(1, len(chroms)):
            if chroms[i] < chroms[i - 1]:
                return False
            if chroms[i] == chroms[i - 1] and starts[i] < starts[i - 1]:
                return False
        return True

    def _get_sam_header_counts(self, path):
        """Get header section counts from a SAM file using pysam."""
        with pysam.AlignmentFile(path, "r") as f:
            header_dict = f.header.to_dict()
        return {
            "SQ": len(header_dict.get("SQ", [])),
            "RG": len(header_dict.get("RG", [])),
            "PG": len(header_dict.get("PG", [])),
        }

    def test_bam_sort_on_write(self, tmp_path):
        """Write BAM with sort_on_write=True, verify coordinate order."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        # Shuffle rows by reversing
        df_shuffled = df.reverse()

        out_path = str(tmp_path / "sorted.bam")
        pb.write_bam(df_shuffled, out_path, sort_on_write=True)

        df_back = pb.read_bam(out_path)
        assert len(df_back) == 2333
        assert self._is_coordinate_sorted(df_back)

    def test_bam_sort_on_write_false(self, tmp_path):
        """Write BAM with sort_on_write=False (default), verify unsorted header."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")

        out_path = str(tmp_path / "unsorted.bam")
        pb.write_bam(df, out_path, sort_on_write=False)

        df_back = pb.read_bam(out_path)
        from polars_bio._metadata import get_metadata

        meta = get_metadata(df_back)
        header = meta.get("header", {})
        assert header.get("sort_order") == "unsorted"

    def test_sam_sort_on_write(self, tmp_path):
        """Write SAM with sort_on_write=True, verify coordinate order and header."""
        df = pb.read_sam(f"{DATA_DIR}/io/sam/test.sam")
        df_shuffled = df.reverse()

        out_path = str(tmp_path / "sorted.sam")
        pb.write_sam(df_shuffled, out_path, sort_on_write=True)

        df_back = pb.read_sam(out_path)
        assert len(df_back) == 2333
        assert self._is_coordinate_sorted(df_back)

        # Verify header has SO:coordinate using pysam
        with pysam.AlignmentFile(out_path, "r") as f:
            header_dict = f.header.to_dict()
        assert header_dict["HD"]["SO"] == "coordinate"

    def test_sink_bam_sort_on_write(self, tmp_path):
        """Streaming BAM write with sort_on_write=True."""
        lf = pb.scan_bam(f"{DATA_DIR}/io/bam/test.bam")

        out_path = str(tmp_path / "sink_sorted.bam")
        pb.sink_bam(lf, out_path, sort_on_write=True)

        df_back = pb.read_bam(out_path)
        assert len(df_back) == 2333
        assert self._is_coordinate_sorted(df_back)

    def test_sink_sam_sort_on_write(self, tmp_path):
        """Streaming SAM write with sort_on_write=True."""
        lf = pb.scan_sam(f"{DATA_DIR}/io/sam/test.sam")

        out_path = str(tmp_path / "sink_sorted.sam")
        pb.sink_sam(lf, out_path, sort_on_write=True)

        df_back = pb.read_sam(out_path)
        assert len(df_back) == 2333
        assert self._is_coordinate_sorted(df_back)

        # Verify header has SO:coordinate using pysam
        with pysam.AlignmentFile(out_path, "r") as f:
            header_dict = f.header.to_dict()
        assert header_dict["HD"]["SO"] == "coordinate"

    def test_sort_preserves_header(self, tmp_path):
        """Sorted write preserves full header (@SQ, @RG, @PG)."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")

        out_path = str(tmp_path / "sorted_header.sam")
        pb.write_sam(df, out_path, sort_on_write=True)

        counts = self._get_sam_header_counts(out_path)
        assert counts["SQ"] == 45
        assert counts["RG"] == 16
        assert counts["PG"] > 0


class TestTemplateLength:
    """Tests for the new template_length (TLEN) column in BAM/SAM."""

    def test_template_length_in_bam_schema(self):
        """template_length column exists in BAM, dtype Int32, non-nullable."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        assert "template_length" in df.columns
        assert df["template_length"].dtype == pl.Int32
        assert df["template_length"].null_count() == 0

    def test_template_length_in_sam_schema(self):
        """template_length column exists in SAM, dtype Int32, non-nullable."""
        df = pb.read_sam(f"{DATA_DIR}/io/sam/test.sam")
        assert "template_length" in df.columns
        assert df["template_length"].dtype == pl.Int32
        assert df["template_length"].null_count() == 0

    def test_template_length_bam_write_roundtrip(self, tmp_path):
        """BAM -> BAM roundtrip preserves template_length values."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        out_path = str(tmp_path / "tlen.bam")
        pb.write_bam(df, out_path)

        df_back = pb.read_bam(out_path)
        assert df_back["template_length"].to_list() == df["template_length"].to_list()

    def test_template_length_sam_write_roundtrip(self, tmp_path):
        """SAM -> SAM roundtrip preserves template_length values."""
        df = pb.read_sam(f"{DATA_DIR}/io/sam/test.sam")
        out_path = str(tmp_path / "tlen.sam")
        pb.write_sam(df, out_path)

        df_back = pb.read_sam(out_path)
        assert df_back["template_length"].to_list() == df["template_length"].to_list()

    def test_template_length_sink_bam_roundtrip(self, tmp_path):
        """Streaming BAM write preserves template_length values."""
        lf = pb.scan_bam(f"{DATA_DIR}/io/bam/test.bam")
        df_orig = lf.collect()
        out_path = str(tmp_path / "sink_tlen.bam")
        pb.sink_bam(pb.scan_bam(f"{DATA_DIR}/io/bam/test.bam"), out_path)

        df_back = pb.read_bam(out_path)
        assert (
            df_back["template_length"].to_list() == df_orig["template_length"].to_list()
        )

    def test_template_length_in_describe_bam(self):
        """describe_bam output includes template_length with Int32 dtype."""
        schema = pb.describe_bam(f"{DATA_DIR}/io/bam/test.bam", sample_size=0)
        columns = schema["column_name"].to_list()
        assert "template_length" in columns

        tlen_row = schema.filter(schema["column_name"] == "template_length")
        assert len(tlen_row) == 1
        assert tlen_row["data_type"][0] == "Int32"

    def test_template_length_in_describe_sam(self):
        """describe_sam output includes template_length with Int32 dtype."""
        schema = pb.describe_sam(f"{DATA_DIR}/io/sam/test.sam", sample_size=0)
        columns = schema["column_name"].to_list()
        assert "template_length" in columns

        tlen_row = schema.filter(schema["column_name"] == "template_length")
        assert len(tlen_row) == 1
        assert tlen_row["data_type"][0] == "Int32"

    def test_template_length_scan_projection(self):
        """Projection pushdown works for template_length column."""
        lf = pb.scan_bam(f"{DATA_DIR}/io/bam/test.bam")
        df = lf.select(["name", "template_length"]).collect()
        assert df.columns == ["name", "template_length"]
        assert len(df) == 2333
        assert df["template_length"].dtype == pl.Int32

    def test_template_length_sql_query(self):
        """SQL SELECT template_length works."""
        pb.register_bam(f"{DATA_DIR}/io/bam/test.bam", "test_tlen")
        result = pb.sql("SELECT template_length FROM test_tlen LIMIT 5").collect()
        assert "template_length" in result.columns
        assert len(result) == 5
        assert result["template_length"].dtype == pl.Int32


class TestMapQ255:
    """Tests for MAPQ 255 handling -- mapping_quality is now non-nullable UInt32."""

    # Small BAM with 3 reads: MAPQ=255, MAPQ=0, MAPQ=60
    MAPQ_BAM = f"{DATA_DIR}/io/bam/mapq255.bam"

    def test_mapq_255_read_as_value(self):
        """MAPQ=255 is read as integer 255, not null."""
        df = pb.read_bam(self.MAPQ_BAM)
        row = df.filter(pl.col("name") == "read_mapq255")
        assert len(row) == 1
        assert row["mapping_quality"][0] == 255
        assert row["mapping_quality"].dtype == pl.UInt32

    def test_mapq_255_all_values(self):
        """All MAPQ values (0, 60, 255) are read correctly from a known BAM."""
        df = pb.read_bam(self.MAPQ_BAM)
        assert len(df) == 3
        assert df["mapping_quality"].null_count() == 0
        mapq_by_name = dict(zip(df["name"].to_list(), df["mapping_quality"].to_list()))
        assert mapq_by_name["read_mapq255"] == 255
        assert mapq_by_name["read_mapq0"] == 0
        assert mapq_by_name["read_mapq60"] == 60

    def test_mapq_255_roundtrip(self, tmp_path):
        """BAM roundtrip preserves MAPQ=255 as 255 (not null or 0)."""
        df = pb.read_bam(self.MAPQ_BAM)
        out_path = str(tmp_path / "mapq255_rt.bam")
        pb.write_bam(df, out_path)

        df_back = pb.read_bam(out_path)
        row = df_back.filter(pl.col("name") == "read_mapq255")
        assert row["mapping_quality"][0] == 255

    def test_mapq_255_template_length(self):
        """template_length values (positive, negative, zero) read correctly."""
        df = pb.read_bam(self.MAPQ_BAM)
        tlen_by_name = dict(zip(df["name"].to_list(), df["template_length"].to_list()))
        assert tlen_by_name["read_mapq255"] == 0
        assert tlen_by_name["read_mapq0"] == 150
        assert tlen_by_name["read_mapq60"] == -150

    def test_mapq_not_null_bam(self):
        """mapping_quality in BAM has null_count==0 and dtype UInt32."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        assert df["mapping_quality"].null_count() == 0
        assert df["mapping_quality"].dtype == pl.UInt32

    def test_mapq_not_null_sam(self):
        """mapping_quality in SAM has null_count==0 and dtype UInt32."""
        df = pb.read_sam(f"{DATA_DIR}/io/sam/test.sam")
        assert df["mapping_quality"].null_count() == 0
        assert df["mapping_quality"].dtype == pl.UInt32

    def test_mapq_bam_write_roundtrip(self, tmp_path):
        """BAM -> BAM roundtrip preserves mapping_quality values (including 255)."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        out_path = str(tmp_path / "mapq.bam")
        pb.write_bam(df, out_path)

        df_back = pb.read_bam(out_path)
        assert df_back["mapping_quality"].to_list() == df["mapping_quality"].to_list()

    def test_mapq_sam_write_roundtrip(self, tmp_path):
        """SAM -> SAM roundtrip preserves mapping_quality values."""
        df = pb.read_sam(f"{DATA_DIR}/io/sam/test.sam")
        out_path = str(tmp_path / "mapq.sam")
        pb.write_sam(df, out_path)

        df_back = pb.read_sam(out_path)
        assert df_back["mapping_quality"].to_list() == df["mapping_quality"].to_list()

    def test_mapq_bam_to_sam_roundtrip(self, tmp_path):
        """BAM -> SAM cross-format preserves mapping_quality values."""
        df_bam = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        out_path = str(tmp_path / "cross.sam")
        pb.write_sam(df_bam, out_path)

        df_sam = pb.read_sam(out_path)
        assert (
            df_sam["mapping_quality"].to_list() == df_bam["mapping_quality"].to_list()
        )

    def test_mapq_filter_255(self):
        """Filtering by mapping_quality == 255 works and matches client-side count."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        client_count = len(df.filter(pl.col("mapping_quality") == 255))

        lf = pb.scan_bam(f"{DATA_DIR}/io/bam/test.bam")
        pushdown_count = len(lf.filter(pl.col("mapping_quality") == 255).collect())
        assert pushdown_count == client_count

    def test_mapq_in_filter(self):
        """IN filter on numeric mapping_quality pushes down correctly."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        client_count = len(df.filter(pl.col("mapping_quality").is_in([0, 29, 255])))

        lf = pb.scan_bam(f"{DATA_DIR}/io/bam/test.bam")
        pushdown_count = len(
            lf.filter(pl.col("mapping_quality").is_in([0, 29, 255])).collect()
        )
        assert pushdown_count == client_count
        assert pushdown_count > 0

    def test_template_length_in_filter(self):
        """IN filter on numeric template_length pushes down correctly."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        # Pick a few actual values from the data
        sample_values = df["template_length"].unique().head(3).to_list()
        client_count = len(df.filter(pl.col("template_length").is_in(sample_values)))

        lf = pb.scan_bam(f"{DATA_DIR}/io/bam/test.bam")
        pushdown_count = len(
            lf.filter(pl.col("template_length").is_in(sample_values)).collect()
        )
        assert pushdown_count == client_count


class TestQNameStar:
    """Tests for QNAME '*' handling -- name column is now non-nullable."""

    def test_qname_not_null_bam(self):
        """name column in BAM has null_count==0."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        assert df["name"].null_count() == 0

    def test_qname_not_null_sam(self):
        """name column in SAM has null_count==0."""
        df = pb.read_sam(f"{DATA_DIR}/io/sam/test.sam")
        assert df["name"].null_count() == 0

    def test_qname_bam_roundtrip(self, tmp_path):
        """BAM -> BAM roundtrip preserves name values."""
        df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")
        out_path = str(tmp_path / "qname.bam")
        pb.write_bam(df, out_path)

        df_back = pb.read_bam(out_path)
        assert df_back["name"].to_list() == df["name"].to_list()

    def test_qname_sam_roundtrip(self, tmp_path):
        """SAM -> SAM roundtrip preserves name values."""
        df = pb.read_sam(f"{DATA_DIR}/io/sam/test.sam")
        out_path = str(tmp_path / "qname.sam")
        pb.write_sam(df, out_path)

        df_back = pb.read_sam(out_path)
        assert df_back["name"].to_list() == df["name"].to_list()
