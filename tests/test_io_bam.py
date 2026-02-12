import json

import polars as pl
import pysam
import pytest
from _expected import DATA_DIR

import polars_bio as pb


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
