import json

import polars as pl
import pysam
from _expected import DATA_DIR

import polars_bio as pb
from polars_bio._metadata import get_metadata


class TestIOCRAM:
    # Test with embedded reference (default)
    df = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")

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
        pb.register_cram(f"{DATA_DIR}/io/cram/test.cram", "test_cram")
        count = pb.sql("select count(*) as cnt from test_cram").collect()
        assert count["cnt"][0] == 2333

        projection = pb.sql("select name, flags from test_cram").collect()
        assert projection["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert projection["flags"][3] == 1123

    def test_scan_cram(self):
        """Test lazy scanning with scan_cram"""
        lf = pb.scan_cram(f"{DATA_DIR}/io/cram/test.cram")
        df = lf.select(["name", "chrom", "start"]).collect()
        assert len(df) == 2333
        assert "name" in df.columns
        assert "chrom" in df.columns
        assert "start" in df.columns

    def test_external_reference(self):
        """Test CRAM reading with external FASTA reference"""
        # Test with external reference from chr20 subset
        df = (
            pb.scan_cram(
                f"{DATA_DIR}/io/cram/external_ref/test_chr20.cram",
                reference_path=f"{DATA_DIR}/io/cram/external_ref/chr20.fa",
            )
            .limit(10)
            .collect()
        )

        assert len(df) == 10
        assert "name" in df.columns
        assert "chrom" in df.columns
        assert "sequence" in df.columns

        # Verify reads are from chr20 (contig name preserved as-is from header)
        assert all(df["chrom"] == "20")

        # Verify first read details (1-based coordinates by default)
        assert df["name"][0] == "SRR622461.74266137"
        assert df["start"][0] == 59993  # 1-based (default)
        assert df["mapping_quality"][0] == 29

    def test_cram_no_tags_default(self):
        """Test backward compatibility - no tags by default"""
        df = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")
        assert len(df.columns) == 12  # Original columns only
        assert "NM" not in df.columns
        assert "AS" not in df.columns

    def test_cram_single_tag(self):
        """Test that CRAM tag_fields parameter includes the tag"""
        df = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram", tag_fields=["NM"])
        assert "NM" in df.columns
        assert len(df.columns) == 13  # 12 + 1 tag

    def test_cram_multiple_tags(self):
        """Test that multiple CRAM tags are included"""
        df = pb.read_cram(
            f"{DATA_DIR}/io/cram/test.cram", tag_fields=["NM", "AS", "MD"]
        )
        assert "NM" in df.columns
        assert "AS" in df.columns
        assert "MD" in df.columns
        assert len(df.columns) == 15  # 12 + 3 tags

    def test_cram_scan_with_tags(self):
        """Test that lazy scan with CRAM tags includes the tags"""
        lf = pb.scan_cram(f"{DATA_DIR}/io/cram/test.cram", tag_fields=["NM", "AS"])
        df = lf.select(["name", "chrom", "NM", "AS"]).collect()
        assert "NM" in df.columns
        assert "AS" in df.columns
        assert len(df.columns) == 4

    def test_cram_sql_with_tags(self):
        """Test that SQL registration with CRAM tags includes the tags"""
        pb.register_cram(
            f"{DATA_DIR}/io/cram/test.cram",
            "test_cram_tags_sql",
            tag_fields=["NM", "AS"],
        )
        result = pb.sql(
            'SELECT name, "NM", "AS" FROM test_cram_tags_sql LIMIT 5'
        ).collect()
        assert "NM" in result.columns
        assert "AS" in result.columns

    def test_describe_cram_no_tags(self):
        """Test describe_cram without tags (sample_size=0 for core columns only)"""
        schema = pb.describe_cram(f"{DATA_DIR}/io/cram/test.cram", sample_size=0)
        assert "column_name" in schema.columns
        assert "data_type" in schema.columns
        assert "category" in schema.columns
        assert len(schema) == 12  # Core columns only
        assert all(schema["category"] == "core")

    def test_describe_cram_with_tags(self):
        """Test describe_cram with automatic tag discovery"""
        schema = pb.describe_cram(f"{DATA_DIR}/io/cram/test.cram", sample_size=100)
        assert "column_name" in schema.columns
        assert "category" in schema.columns
        assert "sam_type" in schema.columns
        assert len(schema) > 12  # Core + tags

        tags = schema.filter(schema["category"] == "tag")
        tag_names = tags["column_name"].to_list()
        # Check for tags that are actually present in test.cram
        assert "RG" in tag_names  # Read group
        assert "MQ" in tag_names  # Mapping quality
        assert len(tag_names) > 0  # At least some tags discovered


class TestCRAMWrite:
    """Tests for CRAM write functionality."""

    # Reference file for CRAM write tests
    REFERENCE = f"{DATA_DIR}/io/cram/external_ref/chr20.fa"
    CRAM_WITH_REF = f"{DATA_DIR}/io/cram/external_ref/test_chr20.cram"

    def test_cram_roundtrip(self, tmp_path):
        """CRAM -> CRAM roundtrip: read, write, read back."""
        df = pb.scan_cram(self.CRAM_WITH_REF, reference_path=self.REFERENCE).collect()
        out_path = str(tmp_path / "roundtrip.cram")

        rows = pb.write_cram(df, out_path, reference_path=self.REFERENCE)
        assert rows == len(df)

        # Read back with polars-bio
        df_back = pb.read_cram(out_path, reference_path=self.REFERENCE)
        assert len(df_back) == len(df)
        assert df_back["name"][0] == df["name"][0]
        assert df_back["chrom"][0] == df["chrom"][0]
        assert df_back["start"][0] == df["start"][0]

    def test_sink_cram_roundtrip(self, tmp_path):
        """Streaming CRAM write roundtrip: scan, sink, read back."""
        lf = pb.scan_cram(self.CRAM_WITH_REF, reference_path=self.REFERENCE)
        out_path = str(tmp_path / "sink_roundtrip.cram")

        pb.sink_cram(lf, out_path, reference_path=self.REFERENCE)

        # Read back with polars-bio
        df_back = pb.read_cram(out_path, reference_path=self.REFERENCE)
        assert len(df_back) == 100

    def test_write_cram_pysam_readable(self, tmp_path):
        """Write CRAM produces a file readable by pysam."""
        df = pb.scan_cram(self.CRAM_WITH_REF, reference_path=self.REFERENCE).collect()
        out_path = str(tmp_path / "output.cram")

        rows = pb.write_cram(df, out_path, reference_path=self.REFERENCE)
        assert rows == len(df)

        # Verify file is valid using pysam
        with pysam.AlignmentFile(
            out_path, "rc", reference_filename=self.REFERENCE
        ) as f:
            count = sum(1 for _ in f)
        assert count == len(df)

    def test_cram_sort_on_write_roundtrip(self, tmp_path):
        """Write CRAM with sort_on_write, verify sorted on read back."""
        df = pb.scan_cram(self.CRAM_WITH_REF, reference_path=self.REFERENCE).collect()
        df_shuffled = df.reverse()
        out_path = str(tmp_path / "sorted.cram")

        pb.write_cram(
            df_shuffled, out_path, reference_path=self.REFERENCE, sort_on_write=True
        )

        # Read back and verify sorted
        df_back = pb.read_cram(out_path, reference_path=self.REFERENCE)
        assert len(df_back) == len(df)

        # Check coordinate sorted
        chroms = df_back["chrom"].to_list()
        starts = df_back["start"].to_list()
        for i in range(1, len(chroms)):
            if chroms[i] == chroms[i - 1]:
                assert starts[i] >= starts[i - 1], "Not coordinate sorted"

        # Verify header has SO:coordinate
        header = get_metadata(df_back).get("header", {})
        assert header.get("sort_order") == "coordinate"

    def test_cram_sort_on_write_pysam_header(self, tmp_path):
        """Write CRAM with sort_on_write, verify pysam sees SO:coordinate."""
        df = pb.scan_cram(self.CRAM_WITH_REF, reference_path=self.REFERENCE).collect()
        out_path = str(tmp_path / "sorted_pysam.cram")

        pb.write_cram(df, out_path, reference_path=self.REFERENCE, sort_on_write=True)

        # Verify header has SO:coordinate using pysam
        with pysam.AlignmentFile(
            out_path, "rc", reference_filename=self.REFERENCE
        ) as f:
            header_dict = f.header.to_dict()
        assert header_dict["HD"]["SO"] == "coordinate"

    def test_write_cram_with_tags(self, tmp_path):
        """CRAM roundtrip with tag fields."""
        df = pb.scan_cram(
            self.CRAM_WITH_REF, reference_path=self.REFERENCE, tag_fields=["RG", "MQ"]
        ).collect()
        assert "RG" in df.columns
        out_path = str(tmp_path / "tags.cram")

        pb.write_cram(df, out_path, reference_path=self.REFERENCE)

        df_back = pb.read_cram(
            out_path, reference_path=self.REFERENCE, tag_fields=["RG", "MQ"]
        )
        assert "RG" in df_back.columns
        assert "MQ" in df_back.columns
        assert len(df_back) == len(df)

    def test_sink_cram_with_tags(self, tmp_path):
        """Streaming CRAM write with tag fields."""
        lf = pb.scan_cram(
            self.CRAM_WITH_REF, reference_path=self.REFERENCE, tag_fields=["RG", "MQ"]
        )
        out_path = str(tmp_path / "sink_tags.cram")

        pb.sink_cram(lf, out_path, reference_path=self.REFERENCE)

        df_back = pb.read_cram(
            out_path, reference_path=self.REFERENCE, tag_fields=["RG", "MQ"]
        )
        assert "RG" in df_back.columns
        assert "MQ" in df_back.columns
        assert len(df_back) == 100

    def test_cram_to_sam_roundtrip(self, tmp_path):
        """Read CRAM, write as SAM, read back and compare."""
        df_cram = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")
        out_path = str(tmp_path / "from_cram.sam")
        rows_written = pb.write_sam(df_cram, out_path)
        assert rows_written == 2333

        df_sam = pb.read_sam(out_path)
        assert len(df_sam) == 2333
        assert df_sam.columns == df_cram.columns

    def test_cram_to_bam_roundtrip(self, tmp_path):
        """Read CRAM, write as BAM, read back and compare."""
        df_cram = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")
        out_path = str(tmp_path / "from_cram.bam")
        rows_written = pb.write_bam(df_cram, out_path)
        assert rows_written == 2333

        df_bam = pb.read_bam(out_path)
        assert len(df_bam) == 2333
        assert df_bam["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert df_bam["flags"][3] == 1123

    def test_cram_to_sam_with_tags(self, tmp_path):
        """CRAM with tags -> SAM roundtrip preserves tags."""
        df_cram = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram", tag_fields=["RG", "MQ"])
        assert "RG" in df_cram.columns
        assert "MQ" in df_cram.columns

        out_path = str(tmp_path / "tags.sam")
        pb.write_sam(df_cram, out_path)

        df_sam = pb.read_sam(out_path, tag_fields=["RG", "MQ"])
        assert "RG" in df_sam.columns
        assert "MQ" in df_sam.columns
        assert len(df_sam) == 2333

    def test_cram_to_bam_with_tags(self, tmp_path):
        """CRAM with tags -> BAM roundtrip preserves tags."""
        df_cram = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram", tag_fields=["RG", "MQ"])
        out_path = str(tmp_path / "tags.bam")
        pb.write_bam(df_cram, out_path)

        df_bam = pb.read_bam(out_path, tag_fields=["RG", "MQ"])
        assert "RG" in df_bam.columns
        assert "MQ" in df_bam.columns
        assert len(df_bam) == 2333
        assert df_bam["RG"][0] == df_cram["RG"][0]

    def test_sink_sam_from_cram(self, tmp_path):
        """Streaming write: scan CRAM, sink as SAM."""
        lf = pb.scan_cram(f"{DATA_DIR}/io/cram/test.cram")
        out_path = str(tmp_path / "sink.sam")
        pb.sink_sam(lf, out_path)

        df_sam = pb.read_sam(out_path)
        assert len(df_sam) == 2333

    def test_sink_bam_from_cram(self, tmp_path):
        """Streaming write: scan CRAM, sink as BAM."""
        lf = pb.scan_cram(f"{DATA_DIR}/io/cram/test.cram")
        out_path = str(tmp_path / "sink.bam")
        pb.sink_bam(lf, out_path)

        df_bam = pb.read_bam(out_path)
        assert len(df_bam) == 2333


class TestCRAMHeaderPreservation:
    """Tests that CRAM header metadata is read and preserved in conversions."""

    def _get_header_metadata(self, df):
        meta = get_metadata(df)
        return meta.get("header", {})

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

    def test_cram_read_has_header_metadata(self):
        """CRAM read should populate header metadata with @SQ, @RG, @PG info."""
        df = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")
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

    def test_cram_to_sam_preserves_header(self, tmp_path):
        """CRAM -> SAM write should preserve full header."""
        df = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")
        out_path = str(tmp_path / "header_test.sam")
        pb.write_sam(df, out_path)

        counts = self._get_sam_header_counts(out_path)
        assert counts["SQ"] == 45
        assert counts["RG"] == 16
        assert counts["PG"] > 0
        assert counts["HD"] == 1

    def test_cram_to_bam_preserves_header(self, tmp_path):
        """CRAM -> BAM write should preserve header metadata."""
        df = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")
        out_path = str(tmp_path / "header_test.bam")
        pb.write_bam(df, out_path)

        df_back = pb.read_bam(out_path)
        header = self._get_header_metadata(df_back)

        ref_seqs = json.loads(header["reference_sequences"])
        assert len(ref_seqs) == 45

        read_groups = json.loads(header["read_groups"])
        assert len(read_groups) == 16

    def test_sink_sam_from_cram_preserves_header(self, tmp_path):
        """sink_sam from CRAM source should preserve full header."""
        lf = pb.scan_cram(f"{DATA_DIR}/io/cram/test.cram")
        out_path = str(tmp_path / "sink_header.sam")
        pb.sink_sam(lf, out_path)

        counts = self._get_sam_header_counts(out_path)
        assert counts["SQ"] == 45
        assert counts["RG"] == 16
        assert counts["PG"] > 0


class TestCRAMSortOnWrite:
    """Tests for sort_on_write with CRAM source data.

    Since CRAM -> CRAM write roundtrip is not yet supported,
    these tests verify sorting through SAM and BAM output formats.
    """

    def _is_coordinate_sorted(self, df):
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

    def test_cram_to_bam_sort_on_write(self, tmp_path):
        """Read CRAM, shuffle, write BAM with sort_on_write=True."""
        df = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")
        df_shuffled = df.reverse()

        out_path = str(tmp_path / "sorted.bam")
        pb.write_bam(df_shuffled, out_path, sort_on_write=True)

        df_back = pb.read_bam(out_path)
        assert len(df_back) == 2333
        assert self._is_coordinate_sorted(df_back)

        header = get_metadata(df_back).get("header", {})
        assert header.get("sort_order") == "coordinate"

    def test_cram_to_sam_sort_on_write(self, tmp_path):
        """Read CRAM, shuffle, write SAM with sort_on_write=True."""
        df = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")
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

    def test_sink_bam_from_cram_sort_on_write(self, tmp_path):
        """Streaming: scan CRAM, sink BAM with sort_on_write=True."""
        lf = pb.scan_cram(f"{DATA_DIR}/io/cram/test.cram")

        out_path = str(tmp_path / "sink_sorted.bam")
        pb.sink_bam(lf, out_path, sort_on_write=True)

        df_back = pb.read_bam(out_path)
        assert len(df_back) == 2333
        assert self._is_coordinate_sorted(df_back)

    def test_sort_preserves_cram_header(self, tmp_path):
        """Sorted write from CRAM preserves full header (@SQ, @RG, @PG)."""
        df = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")

        out_path = str(tmp_path / "sorted_header.sam")
        pb.write_sam(df, out_path, sort_on_write=True)

        counts = self._get_sam_header_counts(out_path)
        assert counts["SQ"] == 45
        assert counts["RG"] == 16
        assert counts["PG"] > 0


class TestCRAMPR66Changes:
    """Tests for PR #66 changes: template_length, MAPQ 255, QNAME '*' in CRAM."""

    REFERENCE = f"{DATA_DIR}/io/cram/external_ref/chr20.fa"
    CRAM_WITH_REF = f"{DATA_DIR}/io/cram/external_ref/test_chr20.cram"

    def test_template_length_in_cram(self):
        """template_length column exists in CRAM, dtype Int32, non-nullable."""
        df = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")
        assert "template_length" in df.columns
        assert df["template_length"].dtype == pl.Int32
        assert df["template_length"].null_count() == 0

    def test_template_length_cram_write_roundtrip(self, tmp_path):
        """CRAM -> CRAM roundtrip preserves template_length values."""
        df = pb.scan_cram(self.CRAM_WITH_REF, reference_path=self.REFERENCE).collect()
        out_path = str(tmp_path / "tlen.cram")
        pb.write_cram(df, out_path, reference_path=self.REFERENCE)

        df_back = pb.read_cram(out_path, reference_path=self.REFERENCE)
        assert "template_length" in df_back.columns
        assert df_back["template_length"].dtype == pl.Int32
        assert df_back["template_length"].null_count() == 0
        assert len(df_back) == len(df)

    def test_template_length_in_describe_cram(self):
        """describe_cram output includes template_length with Int32 dtype."""
        schema = pb.describe_cram(f"{DATA_DIR}/io/cram/test.cram", sample_size=0)
        columns = schema["column_name"].to_list()
        assert "template_length" in columns

        tlen_row = schema.filter(schema["column_name"] == "template_length")
        assert len(tlen_row) == 1
        assert tlen_row["data_type"][0] == "Int32"

    def test_mapq_not_null_cram(self):
        """mapping_quality in CRAM has null_count==0 and dtype UInt32."""
        df = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")
        assert df["mapping_quality"].null_count() == 0
        assert df["mapping_quality"].dtype == pl.UInt32

    def test_mapq_cram_write_roundtrip(self, tmp_path):
        """CRAM -> CRAM roundtrip preserves mapping_quality values."""
        df = pb.scan_cram(self.CRAM_WITH_REF, reference_path=self.REFERENCE).collect()
        out_path = str(tmp_path / "mapq.cram")
        pb.write_cram(df, out_path, reference_path=self.REFERENCE)

        df_back = pb.read_cram(out_path, reference_path=self.REFERENCE)
        assert df_back["mapping_quality"].to_list() == df["mapping_quality"].to_list()

    def test_qname_not_null_cram(self):
        """name column in CRAM has null_count==0."""
        df = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")
        assert df["name"].null_count() == 0

    def test_qname_cram_write_roundtrip(self, tmp_path):
        """CRAM -> CRAM roundtrip preserves name values."""
        df = pb.scan_cram(self.CRAM_WITH_REF, reference_path=self.REFERENCE).collect()
        out_path = str(tmp_path / "qname.cram")
        pb.write_cram(df, out_path, reference_path=self.REFERENCE)

        df_back = pb.read_cram(out_path, reference_path=self.REFERENCE)
        assert df_back["name"].to_list() == df["name"].to_list()

    def test_cram_to_bam_template_length(self, tmp_path):
        """CRAM -> BAM preserves template_length values."""
        df_cram = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")
        out_path = str(tmp_path / "from_cram.bam")
        pb.write_bam(df_cram, out_path)

        df_bam = pb.read_bam(out_path)
        assert (
            df_bam["template_length"].to_list() == df_cram["template_length"].to_list()
        )

    def test_cram_to_sam_template_length(self, tmp_path):
        """CRAM -> SAM preserves template_length values."""
        df_cram = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")
        out_path = str(tmp_path / "from_cram.sam")
        pb.write_sam(df_cram, out_path)

        df_sam = pb.read_sam(out_path)
        assert (
            df_sam["template_length"].to_list() == df_cram["template_length"].to_list()
        )
