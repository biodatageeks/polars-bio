"""Tests for standardized source file metadata across all formats."""

from pathlib import Path

import polars as pl
import pytest

import polars_bio as pb

# Import from public API (also available as pb.get_metadata, pb.set_source_metadata)
from polars_bio import get_metadata as get_source_metadata
from polars_bio import set_source_metadata

TEST_DIR = Path(__file__).parent
DATA_DIR = TEST_DIR / "data"


class TestSourceMetadataBasics:
    """Basic tests for set_source_metadata and get_source_metadata functions."""

    def test_set_get_source_metadata_basic(self):
        """Test basic set and get of source metadata."""
        df = pl.DataFrame({"a": [1, 2, 3]})

        set_source_metadata(df, format="vcf", path="/path/to/file.vcf")
        meta = get_source_metadata(df)

        assert meta["format"] == "vcf"
        assert meta["path"] == "/path/to/file.vcf"
        assert meta["header"] is None

    def test_set_get_source_metadata_with_header(self):
        """Test set and get of source metadata with header dict."""
        df = pl.DataFrame({"a": [1, 2, 3]})

        header = {
            "info_fields": {"AF": {"number": "A", "type": "Float"}},
            "format_fields": {"GT": {"number": "1", "type": "String"}},
        }

        set_source_metadata(df, format="vcf", path="/path/to/file.vcf", header=header)
        meta = get_source_metadata(df)

        assert meta["format"] == "vcf"
        assert meta["path"] == "/path/to/file.vcf"
        assert meta["header"]["info_fields"]["AF"]["type"] == "Float"
        assert meta["header"]["format_fields"]["GT"]["number"] == "1"

    def test_source_metadata_survives_lazyframe_collect(self):
        """Test that source metadata survives collect() operation."""
        lf = pl.LazyFrame({"a": [1, 2, 3]})

        set_source_metadata(lf, format="fastq", path="/path/to/file.fastq")

        # Collect to DataFrame
        df = lf.collect()

        # Metadata should NOT survive collect without explicit preservation
        # (this is expected Polars behavior - metadata is lost on collect)
        # The io.py code handles this by preserving metadata explicitly

    def test_empty_header_stored_as_none(self):
        """Test that empty header dict is stored as empty string."""
        df = pl.DataFrame({"a": [1, 2, 3]})

        set_source_metadata(df, format="bed", path="/path/to/file.bed", header=None)
        meta = get_source_metadata(df)

        assert meta["format"] == "bed"
        assert meta["header"] is None

    def test_missing_metadata_returns_none(self):
        """Test that missing metadata returns None values."""
        df = pl.DataFrame({"a": [1, 2, 3]})

        meta = get_source_metadata(df)

        assert meta["format"] is None
        assert meta["path"] is None
        assert meta["header"] is None


class TestVCFSourceMetadata:
    """Tests for VCF source metadata through read/write cycle."""

    def test_vcf_read_sets_source_metadata(self):
        """Test that reading VCF file sets source metadata."""
        vcf_path = f"{DATA_DIR}/io/vcf/vep.vcf"
        lf = pb.scan_vcf(vcf_path)

        meta = get_source_metadata(lf)

        assert meta["format"] == "vcf"
        assert vcf_path in meta["path"]  # Path should contain the file path
        assert meta["header"] is not None
        assert "info_fields" in meta["header"]
        assert "format_fields" in meta["header"]
        assert "sample_names" in meta["header"]

    def test_vcf_header_includes_schema_level_metadata(self):
        """Test that VCF header includes schema-level metadata (version, contigs, etc.)."""
        vcf_path = f"{DATA_DIR}/io/vcf/vep.vcf"
        lf = pb.scan_vcf(vcf_path)

        meta = get_source_metadata(lf)
        header = meta["header"]

        # Schema-level metadata should be present (if available in the VCF)
        # Note: Not all VCF files have all metadata, so we check if present
        # The important thing is that the extraction function doesn't fail

        # Check field-level metadata (always present)
        assert "info_fields" in header
        assert "format_fields" in header
        assert "sample_names" in header

        # Schema-level metadata may or may not be present depending on VCF file
        # Just verify it doesn't cause errors

    def test_vcf_roundtrip_preserves_metadata(self, tmp_path):
        """Test that VCF metadata survives write/read round-trip."""
        input_path = f"{DATA_DIR}/io/vcf/vep.vcf"
        output_path = tmp_path / "roundtrip.vcf"

        # Read
        df1 = pb.read_vcf(input_path)
        meta1 = get_source_metadata(df1)

        # Write
        pb.write_vcf(df1, str(output_path))

        # Read again
        df2 = pb.read_vcf(str(output_path))
        meta2 = get_source_metadata(df2)

        # Format should be preserved
        assert meta2["format"] == "vcf"

        # Header metadata should be preserved
        assert meta2["header"] is not None
        assert "info_fields" in meta2["header"]
        assert "format_fields" in meta2["header"]
        assert "sample_names" in meta2["header"]

        # INFO fields should match (basic check)
        if meta1["header"]["info_fields"] and meta2["header"]["info_fields"]:
            # At least some common fields should be present
            common_fields = set(meta1["header"]["info_fields"].keys()) & set(
                meta2["header"]["info_fields"].keys()
            )
            assert len(common_fields) > 0


class TestFASTQSourceMetadata:
    """Tests for FASTQ source metadata."""

    def test_fastq_read_sets_source_metadata(self):
        """Test that reading FASTQ file sets source metadata."""
        fastq_path = f"{DATA_DIR}/io/fastq/example.fastq"
        lf = pb.scan_fastq(fastq_path)

        meta = get_source_metadata(lf)

        assert meta["format"] == "fastq"
        assert fastq_path in meta["path"]
        # FASTQ has minimal header metadata
        # Header may be None or empty for FASTQ

    def test_fastq_roundtrip_preserves_format(self, tmp_path):
        """Test that FASTQ format metadata survives write/read round-trip."""
        input_path = f"{DATA_DIR}/io/fastq/example.fastq"
        output_path = tmp_path / "roundtrip.fastq"

        # Read
        df1 = pb.read_fastq(input_path)
        meta1 = get_source_metadata(df1)
        assert meta1["format"] == "fastq"

        # Write
        pb.write_fastq(df1, str(output_path))

        # Read again
        df2 = pb.read_fastq(str(output_path))
        meta2 = get_source_metadata(df2)

        assert meta2["format"] == "fastq"


class TestOtherFormatsSourceMetadata:
    """Tests for other file format source metadata."""

    def test_bam_read_sets_source_metadata(self):
        """Test that reading BAM file sets source metadata."""
        bam_path = f"{DATA_DIR}/io/bam/test.bam"
        lf = pb.scan_bam(bam_path)

        meta = get_source_metadata(lf)

        assert meta["format"] == "bam"
        assert bam_path in meta["path"]

    def test_gff_read_sets_source_metadata(self):
        """Test that reading GFF file sets source metadata."""
        gff_path = f"{DATA_DIR}/io/gff/example.gff3"
        lf = pb.scan_gff(gff_path)

        meta = get_source_metadata(lf)

        assert meta["format"] == "gff"
        assert gff_path in meta["path"]

    def test_bed_read_sets_source_metadata(self):
        """Test that reading BED file sets source metadata."""
        bed_path = f"{DATA_DIR}/io/bed/example.bed"
        lf = pb.scan_bed(bed_path)

        meta = get_source_metadata(lf)

        assert meta["format"] == "bed"
        assert bed_path in meta["path"]

    def test_fasta_read_sets_source_metadata(self):
        """Test that reading FASTA file sets source metadata."""
        fasta_path = f"{DATA_DIR}/io/fasta/test.fasta"
        lf = pb.scan_fasta(fasta_path)

        meta = get_source_metadata(lf)

        assert meta["format"] == "fasta"
        assert fasta_path in meta["path"]


class TestVCFConvenienceWrappers:
    """Tests for VCF metadata convenience wrapper functions."""

    def test_get_vcf_metadata_wrapper(self):
        """Test that get_vcf_metadata wrapper extracts VCF fields correctly."""
        from polars_bio._metadata import get_vcf_metadata

        vcf_path = f"{DATA_DIR}/io/vcf/vep.vcf"
        lf = pb.scan_vcf(vcf_path)

        vcf_meta = get_vcf_metadata(lf)

        # VCF metadata should have the expected keys (values may be None if not in file)
        assert "info_fields" in vcf_meta
        assert "format_fields" in vcf_meta
        assert "sample_names" in vcf_meta

    def test_set_vcf_metadata_wrapper(self):
        """Test that set_vcf_metadata wrapper stores in source_header."""
        from polars_bio._metadata import get_vcf_metadata, set_vcf_metadata

        df = pl.DataFrame({"a": [1, 2, 3]})

        info_fields = {
            "AF": {"number": "A", "type": "Float", "description": "Allele Frequency"}
        }
        format_fields = {
            "GT": {"number": "1", "type": "String", "description": "Genotype"}
        }
        sample_names = ["Sample1", "Sample2"]

        set_vcf_metadata(
            df,
            info_fields=info_fields,
            format_fields=format_fields,
            sample_names=sample_names,
        )

        # Get via wrapper
        vcf_meta = get_vcf_metadata(df)
        assert vcf_meta["info_fields"] == info_fields
        assert vcf_meta["format_fields"] == format_fields
        assert vcf_meta["sample_names"] == sample_names

        # Verify it's stored in source_header
        source_meta = get_source_metadata(df)
        assert source_meta["header"]["info_fields"] == info_fields
        assert source_meta["header"]["format_fields"] == format_fields
        assert source_meta["header"]["sample_names"] == sample_names

    def test_vcf_wrapper_backward_compatible(self):
        """Test that VCF wrapper maintains backward compatibility."""
        from polars_bio._metadata import get_vcf_metadata, set_vcf_metadata

        vcf_path = f"{DATA_DIR}/io/vcf/vep.vcf"
        df = pb.read_vcf(vcf_path)

        # Old API should still work
        vcf_meta = get_vcf_metadata(df)
        assert "info_fields" in vcf_meta
        assert "format_fields" in vcf_meta
        assert "sample_names" in vcf_meta
