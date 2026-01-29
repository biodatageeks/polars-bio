"""
Tests for comprehensive metadata extraction from all formats.

This test suite verifies the new unified metadata system that:
1. Extracts ALL metadata from Arrow schemas (schema-level and field-level)
2. Works for all formats (VCF, FASTQ, BAM, GFF, etc.)
3. Provides easy access to both raw and parsed metadata
"""

import polars_bio as pb


class TestComprehensiveMetadataExtraction:
    """Test comprehensive metadata extraction functionality."""

    def test_get_full_metadata_vcf(self):
        """Test extracting metadata from VCF."""
        lf = pb.scan_vcf("tests/data/io/vcf/multisample.vcf")
        meta = pb.get_metadata(lf)
        header = meta["header"]

        assert header is not None
        assert "info_fields" in header
        assert "format_fields" in header
        assert "sample_names" in header
        assert "version" in header

        # Should have detected VCF format
        assert meta["format"] == "vcf"

    def test_schema_metadata_keys(self):
        """Test getting schema-level metadata."""
        lf = pb.scan_vcf("tests/data/io/vcf/vep.vcf.gz")
        meta = pb.get_metadata(lf)

        # Should have VCF metadata
        assert meta["format"] == "vcf"
        assert meta["coordinate_system_zero_based"] == False  # VCF is 1-based
        assert "version" in meta["header"]

    def test_field_metadata(self):
        """Test getting field-level metadata."""
        lf = pb.scan_vcf("tests/data/io/vcf/multisample.vcf")
        meta = pb.get_metadata(lf)

        # Check FORMAT field metadata
        format_fields = meta["header"]["format_fields"]
        assert "GT" in format_fields
        gt_field = format_fields["GT"]
        assert gt_field["type"] == "String"
        assert "Genotype" in gt_field["description"]

    def test_format_specific_metadata_vcf(self):
        """Test VCF-specific metadata extraction."""
        lf = pb.scan_vcf("tests/data/io/vcf/multisample.vcf")
        meta = pb.get_metadata(lf)
        vcf_meta = meta["header"]

        assert vcf_meta is not None
        assert "version" in vcf_meta
        assert "info_fields" in vcf_meta
        assert "format_fields" in vcf_meta
        assert "sample_names" in vcf_meta

        # Check specific values
        assert vcf_meta["version"] == "VCFv4.2"
        assert len(vcf_meta["sample_names"]) == 3
        assert "NA12878" in vcf_meta["sample_names"]

        # Check INFO fields parsed correctly
        assert "AF" in vcf_meta["info_fields"]
        af_field = vcf_meta["info_fields"]["AF"]
        assert "type" in af_field
        assert "number" in af_field
        assert "description" in af_field

        # Check FORMAT fields parsed correctly
        assert "GT" in vcf_meta["format_fields"]
        assert "DP" in vcf_meta["format_fields"]
        assert "GQ" in vcf_meta["format_fields"]

    def test_basic_metadata_access(self):
        """Test basic metadata access."""
        lf = pb.scan_vcf("tests/data/io/vcf/vep.vcf.gz")

        # get_metadata should work
        meta = pb.get_metadata(lf)

        assert meta is not None
        assert meta["format"] == "vcf"
        assert "path" in meta

        # VCF header metadata should be present
        if "header" in meta:
            header = meta["header"]
            assert "version" in header
            assert "info_fields" in header
            assert "format_fields" in header

    def test_metadata_all_formats(self):
        """Test metadata extraction works for all formats."""
        test_cases = [
            ("vcf", pb.scan_vcf, "tests/data/io/vcf/vep.vcf.gz"),
            ("fastq", pb.scan_fastq, "tests/data/io/fastq/test.fastq.gz"),
            ("gff", pb.scan_gff, "tests/data/io/gff/Homo_sapiens.GRCh38.111.gff3.gz"),
            ("bed", pb.scan_bed, "tests/data/io/bed/test.bed"),
            ("bam", pb.scan_bam, "tests/data/io/bam/test.bam"),
        ]

        for format_name, scan_func, file_path in test_cases:
            lf = scan_func(file_path)

            # Should be able to get metadata for all formats
            meta = pb.get_metadata(lf)
            assert meta is not None, f"No metadata for {format_name}"
            assert meta["format"] == format_name, f"Format mismatch for {format_name}"

            # VCF should have header metadata, other formats may not
            if format_name == "vcf":
                assert (
                    meta["header"] is not None
                ), f"No header metadata for {format_name}"

    def test_print_metadata_summary(self):
        """Test metadata summary printing (no assertion, just smoke test)."""
        lf = pb.scan_vcf("tests/data/io/vcf/multisample.vcf")

        # Should not raise an exception
        pb.print_metadata_summary(lf)

    def test_metadata_preserved_after_operations(self):
        """Test that metadata is preserved after LazyFrame operations."""
        lf = pb.scan_vcf("tests/data/io/vcf/vep.vcf.gz")

        # Get original metadata
        orig_meta = pb.get_metadata(lf)
        assert orig_meta["format"] == "vcf"
        assert "version" in orig_meta["header"]

        # After head()
        lf_head = lf.head(5)
        head_meta = pb.get_metadata(lf_head)
        assert head_meta["format"] == "vcf"
        assert "version" in head_meta["header"]

        # Metadata should be the same
        assert head_meta["format"] == orig_meta["format"]
        assert head_meta["header"]["version"] == orig_meta["header"]["version"]

    def test_vcf_info_format_fields_detailed(self):
        """Test detailed VCF INFO/FORMAT field metadata."""
        lf = pb.scan_vcf("tests/data/io/vcf/multisample.vcf")
        meta = pb.get_metadata(lf)
        vcf_meta = meta["header"]

        # Test INFO field structure
        info_fields = vcf_meta["info_fields"]
        assert len(info_fields) > 0

        # Each INFO field should have required attributes
        for field_name, field_meta in info_fields.items():
            assert "type" in field_meta
            assert "number" in field_meta
            assert "description" in field_meta
            assert "id" in field_meta

        # Test FORMAT field structure
        format_fields = vcf_meta["format_fields"]
        assert len(format_fields) > 0

        # Each FORMAT field should have required attributes
        for field_name, field_meta in format_fields.items():
            assert "type" in field_meta
            assert "number" in field_meta
            assert "description" in field_meta


class TestMetadataEdgeCases:
    """Test edge cases in metadata extraction."""

    def test_metadata_on_non_metadata_lazyframe(self):
        """Test metadata functions on LazyFrame without metadata."""
        import polars as pl

        # Create a plain LazyFrame without source metadata
        lf = pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

        # Should return empty result, not raise an error
        meta = pb.get_metadata(lf)
        assert meta["format"] is None
        assert meta["path"] is None
        assert meta["header"] is None

    def test_empty_format_specific_metadata(self):
        """Test format-specific metadata when format not detected."""
        lf = pb.scan_bed("tests/data/io/bed/test.bed")

        # BED might not have format-specific metadata
        meta = pb.get_metadata(lf)

        # Should return metadata, not raise
        assert meta is not None
        # (Acceptable for simple formats)


class TestMetadataHelpers:
    """Test helper functions for metadata access."""

    def test_get_format_specific_auto_detect(self):
        """Test auto-detection of format in get_metadata."""
        lf = pb.scan_vcf("tests/data/io/vcf/vep.vcf.gz")

        # Should detect VCF and include metadata
        meta = pb.get_metadata(lf)

        assert meta is not None
        assert meta["format"] == "vcf"
        assert "version" in meta["header"]

    def test_metadata_access(self):
        """Test access to parsed metadata."""
        lf = pb.scan_vcf("tests/data/io/vcf/multisample.vcf")
        meta = pb.get_metadata(lf)

        # Basic metadata should be accessible
        assert meta["format"] == "vcf"
        assert meta["coordinate_system_zero_based"] == False

        # VCF header should be accessible
        header = meta["header"]
        assert isinstance(header, dict)
        assert header["version"] == "VCFv4.2"

        # INFO fields should be accessible
        info_fields = header["info_fields"]
        assert isinstance(info_fields, dict)
        assert "AF" in info_fields

        # FORMAT fields should be accessible
        format_fields = header["format_fields"]
        assert isinstance(format_fields, dict)
        assert "GT" in format_fields
