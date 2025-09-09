"""Test VCF projection pushdown functionality without info_fields parameter."""

import polars as pl
import pytest

import polars_bio as pb
from tests._expected import DATA_DIR


class TestVcfProjectionPushdown:
    """Test cases for VCF projection pushdown optimization."""

    def test_vcf_scan_without_select_returns_all_columns(self):
        """Test that pb.scan_vcf without .select() returns all available columns."""
        vcf_path = f"{DATA_DIR}/io/vcf/vep.vcf.bgz"

        # Scan without column selection should return all columns
        lazy_frame = pb.scan_vcf(vcf_path)
        result = lazy_frame.collect()

        # Should contain static VCF columns
        static_columns = ["chrom", "start", "end", "id", "ref", "alt", "qual", "filter"]
        for col in static_columns:
            assert col in result.columns, f"Static column '{col}' missing"

        # Should also contain INFO fields (e.g., CSQ from the VEP file)
        columns_lower = [col.lower() for col in result.columns]
        assert "csq" in columns_lower, "INFO field 'csq' should be present"

        # Verify we have data
        assert len(result) > 0, "Should have some rows of data"

    def test_vcf_scan_with_select_static_columns_only(self):
        """Test that pb.scan_vcf with .select() containing only static columns works."""
        vcf_path = f"{DATA_DIR}/io/vcf/vep.vcf.bgz"

        # Select only static columns
        lazy_frame = pb.scan_vcf(vcf_path)
        result = lazy_frame.select(["chrom", "start", "ref", "alt"]).collect()

        # Should only have the selected columns
        expected_columns = {"chrom", "start", "ref", "alt"}
        actual_columns = set(result.columns)
        assert (
            actual_columns == expected_columns
        ), f"Expected {expected_columns}, got {actual_columns}"

        # Verify we have data
        assert len(result) > 0, "Should have some rows of data"

    def test_vcf_scan_with_select_mixed_columns(self):
        """Test that pb.scan_vcf with .select() containing both static and info fields works."""
        vcf_path = f"{DATA_DIR}/io/vcf/vep.vcf.bgz"

        # Select mix of static columns and info fields
        lazy_frame = pb.scan_vcf(vcf_path)
        # First check what the actual column name is
        all_cols = lazy_frame.collect().columns
        csq_col = next((col for col in all_cols if col.lower() == "csq"), None)
        if csq_col is None:
            pytest.skip("CSQ column not found in test data")
        result = lazy_frame.select(["chrom", "start", csq_col]).collect()

        # Should only have the selected columns
        expected_columns = {"chrom", "start", csq_col}
        actual_columns = set(result.columns)
        assert (
            actual_columns == expected_columns
        ), f"Expected {expected_columns}, got {actual_columns}"

        # Verify we have data
        assert len(result) > 0, "Should have some rows of data"

    def test_vcf_scan_with_projection_pushdown_enabled(self):
        """Test VCF scan with projection_pushdown=True."""
        vcf_path = f"{DATA_DIR}/io/vcf/vep.vcf.bgz"

        # Test with projection pushdown enabled
        lazy_frame = pb.scan_vcf(vcf_path, projection_pushdown=True)
        # First check what the actual column name is
        all_cols = lazy_frame.collect().columns
        csq_col = next((col for col in all_cols if col.lower() == "csq"), None)
        if csq_col is None:
            pytest.skip("CSQ column not found in test data")
        result = lazy_frame.select(["chrom", "start", csq_col]).collect()

        # Should only have the selected columns
        expected_columns = {"chrom", "start", csq_col}
        actual_columns = set(result.columns)
        assert (
            actual_columns == expected_columns
        ), f"Expected {expected_columns}, got {actual_columns}"

        # Verify we have data
        assert len(result) > 0, "Should have some rows of data"

    def test_vcf_projection_pushdown_comparison(self):
        """Compare results with and without projection pushdown - they should be identical."""
        vcf_path = f"{DATA_DIR}/io/vcf/vep.vcf.bgz"

        # Get result without projection pushdown
        result_without = (
            pb.scan_vcf(vcf_path, projection_pushdown=False)
            .select(["chrom", "start", "ref", "alt"])
            .collect()
        )

        # Get result with projection pushdown
        result_with = (
            pb.scan_vcf(vcf_path, projection_pushdown=True)
            .select(["chrom", "start", "ref", "alt"])
            .collect()
        )

        # Results should be identical
        assert result_without.equals(
            result_with
        ), "Results should be identical with and without projection pushdown"

    def test_vcf_info_field_detection(self):
        """Test that info fields are correctly detected from column selection."""
        vcf_path = f"{DATA_DIR}/io/vcf/vep.vcf.bgz"

        # Get all available columns first
        full_result = pb.scan_vcf(vcf_path).collect()
        all_columns = set(full_result.columns)

        # Static VCF columns
        static_columns = {"chrom", "start", "end", "id", "ref", "alt", "qual", "filter"}

        # Info fields should be columns not in static set
        info_fields = all_columns - static_columns
        assert len(info_fields) > 0, "Should have some info fields"
        # Check for CSQ in case-insensitive way
        info_fields_lower = {col.lower() for col in info_fields}
        assert "csq" in info_fields_lower, "CSQ should be detected as info field"

        # Test selecting only info fields - find CSQ column case-insensitively
        csq_col = next((col for col in info_fields if col.lower() == "csq"), None)
        selected_info_fields = [csq_col] if csq_col else list(info_fields)[:1]
        if selected_info_fields:
            result = (
                pb.scan_vcf(vcf_path, projection_pushdown=True)
                .select(selected_info_fields)
                .collect()
            )

            # Should only have the selected info fields
            expected_columns = set(selected_info_fields)
            actual_columns = set(result.columns)
            assert (
                actual_columns == expected_columns
            ), f"Expected {expected_columns}, got {actual_columns}"

    def test_vcf_scan_with_info_fields_parameter(self):
        """Test that VCF scan functions accept info_fields parameter."""
        vcf_path = f"{DATA_DIR}/io/vcf/vep.vcf.bgz"

        # Get the actual column name first
        full_result = pb.scan_vcf(vcf_path).collect()
        all_columns = set(full_result.columns)
        static_columns = {"chrom", "start", "end", "id", "ref", "alt", "qual", "filter"}
        info_fields = all_columns - static_columns
        csq_col = "CSQ"

        if csq_col:
            # These should work with info_fields parameter
            result1 = pb.scan_vcf(vcf_path, info_fields=[csq_col]).collect()
            assert csq_col.lower() in result1.columns

            result2 = pb.read_vcf(vcf_path, info_fields=[csq_col])
            assert csq_col.lower() in result2.columns

    def test_vcf_scan_with_special_chars_in_column_name(self):
        """Test that pb.scan_vcf works with columns that have special characters."""
        vcf_path = f"{DATA_DIR}/io/vcf/ensembl-2.vcf"
        result = pb.scan_vcf(vcf_path, projection_pushdown=True).count().collect()
        assert len(result) == 1
