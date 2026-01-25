"""Tests for VCF FORMAT column support.

Tests reading per-sample genotype data (GT, DP, GQ, etc.) from VCF files.

Column naming convention:
- Single-sample VCF: columns are named directly by FORMAT field (e.g., GT, DP)
- Multi-sample VCF: columns are named {sample_name}_{format_field} (e.g., NA12878_GT, NA12879_DP)
"""

import polars as pl

import polars_bio as pb

# =============================================================================
# Single-sample VCF tests (antku_small.vcf.gz has sample "default")
# =============================================================================


def test_vcf_format_columns_single_sample_specific_fields():
    """Test reading single-sample VCF with specific FORMAT fields."""
    vcf_path = "tests/data/io/vcf/antku_small.vcf.gz"
    df = pb.read_vcf(vcf_path, format_fields=["GT", "DP"])

    # Single-sample VCF: columns named directly by FORMAT field
    assert "GT" in df.columns, f"GT not found in columns: {df.columns}"
    assert "DP" in df.columns, f"DP not found in columns: {df.columns}"


def test_vcf_format_columns_single_sample_gt_only():
    """Test reading single-sample VCF with only GT FORMAT field."""
    vcf_path = "tests/data/io/vcf/antku_small.vcf.gz"
    df = pb.read_vcf(vcf_path, format_fields=["GT"])

    assert "GT" in df.columns
    # GT should be string type
    assert df.schema["GT"] == pl.Utf8


def test_vcf_format_single_sample_gt_values():
    """Test that GT field values have proper separator format."""
    vcf_path = "tests/data/io/vcf/antku_small.vcf.gz"
    df = pb.read_vcf(vcf_path, format_fields=["GT"])

    # Check GT values format - should contain / (unphased) or | (phased)
    gt_values = df["GT"].to_list()
    non_null_values = [v for v in gt_values if v is not None]
    assert len(non_null_values) > 0, "No GT values found"

    for v in non_null_values:
        assert "/" in v or "|" in v, f"GT value '{v}' missing separator"


def test_vcf_format_single_sample_dp_type():
    """Test that DP field has numeric type in single-sample VCF."""
    vcf_path = "tests/data/io/vcf/antku_small.vcf.gz"
    df = pb.read_vcf(vcf_path, format_fields=["DP"])

    assert "DP" in df.columns
    # DP (depth) should be integer type
    assert df.schema["DP"] in [pl.Int32, pl.Int64, pl.UInt32, pl.UInt64]


def test_vcf_single_sample_mixed_info_and_format():
    """Test reading single-sample VCF with both INFO and FORMAT fields."""
    vcf_path = "tests/data/io/vcf/antku_small.vcf.gz"
    df = pb.read_vcf(vcf_path, info_fields=["END"], format_fields=["GT", "DP"])

    # Verify INFO field
    assert "END" in df.columns, "END INFO field not found"

    # Verify FORMAT fields (single-sample naming)
    assert "GT" in df.columns, "GT FORMAT field not found"
    assert "DP" in df.columns, "DP FORMAT field not found"


def test_scan_vcf_single_sample_format_columns():
    """Test lazy scan_vcf with FORMAT fields on single-sample VCF."""
    vcf_path = "tests/data/io/vcf/antku_small.vcf.gz"
    lf = pb.scan_vcf(vcf_path, format_fields=["GT"])
    df = lf.collect()

    assert "GT" in df.columns


def test_vcf_format_fields_auto_detected_by_default():
    """Test that FORMAT fields ARE auto-detected by default (when format_fields=None)."""
    vcf_path = "tests/data/io/vcf/antku_small.vcf.gz"
    df = pb.read_vcf(vcf_path)

    # FORMAT columns should be present when format_fields=None (auto-detect)
    # Single-sample VCF: columns named directly by FORMAT field
    assert "GT" in df.columns, "GT FORMAT field should be auto-detected"
    assert "DP" in df.columns, "DP FORMAT field should be auto-detected"
    assert "GQ" in df.columns, "GQ FORMAT field should be auto-detected"


# =============================================================================
# Multi-sample VCF tests (multisample.vcf has samples NA12878, NA12879, NA12880)
# =============================================================================


def test_vcf_format_columns_multisample_specific_fields():
    """Test reading multi-sample VCF with specific FORMAT fields."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(vcf_path, format_fields=["GT", "DP"])

    # Multi-sample VCF: columns named {sample_name}_{format_field}
    assert "NA12878_GT" in df.columns, f"NA12878_GT not found in columns: {df.columns}"
    assert "NA12878_DP" in df.columns, f"NA12878_DP not found in columns: {df.columns}"
    assert "NA12879_GT" in df.columns, f"NA12879_GT not found in columns: {df.columns}"
    assert "NA12879_DP" in df.columns, f"NA12879_DP not found in columns: {df.columns}"
    assert "NA12880_GT" in df.columns, f"NA12880_GT not found in columns: {df.columns}"
    assert "NA12880_DP" in df.columns, f"NA12880_DP not found in columns: {df.columns}"


def test_vcf_format_multisample_gt_type():
    """Test that GT field has string type in multi-sample VCF."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(vcf_path, format_fields=["GT"])

    # All GT columns should be string type
    assert df.schema["NA12878_GT"] == pl.Utf8
    assert df.schema["NA12879_GT"] == pl.Utf8
    assert df.schema["NA12880_GT"] == pl.Utf8


def test_vcf_format_multisample_dp_type():
    """Test that DP field has numeric type in multi-sample VCF."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(vcf_path, format_fields=["DP"])

    # All DP columns should be integer type
    assert df.schema["NA12878_DP"] in [pl.Int32, pl.Int64, pl.UInt32, pl.UInt64]
    assert df.schema["NA12879_DP"] in [pl.Int32, pl.Int64, pl.UInt32, pl.UInt64]
    assert df.schema["NA12880_DP"] in [pl.Int32, pl.Int64, pl.UInt32, pl.UInt64]


def test_vcf_format_multisample_gt_values():
    """Test that GT values are correctly parsed in multi-sample VCF."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(vcf_path, format_fields=["GT"])

    # Check specific values from our test file
    # Row 1: NA12878=0/1, NA12879=1/1, NA12880=0/0
    assert df["NA12878_GT"][0] == "0/1"
    assert df["NA12879_GT"][0] == "1/1"
    assert df["NA12880_GT"][0] == "0/0"


def test_vcf_format_multisample_dp_values():
    """Test that DP values are correctly parsed in multi-sample VCF."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(vcf_path, format_fields=["DP"])

    # Check specific values from our test file
    # Row 1: NA12878=25, NA12879=30, NA12880=20
    assert df["NA12878_DP"][0] == 25
    assert df["NA12879_DP"][0] == 30
    assert df["NA12880_DP"][0] == 20


def test_vcf_multisample_mixed_info_and_format():
    """Test reading multi-sample VCF with both INFO and FORMAT fields."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(vcf_path, info_fields=["AF"], format_fields=["GT", "GQ"])

    # Verify INFO field
    assert "AF" in df.columns, "AF INFO field not found"

    # Verify FORMAT fields (multi-sample naming)
    assert "NA12878_GT" in df.columns
    assert "NA12878_GQ" in df.columns
    assert "NA12879_GT" in df.columns
    assert "NA12880_GQ" in df.columns


def test_scan_vcf_multisample_format_columns():
    """Test lazy scan_vcf with FORMAT fields on multi-sample VCF."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    lf = pb.scan_vcf(vcf_path, format_fields=["GT", "DP"])
    df = lf.collect()

    # Verify multi-sample column naming
    assert "NA12878_GT" in df.columns
    assert "NA12879_DP" in df.columns
