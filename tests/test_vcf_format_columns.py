"""Tests for VCF FORMAT column support.

Tests reading per-sample genotype data (GT, DP, GQ, etc.) from VCF files.
FORMAT columns are named {sample_name}_{format_field} (e.g., default_GT, NA12878_DP).
"""

import polars as pl

import polars_bio as pb


def test_vcf_format_columns_with_specific_fields():
    """Test reading VCF with specific FORMAT fields."""
    vcf_path = "tests/data/io/vcf/antku_small.vcf.gz"
    df = pb.read_vcf(vcf_path, format_fields=["GT", "DP"])

    # Verify FORMAT columns exist with pattern: default_{FORMAT}
    # The sample name in antku_small.vcf.gz is "default"
    assert "default_GT" in df.columns, f"default_GT not found in columns: {df.columns}"
    assert "default_DP" in df.columns, f"default_DP not found in columns: {df.columns}"


def test_vcf_format_columns_gt_only():
    """Test reading VCF with only GT FORMAT field."""
    vcf_path = "tests/data/io/vcf/antku_small.vcf.gz"
    df = pb.read_vcf(vcf_path, format_fields=["GT"])

    assert "default_GT" in df.columns
    # GT should be string type
    assert df.schema["default_GT"] == pl.Utf8


def test_vcf_format_gt_values():
    """Test that GT field values have proper separator format."""
    vcf_path = "tests/data/io/vcf/antku_small.vcf.gz"
    df = pb.read_vcf(vcf_path, format_fields=["GT"])

    # Check GT values format - should contain / (unphased) or | (phased)
    gt_values = df["default_GT"].to_list()
    non_null_values = [v for v in gt_values if v is not None]
    assert len(non_null_values) > 0, "No GT values found"

    for v in non_null_values:
        assert "/" in v or "|" in v, f"GT value '{v}' missing separator"


def test_vcf_format_dp_type():
    """Test that DP field has numeric type."""
    vcf_path = "tests/data/io/vcf/antku_small.vcf.gz"
    df = pb.read_vcf(vcf_path, format_fields=["DP"])

    assert "default_DP" in df.columns
    # DP (depth) should be integer type
    assert df.schema["default_DP"] in [pl.Int32, pl.Int64, pl.UInt32, pl.UInt64]


def test_vcf_mixed_info_and_format():
    """Test reading VCF with both INFO and FORMAT fields."""
    vcf_path = "tests/data/io/vcf/antku_small.vcf.gz"
    df = pb.read_vcf(vcf_path, info_fields=["END"], format_fields=["GT", "DP"])

    # Verify INFO field
    assert "END" in df.columns, "END INFO field not found"

    # Verify FORMAT fields
    assert "default_GT" in df.columns, "default_GT FORMAT field not found"
    assert "default_DP" in df.columns, "default_DP FORMAT field not found"


def test_scan_vcf_format_columns():
    """Test lazy scan_vcf with FORMAT fields."""
    vcf_path = "tests/data/io/vcf/antku_small.vcf.gz"
    lf = pb.scan_vcf(vcf_path, format_fields=["GT"])
    df = lf.collect()

    assert "default_GT" in df.columns


def test_vcf_no_format_fields_by_default():
    """Test that FORMAT fields are NOT included by default (when format_fields=None)."""
    vcf_path = "tests/data/io/vcf/antku_small.vcf.gz"
    df = pb.read_vcf(vcf_path)

    # FORMAT columns should not be present when format_fields=None
    format_columns = [col for col in df.columns if col.startswith("default_")]
    assert (
        len(format_columns) == 0
    ), f"FORMAT columns should not be present by default: {format_columns}"
