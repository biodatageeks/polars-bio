"""Tests for VCF FORMAT column support.

Tests reading per-sample genotype data (GT, DP, GQ, etc.) from VCF files.

Column naming convention:
- Single-sample VCF: columns are named directly by FORMAT field (e.g., GT, DP)
- Multi-sample VCF: FORMAT values are nested in a single `genotypes` column
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
# Multi-sample VCF tests (samples: NA12878, NA12879, NA12880)
# =============================================================================


def _genotypes_by_sample(df: pl.DataFrame, row_idx: int = 0) -> dict:
    """Convert one row of nested genotypes to sample->values mapping."""
    entries = df["genotypes"].to_list()[row_idx]
    assert isinstance(entries, list), f"Expected list entries, got: {type(entries)}"
    result = {}
    for entry in entries:
        sample_id = entry.get("sample_id")
        values = entry.get("values") or {}
        result[sample_id] = values
    return result


def _sample_ids(df: pl.DataFrame, row_idx: int = 0) -> list[str]:
    """Extract ordered sample IDs from one row of nested genotypes."""
    entries = df["genotypes"].to_list()[row_idx]
    assert isinstance(entries, list), f"Expected list entries, got: {type(entries)}"
    return [entry.get("sample_id") for entry in entries]


def test_vcf_format_columns_multisample_specific_fields():
    """Test reading multi-sample VCF with specific FORMAT fields."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(vcf_path, format_fields=["GT", "DP"])

    assert "genotypes" in df.columns, f"genotypes not found in columns: {df.columns}"
    assert "NA12878_GT" not in df.columns


def test_vcf_format_multisample_gt_type():
    """Test that GT field has string type in multi-sample VCF."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(vcf_path, format_fields=["GT"])

    sample_map = _genotypes_by_sample(df, 0)
    assert sample_map["NA12878"]["GT"] == "0/1"
    assert sample_map["NA12879"]["GT"] == "1/1"
    assert sample_map["NA12880"]["GT"] == "0/0"


def test_vcf_format_multisample_dp_type():
    """Test that DP field has numeric type in multi-sample VCF."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(vcf_path, format_fields=["DP"])

    sample_map = _genotypes_by_sample(df, 0)
    assert isinstance(sample_map["NA12878"]["DP"], int)
    assert isinstance(sample_map["NA12879"]["DP"], int)
    assert isinstance(sample_map["NA12880"]["DP"], int)


def test_vcf_format_multisample_gt_values():
    """Test GT values are correctly parsed in nested multi-sample genotypes."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(vcf_path, format_fields=["GT"])

    sample_map = _genotypes_by_sample(df, 0)
    assert sample_map["NA12878"]["GT"] == "0/1"
    assert sample_map["NA12879"]["GT"] == "1/1"
    assert sample_map["NA12880"]["GT"] == "0/0"


def test_vcf_format_multisample_dp_values():
    """Test DP values are correctly parsed in nested multi-sample genotypes."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(vcf_path, format_fields=["DP"])

    sample_map = _genotypes_by_sample(df, 0)
    assert sample_map["NA12878"]["DP"] == 25
    assert sample_map["NA12879"]["DP"] == 30
    assert sample_map["NA12880"]["DP"] == 20


def test_vcf_multisample_mixed_info_and_format():
    """Test reading multi-sample VCF with both INFO and FORMAT fields."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(vcf_path, info_fields=["AF"], format_fields=["GT", "GQ"])

    # Verify INFO field
    assert "AF" in df.columns, "AF INFO field not found"

    # Verify nested FORMAT field storage
    assert "genotypes" in df.columns


def test_scan_vcf_multisample_format_columns():
    """Test lazy scan_vcf with FORMAT fields on multi-sample VCF."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    lf = pb.scan_vcf(vcf_path, format_fields=["GT", "DP"])
    df = lf.collect()

    assert "genotypes" in df.columns


def test_vcf_multisample_samples_subset_respects_requested_order():
    """Requested sample order should be preserved in nested genotype output."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(
        vcf_path,
        format_fields=["GT"],
        samples=["NA12880", "NA12878"],
    )

    assert _sample_ids(df, 0) == ["NA12880", "NA12878"]


def test_scan_vcf_multisample_samples_subset():
    """scan_vcf should apply the same sample subset filtering as read_vcf."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.scan_vcf(
        vcf_path,
        format_fields=["GT"],
        samples=["NA12879"],
    ).collect()

    assert _sample_ids(df, 0) == ["NA12879"]


def test_vcf_multisample_samples_missing_are_skipped():
    """Unknown sample names should be skipped without raising."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(
        vcf_path,
        format_fields=["GT"],
        samples=["MISSING_SAMPLE", "NA12878"],
    )

    assert _sample_ids(df, 0) == ["NA12878"]


def test_vcf_multisample_samples_duplicates_deduplicated():
    """Duplicate requested sample names should appear once in output."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(
        vcf_path,
        format_fields=["GT"],
        samples=["NA12879", "NA12879", "NA12880"],
    )

    assert _sample_ids(df, 0) == ["NA12879", "NA12880"]


def test_vcf_multisample_samples_none_regression():
    """Default behavior with samples=None should keep all multisample entries."""
    vcf_path = "tests/data/io/vcf/multisample.vcf"
    df = pb.read_vcf(vcf_path, format_fields=["GT"])

    assert _sample_ids(df, 0) == ["NA12878", "NA12879", "NA12880"]


def test_vcf_single_sample_samples_filter_keeps_format_columns():
    """Selecting the existing single-sample name should keep top-level FORMAT columns."""
    vcf_path = "tests/data/io/vcf/antku_small.vcf.gz"
    df = pb.read_vcf(vcf_path, format_fields=["GT"], samples=["default"])

    assert "GT" in df.columns
