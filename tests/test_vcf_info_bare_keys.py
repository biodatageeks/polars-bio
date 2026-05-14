"""Regression tests for bare non-Flag VCF INFO keys.

Tracks https://github.com/biodatageeks/polars-bio/issues/380 and the upstream
fix in https://github.com/biodatageeks/datafusion-bio-formats/pull/183.
"""

import pytest

import polars_bio as pb

VCF_PATH = "tests/data/io/vcf/info_bare_key.vcf"
REALDATA_VCF_PATH = "tests/data/io/vcf/info_bare_key_realdata.vcf"
INVALID_FLAG_VCF_PATH = "tests/data/io/vcf/info_invalid_flag_value.vcf"


def test_read_vcf_bare_scalar_info_key_yields_null():
    """Bare scalar non-Flag INFO keys should be null, not scan errors."""
    df = pb.read_vcf(VCF_PATH, info_fields=["DP", "AF", "ALLELE_ID", "DB"])

    assert len(df) == 4
    assert df["DP"][0] is None
    assert df["DP"][1] == 42
    assert df["DP"][2] == 7
    assert df["DP"][3] == 9

    af0 = df["AF"][0].to_list()
    assert len(af0) == 1
    assert abs(af0[0] - 0.5) < 1e-6

    assert bool(df["DB"][0]) is True
    assert bool(df["DB"][1]) is False
    assert bool(df["DB"][2]) is False
    assert bool(df["DB"][3]) is True


def test_scan_vcf_bare_array_info_keys_yield_null():
    """Bare Number=A and Number=. INFO keys should be null in lazy scans."""
    df = pb.scan_vcf(VCF_PATH, info_fields=["DP", "AF", "ALLELE_ID"]).collect()

    assert len(df) == 4
    assert df["AF"][1] is None
    assert df["ALLELE_ID"][2] is None

    af2 = df["AF"][2].to_list()
    assert len(af2) == 1
    assert abs(af2[0] - 0.2) < 1e-6


def test_unrequested_bare_info_key_does_not_abort_projection():
    """Bare INFO keys outside info_fields should not abort projected scans."""
    df = pb.scan_vcf(VCF_PATH, info_fields=["AF"]).select(["chrom", "AF"]).collect()

    assert len(df) == 4
    assert df["chrom"][0] == "chr1"
    assert df["AF"][1] is None


def test_real_data_evidence_bare_key_yields_null():
    """Real chrX:1946351 EVIDENCE bare key should parse as null."""
    df = pb.read_vcf(REALDATA_VCF_PATH, info_fields=["AC", "AF", "EVIDENCE"])

    assert len(df) == 1
    assert df["AC"][0].to_list() == [2]
    assert abs(df["AF"][0].to_list()[0] - 0.998595) < 1e-6
    assert df["EVIDENCE"][0] is None


def test_explicit_value_for_flag_still_errors():
    """Flag INFO fields with explicit values should still fail."""
    with pytest.raises(Exception, match="invalid flag|Error reading INFO field"):
        pb.read_vcf(INVALID_FLAG_VCF_PATH, info_fields=["DB"])
