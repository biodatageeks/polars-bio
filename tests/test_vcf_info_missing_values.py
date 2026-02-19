"""Test VCF INFO array fields with missing values (`.`).

Regression test for https://github.com/biodatageeks/datafusion-bio-formats/pull/74
and https://github.com/biodatageeks/polars-bio/issues/312.

VCF INFO array fields (Number=R, Number=A, Number=.) can contain `.` as the
standard missing value within comma-separated elements (e.g. AD=.,15 or AF=0.5,.).
Previously this caused a panic and silent data truncation.
"""

import polars_bio as pb

VCF_PATH = "tests/data/io/vcf/info_missing_array.vcf"


def test_info_array_missing_values_no_row_loss():
    """All 4 rows must be returned — missing '.' must not cause silent data loss."""
    df = pb.read_vcf(VCF_PATH, info_fields=["AD", "AF", "ALLELE_ID"])
    assert (
        len(df) == 4
    ), f"Expected 4 rows, got {len(df)} — missing value caused row loss"


def test_info_array_missing_integer():
    """AD (Number=R, Type=Integer) with '.' elements → null in list."""
    df = pb.read_vcf(VCF_PATH, info_fields=["AD"])

    # Row 0: AD=.,15 → [null, 15]
    ad0 = df["AD"][0].to_list()
    assert len(ad0) == 2
    assert ad0[0] is None
    assert ad0[1] == 15

    # Row 1: AD=10,.,5 → [10, null, 5]
    ad1 = df["AD"][1].to_list()
    assert len(ad1) == 3
    assert ad1[0] == 10
    assert ad1[1] is None
    assert ad1[2] == 5

    # Row 3: AD=20,30 → [20, 30] (no missing — still works)
    ad3 = df["AD"][3].to_list()
    assert ad3 == [20, 30]


def test_info_array_missing_float():
    """AF (Number=A, Type=Float) with '.' elements → null in list."""
    df = pb.read_vcf(VCF_PATH, info_fields=["AF"])

    # Row 0: AF=0.5 → [0.5]
    af0 = df["AF"][0].to_list()
    assert len(af0) == 1
    assert abs(af0[0] - 0.5) < 1e-6

    # Row 1: AF=.,0.3 → [null, 0.3]
    af1 = df["AF"][1].to_list()
    assert len(af1) == 2
    assert af1[0] is None
    assert abs(af1[1] - 0.3) < 1e-6

    # Row 2: AF=0.3,. → [0.3, null]
    af2 = df["AF"][2].to_list()
    assert len(af2) == 2
    assert abs(af2[0] - 0.3) < 1e-6
    assert af2[1] is None


def test_info_array_missing_string():
    """ALLELE_ID (Number=., Type=String) with '.' elements → null in list."""
    df = pb.read_vcf(VCF_PATH, info_fields=["ALLELE_ID"])

    # Row 0: ALLELE_ID=.,alt1 → [null, "alt1"]
    aid0 = df["ALLELE_ID"][0].to_list()
    assert len(aid0) == 2
    assert aid0[0] is None
    assert aid0[1] == "alt1"

    # Row 1: ALLELE_ID=ref2,.,alt2 → ["ref2", null, "alt2"]
    aid1 = df["ALLELE_ID"][1].to_list()
    assert len(aid1) == 3
    assert aid1[0] == "ref2"
    assert aid1[1] is None
    assert aid1[2] == "alt2"

    # Row 2: ALLELE_ID=ref3,alt3a,. → ["ref3", "alt3a", null]
    aid2 = df["ALLELE_ID"][2].to_list()
    assert len(aid2) == 3
    assert aid2[0] == "ref3"
    assert aid2[1] == "alt3a"
    assert aid2[2] is None

    # Row 3: ALLELE_ID=ref4,alt4 → ["ref4", "alt4"] (no missing)
    aid3 = df["ALLELE_ID"][3].to_list()
    assert aid3 == ["ref4", "alt4"]


def test_scan_vcf_info_array_missing_values():
    """Lazy scan should also handle missing INFO array values."""
    lf = pb.scan_vcf(VCF_PATH, info_fields=["AD", "AF", "ALLELE_ID"])
    df = lf.collect()
    assert len(df) == 4
    assert "AD" in df.columns
    assert "AF" in df.columns
    assert "ALLELE_ID" in df.columns
