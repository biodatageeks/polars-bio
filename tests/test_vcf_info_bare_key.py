"""Test VCF INFO fields where a non-Flag key appears bare (no `=value`).

Regression test for https://github.com/biodatageeks/polars-bio/issues/380 and
https://github.com/biodatageeks/datafusion-bio-formats/pull/178.

The VCF 4.x spec allows INFO fields where a key is present without an
`=value` separator. For Flag types this means "true". For non-Flag types
(e.g. ``Number=1, Type=Integer``) the value should be read as missing
(null) rather than aborting the entire RecordBatch.

Pre-fix, scanning a record like ``DP;AF=0.5`` (with ``DP`` declared
``Number=1, Type=Integer``) raised
``InvalidArgumentError("Error reading INFO field: missing value")``
because noodles' ``info.iter()`` returns ``io::Error("missing value")``
for the bare key and ``load_infos_single_pass`` wrapped every error into
an Arrow error instead of mapping the missing case to ``append_null``.
"""

import polars_bio as pb

VCF_PATH = "tests/data/io/vcf/info_bare_key.vcf"


def test_bare_info_key_does_not_raise():
    """Scanning a VCF with a bare INFO key must not raise."""
    df = pb.read_vcf(VCF_PATH, info_fields=["DP", "AF"])
    assert len(df) == 4, f"Expected 4 rows, got {len(df)}"


def test_bare_info_key_yields_null():
    """Bare ``DP`` (Number=1, Integer) must read as null; AF still parses."""
    df = pb.read_vcf(VCF_PATH, info_fields=["DP", "AF"])

    # Row 0: INFO=DP;AF=0.5 → DP=null, AF=[0.5]
    assert df["DP"][0] is None
    af0 = df["AF"][0].to_list()
    assert len(af0) == 1
    assert abs(af0[0] - 0.5) < 1e-6

    # Row 1: INFO=DP=42;AF=0.25 → DP=42, AF=[0.25] (control row)
    assert df["DP"][1] == 42
    af1 = df["AF"][1].to_list()
    assert abs(af1[0] - 0.25) < 1e-6


def test_bare_info_array_key_yields_null():
    """Bare ``AC`` (Number=A, Integer) must read as null in lazy scan."""
    lf = pb.scan_vcf(VCF_PATH, info_fields=["AC", "DP", "AF"])
    df = lf.collect()
    assert len(df) == 4

    # Row 2: INFO=AC;DP=15;AF=0.1 → AC=null, DP=15
    assert df["AC"][2] is None
    assert df["DP"][2] == 15
    af2 = df["AF"][2].to_list()
    assert abs(af2[0] - 0.1) < 1e-6


def test_flag_key_still_works_alongside_bare_keys():
    """Flag INFO fields ('GT_FLAG' present means true) coexist with bare
    non-Flag keys without disturbing the record."""
    df = pb.read_vcf(VCF_PATH, info_fields=["GT_FLAG", "DP", "AF"])

    # Row 3: INFO=GT_FLAG;DP=20;AF=0.3 → flag set, DP=20, AF=[0.3]
    assert bool(df["GT_FLAG"][3]) is True
    assert df["DP"][3] == 20
    af3 = df["AF"][3].to_list()
    assert abs(af3[0] - 0.3) < 1e-6

    # Rows without the flag should be False (or null, depending on schema)
    # Just ensure the flag row is the truthy one.
    assert bool(df["GT_FLAG"][0]) is False
