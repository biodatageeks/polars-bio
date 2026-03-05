"""Tests for custom SAM tag type inference in BAM, SAM, and CRAM files.

Uses nanopore_custom_tags.bam/cram test data (20 reads, Oxford Nanopore DRS)
with 26 custom tags (BAM) / 25 custom tags (CRAM, no pa array).

Covers all API variants:
- read_bam / scan_bam / register_bam
- read_sam / scan_sam / register_sam  (via SAM converted from nanopore BAM)
- read_cram / scan_cram / register_cram

Test scenarios:
- Tag type inference enabled (default): unknown tags get correct types from file
- Tag type inference disabled: unknown tags fall back to Utf8
- Tag type hints as fallback when inference disabled
- Inference overrides hints (file data wins)
- Invalid hint format returns error
- Mixed known (SAM spec) and unknown (nanopore) tags
- All 26/25 nanopore tags with correct types
- Custom sample size for inference
- Value validation (pt >= 0, 0 <= de <= 1)
"""

import polars as pl
import pysam
import pytest
from _expected import DATA_DIR

import polars_bio as pb

NANOPORE_BAM = f"{DATA_DIR}/io/bam/nanopore_custom_tags.bam"
NANOPORE_CRAM = f"{DATA_DIR}/io/cram/nanopore_custom_tags.cram"

# All 26 tags in the nanopore BAM
NANOPORE_BAM_TAGS = [
    "qs",
    "du",
    "ns",
    "ts",
    "mx",
    "ch",
    "st",
    "rn",
    "fn",
    "sm",
    "sd",
    "sv",
    "dx",
    "RG",
    "NM",
    "ms",
    "AS",
    "nn",
    "de",
    "tp",
    "cm",
    "s1",
    "MD",
    "rl",
    "pt",
    "pa",
]

# CRAM has 25 tags (no pa array — noodles CRAM decoder doesn't round-trip B-type arrays)
NANOPORE_CRAM_TAGS = [t for t in NANOPORE_BAM_TAGS if t != "pa"]

# Expected type categories for nanopore tags
INTEGER_TAGS = ["pt", "ch", "cm", "dx", "ms", "mx", "nn", "ns", "rl", "rn", "ts", "s1"]
FLOAT_TAGS = ["de", "du", "qs", "sd", "sm"]
STRING_TAGS = ["fn", "st", "sv"]
# Known SAM spec tags
KNOWN_INT_TAGS = ["NM", "AS"]
KNOWN_STR_TAGS = ["MD", "RG"]


@pytest.fixture(scope="module")
def nanopore_sam(tmp_path_factory):
    """Convert nanopore BAM to SAM for SAM-specific tests."""
    sam_path = str(tmp_path_factory.mktemp("sam") / "nanopore_custom_tags.sam")
    with pysam.AlignmentFile(NANOPORE_BAM, "rb") as bam_in:
        with pysam.AlignmentFile(sam_path, "wh", header=bam_in.header) as sam_out:
            for read in bam_in:
                sam_out.write(read)
    return sam_path


# =============================================================================
# BAM: read_bam tests
# =============================================================================


class TestBamReadTagInference:
    """Tests for custom tag type inference via read_bam."""

    def test_inference_enabled_integer_tag(self):
        """pt (poly-T tail length) should be inferred as Int32."""
        df = pb.read_bam(NANOPORE_BAM, tag_fields=["pt"])
        assert "pt" in df.columns
        assert df["pt"].dtype == pl.Int32

    def test_inference_enabled_float_tag(self):
        """de (error rate) should be inferred as Float32."""
        df = pb.read_bam(NANOPORE_BAM, tag_fields=["de"])
        assert "de" in df.columns
        assert df["de"].dtype == pl.Float32

    def test_inference_enabled_array_tag(self):
        """pa (poly-A boundaries) should be inferred as List type."""
        df = pb.read_bam(NANOPORE_BAM, tag_fields=["pa"])
        assert "pa" in df.columns
        assert df["pa"].dtype == pl.List(pl.Int32)

    def test_inference_enabled_char_tag(self):
        """tp (alignment type) is SAM type 'A' (character) -> Utf8."""
        df = pb.read_bam(NANOPORE_BAM, tag_fields=["tp"])
        assert "tp" in df.columns
        assert df["tp"].dtype == pl.Utf8

    def test_inference_disabled_falls_back_to_utf8(self):
        """With infer_tag_types=False, unknown tags should default to Utf8."""
        df = pb.read_bam(NANOPORE_BAM, tag_fields=["pt"], infer_tag_types=False)
        assert df["pt"].dtype == pl.Utf8

    def test_type_hints_when_inference_disabled(self):
        """With inference disabled, tag_type_hints should provide the type."""
        df = pb.read_bam(
            NANOPORE_BAM,
            tag_fields=["pt"],
            infer_tag_types=False,
            tag_type_hints=["pt:i"],
        )
        assert df["pt"].dtype == pl.Int32

    def test_float_hint_when_inference_disabled(self):
        """Float type hint (de:f) should work."""
        df = pb.read_bam(
            NANOPORE_BAM,
            tag_fields=["de"],
            infer_tag_types=False,
            tag_type_hints=["de:f"],
        )
        assert df["de"].dtype == pl.Float32

    def test_inference_overrides_hints(self):
        """Inference should win over hints: hint says Utf8, but file says Int32."""
        df = pb.read_bam(
            NANOPORE_BAM,
            tag_fields=["pt"],
            infer_tag_types=True,
            tag_type_hints=["pt:Z"],  # hint says string
        )
        assert df["pt"].dtype == pl.Int32

    def test_invalid_hint_missing_type(self):
        """Malformed hint (missing :TYPE) should raise an error."""
        with pytest.raises(BaseException):
            pb.read_bam(
                NANOPORE_BAM,
                tag_fields=["pt"],
                infer_tag_types=False,
                tag_type_hints=["pt"],  # missing :TYPE
            )

    def test_invalid_hint_extra_colons(self):
        """Malformed hint (too many colons) should raise an error."""
        with pytest.raises(BaseException):
            pb.read_bam(
                NANOPORE_BAM,
                tag_fields=["pt"],
                infer_tag_types=False,
                tag_type_hints=["pt:X:extra"],
            )

    def test_custom_sample_size(self):
        """Inference with a small sample size should still work."""
        df = pb.read_bam(
            NANOPORE_BAM,
            tag_fields=["pt", "de"],
            infer_tag_sample_size=5,
        )
        assert df["pt"].dtype == pl.Int32
        assert df["de"].dtype == pl.Float32

    def test_mixed_known_and_unknown_tags(self):
        """Mix of SAM spec tags (NM, MD) and nanopore custom tags (pt, de)."""
        df = pb.read_bam(NANOPORE_BAM, tag_fields=["NM", "MD", "pt", "de"])
        assert df["NM"].dtype == pl.Int32
        assert df["MD"].dtype == pl.Utf8
        assert df["pt"].dtype == pl.Int32
        assert df["de"].dtype == pl.Float32

    def test_all_26_nanopore_tags(self):
        """Read all 26 nanopore tags and verify their inferred types."""
        df = pb.read_bam(NANOPORE_BAM, tag_fields=NANOPORE_BAM_TAGS)
        assert len(df) == 20
        assert len(df.columns) == 12 + 26  # 12 core + 26 tags

        for tag in KNOWN_INT_TAGS:
            assert df[tag].dtype == pl.Int32, f"{tag} should be Int32"
        for tag in KNOWN_STR_TAGS:
            assert df[tag].dtype == pl.Utf8, f"{tag} should be Utf8"
        for tag in INTEGER_TAGS:
            assert df[tag].dtype == pl.Int32, f"{tag} should be Int32"
        for tag in FLOAT_TAGS:
            assert df[tag].dtype == pl.Float32, f"{tag} should be Float32"
        for tag in STRING_TAGS:
            assert df[tag].dtype == pl.Utf8, f"{tag} should be Utf8"
        assert df["tp"].dtype == pl.Utf8
        assert df["pa"].dtype == pl.List(pl.Int32)

    def test_row_count(self):
        """Nanopore BAM should have 20 reads."""
        df = pb.read_bam(NANOPORE_BAM)
        assert len(df) == 20

    def test_pt_values_non_negative(self):
        """pt (poly-T tail length) values should be non-negative integers."""
        df = pb.read_bam(NANOPORE_BAM, tag_fields=["pt"])
        non_null = df.filter(pl.col("pt").is_not_null())
        assert len(non_null) > 0
        assert all(v >= 0 for v in non_null["pt"].to_list())

    def test_de_values_float_range(self):
        """de (error rate) values should be floats between 0 and 1."""
        df = pb.read_bam(NANOPORE_BAM, tag_fields=["de"])
        non_null = df.filter(pl.col("de").is_not_null())
        assert len(non_null) > 0
        for v in non_null["de"].to_list():
            assert 0.0 <= v <= 1.0, f"de value {v} should be between 0 and 1"


# =============================================================================
# BAM: scan_bam tests
# =============================================================================


class TestBamScanTagInference:
    """Tests for tag inference via scan_bam (lazy) API."""

    def test_scan_with_inferred_tags(self):
        """scan_bam with inferred tags should produce correct types."""
        lf = pb.scan_bam(NANOPORE_BAM, tag_fields=["pt", "de", "ch"])
        df = lf.collect()
        assert df["pt"].dtype == pl.Int32
        assert df["de"].dtype == pl.Float32
        assert df["ch"].dtype == pl.Int32

    def test_scan_with_inference_disabled(self):
        """scan_bam with inference disabled falls back to Utf8."""
        lf = pb.scan_bam(NANOPORE_BAM, tag_fields=["pt"], infer_tag_types=False)
        df = lf.collect()
        assert df["pt"].dtype == pl.Utf8

    def test_scan_with_hints(self):
        """scan_bam with hints when inference disabled."""
        lf = pb.scan_bam(
            NANOPORE_BAM,
            tag_fields=["pt"],
            infer_tag_types=False,
            tag_type_hints=["pt:i"],
        )
        df = lf.collect()
        assert df["pt"].dtype == pl.Int32

    def test_scan_inference_overrides_hints(self):
        """scan_bam: inference should override hints."""
        lf = pb.scan_bam(
            NANOPORE_BAM,
            tag_fields=["pt"],
            infer_tag_types=True,
            tag_type_hints=["pt:Z"],
        )
        df = lf.collect()
        assert df["pt"].dtype == pl.Int32

    def test_scan_projection_pushdown(self):
        """Selecting only specific inferred tag columns works."""
        lf = pb.scan_bam(NANOPORE_BAM, tag_fields=["pt", "de", "NM"])
        df = lf.select(["name", "pt", "de"]).collect()
        assert df.columns == ["name", "pt", "de"]
        assert len(df) == 20

    def test_scan_filter_on_inferred_tag(self):
        """Filtering on inferred integer tag works."""
        lf = pb.scan_bam(NANOPORE_BAM, tag_fields=["pt"])
        df = lf.filter(pl.col("pt").is_not_null()).collect()
        assert len(df) > 0
        assert all(isinstance(v, int) for v in df["pt"].to_list())

    def test_scan_custom_sample_size(self):
        """scan_bam with custom sample size."""
        lf = pb.scan_bam(
            NANOPORE_BAM,
            tag_fields=["pt", "de"],
            infer_tag_sample_size=3,
        )
        df = lf.collect()
        assert df["pt"].dtype == pl.Int32
        assert df["de"].dtype == pl.Float32


# =============================================================================
# BAM: register_bam / SQL tests
# =============================================================================


class TestBamRegisterTagInference:
    """Tests for tag inference via register_bam + SQL API."""

    def test_register_with_inferred_tags(self):
        """register_bam with inferred tags should allow SQL queries."""
        pb.register_bam(
            NANOPORE_BAM,
            "nano_bam_infer",
            tag_fields=["pt", "de"],
        )
        result = pb.sql('SELECT "pt", "de" FROM nano_bam_infer LIMIT 5').collect()
        assert "pt" in result.columns
        assert "de" in result.columns
        assert result["pt"].dtype == pl.Int32
        assert result["de"].dtype == pl.Float32

    def test_register_with_inference_disabled(self):
        """register_bam with inference disabled falls back to Utf8."""
        pb.register_bam(
            NANOPORE_BAM,
            "nano_bam_no_infer",
            tag_fields=["pt"],
            infer_tag_types=False,
        )
        result = pb.sql('SELECT "pt" FROM nano_bam_no_infer LIMIT 5').collect()
        assert result["pt"].dtype == pl.Utf8

    def test_register_with_hints(self):
        """register_bam with hints when inference disabled."""
        pb.register_bam(
            NANOPORE_BAM,
            "nano_bam_hints",
            tag_fields=["pt"],
            infer_tag_types=False,
            tag_type_hints=["pt:i"],
        )
        result = pb.sql('SELECT "pt" FROM nano_bam_hints LIMIT 5').collect()
        assert result["pt"].dtype == pl.Int32

    def test_register_mixed_tags(self):
        """register_bam with mixed known and custom tags."""
        pb.register_bam(
            NANOPORE_BAM,
            "nano_bam_mixed",
            tag_fields=["pt", "NM", "de", "MD"],
        )
        result = pb.sql(
            'SELECT "pt", "NM", "de", "MD" FROM nano_bam_mixed LIMIT 5'
        ).collect()
        assert result["pt"].dtype == pl.Int32
        assert result["NM"].dtype == pl.Int32
        assert result["de"].dtype == pl.Float32
        assert result["MD"].dtype == pl.Utf8


# =============================================================================
# SAM: read_sam / scan_sam / register_sam tests
# =============================================================================


class TestSamReadTagInference:
    """Tests for custom tag type inference via read_sam."""

    def test_inference_enabled_integer_tag(self, nanopore_sam):
        """pt should be inferred as Int32 via read_sam."""
        df = pb.read_sam(nanopore_sam, tag_fields=["pt"])
        assert df["pt"].dtype == pl.Int32

    def test_inference_enabled_float_tag(self, nanopore_sam):
        """de should be inferred as Float32 via read_sam."""
        df = pb.read_sam(nanopore_sam, tag_fields=["de"])
        assert df["de"].dtype == pl.Float32

    def test_inference_disabled_falls_back_to_utf8(self, nanopore_sam):
        """read_sam with infer_tag_types=False should fall back to Utf8."""
        df = pb.read_sam(nanopore_sam, tag_fields=["pt"], infer_tag_types=False)
        assert df["pt"].dtype == pl.Utf8

    def test_type_hints_when_inference_disabled(self, nanopore_sam):
        """read_sam with hints when inference disabled."""
        df = pb.read_sam(
            nanopore_sam,
            tag_fields=["pt"],
            infer_tag_types=False,
            tag_type_hints=["pt:i"],
        )
        assert df["pt"].dtype == pl.Int32

    def test_inference_overrides_hints(self, nanopore_sam):
        """read_sam: inference should override hints."""
        df = pb.read_sam(
            nanopore_sam,
            tag_fields=["pt"],
            infer_tag_types=True,
            tag_type_hints=["pt:Z"],
        )
        assert df["pt"].dtype == pl.Int32

    def test_mixed_known_and_unknown_tags(self, nanopore_sam):
        """read_sam with mix of known and custom tags."""
        df = pb.read_sam(nanopore_sam, tag_fields=["NM", "MD", "pt", "de"])
        assert df["NM"].dtype == pl.Int32
        assert df["MD"].dtype == pl.Utf8
        assert df["pt"].dtype == pl.Int32
        assert df["de"].dtype == pl.Float32

    def test_row_count(self, nanopore_sam):
        """Nanopore SAM should have 20 reads."""
        df = pb.read_sam(nanopore_sam)
        assert len(df) == 20


class TestSamScanTagInference:
    """Tests for tag inference via scan_sam (lazy) API."""

    def test_scan_with_inferred_tags(self, nanopore_sam):
        """scan_sam with inferred tags should produce correct types."""
        lf = pb.scan_sam(nanopore_sam, tag_fields=["pt", "de"])
        df = lf.collect()
        assert df["pt"].dtype == pl.Int32
        assert df["de"].dtype == pl.Float32

    def test_scan_with_inference_disabled(self, nanopore_sam):
        """scan_sam with inference disabled falls back to Utf8."""
        lf = pb.scan_sam(nanopore_sam, tag_fields=["pt"], infer_tag_types=False)
        df = lf.collect()
        assert df["pt"].dtype == pl.Utf8

    def test_scan_with_hints(self, nanopore_sam):
        """scan_sam with hints when inference disabled."""
        lf = pb.scan_sam(
            nanopore_sam,
            tag_fields=["pt"],
            infer_tag_types=False,
            tag_type_hints=["pt:i"],
        )
        df = lf.collect()
        assert df["pt"].dtype == pl.Int32

    def test_scan_projection(self, nanopore_sam):
        """scan_sam projection pushdown with inferred tags."""
        lf = pb.scan_sam(nanopore_sam, tag_fields=["pt", "de", "NM"])
        df = lf.select(["name", "pt"]).collect()
        assert df.columns == ["name", "pt"]
        assert len(df) == 20


class TestSamRegisterTagInference:
    """Tests for tag inference via register_sam + SQL API."""

    def test_register_with_inferred_tags(self, nanopore_sam):
        """register_sam with inferred tags should allow SQL queries."""
        pb.register_sam(
            nanopore_sam,
            "nano_sam_infer",
            tag_fields=["pt", "de"],
        )
        result = pb.sql('SELECT "pt", "de" FROM nano_sam_infer LIMIT 5').collect()
        assert result["pt"].dtype == pl.Int32
        assert result["de"].dtype == pl.Float32

    def test_register_with_inference_disabled(self, nanopore_sam):
        """register_sam with inference disabled falls back to Utf8."""
        pb.register_sam(
            nanopore_sam,
            "nano_sam_no_infer",
            tag_fields=["pt"],
            infer_tag_types=False,
        )
        result = pb.sql('SELECT "pt" FROM nano_sam_no_infer LIMIT 5').collect()
        assert result["pt"].dtype == pl.Utf8

    def test_register_with_hints(self, nanopore_sam):
        """register_sam with hints when inference disabled."""
        pb.register_sam(
            nanopore_sam,
            "nano_sam_hints",
            tag_fields=["pt"],
            infer_tag_types=False,
            tag_type_hints=["pt:i"],
        )
        result = pb.sql('SELECT "pt" FROM nano_sam_hints LIMIT 5').collect()
        assert result["pt"].dtype == pl.Int32


# =============================================================================
# CRAM: read_cram tests
# =============================================================================


class TestCramReadTagInference:
    """Tests for custom tag type inference via read_cram."""

    def test_inference_enabled_integer_tag(self):
        """pt should be inferred as Int32."""
        df = pb.read_cram(NANOPORE_CRAM, tag_fields=["pt"])
        assert df["pt"].dtype == pl.Int32

    def test_inference_enabled_float_tag(self):
        """de should be inferred as Float32."""
        df = pb.read_cram(NANOPORE_CRAM, tag_fields=["de"])
        assert df["de"].dtype == pl.Float32

    def test_inference_disabled_falls_back_to_utf8(self):
        """With infer_tag_types=False, unknown tags should default to Utf8."""
        df = pb.read_cram(NANOPORE_CRAM, tag_fields=["pt"], infer_tag_types=False)
        assert df["pt"].dtype == pl.Utf8

    def test_type_hints_when_inference_disabled(self):
        """With inference disabled, tag_type_hints should provide the type."""
        df = pb.read_cram(
            NANOPORE_CRAM,
            tag_fields=["pt"],
            infer_tag_types=False,
            tag_type_hints=["pt:i"],
        )
        assert df["pt"].dtype == pl.Int32

    def test_float_hint_when_inference_disabled(self):
        """Float type hint (de:f) should work."""
        df = pb.read_cram(
            NANOPORE_CRAM,
            tag_fields=["de"],
            infer_tag_types=False,
            tag_type_hints=["de:f"],
        )
        assert df["de"].dtype == pl.Float32

    def test_inference_overrides_hints(self):
        """Inference should win over hints."""
        df = pb.read_cram(
            NANOPORE_CRAM,
            tag_fields=["pt"],
            infer_tag_types=True,
            tag_type_hints=["pt:Z"],
        )
        assert df["pt"].dtype == pl.Int32

    def test_invalid_hint_missing_type(self):
        """Malformed hint should raise an error."""
        with pytest.raises(BaseException):
            pb.read_cram(
                NANOPORE_CRAM,
                tag_fields=["pt"],
                infer_tag_types=False,
                tag_type_hints=["pt"],
            )

    def test_invalid_hint_extra_colons(self):
        """Malformed hint should raise an error."""
        with pytest.raises(BaseException):
            pb.read_cram(
                NANOPORE_CRAM,
                tag_fields=["pt"],
                infer_tag_types=False,
                tag_type_hints=["pt:X:extra"],
            )

    def test_custom_sample_size(self):
        """Inference with a small sample size should still work."""
        df = pb.read_cram(
            NANOPORE_CRAM,
            tag_fields=["pt", "de"],
            infer_tag_sample_size=5,
        )
        assert df["pt"].dtype == pl.Int32
        assert df["de"].dtype == pl.Float32

    def test_mixed_known_and_unknown_tags(self):
        """Mix of SAM spec tags and nanopore custom tags."""
        df = pb.read_cram(NANOPORE_CRAM, tag_fields=["NM", "MD", "pt", "de"])
        assert df["NM"].dtype == pl.Int32
        assert df["MD"].dtype == pl.Utf8
        assert df["pt"].dtype == pl.Int32
        assert df["de"].dtype == pl.Float32

    def test_all_25_nanopore_tags(self):
        """Read all 25 CRAM nanopore tags and verify inferred types."""
        df = pb.read_cram(NANOPORE_CRAM, tag_fields=NANOPORE_CRAM_TAGS)
        assert len(df) >= 20
        assert len(df.columns) == 12 + 25

        for tag in KNOWN_INT_TAGS:
            assert df[tag].dtype == pl.Int32, f"{tag} should be Int32"
        for tag in KNOWN_STR_TAGS:
            assert df[tag].dtype == pl.Utf8, f"{tag} should be Utf8"
        for tag in INTEGER_TAGS:
            assert df[tag].dtype == pl.Int32, f"{tag} should be Int32"
        for tag in FLOAT_TAGS:
            assert df[tag].dtype == pl.Float32, f"{tag} should be Float32"
        for tag in STRING_TAGS:
            assert df[tag].dtype == pl.Utf8, f"{tag} should be Utf8"
        assert df["tp"].dtype == pl.Utf8

    def test_row_count(self):
        """Nanopore CRAM should have at least 20 reads."""
        df = pb.read_cram(NANOPORE_CRAM)
        assert len(df) >= 20

    def test_pt_values_non_negative(self):
        """pt values should be non-negative integers."""
        df = pb.read_cram(NANOPORE_CRAM, tag_fields=["pt"])
        non_null = df.filter(pl.col("pt").is_not_null())
        assert len(non_null) > 0
        assert all(v >= 0 for v in non_null["pt"].to_list())

    def test_de_values_float_range(self):
        """de (error rate) values should be floats between 0 and 1."""
        df = pb.read_cram(NANOPORE_CRAM, tag_fields=["de"])
        non_null = df.filter(pl.col("de").is_not_null())
        assert len(non_null) > 0
        for v in non_null["de"].to_list():
            assert 0.0 <= v <= 1.0, f"de value {v} should be between 0 and 1"


# =============================================================================
# CRAM: scan_cram tests
# =============================================================================


class TestCramScanTagInference:
    """Tests for tag inference via scan_cram (lazy) API."""

    def test_scan_with_inferred_tags(self):
        """scan_cram with inferred tags should produce correct types."""
        lf = pb.scan_cram(NANOPORE_CRAM, tag_fields=["pt", "de", "ch"])
        df = lf.collect()
        assert df["pt"].dtype == pl.Int32
        assert df["de"].dtype == pl.Float32
        assert df["ch"].dtype == pl.Int32

    def test_scan_with_inference_disabled(self):
        """scan_cram with inference disabled falls back to Utf8."""
        lf = pb.scan_cram(NANOPORE_CRAM, tag_fields=["pt"], infer_tag_types=False)
        df = lf.collect()
        assert df["pt"].dtype == pl.Utf8

    def test_scan_with_hints(self):
        """scan_cram with hints when inference disabled."""
        lf = pb.scan_cram(
            NANOPORE_CRAM,
            tag_fields=["pt"],
            infer_tag_types=False,
            tag_type_hints=["pt:i"],
        )
        df = lf.collect()
        assert df["pt"].dtype == pl.Int32

    def test_scan_inference_overrides_hints(self):
        """scan_cram: inference should override hints."""
        lf = pb.scan_cram(
            NANOPORE_CRAM,
            tag_fields=["pt"],
            infer_tag_types=True,
            tag_type_hints=["pt:Z"],
        )
        df = lf.collect()
        assert df["pt"].dtype == pl.Int32

    def test_scan_projection_pushdown(self):
        """Selecting only specific inferred tag columns works."""
        lf = pb.scan_cram(NANOPORE_CRAM, tag_fields=["pt", "de", "NM"])
        df = lf.select(["name", "pt", "de"]).collect()
        assert df.columns == ["name", "pt", "de"]
        assert len(df) >= 20

    def test_scan_filter_on_inferred_tag(self):
        """Filtering on inferred integer tag works."""
        lf = pb.scan_cram(NANOPORE_CRAM, tag_fields=["pt"])
        df = lf.filter(pl.col("pt").is_not_null()).collect()
        assert len(df) > 0
        assert all(isinstance(v, int) for v in df["pt"].to_list())

    def test_scan_custom_sample_size(self):
        """scan_cram with custom sample size."""
        lf = pb.scan_cram(
            NANOPORE_CRAM,
            tag_fields=["pt", "de"],
            infer_tag_sample_size=3,
        )
        df = lf.collect()
        assert df["pt"].dtype == pl.Int32
        assert df["de"].dtype == pl.Float32


# =============================================================================
# CRAM: register_cram / SQL tests
# =============================================================================


class TestCramRegisterTagInference:
    """Tests for tag inference via register_cram + SQL API."""

    def test_register_with_inferred_tags(self):
        """register_cram with inferred tags should allow SQL queries."""
        pb.register_cram(
            NANOPORE_CRAM,
            "nano_cram_infer",
            tag_fields=["pt", "de"],
        )
        result = pb.sql('SELECT "pt", "de" FROM nano_cram_infer LIMIT 5').collect()
        assert result["pt"].dtype == pl.Int32
        assert result["de"].dtype == pl.Float32

    def test_register_with_inference_disabled(self):
        """register_cram with inference disabled falls back to Utf8."""
        pb.register_cram(
            NANOPORE_CRAM,
            "nano_cram_no_infer",
            tag_fields=["pt"],
            infer_tag_types=False,
        )
        result = pb.sql('SELECT "pt" FROM nano_cram_no_infer LIMIT 5').collect()
        assert result["pt"].dtype == pl.Utf8

    def test_register_with_hints(self):
        """register_cram with hints when inference disabled."""
        pb.register_cram(
            NANOPORE_CRAM,
            "nano_cram_hints",
            tag_fields=["pt"],
            infer_tag_types=False,
            tag_type_hints=["pt:i"],
        )
        result = pb.sql('SELECT "pt" FROM nano_cram_hints LIMIT 5').collect()
        assert result["pt"].dtype == pl.Int32

    def test_register_mixed_tags(self):
        """register_cram with mixed known and custom tags."""
        pb.register_cram(
            NANOPORE_CRAM,
            "nano_cram_mixed",
            tag_fields=["pt", "NM", "de", "MD"],
        )
        result = pb.sql(
            'SELECT "pt", "NM", "de", "MD" FROM nano_cram_mixed LIMIT 5'
        ).collect()
        assert result["pt"].dtype == pl.Int32
        assert result["NM"].dtype == pl.Int32
        assert result["de"].dtype == pl.Float32
        assert result["MD"].dtype == pl.Utf8
