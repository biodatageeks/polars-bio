"""Tests for VCF write functionality."""

import subprocess
import warnings
from pathlib import Path

import polars as pl
import pytest

import polars_bio as pb

TEST_DIR = Path(__file__).parent
DATA_DIR = TEST_DIR / "data"


def assert_dataframes_equal(
    df1: pl.DataFrame, df2: pl.DataFrame, float_tolerance: float = 1e-6
):
    """Compare DataFrames with tolerance for float columns."""
    assert df1.shape == df2.shape, f"Shape mismatch: {df1.shape} vs {df2.shape}"
    assert set(df1.columns) == set(
        df2.columns
    ), f"Column mismatch: {set(df1.columns)} vs {set(df2.columns)}"

    for col in df1.columns:
        if df1[col].dtype in (pl.Float32, pl.Float64):
            # Float comparison with tolerance
            if df1[col].null_count() == len(df1) and df2[col].null_count() == len(df2):
                continue  # Both all null
            diff = (df1[col] - df2[col]).abs()
            max_diff = diff.max()
            if max_diff is not None:
                assert (
                    max_diff < float_tolerance
                ), f"Float mismatch in {col}: max diff {max_diff}"
        else:
            assert df1[col].to_list() == df2[col].to_list(), f"Value mismatch in {col}"


class TestVcfWriteBasic:
    """Basic VCF write tests."""

    def test_write_vcf_uncompressed(self, tmp_path):
        """Test writing uncompressed VCF."""
        input_path = f"{DATA_DIR}/io/vcf/vep.vcf"
        output_path = tmp_path / "output.vcf"

        df = pb.read_vcf(input_path)
        row_count = pb.write_vcf(df, str(output_path))

        assert row_count == len(df)
        assert output_path.exists()

        # Verify we can read it back
        df2 = pb.read_vcf(str(output_path))
        assert len(df2) == len(df)

    def test_write_vcf_gz(self, tmp_path):
        """Test writing gzip-compressed VCF."""
        input_path = f"{DATA_DIR}/io/vcf/vep.vcf"
        output_path = tmp_path / "output.vcf.gz"

        df = pb.read_vcf(input_path)
        row_count = pb.write_vcf(df, str(output_path))

        assert row_count == len(df)
        assert output_path.exists()

        # Verify we can read it back
        df2 = pb.read_vcf(str(output_path))
        assert len(df2) == len(df)

    def test_write_auto_compression_detection(self, tmp_path):
        """Test that compression is auto-detected from extension."""
        input_path = f"{DATA_DIR}/io/vcf/vep.vcf"

        # Uncompressed
        output_vcf = tmp_path / "test.vcf"
        df = pb.read_vcf(input_path)
        pb.write_vcf(df, str(output_vcf))

        # Read first bytes to check it's not compressed
        with open(output_vcf, "rb") as f:
            first_bytes = f.read(2)
        assert first_bytes != b"\x1f\x8b", "File should not be gzip compressed"

        # Compressed
        output_gz = tmp_path / "test.vcf.gz"
        pb.write_vcf(df, str(output_gz))

        # Read first bytes to check it is compressed
        with open(output_gz, "rb") as f:
            first_bytes = f.read(2)
        assert first_bytes == b"\x1f\x8b", "File should be gzip compressed"


class TestVcfRoundTrip:
    """Round-trip tests for VCF read-write-read."""

    def test_basic_roundtrip(self, tmp_path):
        """Basic round-trip: read -> write -> read."""
        input_path = f"{DATA_DIR}/io/vcf/vep.vcf"
        output_path = tmp_path / "roundtrip.vcf"

        df1 = pb.read_vcf(input_path)
        pb.write_vcf(df1, str(output_path))
        df2 = pb.read_vcf(str(output_path))

        assert df1.shape[0] == df2.shape[0]
        # Check core columns are preserved
        assert df1["chrom"].to_list() == df2["chrom"].to_list()
        assert df1["ref"].to_list() == df2["ref"].to_list()

    def test_coordinate_system_roundtrip_one_based(self, tmp_path):
        """Test round-trip preserves coordinates in 1-based system.

        Coordinate system is read from DataFrame metadata automatically.
        """
        input_path = f"{DATA_DIR}/io/vcf/vep.vcf"
        output_path = tmp_path / "onebased.vcf"

        df1 = pb.read_vcf(input_path, use_zero_based=False)
        pb.write_vcf(df1, str(output_path))  # uses metadata
        df2 = pb.read_vcf(str(output_path), use_zero_based=False)

        assert df1["start"].to_list() == df2["start"].to_list()

    def test_coordinate_system_roundtrip_zero_based(self, tmp_path):
        """Test round-trip preserves coordinates in 0-based system.

        Coordinate system is read from DataFrame metadata automatically.
        """
        input_path = f"{DATA_DIR}/io/vcf/vep.vcf"
        output_path = tmp_path / "zerobased.vcf"

        df1 = pb.read_vcf(input_path, use_zero_based=True)
        pb.write_vcf(df1, str(output_path))  # uses metadata
        df2 = pb.read_vcf(str(output_path), use_zero_based=True)

        assert df1["start"].to_list() == df2["start"].to_list()


class TestEnsemblVcfRoundTrip:
    """Round-trip tests for ensembl.vcf - INFO-only single-sample VCF."""

    ENSEMBL_VCF = f"{DATA_DIR}/io/vcf/ensembl.vcf"

    def test_ensembl_basic_roundtrip(self, tmp_path):
        """Basic round-trip: read -> write -> read, verify row count and columns."""
        df1 = pb.read_vcf(self.ENSEMBL_VCF)
        output_path = tmp_path / "ensembl_out.vcf"
        pb.write_vcf(df1, str(output_path))
        df2 = pb.read_vcf(str(output_path))

        assert df1.shape[0] == df2.shape[0]
        # Core columns should match
        for col in ["chrom", "ref", "alt"]:
            if col in df1.columns and col in df2.columns:
                assert df1[col].to_list() == df2[col].to_list(), f"Mismatch in {col}"

    def test_ensembl_chrom_pos_preserved(self, tmp_path):
        """Verify chromosome and position are exactly preserved."""
        df1 = pb.read_vcf(self.ENSEMBL_VCF)
        output_path = tmp_path / "ensembl_coords.vcf"
        pb.write_vcf(df1, str(output_path))
        df2 = pb.read_vcf(str(output_path))

        # Chromosome 21 for both records
        assert df1["chrom"].to_list() == df2["chrom"].to_list()
        # Start positions should match
        assert df1["start"].to_list() == df2["start"].to_list()

    def test_ensembl_ref_alt_preserved(self, tmp_path):
        """Verify REF and ALT alleles are preserved."""
        df1 = pb.read_vcf(self.ENSEMBL_VCF)
        output_path = tmp_path / "ensembl_alleles.vcf"
        pb.write_vcf(df1, str(output_path))
        df2 = pb.read_vcf(str(output_path))

        assert df1["ref"].to_list() == df2["ref"].to_list()
        assert df1["alt"].to_list() == df2["alt"].to_list()

    def test_ensembl_compressed_roundtrip(self, tmp_path):
        """Test round-trip with GZIP compression."""
        df1 = pb.read_vcf(self.ENSEMBL_VCF)
        output_path = tmp_path / "ensembl_out.vcf.gz"
        pb.write_vcf(df1, str(output_path))
        df2 = pb.read_vcf(str(output_path))

        assert df1.shape[0] == df2.shape[0]


class TestMultisampleVcfRoundTrip:
    """Round-trip tests for multisample.vcf.gz - multi-sample VCF with FORMAT fields."""

    MULTISAMPLE_VCF = f"{DATA_DIR}/io/vcf/multisample.vcf.gz"
    FORMAT_FIELDS = ["GT"]

    def test_multisample_basic_roundtrip(self, tmp_path):
        """Basic round-trip: read -> write -> read.

        Note: We read without INFO fields to avoid metadata mismatch issues.
        VCF metadata (Number=A vs Number=.) is not preserved through Polars.
        """
        # Use explicit FORMAT projection to avoid parsing unsupported tags in output.
        df1 = pb.read_vcf(
            self.MULTISAMPLE_VCF, info_fields=[], format_fields=self.FORMAT_FIELDS
        )
        output_path = tmp_path / "multisample_out.vcf"
        pb.write_vcf(df1, str(output_path))
        df2 = pb.read_vcf(
            str(output_path), info_fields=[], format_fields=self.FORMAT_FIELDS
        )

        assert df1.shape[0] == df2.shape[0]

    def test_multisample_core_columns_preserved(self, tmp_path):
        """Verify core VCF columns are preserved."""
        df1 = pb.read_vcf(
            self.MULTISAMPLE_VCF, info_fields=[], format_fields=self.FORMAT_FIELDS
        )
        output_path = tmp_path / "multisample_core.vcf"
        pb.write_vcf(df1, str(output_path))
        df2 = pb.read_vcf(
            str(output_path), info_fields=[], format_fields=self.FORMAT_FIELDS
        )

        # Check core columns
        for col in ["chrom", "ref", "alt"]:
            if col in df1.columns and col in df2.columns:
                assert df1[col].to_list() == df2[col].to_list(), f"Mismatch in {col}"

    def test_multisample_format_columns_preserved(self, tmp_path):
        """Verify nested multi-sample FORMAT values and sample ids survive roundtrip."""
        df1 = pb.read_vcf(
            self.MULTISAMPLE_VCF, info_fields=[], format_fields=self.FORMAT_FIELDS
        )
        output_path = tmp_path / "multisample_format.vcf"
        pb.write_vcf(df1, str(output_path))
        df2 = pb.read_vcf(
            str(output_path), info_fields=[], format_fields=self.FORMAT_FIELDS
        )

        assert "genotypes" in df1.columns
        assert "genotypes" in df2.columns
        first_sample_ids_1 = [
            entry["sample_id"] for entry in df1["genotypes"].to_list()[0]
        ]
        first_sample_ids_2 = [
            entry["sample_id"] for entry in df2["genotypes"].to_list()[0]
        ]
        assert first_sample_ids_1 == first_sample_ids_2

        # Validate that first-row GT values are preserved (not converted to nulls)
        first_values_1 = {
            entry["sample_id"]: (entry.get("values") or {}).get("GT")
            for entry in df1["genotypes"].to_list()[0]
        }
        first_values_2 = {
            entry["sample_id"]: (entry.get("values") or {}).get("GT")
            for entry in df2["genotypes"].to_list()[0]
        }
        assert first_values_1 == first_values_2


class TestVcfSink:
    """Tests for sink_vcf streaming write."""

    def test_sink_vcf_lazy_evaluation(self, tmp_path):
        """Test sink_vcf with LazyFrame."""
        input_path = f"{DATA_DIR}/io/vcf/vep.vcf"
        output_path = tmp_path / "sink_output.vcf"

        lf = pb.scan_vcf(input_path)
        pb.sink_vcf(lf, str(output_path))

        assert output_path.exists()
        df = pb.read_vcf(str(output_path))
        assert len(df) == 2

    def test_sink_vcf_with_filter(self, tmp_path):
        """Test sink_vcf with filtered LazyFrame."""
        input_path = f"{DATA_DIR}/io/vcf/multisample.vcf"
        output_path = tmp_path / "sink_filtered.vcf"

        lf = pb.scan_vcf(input_path).filter(pl.col("chrom") == "1")
        pb.sink_vcf(lf, str(output_path))

        assert output_path.exists()
        df = pb.read_vcf(str(output_path))
        assert len(df) == 3  # All 3 records are on chromosome 1

    def test_sink_vcf_multisample_format_header_metadata(self, tmp_path):
        """Test sink_vcf preserves multisample FORMAT Number/Type/Description."""
        input_path = f"{DATA_DIR}/io/vcf/multisample.vcf.gz"
        output_path = tmp_path / "sink_multisample_header.vcf"

        lf = pb.scan_vcf(input_path, info_fields=[], format_fields=["GT", "DP", "GQ"])
        expected_format_meta = pb.get_metadata(lf)["header"]["format_fields"]
        pb.sink_vcf(lf, str(output_path))

        format_lines = {}
        with open(output_path, "rt") as handle:
            for line in handle:
                if line.startswith("##FORMAT=<ID=GT,"):
                    format_lines["GT"] = line.strip()
                elif line.startswith("##FORMAT=<ID=DP,"):
                    format_lines["DP"] = line.strip()
                elif line.startswith("##FORMAT=<ID=GQ,"):
                    format_lines["GQ"] = line.strip()
                elif line.startswith("#CHROM"):
                    break

        for format_id in ["GT", "DP", "GQ"]:
            expected = expected_format_meta[format_id]
            line = format_lines[format_id]
            assert f'Number={expected["number"]}' in line
            assert f'Type={expected["type"]}' in line
            assert f'Description="{expected["description"]}"' in line
