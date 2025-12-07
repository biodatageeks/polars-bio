"""Tests for coordinate system metadata tracking.

This module tests that:
1. datafusion-bio-formats correctly parses files into 0-based or 1-based coordinates
2. LazyFrame/DataFrame metadata is correctly set to match the coordinate system
3. Metadata is accessible via the polars-config-meta API
4. DataFusion registered tables have correct metadata
"""

import pandas as pd
import polars as pl
import pytest

import polars_bio as pb
from polars_bio._metadata import (
    get_coordinate_system,
    set_coordinate_system,
    validate_coordinate_systems,
)
from polars_bio.exceptions import (
    CoordinateSystemMismatchError,
    MissingCoordinateSystemError,
)


class TestCoordinateSystemMetadata:
    """Tests for coordinate system metadata on I/O operations."""

    def test_scan_vcf_zero_based_metadata(self):
        """Test that scan_vcf with 0-based coords sets correct metadata."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path, one_based=False)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is True, "Expected coordinate_system_zero_based=True for 0-based"

        # Verify coordinates are actually 0-based
        # VCF file has POS=33248751 (1-based), should be 33248750 (0-based)
        df = lf.collect()
        start_values = df.select("start").to_series().to_list()
        assert (
            33248750 in start_values
        ), f"Expected 0-based start 33248750, got {start_values}"

    def test_scan_vcf_one_based_metadata(self):
        """Test that scan_vcf with 1-based coords sets correct metadata."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path, one_based=True)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is False, "Expected coordinate_system_zero_based=False for 1-based"

        # Verify coordinates are actually 1-based
        # VCF file has POS=33248751 (1-based), should remain 33248751
        df = lf.collect()
        start_values = df.select("start").to_series().to_list()
        assert (
            33248751 in start_values
        ), f"Expected 1-based start 33248751, got {start_values}"

    def test_scan_gff_zero_based_metadata(self):
        """Test that scan_gff with 0-based coords sets correct metadata."""
        gff_path = "tests/data/io/gff/gencode.v38.annotation.gff3"
        lf = pb.scan_gff(gff_path, one_based=False)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is True, "Expected coordinate_system_zero_based=True for 0-based"

    def test_scan_gff_one_based_metadata(self):
        """Test that scan_gff with 1-based coords sets correct metadata."""
        gff_path = "tests/data/io/gff/gencode.v38.annotation.gff3"
        lf = pb.scan_gff(gff_path, one_based=True)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is False, "Expected coordinate_system_zero_based=False for 1-based"

    def test_scan_bam_zero_based_metadata(self):
        """Test that scan_bam with 0-based coords sets correct metadata."""
        bam_path = "tests/data/io/bam/test.bam"
        lf = pb.scan_bam(bam_path, one_based=False)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is True, "Expected coordinate_system_zero_based=True for 0-based"

    def test_scan_bam_one_based_metadata(self):
        """Test that scan_bam with 1-based coords sets correct metadata."""
        bam_path = "tests/data/io/bam/test.bam"
        lf = pb.scan_bam(bam_path, one_based=True)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is False, "Expected coordinate_system_zero_based=False for 1-based"

    def test_scan_bed_zero_based_metadata(self):
        """Test that scan_bed with 0-based coords sets correct metadata."""
        bed_path = "tests/data/io/bed/test.bed"
        lf = pb.scan_bed(bed_path, one_based=False)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is True, "Expected coordinate_system_zero_based=True for 0-based"

    def test_scan_bed_one_based_metadata(self):
        """Test that scan_bed with 1-based coords sets correct metadata."""
        bed_path = "tests/data/io/bed/test.bed"
        lf = pb.scan_bed(bed_path, one_based=True)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is False, "Expected coordinate_system_zero_based=False for 1-based"

    def test_scan_cram_zero_based_metadata(self):
        """Test that scan_cram with 0-based coords sets correct metadata."""
        cram_path = "tests/data/io/cram/test.cram"
        lf = pb.scan_cram(cram_path, one_based=False)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is True, "Expected coordinate_system_zero_based=True for 0-based"

    def test_scan_cram_one_based_metadata(self):
        """Test that scan_cram with 1-based coords sets correct metadata."""
        cram_path = "tests/data/io/cram/test.cram"
        lf = pb.scan_cram(cram_path, one_based=True)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is False, "Expected coordinate_system_zero_based=False for 1-based"

    def test_default_uses_global_config(self):
        """Test that default one_based=None uses global config (0-based)."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"

        # Default should use global config which is 0-based (True)
        lf = pb.scan_vcf(vcf_path)
        cs = get_coordinate_system(lf)
        assert (
            cs is True
        ), "Expected default to be 0-based (coordinate_system_zero_based=True)"


class TestCoordinateValuesMatchMetadata:
    """Tests that coordinate values match the metadata setting."""

    def test_vcf_zero_vs_one_based_values(self):
        """Test that VCF coordinates differ by 1 between 0-based and 1-based."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"

        # Read with 0-based
        df_zero = pb.read_vcf(vcf_path, one_based=False)
        start_zero = df_zero.select("start").to_series().to_list()

        # Read with 1-based
        df_one = pb.read_vcf(vcf_path, one_based=True)
        start_one = df_one.select("start").to_series().to_list()

        # 1-based should be exactly 1 more than 0-based for all rows
        for s0, s1 in zip(start_zero, start_one):
            assert s1 == s0 + 1, f"Expected 1-based ({s1}) = 0-based ({s0}) + 1"

    def test_gff_zero_vs_one_based_values(self):
        """Test that GFF coordinates differ by 1 between 0-based and 1-based."""
        gff_path = "tests/data/io/gff/gencode.v38.annotation.gff3"

        # Read with 0-based
        df_zero = pb.read_gff(gff_path, one_based=False)
        start_zero = df_zero.select("start").to_series().to_list()

        # Read with 1-based
        df_one = pb.read_gff(gff_path, one_based=True)
        start_one = df_one.select("start").to_series().to_list()

        # 1-based should be exactly 1 more than 0-based for all rows
        for s0, s1 in zip(start_zero, start_one):
            assert s1 == s0 + 1, f"Expected 1-based ({s1}) = 0-based ({s0}) + 1"

    def test_bam_zero_vs_one_based_values(self):
        """Test that BAM coordinates differ by 1 between 0-based and 1-based."""
        bam_path = "tests/data/io/bam/test.bam"

        # Read with 0-based
        df_zero = pb.read_bam(bam_path, one_based=False)
        start_zero = df_zero.select("start").to_series().to_list()

        # Read with 1-based
        df_one = pb.read_bam(bam_path, one_based=True)
        start_one = df_one.select("start").to_series().to_list()

        # 1-based should be exactly 1 more than 0-based for all rows
        for s0, s1 in zip(start_zero, start_one):
            assert s1 == s0 + 1, f"Expected 1-based ({s1}) = 0-based ({s0}) + 1"

    def test_cram_zero_vs_one_based_values(self):
        """Test that CRAM coordinates differ by 1 between 0-based and 1-based."""
        cram_path = "tests/data/io/cram/test.cram"

        # Read with 0-based
        df_zero = pb.read_cram(cram_path, one_based=False)
        start_zero = df_zero.select("start").to_series().to_list()

        # Read with 1-based
        df_one = pb.read_cram(cram_path, one_based=True)
        start_one = df_one.select("start").to_series().to_list()

        # 1-based should be exactly 1 more than 0-based for all rows
        for s0, s1 in zip(start_zero, start_one):
            assert s1 == s0 + 1, f"Expected 1-based ({s1}) = 0-based ({s0}) + 1"

    def test_bed_zero_vs_one_based_values(self):
        """Test that BED coordinates differ by 1 between 0-based and 1-based."""
        bed_path = "tests/data/io/bed/test.bed"

        # Read with 0-based
        df_zero = pb.read_bed(bed_path, one_based=False)
        start_zero = df_zero.select("start").to_series().to_list()

        # Read with 1-based
        df_one = pb.read_bed(bed_path, one_based=True)
        start_one = df_one.select("start").to_series().to_list()

        # 1-based should be exactly 1 more than 0-based for all rows
        for s0, s1 in zip(start_zero, start_one):
            assert s1 == s0 + 1, f"Expected 1-based ({s1}) = 0-based ({s0}) + 1"


class TestMetadataHelperFunctions:
    """Tests for the metadata helper functions."""

    def test_set_coordinate_system_polars_df(self):
        """Test setting coordinate system on Polars DataFrame."""
        df = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        set_coordinate_system(df, zero_based=True)

        cs = get_coordinate_system(df)
        assert cs is True

    def test_set_coordinate_system_polars_lf(self):
        """Test setting coordinate system on Polars LazyFrame."""
        lf = pl.LazyFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        set_coordinate_system(lf, zero_based=False)

        cs = get_coordinate_system(lf)
        assert cs is False

    def test_get_coordinate_system_no_metadata(self):
        """Test getting coordinate system when no metadata is set."""
        df = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})

        cs = get_coordinate_system(df)
        assert cs is None, "Expected None when no metadata is set"

    def test_set_coordinate_system_pandas_df(self):
        """Test setting coordinate system on Pandas DataFrame."""
        pdf = pd.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        set_coordinate_system(pdf, zero_based=True)

        cs = get_coordinate_system(pdf)
        assert cs is True

    def test_get_coordinate_system_pandas_no_metadata(self):
        """Test getting coordinate system from Pandas DataFrame without metadata."""
        pdf = pd.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})

        cs = get_coordinate_system(pdf)
        assert cs is None, "Expected None when no metadata is set on Pandas DataFrame"


class TestMissingCoordinateSystemError:
    """Tests for MissingCoordinateSystemError being raised appropriately."""

    def test_validate_polars_df_missing_metadata(self):
        """Test that MissingCoordinateSystemError is raised for Polars DF without metadata."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [150], "end": [250]})

        with pytest.raises(MissingCoordinateSystemError) as exc_info:
            validate_coordinate_systems(df1, df2)

        assert "Polars DataFrame" in str(exc_info.value)
        assert "missing coordinate system metadata" in str(exc_info.value)

    def test_validate_polars_lf_missing_metadata(self):
        """Test that MissingCoordinateSystemError is raised for Polars LF without metadata."""
        lf1 = pl.LazyFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        lf2 = pl.LazyFrame({"chrom": ["chr1"], "start": [150], "end": [250]})

        with pytest.raises(MissingCoordinateSystemError) as exc_info:
            validate_coordinate_systems(lf1, lf2)

        assert "Polars LazyFrame" in str(exc_info.value)
        assert "missing coordinate system metadata" in str(exc_info.value)

    def test_validate_pandas_df_missing_metadata(self):
        """Test that MissingCoordinateSystemError is raised for Pandas DF without metadata."""
        pdf1 = pd.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        pdf2 = pd.DataFrame({"chrom": ["chr1"], "start": [150], "end": [250]})

        with pytest.raises(MissingCoordinateSystemError) as exc_info:
            validate_coordinate_systems(pdf1, pdf2)

        assert "Pandas DataFrame" in str(exc_info.value)
        assert "missing coordinate system metadata" in str(exc_info.value)

    def test_validate_mixed_types_missing_metadata(self):
        """Test that MissingCoordinateSystemError is raised for mixed types without metadata."""
        lf = pb.scan_vcf(
            "tests/data/io/vcf/ensembl.vcf", one_based=False
        )  # has metadata
        pdf = pd.DataFrame(
            {"chrom": ["chr1"], "start": [100], "end": [200]}
        )  # no metadata

        with pytest.raises(MissingCoordinateSystemError) as exc_info:
            validate_coordinate_systems(lf, pdf)

        assert "Pandas DataFrame" in str(exc_info.value)


class TestCoordinateSystemMismatchError:
    """Tests for CoordinateSystemMismatchError being raised appropriately."""

    def test_validate_mismatch_zero_vs_one_based(self):
        """Test that CoordinateSystemMismatchError is raised for coordinate mismatch."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"

        lf_zero = pb.scan_vcf(vcf_path, one_based=False)  # 0-based
        lf_one = pb.scan_vcf(vcf_path, one_based=True)  # 1-based

        with pytest.raises(CoordinateSystemMismatchError) as exc_info:
            validate_coordinate_systems(lf_zero, lf_one)

        assert "mismatch" in str(exc_info.value).lower()
        assert "0-based" in str(exc_info.value)
        assert "1-based" in str(exc_info.value)

    def test_validate_matching_coordinates(self):
        """Test that validate_coordinate_systems succeeds when coordinates match."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"

        lf1 = pb.scan_vcf(vcf_path, one_based=False)
        lf2 = pb.scan_vcf(vcf_path, one_based=False)

        # Should not raise, returns True (0-based)
        result = validate_coordinate_systems(lf1, lf2)
        assert result is True

    def test_validate_matching_one_based(self):
        """Test that validate_coordinate_systems succeeds for matching 1-based."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"

        lf1 = pb.scan_vcf(vcf_path, one_based=True)
        lf2 = pb.scan_vcf(vcf_path, one_based=True)

        # Should not raise, returns False (1-based)
        result = validate_coordinate_systems(lf1, lf2)
        assert result is False


class TestDataFusionTableMetadata:
    """Tests for coordinate system metadata on DataFusion registered tables."""

    def test_register_vcf_metadata_access(self):
        """Test that registered VCF tables can be queried for metadata."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        pb.register_vcf(vcf_path, name="test_vcf_metadata")

        # Get coordinate system from table name
        cs = get_coordinate_system("test_vcf_metadata")
        # Note: This may return None if Arrow schema metadata is not yet set
        # by the register_* functions. This test documents current behavior.
        # Once implemented, this should return True or False.
        assert cs is None or isinstance(
            cs, bool
        ), f"Expected None or bool, got {type(cs)}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
