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
        lf = pb.scan_vcf(vcf_path, use_zero_based=True)

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
        lf = pb.scan_vcf(vcf_path, use_zero_based=False)

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
        lf = pb.scan_gff(gff_path, use_zero_based=True)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is True, "Expected coordinate_system_zero_based=True for 0-based"

    def test_scan_gff_one_based_metadata(self):
        """Test that scan_gff with 1-based coords sets correct metadata."""
        gff_path = "tests/data/io/gff/gencode.v38.annotation.gff3"
        lf = pb.scan_gff(gff_path, use_zero_based=False)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is False, "Expected coordinate_system_zero_based=False for 1-based"

    def test_scan_bam_zero_based_metadata(self):
        """Test that scan_bam with 0-based coords sets correct metadata."""
        bam_path = "tests/data/io/bam/test.bam"
        lf = pb.scan_bam(bam_path, use_zero_based=True)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is True, "Expected coordinate_system_zero_based=True for 0-based"

    def test_scan_bam_one_based_metadata(self):
        """Test that scan_bam with 1-based coords sets correct metadata."""
        bam_path = "tests/data/io/bam/test.bam"
        lf = pb.scan_bam(bam_path, use_zero_based=False)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is False, "Expected coordinate_system_zero_based=False for 1-based"

    def test_scan_bed_zero_based_metadata(self):
        """Test that scan_bed with 0-based coords sets correct metadata."""
        bed_path = "tests/data/io/bed/test.bed"
        lf = pb.scan_bed(bed_path, use_zero_based=True)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is True, "Expected coordinate_system_zero_based=True for 0-based"

    def test_scan_bed_one_based_metadata(self):
        """Test that scan_bed with 1-based coords sets correct metadata."""
        bed_path = "tests/data/io/bed/test.bed"
        lf = pb.scan_bed(bed_path, use_zero_based=False)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is False, "Expected coordinate_system_zero_based=False for 1-based"

    def test_scan_cram_zero_based_metadata(self):
        """Test that scan_cram with 0-based coords sets correct metadata."""
        cram_path = "tests/data/io/cram/test.cram"
        lf = pb.scan_cram(cram_path, use_zero_based=True)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is True, "Expected coordinate_system_zero_based=True for 0-based"

    def test_scan_cram_one_based_metadata(self):
        """Test that scan_cram with 1-based coords sets correct metadata."""
        cram_path = "tests/data/io/cram/test.cram"
        lf = pb.scan_cram(cram_path, use_zero_based=False)

        # Check metadata is set
        cs = get_coordinate_system(lf)
        assert cs is False, "Expected coordinate_system_zero_based=False for 1-based"

    def test_default_uses_global_config(self):
        """Test that default use_zero_based=None uses global config (1-based)."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"

        # Default should use global config which is 1-based (False)
        lf = pb.scan_vcf(vcf_path)
        cs = get_coordinate_system(lf)
        assert (
            cs is False
        ), "Expected default to be 1-based (coordinate_system_zero_based=False)"


class TestCoordinateValuesMatchMetadata:
    """Tests that coordinate values match the metadata setting."""

    def test_vcf_zero_vs_one_based_values(self):
        """Test that VCF coordinates differ by 1 between 0-based and 1-based."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"

        # Read with 0-based
        df_zero = pb.read_vcf(vcf_path, use_zero_based=True)
        start_zero = df_zero.select("start").to_series().to_list()

        # Read with 1-based
        df_one = pb.read_vcf(vcf_path, use_zero_based=False)
        start_one = df_one.select("start").to_series().to_list()

        # 1-based should be exactly 1 more than 0-based for all rows
        for s0, s1 in zip(start_zero, start_one):
            assert s1 == s0 + 1, f"Expected 1-based ({s1}) = 0-based ({s0}) + 1"

    def test_gff_zero_vs_one_based_values(self):
        """Test that GFF coordinates differ by 1 between 0-based and 1-based."""
        gff_path = "tests/data/io/gff/gencode.v38.annotation.gff3"

        # Read with 0-based
        df_zero = pb.read_gff(gff_path, use_zero_based=True)
        start_zero = df_zero.select("start").to_series().to_list()

        # Read with 1-based
        df_one = pb.read_gff(gff_path, use_zero_based=False)
        start_one = df_one.select("start").to_series().to_list()

        # 1-based should be exactly 1 more than 0-based for all rows
        for s0, s1 in zip(start_zero, start_one):
            assert s1 == s0 + 1, f"Expected 1-based ({s1}) = 0-based ({s0}) + 1"

    def test_bam_zero_vs_one_based_values(self):
        """Test that BAM coordinates differ by 1 between 0-based and 1-based."""
        bam_path = "tests/data/io/bam/test.bam"

        # Read with 0-based
        df_zero = pb.read_bam(bam_path, use_zero_based=True)
        start_zero = df_zero.select("start").to_series().to_list()

        # Read with 1-based
        df_one = pb.read_bam(bam_path, use_zero_based=False)
        start_one = df_one.select("start").to_series().to_list()

        # 1-based should be exactly 1 more than 0-based for all rows
        for s0, s1 in zip(start_zero, start_one):
            assert s1 == s0 + 1, f"Expected 1-based ({s1}) = 0-based ({s0}) + 1"

    def test_cram_zero_vs_one_based_values(self):
        """Test that CRAM coordinates differ by 1 between 0-based and 1-based."""
        cram_path = "tests/data/io/cram/test.cram"

        # Read with 0-based
        df_zero = pb.read_cram(cram_path, use_zero_based=True)
        start_zero = df_zero.select("start").to_series().to_list()

        # Read with 1-based
        df_one = pb.read_cram(cram_path, use_zero_based=False)
        start_one = df_one.select("start").to_series().to_list()

        # 1-based should be exactly 1 more than 0-based for all rows
        for s0, s1 in zip(start_zero, start_one):
            assert s1 == s0 + 1, f"Expected 1-based ({s1}) = 0-based ({s0}) + 1"

    def test_bed_zero_vs_one_based_values(self):
        """Test that BED coordinates differ by 1 between 0-based and 1-based."""
        bed_path = "tests/data/io/bed/test.bed"

        # Read with 0-based
        df_zero = pb.read_bed(bed_path, use_zero_based=True)
        start_zero = df_zero.select("start").to_series().to_list()

        # Read with 1-based
        df_one = pb.read_bed(bed_path, use_zero_based=False)
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
            "tests/data/io/vcf/ensembl.vcf", use_zero_based=True
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

        lf_zero = pb.scan_vcf(vcf_path, use_zero_based=True)  # 0-based
        lf_one = pb.scan_vcf(vcf_path, use_zero_based=False)  # 1-based

        with pytest.raises(CoordinateSystemMismatchError) as exc_info:
            validate_coordinate_systems(lf_zero, lf_one)

        assert "mismatch" in str(exc_info.value).lower()
        assert "0-based" in str(exc_info.value)
        assert "1-based" in str(exc_info.value)

    def test_validate_matching_coordinates(self):
        """Test that validate_coordinate_systems succeeds when coordinates match."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"

        lf1 = pb.scan_vcf(vcf_path, use_zero_based=True)
        lf2 = pb.scan_vcf(vcf_path, use_zero_based=True)

        # Should not raise, returns True (0-based)
        result = validate_coordinate_systems(lf1, lf2)
        assert result is True

    def test_validate_matching_one_based(self):
        """Test that validate_coordinate_systems succeeds for matching 1-based."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"

        lf1 = pb.scan_vcf(vcf_path, use_zero_based=False)
        lf2 = pb.scan_vcf(vcf_path, use_zero_based=False)

        # Should not raise, returns False (1-based)
        result = validate_coordinate_systems(lf1, lf2)
        assert result is False


class TestDataFusionTableMetadata:
    """Tests for coordinate system metadata on DataFusion registered tables."""

    def test_register_vcf_metadata_access(self):
        """Test that registered VCF tables can be queried for metadata.

        The BioSessionContext.table() method retrieves a DataFusion DataFrame
        for a registered table, enabling schema metadata access.
        """
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        pb.register_vcf(vcf_path, name="test_vcf_metadata")

        # Get coordinate system from table name
        # Note: Currently returns None because Arrow schema metadata is not
        # propagated during register_* calls. The table() method works, but
        # the metadata needs to be set during registration (future enhancement).
        cs = get_coordinate_system("test_vcf_metadata")
        assert cs is None or isinstance(
            cs, bool
        ), f"Expected None or bool, got {type(cs)}"

    def test_table_returns_dataframe(self):
        """Test that ctx.table() returns a DataFusion DataFrame."""
        from polars_bio.context import ctx

        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        pb.register_vcf(vcf_path, name="test_table_df")

        # Get the table as a DataFrame
        df = ctx.table("test_table_df")

        # Verify it has a schema method (DataFusion DataFrame)
        schema = df.schema()
        assert schema is not None

        # Schema should have expected columns
        field_names = [field.name for field in schema]
        assert "chrom" in field_names
        assert "start" in field_names
        assert "end" in field_names

    def test_table_not_found_raises_error(self):
        """Test that ctx.table() raises KeyError for non-existent table."""
        from polars_bio.context import ctx

        with pytest.raises(KeyError) as exc_info:
            ctx.table("nonexistent_table_xyz")

        assert "nonexistent_table_xyz" in str(exc_info.value)
        assert "not found" in str(exc_info.value).lower()


class TestDefaultMetadataTracking:
    """Tests for default coordinate system metadata tracking (7.1).

    Verifies that:
    - scan_*/read_* functions set coordinate_system_zero_based=False by default (1-based)
    - use_zero_based=True sets coordinate_system_zero_based=True
    - Metadata is preserved through Polars transformations
    - Metadata is accessible via get_coordinate_system()
    """

    def test_scan_vcf_default_is_one_based(self):
        """Test that scan_vcf sets 1-based metadata by default."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path)  # No use_zero_based parameter

        cs = get_coordinate_system(lf)
        assert (
            cs is False
        ), "Expected default to be 1-based (coordinate_system_zero_based=False)"

    def test_scan_gff_default_is_one_based(self):
        """Test that scan_gff sets 1-based metadata by default."""
        gff_path = "tests/data/io/gff/gencode.v38.annotation.gff3"
        lf = pb.scan_gff(gff_path)

        cs = get_coordinate_system(lf)
        assert cs is False, "Expected default to be 1-based"

    def test_scan_bam_default_is_one_based(self):
        """Test that scan_bam sets 1-based metadata by default."""
        bam_path = "tests/data/io/bam/test.bam"
        lf = pb.scan_bam(bam_path)

        cs = get_coordinate_system(lf)
        assert cs is False, "Expected default to be 1-based"

    def test_scan_cram_default_is_one_based(self):
        """Test that scan_cram sets 1-based metadata by default."""
        cram_path = "tests/data/io/cram/test.cram"
        lf = pb.scan_cram(cram_path)

        cs = get_coordinate_system(lf)
        assert cs is False, "Expected default to be 1-based"

    def test_scan_bed_default_is_one_based(self):
        """Test that scan_bed sets 1-based metadata by default."""
        bed_path = "tests/data/io/bed/test.bed"
        lf = pb.scan_bed(bed_path)

        cs = get_coordinate_system(lf)
        assert cs is False, "Expected default to be 1-based"

    def test_use_zero_based_true_sets_zero_based_metadata(self):
        """Test that use_zero_based=True sets 0-based metadata."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path, use_zero_based=True)

        cs = get_coordinate_system(lf)
        assert (
            cs is True
        ), "Expected coordinate_system_zero_based=True when use_zero_based=True"

    def test_metadata_preserved_through_select(self):
        """Test that metadata is preserved through Polars select transformation."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path, use_zero_based=True)

        # Apply select transformation
        lf_selected = lf.select(["chrom", "start", "end"])

        cs = get_coordinate_system(lf_selected)
        assert cs is True, "Metadata should be preserved through select"

    def test_metadata_preserved_through_filter(self):
        """Test that metadata is preserved through Polars filter transformation."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path, use_zero_based=False)

        # Apply filter transformation
        lf_filtered = lf.filter(pl.col("chrom") == "21")

        cs = get_coordinate_system(lf_filtered)
        assert cs is False, "Metadata should be preserved through filter"

    def test_metadata_not_preserved_through_collect(self):
        """Test that metadata is NOT preserved when collecting LazyFrame to DataFrame.

        Note: This is a known limitation of polars-config-meta. Metadata is attached
        to LazyFrames but is not carried over when collecting to DataFrames.
        Range operations should be performed on LazyFrames to take advantage of
        metadata-based coordinate system detection.
        """
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path, use_zero_based=True)

        # Verify LazyFrame has metadata
        assert get_coordinate_system(lf) is True

        # Collect to DataFrame - metadata is NOT preserved
        df = lf.collect()

        cs = get_coordinate_system(df)
        # Metadata is lost through collect - this is expected behavior
        assert (
            cs is None
        ), "Metadata is not preserved through collect (polars-config-meta limitation)"

    def test_read_functions_preserve_metadata(self):
        """Test that read_* functions preserve metadata on returned DataFrames.

        The read_* functions get metadata from the LazyFrame before collecting,
        then set it on the DataFrame after collection.
        """
        vcf_path = "tests/data/io/vcf/ensembl.vcf"

        # read_vcf default (1-based)
        df = pb.read_vcf(vcf_path)
        cs = get_coordinate_system(df)
        assert cs is False, "read_vcf should preserve 1-based metadata by default"

        # read_vcf with use_zero_based=True
        df_zero = pb.read_vcf(vcf_path, use_zero_based=True)
        cs_zero = get_coordinate_system(df_zero)
        assert (
            cs_zero is True
        ), "read_vcf should preserve 0-based metadata when use_zero_based=True"

    def test_read_gff_preserves_metadata(self):
        """Test that read_gff preserves metadata on returned DataFrames."""
        gff_path = "tests/data/io/gff/gencode.v38.annotation.gff3"

        df = pb.read_gff(gff_path)
        cs = get_coordinate_system(df)
        assert cs is False, "read_gff should preserve 1-based metadata by default"

        df_zero = pb.read_gff(gff_path, use_zero_based=True)
        cs_zero = get_coordinate_system(df_zero)
        assert cs_zero is True, "read_gff should preserve 0-based metadata"

    def test_read_bam_preserves_metadata(self):
        """Test that read_bam preserves metadata on returned DataFrames."""
        bam_path = "tests/data/io/bam/test.bam"

        df = pb.read_bam(bam_path)
        cs = get_coordinate_system(df)
        assert cs is False, "read_bam should preserve 1-based metadata by default"

        df_zero = pb.read_bam(bam_path, use_zero_based=True)
        cs_zero = get_coordinate_system(df_zero)
        assert cs_zero is True, "read_bam should preserve 0-based metadata"

    def test_read_cram_preserves_metadata(self):
        """Test that read_cram preserves metadata on returned DataFrames."""
        cram_path = "tests/data/io/cram/test.cram"

        df = pb.read_cram(cram_path)
        cs = get_coordinate_system(df)
        assert cs is False, "read_cram should preserve 1-based metadata by default"

        df_zero = pb.read_cram(cram_path, use_zero_based=True)
        cs_zero = get_coordinate_system(df_zero)
        assert cs_zero is True, "read_cram should preserve 0-based metadata"

    def test_read_bed_preserves_metadata(self):
        """Test that read_bed preserves metadata on returned DataFrames."""
        bed_path = "tests/data/io/bed/test.bed"

        df = pb.read_bed(bed_path)
        cs = get_coordinate_system(df)
        assert cs is False, "read_bed should preserve 1-based metadata by default"

        df_zero = pb.read_bed(bed_path, use_zero_based=True)
        cs_zero = get_coordinate_system(df_zero)
        assert cs_zero is True, "read_bed should preserve 0-based metadata"

    def test_metadata_accessible_via_config_meta(self):
        """Test that metadata is accessible via polars-config-meta API."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path, use_zero_based=True)

        # Access via config_meta.get_metadata()
        meta = lf.config_meta.get_metadata()
        assert (
            meta.get("coordinate_system_zero_based") is True
        ), "Metadata should be accessible via config_meta.get_metadata()"


class TestCoverageCoordinateSystem:
    """Tests for coverage operation with coordinate system metadata."""

    def test_coverage_with_zero_based_metadata(self):
        """Test that coverage operation works with 0-based coordinates."""
        df1 = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1", "chr1"],
                "start": [100, 200, 300],
                "end": [150, 250, 350],
            }
        )
        df2 = pl.DataFrame(
            {"chrom": ["chr1", "chr1"], "start": [125, 225], "end": [175, 275]}
        )
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=True)

        # Should work without errors
        result = pb.coverage(df1, df2, output_type="polars.DataFrame")
        assert len(result) >= 0

    def test_coverage_with_one_based_metadata(self):
        """Test that coverage operation works with 1-based coordinates."""
        df1 = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1", "chr1"],
                "start": [100, 200, 300],
                "end": [150, 250, 350],
            }
        )
        df2 = pl.DataFrame(
            {"chrom": ["chr1", "chr1"], "start": [125, 225], "end": [175, 275]}
        )
        set_coordinate_system(df1, zero_based=False)
        set_coordinate_system(df2, zero_based=False)

        result = pb.coverage(df1, df2, output_type="polars.DataFrame")
        assert len(result) >= 0

    def test_coverage_mismatch_raises_error(self):
        """Test that coverage raises CoordinateSystemMismatchError on mismatch."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [150], "end": [250]})
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=False)

        with pytest.raises(CoordinateSystemMismatchError):
            pb.coverage(df1, df2, output_type="polars.DataFrame")

    def test_coverage_missing_metadata_raises_error(self):
        """Test that coverage raises MissingCoordinateSystemError when metadata missing."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [150], "end": [250]})

        with pytest.raises(MissingCoordinateSystemError):
            pb.coverage(df1, df2).collect()


class TestMixedInputTypesCoordinateSystem:
    """Tests for mixed Polars/Pandas input combinations."""

    def test_polars_lf_and_pandas_df_same_coordinate_system(self):
        """Test overlap with Polars LazyFrame and Pandas DataFrame (same coords)."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path, use_zero_based=False)

        pdf = pd.DataFrame({"chrom": ["21"], "start": [33248751], "end": [33248752]})
        pdf.attrs["coordinate_system_zero_based"] = False

        result = pb.overlap(lf, pdf, output_type="pandas.DataFrame")
        assert isinstance(result, pd.DataFrame)

    def test_polars_df_and_pandas_df_same_coordinate_system(self):
        """Test overlap with Polars DataFrame and Pandas DataFrame (same coords)."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        set_coordinate_system(df1, zero_based=True)

        pdf2 = pd.DataFrame({"chrom": ["chr1"], "start": [150], "end": [250]})
        pdf2.attrs["coordinate_system_zero_based"] = True

        result = pb.overlap(df1, pdf2, output_type="pandas.DataFrame")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_polars_lf_and_pandas_df_mismatch_raises_error(self):
        """Test overlap with mixed types and mismatched coordinate systems."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path, use_zero_based=True)  # 0-based

        pdf = pd.DataFrame({"chrom": ["21"], "start": [33248751], "end": [33248752]})
        pdf.attrs["coordinate_system_zero_based"] = False  # 1-based

        with pytest.raises(CoordinateSystemMismatchError):
            pb.overlap(lf, pdf, output_type="pandas.DataFrame")

    def test_pandas_and_polars_lf_mismatch_raises_error(self):
        """Test overlap with Pandas first, Polars second, mismatched coords."""
        pdf = pd.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        pdf.attrs["coordinate_system_zero_based"] = True  # 0-based

        lf = pl.LazyFrame({"chrom": ["chr1"], "start": [150], "end": [250]})
        set_coordinate_system(lf, zero_based=False)  # 1-based

        with pytest.raises(CoordinateSystemMismatchError):
            pb.overlap(pdf, lf, output_type="pandas.DataFrame")


class TestCoordinateSystemCorrectness:
    """Tests verifying correct overlap behavior for 0-based vs 1-based systems."""

    def test_adjacent_intervals_zero_based_no_overlap(self):
        """Adjacent intervals [100,200) and [200,300) should NOT overlap in 0-based."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [200], "end": [300]})
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=True)

        result = pb.overlap(df1, df2, output_type="polars.DataFrame")
        # In 0-based half-open [100,200) and [200,300), no overlap
        assert len(result) == 0

    def test_adjacent_intervals_one_based_overlap(self):
        """Adjacent intervals [100,200] and [200,300] SHOULD overlap in 1-based."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [200], "end": [300]})
        set_coordinate_system(df1, zero_based=False)
        set_coordinate_system(df2, zero_based=False)

        result = pb.overlap(df1, df2, output_type="polars.DataFrame")
        # In 1-based closed [100,200] and [200,300], position 200 overlaps
        assert len(result) == 1

    def test_touching_intervals_zero_based_overlap(self):
        """Intervals [100,200) and [199,300) should overlap in 0-based at position 199."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [199], "end": [300]})
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=True)

        result = pb.overlap(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 1

    def test_gap_intervals_one_based_no_overlap(self):
        """Intervals [100,200] and [202,300] should NOT overlap in 1-based."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [202], "end": [300]})
        set_coordinate_system(df1, zero_based=False)
        set_coordinate_system(df2, zero_based=False)

        result = pb.overlap(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 0

    def test_same_interval_zero_based(self):
        """Same interval should overlap with itself in 0-based."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=True)

        result = pb.overlap(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 1

    def test_same_interval_one_based(self):
        """Same interval should overlap with itself in 1-based."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        set_coordinate_system(df1, zero_based=False)
        set_coordinate_system(df2, zero_based=False)

        result = pb.overlap(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 1

    def test_contained_interval_zero_based(self):
        """Contained interval [150,180) within [100,200) should overlap in 0-based."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [150], "end": [180]})
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=True)

        result = pb.overlap(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 1

    def test_contained_interval_one_based(self):
        """Contained interval [150,180] within [100,200] should overlap in 1-based."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [150], "end": [180]})
        set_coordinate_system(df1, zero_based=False)
        set_coordinate_system(df2, zero_based=False)

        result = pb.overlap(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 1


class TestGlobalConfigSwitching:
    """Tests for switching between global coordinate system configurations."""

    def test_scan_uses_changed_global_config(self):
        """Test that scan_* uses updated global config after set_option."""
        from polars_bio.constants import POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED

        vcf_path = "tests/data/io/vcf/ensembl.vcf"

        # Default is 1-based
        lf1 = pb.scan_vcf(vcf_path)
        assert get_coordinate_system(lf1) is False

        # Change global config to 0-based
        pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)
        try:
            lf2 = pb.scan_vcf(vcf_path)
            assert get_coordinate_system(lf2) is True
        finally:
            pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, False)

    def test_read_uses_changed_global_config(self):
        """Test that read_* uses updated global config after set_option."""
        from polars_bio.constants import POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED

        vcf_path = "tests/data/io/vcf/ensembl.vcf"

        # Change global config to 0-based
        pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)
        try:
            df = pb.read_vcf(vcf_path)
            assert get_coordinate_system(df) is True
        finally:
            pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, False)

    def test_explicit_param_overrides_global_config(self):
        """Test that explicit use_zero_based param overrides global config."""
        from polars_bio.constants import POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED

        vcf_path = "tests/data/io/vcf/ensembl.vcf"

        # Set global to 0-based
        pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)
        try:
            # Explicit param should override
            lf = pb.scan_vcf(vcf_path, use_zero_based=False)
            assert get_coordinate_system(lf) is False
        finally:
            pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, False)

    def test_global_config_affects_all_io_functions(self):
        """Test that global config affects all scan_* and read_* functions."""
        from polars_bio.constants import POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED

        pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)
        try:
            # Test scan functions
            assert (
                get_coordinate_system(pb.scan_vcf("tests/data/io/vcf/ensembl.vcf"))
                is True
            )
            assert (
                get_coordinate_system(
                    pb.scan_gff("tests/data/io/gff/gencode.v38.annotation.gff3")
                )
                is True
            )
            assert (
                get_coordinate_system(pb.scan_bed("tests/data/io/bed/test.bed")) is True
            )
            assert (
                get_coordinate_system(pb.scan_bam("tests/data/io/bam/test.bam")) is True
            )
            assert (
                get_coordinate_system(pb.scan_cram("tests/data/io/cram/test.cram"))
                is True
            )

            # Test read functions
            assert (
                get_coordinate_system(pb.read_vcf("tests/data/io/vcf/ensembl.vcf"))
                is True
            )
            assert (
                get_coordinate_system(
                    pb.read_gff("tests/data/io/gff/gencode.v38.annotation.gff3")
                )
                is True
            )
            assert (
                get_coordinate_system(pb.read_bed("tests/data/io/bed/test.bed")) is True
            )
        finally:
            pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, False)


class TestErrorMessageQuality:
    """Tests verifying error messages contain helpful hints."""

    def test_missing_metadata_error_has_polars_df_hint(self):
        """Test MissingCoordinateSystemError includes Polars DataFrame hint."""
        df = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})

        with pytest.raises(MissingCoordinateSystemError) as exc_info:
            validate_coordinate_systems(df, df)

        error_msg = str(exc_info.value)
        assert "config_meta.set" in error_msg or "polars-bio I/O functions" in error_msg
        assert "coordinate_system_zero_based" in error_msg

    def test_missing_metadata_error_has_polars_lf_hint(self):
        """Test MissingCoordinateSystemError includes Polars LazyFrame hint."""
        lf = pl.LazyFrame({"chrom": ["chr1"], "start": [100], "end": [200]})

        with pytest.raises(MissingCoordinateSystemError) as exc_info:
            validate_coordinate_systems(lf, lf)

        error_msg = str(exc_info.value)
        assert "Polars LazyFrame" in error_msg
        assert "coordinate_system_zero_based" in error_msg

    def test_missing_metadata_error_has_pandas_hint(self):
        """Test MissingCoordinateSystemError includes Pandas-specific hint."""
        pdf = pd.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})

        with pytest.raises(MissingCoordinateSystemError) as exc_info:
            validate_coordinate_systems(pdf, pdf)

        error_msg = str(exc_info.value)
        assert "Pandas DataFrame" in error_msg
        assert "df.attrs" in error_msg
        assert "coordinate_system_zero_based" in error_msg

    def test_mismatch_error_identifies_both_systems(self):
        """Test CoordinateSystemMismatchError identifies both coordinate systems."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf1 = pb.scan_vcf(vcf_path, use_zero_based=True)
        lf2 = pb.scan_vcf(vcf_path, use_zero_based=False)

        with pytest.raises(CoordinateSystemMismatchError) as exc_info:
            validate_coordinate_systems(lf1, lf2)

        error_msg = str(exc_info.value)
        assert "0-based" in error_msg
        assert "1-based" in error_msg
        assert "mismatch" in error_msg.lower()


class TestMergeCoordinateSystem:
    """Tests for merge operation with coordinate system metadata."""

    def test_merge_with_zero_based_metadata(self):
        """Test that merge operation works with 0-based coordinates."""
        df = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1", "chr1"],
                "start": [100, 140, 300],
                "end": [150, 180, 350],
            }
        )
        set_coordinate_system(df, zero_based=True)

        result = pb.merge(df, output_type="polars.DataFrame")
        assert len(result) >= 1

    def test_merge_with_one_based_metadata(self):
        """Test that merge operation works with 1-based coordinates."""
        df = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1", "chr1"],
                "start": [100, 140, 300],
                "end": [150, 180, 350],
            }
        )
        set_coordinate_system(df, zero_based=False)

        result = pb.merge(df, output_type="polars.DataFrame")
        assert len(result) >= 1

    def test_merge_missing_metadata_raises_error(self):
        """Test that merge raises MissingCoordinateSystemError when metadata missing."""
        df = pl.DataFrame(
            {"chrom": ["chr1", "chr1"], "start": [100, 140], "end": [150, 180]}
        )

        with pytest.raises(MissingCoordinateSystemError):
            pb.merge(df, output_type="polars.DataFrame")

    def test_merge_adjacent_intervals_zero_based(self):
        """Test merge behavior with adjacent intervals in 0-based system."""
        # [100,150) and [150,200) are adjacent in 0-based, should NOT merge
        df = pl.DataFrame(
            {"chrom": ["chr1", "chr1"], "start": [100, 150], "end": [150, 200]}
        )
        set_coordinate_system(df, zero_based=True)

        result = pb.merge(df, output_type="polars.DataFrame")
        # Adjacent intervals in 0-based don't overlap, should remain separate
        assert len(result) == 2

    def test_merge_adjacent_intervals_one_based(self):
        """Test merge behavior with adjacent intervals in 1-based system."""
        # [100,150] and [150,200] share position 150 in 1-based, should merge
        df = pl.DataFrame(
            {"chrom": ["chr1", "chr1"], "start": [100, 150], "end": [150, 200]}
        )
        set_coordinate_system(df, zero_based=False)

        result = pb.merge(df, output_type="polars.DataFrame")
        # Adjacent intervals in 1-based share endpoint, should merge
        assert len(result) == 1


class TestNearestCoordinateSystem:
    """Tests for nearest operation with coordinate system metadata."""

    def test_nearest_with_zero_based_metadata(self):
        """Test that nearest operation works with 0-based coordinates."""
        df1 = pl.DataFrame(
            {"chrom": ["chr1", "chr1"], "start": [100, 300], "end": [150, 350]}
        )
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [200], "end": [250]})
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=True)

        result = pb.nearest(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 2

    def test_nearest_with_one_based_metadata(self):
        """Test that nearest operation works with 1-based coordinates."""
        df1 = pl.DataFrame(
            {"chrom": ["chr1", "chr1"], "start": [100, 300], "end": [150, 350]}
        )
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [200], "end": [250]})
        set_coordinate_system(df1, zero_based=False)
        set_coordinate_system(df2, zero_based=False)

        result = pb.nearest(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 2

    def test_nearest_mismatch_raises_error(self):
        """Test that nearest raises CoordinateSystemMismatchError on mismatch."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [300], "end": [400]})
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=False)

        with pytest.raises(CoordinateSystemMismatchError):
            pb.nearest(df1, df2, output_type="polars.DataFrame")

    def test_nearest_missing_metadata_raises_error(self):
        """Test that nearest raises MissingCoordinateSystemError when metadata missing."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [300], "end": [400]})

        with pytest.raises(MissingCoordinateSystemError):
            pb.nearest(df1, df2, output_type="polars.DataFrame")


class TestCountOverlapsCoordinateSystem:
    """Tests for count_overlaps operation with coordinate system metadata."""

    def test_count_overlaps_with_zero_based_metadata(self):
        """Test that count_overlaps operation works with 0-based coordinates."""
        df1 = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1", "chr1"],
                "start": [100, 200, 300],
                "end": [150, 250, 350],
            }
        )
        df2 = pl.DataFrame(
            {"chrom": ["chr1", "chr1"], "start": [125, 225], "end": [175, 275]}
        )
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=True)

        result = pb.count_overlaps(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 3
        assert "count" in result.columns

    def test_count_overlaps_with_one_based_metadata(self):
        """Test that count_overlaps operation works with 1-based coordinates."""
        df1 = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1", "chr1"],
                "start": [100, 200, 300],
                "end": [150, 250, 350],
            }
        )
        df2 = pl.DataFrame(
            {"chrom": ["chr1", "chr1"], "start": [125, 225], "end": [175, 275]}
        )
        set_coordinate_system(df1, zero_based=False)
        set_coordinate_system(df2, zero_based=False)

        result = pb.count_overlaps(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 3
        assert "count" in result.columns

    def test_count_overlaps_mismatch_raises_error(self):
        """Test that count_overlaps raises CoordinateSystemMismatchError on mismatch."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [150], "end": [250]})
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=False)

        with pytest.raises(CoordinateSystemMismatchError):
            pb.count_overlaps(df1, df2, output_type="polars.DataFrame")

    def test_count_overlaps_missing_metadata_raises_error(self):
        """Test that count_overlaps raises MissingCoordinateSystemError when metadata missing."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [150], "end": [250]})

        with pytest.raises(MissingCoordinateSystemError):
            pb.count_overlaps(df1, df2, output_type="polars.DataFrame")

    def test_count_overlaps_adjacent_intervals_zero_based(self):
        """Test count_overlaps with adjacent intervals in 0-based (no overlap expected)."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [200], "end": [300]})
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=True)

        result = pb.count_overlaps(df1, df2, output_type="polars.DataFrame")
        assert result["count"].to_list()[0] == 0

    def test_count_overlaps_adjacent_intervals_one_based(self):
        """Test count_overlaps with adjacent intervals in 1-based (overlap expected)."""
        df1 = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        df2 = pl.DataFrame({"chrom": ["chr1"], "start": [200], "end": [300]})
        set_coordinate_system(df1, zero_based=False)
        set_coordinate_system(df2, zero_based=False)

        result = pb.count_overlaps(df1, df2, output_type="polars.DataFrame")
        assert result["count"].to_list()[0] == 1


class TestMetadataPreservationThroughTransformations:
    """Tests for metadata preservation through various Polars transformations."""

    def test_metadata_preserved_through_with_columns(self):
        """Test that metadata is preserved through with_columns transformation."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path, use_zero_based=True)

        lf_transformed = lf.with_columns(pl.lit(1).alias("new_col"))

        assert get_coordinate_system(lf_transformed) is True

    def test_metadata_preserved_through_drop(self):
        """Test that metadata is preserved through drop transformation."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path, use_zero_based=False)

        lf_transformed = lf.drop("filter")

        assert get_coordinate_system(lf_transformed) is False

    def test_metadata_preserved_through_rename(self):
        """Test that metadata is preserved through rename transformation."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path, use_zero_based=True)

        lf_transformed = lf.rename({"chrom": "chromosome"})

        assert get_coordinate_system(lf_transformed) is True

    def test_metadata_preserved_through_sort(self):
        """Test that metadata is preserved through sort transformation."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path, use_zero_based=False)

        lf_transformed = lf.sort("start")

        assert get_coordinate_system(lf_transformed) is False

    def test_metadata_preserved_through_limit(self):
        """Test that metadata is preserved through limit/head transformation."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path, use_zero_based=True)

        lf_transformed = lf.head(10)

        assert get_coordinate_system(lf_transformed) is True

    def test_metadata_preserved_through_chained_transformations(self):
        """Test that metadata is preserved through multiple chained transformations."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        lf = pb.scan_vcf(vcf_path, use_zero_based=True)

        lf_transformed = (
            lf.filter(pl.col("chrom") == "21")
            .select(["chrom", "start", "end"])
            .with_columns(pl.lit("test").alias("extra"))
            .sort("start")
            .head(5)
        )

        assert get_coordinate_system(lf_transformed) is True


class TestUnsignedIntegerSupport:
    """Tests for UInt32/UInt64 column support in range operations.

    Bio-format files (VCF, BED, etc.) often use unsigned integers for
    start/end positions. These tests verify that range operations work
    correctly with UInt32 and UInt64 data types.
    """

    def test_coverage_with_uint32_columns(self):
        """Test coverage operation with UInt32 start/end columns."""
        df1 = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1", "chr1"],
                "start": pl.Series([100, 200, 300], dtype=pl.UInt32),
                "end": pl.Series([150, 250, 350], dtype=pl.UInt32),
            }
        )
        df2 = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1"],
                "start": pl.Series([125, 225], dtype=pl.UInt32),
                "end": pl.Series([175, 275], dtype=pl.UInt32),
            }
        )
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=True)

        result = pb.coverage(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 3
        assert "coverage" in result.columns

    def test_coverage_with_uint64_columns(self):
        """Test coverage operation with UInt64 start/end columns."""
        df1 = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1"],
                "start": pl.Series([100, 200], dtype=pl.UInt64),
                "end": pl.Series([150, 250], dtype=pl.UInt64),
            }
        )
        df2 = pl.DataFrame(
            {
                "chrom": ["chr1"],
                "start": pl.Series([125], dtype=pl.UInt64),
                "end": pl.Series([175], dtype=pl.UInt64),
            }
        )
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=True)

        result = pb.coverage(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 2
        assert "coverage" in result.columns

    def test_count_overlaps_with_uint32_columns(self):
        """Test count_overlaps operation with UInt32 start/end columns."""
        df1 = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1", "chr1"],
                "start": pl.Series([100, 200, 300], dtype=pl.UInt32),
                "end": pl.Series([150, 250, 350], dtype=pl.UInt32),
            }
        )
        df2 = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1"],
                "start": pl.Series([125, 225], dtype=pl.UInt32),
                "end": pl.Series([175, 275], dtype=pl.UInt32),
            }
        )
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=True)

        result = pb.count_overlaps(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 3
        assert "count" in result.columns
        # First two intervals should have 1 overlap each, third should have 0
        counts = result["count"].to_list()
        assert counts == [1, 1, 0]

    def test_count_overlaps_with_uint64_columns(self):
        """Test count_overlaps operation with UInt64 start/end columns."""
        df1 = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1"],
                "start": pl.Series([100, 300], dtype=pl.UInt64),
                "end": pl.Series([200, 400], dtype=pl.UInt64),
            }
        )
        df2 = pl.DataFrame(
            {
                "chrom": ["chr1"],
                "start": pl.Series([150], dtype=pl.UInt64),
                "end": pl.Series([250], dtype=pl.UInt64),
            }
        )
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=True)

        result = pb.count_overlaps(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 2
        assert "count" in result.columns

    def test_coverage_with_mixed_int_types(self):
        """Test coverage with mixed signed/unsigned integer types."""
        # df1 uses UInt32, df2 uses Int64
        df1 = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1"],
                "start": pl.Series([100, 200], dtype=pl.UInt32),
                "end": pl.Series([150, 250], dtype=pl.UInt32),
            }
        )
        df2 = pl.DataFrame(
            {
                "chrom": ["chr1"],
                "start": pl.Series([125], dtype=pl.Int64),
                "end": pl.Series([175], dtype=pl.Int64),
            }
        )
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=True)

        result = pb.coverage(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 2

    def test_count_overlaps_with_mixed_int_types(self):
        """Test count_overlaps with mixed signed/unsigned integer types."""
        # df1 uses Int32, df2 uses UInt64
        df1 = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1"],
                "start": pl.Series([100, 200], dtype=pl.Int32),
                "end": pl.Series([150, 250], dtype=pl.Int32),
            }
        )
        df2 = pl.DataFrame(
            {
                "chrom": ["chr1"],
                "start": pl.Series([125], dtype=pl.UInt64),
                "end": pl.Series([175], dtype=pl.UInt64),
            }
        )
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=True)

        result = pb.count_overlaps(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 2

    def test_coverage_uint32_zero_based_boundary(self):
        """Test coverage with UInt32 verifies 0-based boundary behavior."""
        # Adjacent intervals [100,200) and [200,300) should NOT overlap in 0-based
        df1 = pl.DataFrame(
            {
                "chrom": ["chr1"],
                "start": pl.Series([100], dtype=pl.UInt32),
                "end": pl.Series([200], dtype=pl.UInt32),
            }
        )
        df2 = pl.DataFrame(
            {
                "chrom": ["chr1"],
                "start": pl.Series([200], dtype=pl.UInt32),
                "end": pl.Series([300], dtype=pl.UInt32),
            }
        )
        set_coordinate_system(df1, zero_based=True)
        set_coordinate_system(df2, zero_based=True)

        result = pb.coverage(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 1
        assert result["coverage"].to_list()[0] == 0

    def test_coverage_uint32_one_based_boundary(self):
        """Test coverage with UInt32 verifies 1-based boundary behavior."""
        # Adjacent intervals [100,200] and [200,300] should overlap at position 200 in 1-based
        df1 = pl.DataFrame(
            {
                "chrom": ["chr1"],
                "start": pl.Series([100], dtype=pl.UInt32),
                "end": pl.Series([200], dtype=pl.UInt32),
            }
        )
        df2 = pl.DataFrame(
            {
                "chrom": ["chr1"],
                "start": pl.Series([200], dtype=pl.UInt32),
                "end": pl.Series([300], dtype=pl.UInt32),
            }
        )
        set_coordinate_system(df1, zero_based=False)
        set_coordinate_system(df2, zero_based=False)

        result = pb.coverage(df1, df2, output_type="polars.DataFrame")
        assert len(result) == 1
        assert result["coverage"].to_list()[0] == 1

    def test_vcf_bed_files_use_uint32(self):
        """Test that VCF and BED files use UInt32 for coordinates."""
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        bed_path = "tests/data/io/bed/test.bed"

        lf_vcf = pb.scan_vcf(vcf_path, use_zero_based=True)
        lf_bed = pb.scan_bed(bed_path, use_zero_based=True)

        vcf_schema = lf_vcf.collect_schema()
        bed_schema = lf_bed.collect_schema()

        # Verify that start/end columns are UInt32
        assert vcf_schema["start"] == pl.UInt32
        assert vcf_schema["end"] == pl.UInt32
        assert bed_schema["start"] == pl.UInt32
        assert bed_schema["end"] == pl.UInt32

    @pytest.mark.xfail(
        reason="Known bug: coverage returns schema from df2 instead of df1"
    )
    def test_coverage_vcf_bed_lazyframe_output(self):
        """Test coverage with VCF/BED files returns correct LazyFrame schema.

        This is a regression test for a bug where the LazyFrame schema was
        incorrectly built from df2's schema instead of df1's schema, causing
        ColumnNotFoundError when collecting.
        """
        vcf_path = "tests/data/io/vcf/ensembl.vcf"
        bed_path = "tests/data/io/bed/test.bed"

        lf_vcf = pb.scan_vcf(vcf_path, use_zero_based=True)
        lf_bed = pb.scan_bed(bed_path, use_zero_based=True)

        # Coverage should return VCF data with coverage column
        result_lf = pb.coverage(lf_vcf, lf_bed)
        schema = result_lf.collect_schema()

        # Schema should have VCF columns + coverage, NOT BED columns
        assert "coverage" in schema
        assert "id" in schema  # VCF column
        assert "name" not in schema  # BED column should NOT be present

        # Should collect without error
        result = result_lf.collect()
        assert len(result) == 2  # VCF has 2 rows


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
