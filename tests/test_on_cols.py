"""Tests for on_cols parameter in genomic range operations."""
import pandas as pd
import polars as pl
import pytest

import polars_bio as pb

# Set up test context
pb.ctx.set_option("datafusion.execution.parquet.schema_force_view_types", "true", False)


@pytest.fixture
def df1_with_strand():
    """Create a test DataFrame with strand information."""
    data = {
        "chrom": ["chr1", "chr1", "chr1", "chr2"],
        "start": [10, 20, 30, 10],
        "end": [20, 30, 40, 20],
        "strand": ["+", "+", "-", "+"],
        "name": ["a", "b", "c", "d"],
    }
    df = pd.DataFrame(data)
    df.attrs["coordinate_system_zero_based"] = True
    return df


@pytest.fixture
def df2_with_strand():
    """Create a second test DataFrame with strand information."""
    data = {
        "chrom": ["chr1", "chr1", "chr1", "chr2"],
        "start": [15, 25, 25, 15],
        "end": [25, 35, 35, 25],
        "strand": ["+", "-", "+", "+"],
        "name": ["x", "y", "z", "w"],
    }
    df = pd.DataFrame(data)
    df.attrs["coordinate_system_zero_based"] = True
    return df


class TestOnColsOverlap:
    """Test on_cols parameter with overlap operation."""

    def test_overlap_without_on_cols(self, df1_with_strand, df2_with_strand):
        """Test overlap without on_cols returns all overlaps."""
        result = pb.overlap(
            df1_with_strand,
            df2_with_strand,
            output_type="pandas.DataFrame",
        )
        # Should return overlaps regardless of strand
        # chr1: (10-20) overlaps (15-25), (20-30) overlaps (15-25) and (25-35), (30-40) overlaps (25-35)
        # chr2: (10-20) overlaps (15-25)
        assert len(result) >= 4, f"Expected at least 4 overlaps, got {len(result)}"

    def test_overlap_with_strand_on_cols(self, df1_with_strand, df2_with_strand):
        """Test overlap with on_cols=['strand'] filters by strand."""
        result = pb.overlap(
            df1_with_strand,
            df2_with_strand,
            on_cols=["strand"],
            output_type="pandas.DataFrame",
        )
        # Should only return overlaps where strand matches
        # chr1 + strand: (10-20) with (15-25), (20-30) with (25-35)
        # chr1 - strand: none (c at 30-40 with - strand, but x,z,w are +)
        # chr2 + strand: (10-20) with (15-25)
        assert len(result) >= 2, f"Expected at least 2 strand-matched overlaps, got {len(result)}"
        
        # Verify all results have matching strands
        if len(result) > 0:
            assert (result["strand_1"] == result["strand_2"]).all(), \
                "All overlaps should have matching strands"

    def test_overlap_with_on_cols_polars_lazyframe(self, df1_with_strand, df2_with_strand):
        """Test overlap with on_cols returns LazyFrame correctly."""
        result = pb.overlap(
            df1_with_strand,
            df2_with_strand,
            on_cols=["strand"],
            output_type="polars.LazyFrame",
        )
        assert isinstance(result, pl.LazyFrame), "Should return LazyFrame"
        
        result_df = result.collect().to_pandas()
        if len(result_df) > 0:
            assert (result_df["strand_1"] == result_df["strand_2"]).all(), \
                "All overlaps should have matching strands"


class TestOnColsNearest:
    """Test on_cols parameter with nearest operation."""

    def test_nearest_with_strand_on_cols(self, df1_with_strand, df2_with_strand):
        """Test nearest with on_cols=['strand'] filters by strand."""
        result = pb.nearest(
            df1_with_strand,
            df2_with_strand,
            on_cols=["strand"],
            output_type="pandas.DataFrame",
        )
        # Should only find nearest intervals with matching strand
        assert len(result) > 0, "Should find nearest intervals"
        
        # Filter out null results (no match found)
        non_null = result[result["strand_2"].notna()]
        if len(non_null) > 0:
            assert (non_null["strand_1"] == non_null["strand_2"]).all(), \
                "All nearest matches should have matching strands"


class TestOnColsCoverage:
    """Test on_cols parameter with coverage operation."""

    def test_coverage_with_on_cols(self, df1_with_strand, df2_with_strand):
        """Test coverage with on_cols=['strand'] raises error."""
        # Coverage operation does not support on_cols filtering
        with pytest.raises(Exception) as exc_info:
            pb.coverage(
                df1_with_strand,
                df2_with_strand,
                on_cols=["strand"],
                output_type="pandas.DataFrame",
            )
        # Check that error message mentions on_cols and coverage
        error_msg = str(exc_info.value)
        assert "on_cols" in error_msg.lower(), "Error should mention on_cols"
        assert "coverage" in error_msg.lower(), "Error should mention coverage operation"


class TestOnColsCountOverlaps:
    """Test on_cols parameter with count_overlaps operation."""

    def test_count_overlaps_with_on_cols_naive(self, df1_with_strand, df2_with_strand):
        """Test count_overlaps with on_cols=['strand'] raises error with naive_query=True."""
        # Naive count_overlaps does not support on_cols filtering
        with pytest.raises(Exception) as exc_info:
            pb.count_overlaps(
                df1_with_strand,
                df2_with_strand,
                on_cols=["strand"],
                naive_query=True,  # Explicit naive mode
                output_type="pandas.DataFrame",
            )
        # Check that error message mentions on_cols and count_overlaps
        error_msg = str(exc_info.value)
        assert "on_cols" in error_msg.lower(), "Error should mention on_cols"
        assert "count_overlaps" in error_msg.lower(), "Error should mention count_overlaps operation"
        
    def test_count_overlaps_with_on_cols_default(self, df1_with_strand, df2_with_strand):
        """Test count_overlaps with on_cols=['strand'] raises error by default (naive_query=True)."""
        # Default naive_query=True, so should also raise error
        with pytest.raises(Exception) as exc_info:
            pb.count_overlaps(
                df1_with_strand,
                df2_with_strand,
                on_cols=["strand"],
                output_type="pandas.DataFrame",
            )
        # Check that error message mentions on_cols
        error_msg = str(exc_info.value)
        assert "on_cols" in error_msg.lower(), "Error should mention on_cols"
    
    def test_count_overlaps_with_on_cols_non_naive(self, df1_with_strand, df2_with_strand):
        """Test count_overlaps with on_cols=['strand'] works with naive_query=False."""
        # Non-naive path should support on_cols filtering
        result = pb.count_overlaps(
            df1_with_strand,
            df2_with_strand,
            on_cols=["strand"],
            naive_query=False,  # Use SQL-based path which supports on_cols
            output_type="pandas.DataFrame",
        )
        # Should count only overlaps with matching strands
        assert len(result) == len(df1_with_strand), \
            "count_overlaps should return one row per input interval"
        assert "count" in result.columns, "Should have count column"
        
        # Verify the counts are different from non-filtered version
        result_no_filter = pb.count_overlaps(
            df1_with_strand,
            df2_with_strand,
            naive_query=False,
            output_type="pandas.DataFrame",
        )
        # At least some counts should differ when filtering by strand
        assert not (result["count"] == result_no_filter["count"]).all(), \
            "Filtered counts should differ from unfiltered counts"


class TestOnColsValidation:
    """Test validation of on_cols parameter."""

    def test_on_cols_must_be_list(self, df1_with_strand, df2_with_strand):
        """Test that on_cols must be a list."""
        with pytest.raises(TypeError, match="on_cols must be a list"):
            pb.overlap(
                df1_with_strand,
                df2_with_strand,
                on_cols="strand",  # Should be a list, not a string
                output_type="pandas.DataFrame",
            )

    def test_on_cols_elements_must_be_strings(self, df1_with_strand, df2_with_strand):
        """Test that all on_cols elements must be strings."""
        with pytest.raises(TypeError, match="All elements in on_cols must be strings"):
            pb.overlap(
                df1_with_strand,
                df2_with_strand,
                on_cols=[123],  # Should be strings, not integers
                output_type="pandas.DataFrame",
            )

    def test_on_cols_cannot_be_empty_list(self, df1_with_strand, df2_with_strand):
        """Test that on_cols cannot be an empty list."""
        with pytest.raises(ValueError, match="on_cols cannot be an empty list"):
            pb.overlap(
                df1_with_strand,
                df2_with_strand,
                on_cols=[],
                output_type="pandas.DataFrame",
            )

    def test_on_cols_none_is_valid(self, df1_with_strand, df2_with_strand):
        """Test that on_cols=None is valid (default behavior)."""
        result = pb.overlap(
            df1_with_strand,
            df2_with_strand,
            on_cols=None,
            output_type="pandas.DataFrame",
        )
        assert len(result) > 0, "Should work with on_cols=None"


class TestOnColsMultipleColumns:
    """Test on_cols with multiple columns."""

    def test_overlap_with_multiple_on_cols(self):
        """Test overlap with multiple columns in on_cols."""
        data1 = {
            "chrom": ["chr1"] * 4,
            "start": [10, 20, 30, 40],
            "end": [20, 30, 40, 50],
            "strand": ["+", "+", "-", "+"],
            "sample": ["A", "A", "B", "B"],
        }
        df1 = pd.DataFrame(data1)
        df1.attrs["coordinate_system_zero_based"] = True

        data2 = {
            "chrom": ["chr1"] * 4,
            "start": [15, 25, 25, 45],
            "end": [25, 35, 35, 55],
            "strand": ["+", "+", "-", "+"],
            "sample": ["A", "B", "B", "A"],
        }
        df2 = pd.DataFrame(data2)
        df2.attrs["coordinate_system_zero_based"] = True

        result = pb.overlap(
            df1,
            df2,
            on_cols=["strand", "sample"],
            output_type="pandas.DataFrame",
        )
        
        # Should only match when both strand AND sample match
        if len(result) > 0:
            assert (result["strand_1"] == result["strand_2"]).all(), \
                "All overlaps should have matching strands"
            assert (result["sample_1"] == result["sample_2"]).all(), \
                "All overlaps should have matching samples"
