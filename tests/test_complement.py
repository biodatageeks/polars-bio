import pandas as pd
import polars as pl
import pytest

import polars_bio as pb


class TestComplement:
    """Test complement operation for finding genomic gaps."""

    def test_complement_basic_1based(self):
        """Test basic complement with 1-based coordinates."""
        # Intervals
        df = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1", "chr1"],
                "start": [100, 300, 500],
                "end": [200, 400, 600],
            }
        ).lazy()
        df.config_meta.set(coordinate_system_zero_based=False)

        # View (chromosome boundaries)
        view = pl.DataFrame({"chrom": ["chr1"], "start": [1], "end": [1000]}).lazy()
        view.config_meta.set(coordinate_system_zero_based=False)

        # Find gaps
        result = pb.complement(df, view_df=view, output_type="polars.DataFrame")

        # Expected: gaps at 1-99, 201-299, 401-499, 601-1000
        # For 1-based closed coordinates [100, 200] means positions 100-200 inclusive
        # So gap before it ends at 99, gap after starts at 201
        assert len(result) == 4
        assert result["chrom"].to_list() == ["chr1", "chr1", "chr1", "chr1"]
        assert result["start"].to_list() == [1, 201, 401, 601]
        assert result["end"].to_list() == [99, 299, 499, 1000]

    def test_complement_basic_0based(self):
        """Test basic complement with 0-based coordinates."""
        # Intervals (0-based half-open)
        df = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1", "chr1"],
                "start": [100, 300, 500],
                "end": [200, 400, 600],
            }
        ).lazy()
        df.config_meta.set(coordinate_system_zero_based=True)

        # View
        view = pl.DataFrame({"chrom": ["chr1"], "start": [0], "end": [1000]}).lazy()
        view.config_meta.set(coordinate_system_zero_based=True)

        # Find gaps
        result = pb.complement(df, view_df=view, output_type="polars.DataFrame")

        # Expected: gaps at 0-100, 200-300, 400-500, 600-1000
        assert len(result) == 4
        assert result["chrom"].to_list() == ["chr1", "chr1", "chr1", "chr1"]
        assert result["start"].to_list() == [0, 200, 400, 600]
        assert result["end"].to_list() == [100, 300, 500, 1000]

    def test_complement_auto_infer_view(self):
        """Test complement with auto-inferred view_df."""
        df = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1", "chr2"],
                "start": [100, 300, 50],
                "end": [200, 400, 150],
            }
        ).lazy()
        df.config_meta.set(coordinate_system_zero_based=False)

        # No view_df provided - should infer from df
        result = pb.complement(df, view_df=None, output_type="polars.DataFrame")

        # Expected: gaps between intervals
        # chr1: 201-299 (1-based: gap after [100,200] starts at 201, before [300,400] ends at 299)
        # chr2: no gaps (only one interval)
        assert len(result) == 1
        assert result["chrom"].to_list() == ["chr1"]
        assert result["start"].to_list() == [201]
        assert result["end"].to_list() == [299]

    def test_complement_overlapping_intervals_merged(self):
        """Test that overlapping intervals are merged before computing complement."""
        df = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1", "chr1"],
                "start": [100, 150, 300],
                "end": [250, 200, 400],
            }
        ).lazy()
        df.config_meta.set(coordinate_system_zero_based=False)

        view = pl.DataFrame({"chrom": ["chr1"], "start": [1], "end": [500]}).lazy()
        view.config_meta.set(coordinate_system_zero_based=False)

        result = pb.complement(df, view_df=view, output_type="polars.DataFrame")

        # Intervals [100-250] and [150-200] overlap -> merged to [100-250]
        # Interval [300-400] is separate
        # Expected gaps: 1-99, 251-299, 401-500 (1-based closed)
        assert len(result) == 3
        assert result["start"].to_list() == [1, 251, 401]
        assert result["end"].to_list() == [99, 299, 500]

    def test_complement_no_gaps(self):
        """Test complement when intervals cover entire view."""
        df = pl.DataFrame(
            {"chrom": ["chr1"], "start": [1], "end": [1000]}
        ).lazy()
        df.config_meta.set(coordinate_system_zero_based=False)

        view = pl.DataFrame({"chrom": ["chr1"], "start": [1], "end": [1000]}).lazy()
        view.config_meta.set(coordinate_system_zero_based=False)

        result = pb.complement(df, view_df=view, output_type="polars.DataFrame")

        # No gaps
        assert len(result) == 0

    def test_complement_empty_df(self):
        """Test complement with empty input DataFrame."""
        df = pl.DataFrame(
            {"chrom": [], "start": [], "end": []}, schema={"chrom": pl.Utf8, "start": pl.Int64, "end": pl.Int64}
        ).lazy()
        df.config_meta.set(coordinate_system_zero_based=False)

        view = pl.DataFrame({"chrom": ["chr1"], "start": [1], "end": [1000]}).lazy()
        view.config_meta.set(coordinate_system_zero_based=False)

        result = pb.complement(df, view_df=view, output_type="polars.DataFrame")

        # Entire view is a gap
        assert len(result) == 1
        assert result["chrom"].to_list() == ["chr1"]
        assert result["start"].to_list() == [1]
        assert result["end"].to_list() == [1000]

    def test_complement_multiple_chromosomes(self):
        """Test complement with multiple chromosomes."""
        df = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1", "chr2", "chr2"],
                "start": [100, 300, 50, 200],
                "end": [200, 400, 150, 300],
            }
        ).lazy()
        df.config_meta.set(coordinate_system_zero_based=False)

        view = pl.DataFrame(
            {
                "chrom": ["chr1", "chr2"],
                "start": [1, 1],
                "end": [500, 400],
            }
        ).lazy()
        view.config_meta.set(coordinate_system_zero_based=False)

        result = pb.complement(df, view_df=view, output_type="polars.DataFrame").sort(
            ["chrom", "start"]
        )

        # chr1: gaps at 1-99, 201-299, 401-500 (1-based closed)
        # chr2: gaps at 1-49, 151-199, 301-400 (1-based closed)
        assert len(result) == 6
        chr1_results = result.filter(pl.col("chrom") == "chr1")
        chr2_results = result.filter(pl.col("chrom") == "chr2")

        assert chr1_results["start"].to_list() == [1, 201, 401]
        assert chr1_results["end"].to_list() == [99, 299, 500]

        assert chr2_results["start"].to_list() == [1, 151, 301]
        assert chr2_results["end"].to_list() == [49, 199, 400]

    def test_complement_pandas_output(self):
        """Test complement with pandas DataFrame output."""
        df = pd.DataFrame(
            {
                "chrom": ["chr1", "chr1"],
                "start": [100, 300],
                "end": [200, 400],
            }
        )
        df.attrs["coordinate_system_zero_based"] = False

        view = pd.DataFrame({"chrom": ["chr1"], "start": [1], "end": [500]})
        view.attrs["coordinate_system_zero_based"] = False

        result = pb.complement(df, view_df=view, output_type="pandas.DataFrame")

        assert len(result) == 3
        assert isinstance(result, pd.DataFrame)
        assert result["start"].tolist() == [1, 201, 401]
        assert result["end"].tolist() == [99, 299, 500]

    def test_complement_lazyframe_output(self):
        """Test complement with LazyFrame output (default)."""
        df = pl.DataFrame(
            {"chrom": ["chr1"], "start": [100], "end": [200]}
        ).lazy()
        df.config_meta.set(coordinate_system_zero_based=False)

        view = pl.DataFrame({"chrom": ["chr1"], "start": [1], "end": [300]}).lazy()
        view.config_meta.set(coordinate_system_zero_based=False)

        result = pb.complement(df, view_df=view)

        assert isinstance(result, pl.LazyFrame)
        result_df = result.collect()
        assert len(result_df) == 2
        assert result_df["start"].to_list() == [1, 201]
        assert result_df["end"].to_list() == [99, 300]

    def test_complement_coordinate_system_mismatch(self):
        """Test that mismatched coordinate systems raise an error."""
        df = pl.DataFrame(
            {"chrom": ["chr1"], "start": [100], "end": [200]}
        ).lazy()
        df.config_meta.set(coordinate_system_zero_based=False)  # 1-based

        view = pl.DataFrame({"chrom": ["chr1"], "start": [0], "end": [300]}).lazy()
        view.config_meta.set(coordinate_system_zero_based=True)  # 0-based

        with pytest.raises(pb.CoordinateSystemMismatchError):
            pb.complement(df, view_df=view)

    def test_complement_custom_columns(self):
        """Test complement with custom column names."""
        df = pl.DataFrame(
            {
                "chromosome": ["chr1", "chr1"],
                "pos_start": [100, 300],
                "pos_end": [200, 400],
            }
        ).lazy()
        df.config_meta.set(coordinate_system_zero_based=False)

        view = pl.DataFrame(
            {"chromosome": ["chr1"], "pos_start": [1], "pos_end": [500]}
        ).lazy()
        view.config_meta.set(coordinate_system_zero_based=False)

        result = pb.complement(
            df,
            view_df=view,
            cols=["chromosome", "pos_start", "pos_end"],
            view_cols=["chromosome", "pos_start", "pos_end"],
            output_type="polars.DataFrame",
        )

        assert len(result) == 3
        assert "chromosome" in result.columns
        assert "pos_start" in result.columns
        assert "pos_end" in result.columns

    def test_complement_polars_extension(self):
        """Test complement via Polars extension method."""
        df = pl.DataFrame(
            {"chrom": ["chr1", "chr1"], "start": [100, 300], "end": [200, 400]}
        ).lazy()
        df.config_meta.set(coordinate_system_zero_based=False)

        view = pl.DataFrame({"chrom": ["chr1"], "start": [1], "end": [500]}).lazy()
        view.config_meta.set(coordinate_system_zero_based=False)

        result = df.pb.complement(view_df=view).collect()

        assert len(result) == 3
        assert result["start"].to_list() == [1, 201, 401]
        assert result["end"].to_list() == [99, 299, 500]

    def test_complement_chromosome_in_view_but_not_in_df(self):
        """Test complement when view_df contains chromosomes not present in df."""
        # chr1 has intervals, chr2 has no intervals
        df = pl.DataFrame(
            {"chrom": ["chr1", "chr1"], "start": [100, 300], "end": [200, 400]}
        ).lazy()
        df.config_meta.set(coordinate_system_zero_based=False)

        view = pl.DataFrame(
            {
                "chrom": ["chr1", "chr2"],
                "start": [1, 1],
                "end": [1000, 500],
            }
        ).lazy()
        view.config_meta.set(coordinate_system_zero_based=False)

        result = pb.complement(df, view_df=view, output_type="polars.DataFrame").sort(
            ["chrom", "start"]
        )

        # chr1: gaps at 1-99, 201-299, 401-1000
        # chr2: entire region is a gap 1-500
        assert len(result) == 4
        chr1_results = result.filter(pl.col("chrom") == "chr1")
        chr2_results = result.filter(pl.col("chrom") == "chr2")

        assert chr1_results["start"].to_list() == [1, 201, 401]
        assert chr1_results["end"].to_list() == [99, 299, 1000]

        assert chr2_results["start"].to_list() == [1]
        assert chr2_results["end"].to_list() == [500]
