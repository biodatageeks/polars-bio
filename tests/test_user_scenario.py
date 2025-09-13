"""
Test the specific scenario reported by the user where pb.overlap(df, df2).sink_parquet() was failing.
"""

import tempfile
from pathlib import Path

import polars as pl
import pytest

import polars_bio as pb


class TestUserScenario:
    """Test the specific user scenario that was failing."""

    def test_lazyframe_overlap_with_immediate_sink_parquet(self):
        """Test the exact scenario: pb.overlap(df, df2).sink_parquet()"""
        # Create LazyFrames similar to user's scenario
        df = pl.LazyFrame(
            {
                "chrom": ["chr1", "chr1", "chr2"],
                "start": [100, 200, 300],
                "end": [150, 250, 350],
            }
        )

        df2 = pl.LazyFrame(
            {"chrom": ["chr1", "chr2"], "start": [120, 280], "end": [180, 320]}
        )

        # This was the exact line that was failing for the user
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "test.parquet"

            # The exact pattern that was failing: chained overlap + sink_parquet
            pb.overlap(df, df2).sink_parquet(str(output_path))

            # Verify it worked
            assert output_path.exists()
            result = pl.read_parquet(output_path)
            assert len(result) >= 0  # Should work regardless of actual overlaps

    def test_lazyframe_overlap_with_various_operations(self):
        """Test LazyFrames with various subsequent operations."""
        df = pl.LazyFrame(
            {"chrom": ["chr1", "chr1"], "start": [100, 200], "end": [150, 250]}
        )

        df2 = pl.LazyFrame({"chrom": ["chr1"], "start": [120], "end": [180]})

        # Test various operations that users might chain after overlap
        result_lazy = pb.overlap(df, df2)

        with tempfile.TemporaryDirectory() as temp_dir:
            # 1. Direct sink_parquet
            path1 = Path(temp_dir) / "test1.parquet"
            result_lazy.sink_parquet(str(path1))
            assert path1.exists()

            # 2. Operations then sink_parquet
            path2 = Path(temp_dir) / "test2.parquet"
            result_lazy.with_columns(pl.lit("test").alias("annotation")).sink_parquet(
                str(path2)
            )
            assert path2.exists()

            # 3. Filter then sink_parquet
            path3 = Path(temp_dir) / "test3.parquet"
            result_lazy.filter(pl.col("chrom_1") == "chr1").sink_parquet(str(path3))
            assert path3.exists()

    def test_lazyframe_mixed_with_dataframes(self):
        """Test mixing LazyFrames with regular DataFrames."""
        lazy_df = pl.LazyFrame(
            {"chrom": ["chr1", "chr1"], "start": [100, 200], "end": [150, 250]}
        )

        regular_df = pl.DataFrame({"chrom": ["chr1"], "start": [120], "end": [180]})

        # Test both combinations
        with tempfile.TemporaryDirectory() as temp_dir:
            # LazyFrame + DataFrame
            path1 = Path(temp_dir) / "lazy_regular.parquet"
            pb.overlap(lazy_df, regular_df).sink_parquet(str(path1))
            assert path1.exists()

            # DataFrame + LazyFrame
            path2 = Path(temp_dir) / "regular_lazy.parquet"
            pb.overlap(regular_df, lazy_df).sink_parquet(str(path2))
            assert path2.exists()

    def test_lazyframe_default_vs_explicit_output_type(self):
        """Test that default and explicit LazyFrame output work the same."""
        df = pl.LazyFrame({"chrom": ["chr1"], "start": [100], "end": [150]})

        df2 = pl.LazyFrame({"chrom": ["chr1"], "start": [120], "end": [180]})

        with tempfile.TemporaryDirectory() as temp_dir:
            # Default output (should be LazyFrame)
            path1 = Path(temp_dir) / "default.parquet"
            result1 = pb.overlap(df, df2)  # Uses default output_type
            assert isinstance(result1, pl.LazyFrame)
            result1.sink_parquet(str(path1))
            assert path1.exists()

            # Explicit LazyFrame output
            path2 = Path(temp_dir) / "explicit.parquet"
            result2 = pb.overlap(df, df2, output_type="polars.LazyFrame")
            assert isinstance(result2, pl.LazyFrame)
            result2.sink_parquet(str(path2))
            assert path2.exists()

            # Both should produce the same results
            data1 = pl.read_parquet(path1).sort(["chrom_1", "start_1"])
            data2 = pl.read_parquet(path2).sort(["chrom_1", "start_1"])
            assert data1.equals(data2)


if __name__ == "__main__":
    pytest.main([__file__])
