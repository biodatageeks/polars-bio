import tempfile
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

import polars_bio as pb


class TestSuffixHandling:
    """Tests to ensure suffix handling is correct for overlap operations."""

    @pytest.fixture
    def sample_dataframes(self):
        """Create sample DataFrames for testing suffix handling."""
        df_a = pl.DataFrame(
            {
                "chrom": ["chr1", "chr1"],
                "start": [1, 5],
                "end": [3, 8],
                "onlyA": [10, 20],
                "shared_col": ["A1", "A2"],
            }
        )

        df_b = pl.DataFrame(
            {
                "chrom": ["chr1"],
                "start": [2],
                "end": [6],
                "onlyB": [99],
                "cluster_id": [7],
                "shared_col": ["B1"],
            }
        )

        return df_a, df_b

    @pytest.fixture
    def sample_parquet_files(self, sample_dataframes):
        """Create temporary parquet files for testing file path scenario."""
        df_a, df_b = sample_dataframes

        with tempfile.TemporaryDirectory() as temp_dir:
            file_a = Path(temp_dir) / "df_a.parquet"
            file_b = Path(temp_dir) / "df_b.parquet"

            df_a.write_parquet(file_a)
            df_b.write_parquet(file_b)

            yield str(file_a), str(file_b)

    def test_suffix_handling_dataframes(self, sample_dataframes):
        """Test suffix handling with DataFrame inputs."""
        df_a, df_b = sample_dataframes

        # Test with default suffixes (_1, _2)
        result = pb.overlap(df_a, df_b, output_type="polars.DataFrame")

        # Check that coordinate columns have correct suffixes
        assert "chrom_1" in result.columns
        assert "start_1" in result.columns
        assert "end_1" in result.columns
        assert "chrom_2" in result.columns
        assert "start_2" in result.columns
        assert "end_2" in result.columns

        # Check that non-coordinate columns have correct suffixes
        # df_a columns should get suffix _1
        assert "onlyA_1" in result.columns
        assert "shared_col_1" in result.columns

        # df_b columns should get suffix _2
        assert "onlyB_2" in result.columns
        assert "cluster_id_2" in result.columns
        assert "shared_col_2" in result.columns

        # Verify no swapped suffixes exist
        assert "onlyA_2" not in result.columns
        assert "onlyB_1" not in result.columns
        assert "cluster_id_1" not in result.columns

        # Test with custom suffixes
        result_custom = pb.overlap(
            df_a, df_b, suffixes=("_left", "_right"), output_type="polars.DataFrame"
        )

        # Check coordinate columns
        assert "chrom_left" in result_custom.columns
        assert "chrom_right" in result_custom.columns

        # Check non-coordinate columns
        assert "onlyA_left" in result_custom.columns
        assert "onlyB_right" in result_custom.columns
        assert "cluster_id_right" in result_custom.columns
        assert "shared_col_left" in result_custom.columns
        assert "shared_col_right" in result_custom.columns

    def test_suffix_handling_file_paths(self, sample_parquet_files):
        """Test suffix handling with file path inputs."""
        file_a, file_b = sample_parquet_files

        # Test with default suffixes (_1, _2)
        result = pb.overlap(file_a, file_b, output_type="polars.DataFrame")

        # Check that coordinate columns have correct suffixes
        assert "chrom_1" in result.columns
        assert "start_1" in result.columns
        assert "end_1" in result.columns
        assert "chrom_2" in result.columns
        assert "start_2" in result.columns
        assert "end_2" in result.columns

        # Check that non-coordinate columns have correct suffixes
        # df_a columns should get suffix _1
        assert "onlyA_1" in result.columns
        assert "shared_col_1" in result.columns

        # df_b columns should get suffix _2
        assert "onlyB_2" in result.columns
        assert "cluster_id_2" in result.columns
        assert "shared_col_2" in result.columns

        # Verify no swapped suffixes exist
        assert "onlyA_2" not in result.columns
        assert "onlyB_1" not in result.columns
        assert "cluster_id_1" not in result.columns

        # Test with custom suffixes
        result_custom = pb.overlap(
            file_a, file_b, suffixes=("_A", "_B"), output_type="polars.DataFrame"
        )

        # Check coordinate columns
        assert "chrom_A" in result_custom.columns
        assert "chrom_B" in result_custom.columns

        # Check non-coordinate columns
        assert "onlyA_A" in result_custom.columns
        assert "onlyB_B" in result_custom.columns
        assert "cluster_id_B" in result_custom.columns
        assert "shared_col_A" in result_custom.columns
        assert "shared_col_B" in result_custom.columns

    def test_suffix_handling_lazy_frame(self, sample_dataframes):
        """Test suffix handling with LazyFrame output."""
        df_a, df_b = sample_dataframes

        result = pb.overlap(df_a, df_b, output_type="polars.LazyFrame")
        collected = result.collect()

        # Check that coordinate columns have correct suffixes
        assert "chrom_1" in collected.columns
        assert "start_1" in collected.columns
        assert "end_1" in collected.columns
        assert "chrom_2" in collected.columns
        assert "start_2" in collected.columns
        assert "end_2" in collected.columns

        # Check that non-coordinate columns have correct suffixes
        assert "onlyA_1" in collected.columns
        assert "shared_col_1" in collected.columns
        assert "onlyB_2" in collected.columns
        assert "cluster_id_2" in collected.columns
        assert "shared_col_2" in collected.columns

    def test_suffix_handling_pandas_output(self, sample_dataframes):
        """Test suffix handling with pandas DataFrame output."""
        df_a, df_b = sample_dataframes

        result = pb.overlap(df_a, df_b, output_type="pandas.DataFrame")

        # Check that coordinate columns have correct suffixes
        assert "chrom_1" in result.columns
        assert "start_1" in result.columns
        assert "end_1" in result.columns
        assert "chrom_2" in result.columns
        assert "start_2" in result.columns
        assert "end_2" in result.columns

        # Check that non-coordinate columns have correct suffixes
        assert "onlyA_1" in result.columns
        assert "shared_col_1" in result.columns
        assert "onlyB_2" in result.columns
        assert "cluster_id_2" in result.columns
        assert "shared_col_2" in result.columns

    def test_data_integrity(self, sample_dataframes):
        """Test that the actual data values are correctly assigned to suffixed columns."""
        df_a, df_b = sample_dataframes

        result = pb.overlap(df_a, df_b, output_type="polars.DataFrame")

        # Check that data from df_a appears in _1 suffixed columns
        assert result["onlyA_1"].to_list() == [10, 20]  # Values from df_a
        assert result["shared_col_1"].to_list() == ["A1", "A2"]  # Values from df_a

        # Check that data from df_b appears in _2 suffixed columns
        assert result["onlyB_2"].to_list() == [
            99,
            99,
        ]  # Values from df_b (repeated for overlaps)
        assert result["cluster_id_2"].to_list() == [
            7,
            7,
        ]  # Values from df_b (repeated for overlaps)
        assert result["shared_col_2"].to_list() == [
            "B1",
            "B1",
        ]  # Values from df_b (repeated for overlaps)
