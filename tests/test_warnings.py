import pytest
from _expected import BIO_PD_DF1, BIO_PD_DF2

import polars_bio as pb
from polars_bio._metadata import (
    CoordinateSystemMismatchError,
    MissingCoordinateSystemError,
)


class TestCoordinateSystemMetadata:
    """Tests for coordinate system metadata handling."""

    def test_missing_coordinate_system_error(self):
        """Test that MissingCoordinateSystemError is raised when metadata is missing."""
        # Create DataFrames without coordinate system metadata
        df1 = BIO_PD_DF1.copy()
        df2 = BIO_PD_DF2.copy()
        # Clear any existing metadata
        df1.attrs.clear()
        df2.attrs.clear()

        with pytest.raises(MissingCoordinateSystemError):
            pb.overlap(
                df1,
                df2,
                cols1=("contig", "pos_start", "pos_end"),
                cols2=("contig", "pos_start", "pos_end"),
                output_type="pandas.DataFrame",
                suffixes=("_1", "_3"),
            )

    def test_coordinate_system_mismatch_error(self):
        """Test that CoordinateSystemMismatchError is raised when coordinate systems don't match."""
        # Create DataFrames with mismatched coordinate systems
        df1 = BIO_PD_DF1.copy()
        df2 = BIO_PD_DF2.copy()
        df1.attrs["coordinate_system_zero_based"] = True
        df2.attrs["coordinate_system_zero_based"] = False

        with pytest.raises(CoordinateSystemMismatchError):
            pb.overlap(
                df1,
                df2,
                cols1=("contig", "pos_start", "pos_end"),
                cols2=("contig", "pos_start", "pos_end"),
                output_type="pandas.DataFrame",
                suffixes=("_1", "_3"),
            )

    def test_zero_based_metadata_works(self):
        """Test that 0-based coordinate system metadata works correctly."""
        df1 = BIO_PD_DF1.copy()
        df2 = BIO_PD_DF2.copy()
        df1.attrs["coordinate_system_zero_based"] = True
        df2.attrs["coordinate_system_zero_based"] = True

        # Should not raise any errors
        result = pb.overlap(
            df1,
            df2,
            cols1=("contig", "pos_start", "pos_end"),
            cols2=("contig", "pos_start", "pos_end"),
            output_type="pandas.DataFrame",
            suffixes=("_1", "_3"),
        )
        assert len(result) > 0

    def test_one_based_metadata_works(self):
        """Test that 1-based coordinate system metadata works correctly."""
        df1 = BIO_PD_DF1.copy()
        df2 = BIO_PD_DF2.copy()
        df1.attrs["coordinate_system_zero_based"] = False
        df2.attrs["coordinate_system_zero_based"] = False

        # Should not raise any errors
        result = pb.overlap(
            df1,
            df2,
            cols1=("contig", "pos_start", "pos_end"),
            cols2=("contig", "pos_start", "pos_end"),
            output_type="pandas.DataFrame",
            suffixes=("_1", "_3"),
        )
        assert len(result) > 0
