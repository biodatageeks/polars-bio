import warnings

import pytest
from _expected import BIO_PD_DF1, BIO_PD_DF2

import polars_bio as pb
from polars_bio._metadata import (
    CoordinateSystemMismatchError,
    MissingCoordinateSystemError,
)
from polars_bio.constants import (
    POLARS_BIO_COORDINATE_SYSTEM_CHECK,
    POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED,
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


class TestCoordinateSystemCheckSessionParameter:
    """Tests for datafusion.bio.coordinate_system_check session parameter behavior."""

    def test_coordinate_system_check_true_raises_error(self):
        """Test that coordinate_system_check=true (default) raises error for missing metadata."""
        df1 = BIO_PD_DF1.copy()
        df2 = BIO_PD_DF2.copy()
        df1.attrs.clear()
        df2.attrs.clear()

        # Ensure default behavior (check=true)
        pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, True)

        try:
            # Default behavior should raise MissingCoordinateSystemError
            with pytest.raises(MissingCoordinateSystemError):
                pb.overlap(
                    df1,
                    df2,
                    cols1=("contig", "pos_start", "pos_end"),
                    cols2=("contig", "pos_start", "pos_end"),
                    output_type="pandas.DataFrame",
                    suffixes=("_1", "_3"),
                )
        finally:
            pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, True)

    def test_coordinate_system_check_false_uses_global_config(self):
        """Test that coordinate_system_check=false uses global config when metadata is missing."""
        df1 = BIO_PD_DF1.copy()
        df2 = BIO_PD_DF2.copy()
        df1.attrs.clear()
        df2.attrs.clear()

        # Disable strict coordinate system check
        pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, False)

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = pb.overlap(
                    df1,
                    df2,
                    cols1=("contig", "pos_start", "pos_end"),
                    cols2=("contig", "pos_start", "pos_end"),
                    output_type="pandas.DataFrame",
                    suffixes=("_1", "_3"),
                )

                # Check that a warning was emitted
                assert len(w) >= 1
                warning_messages = [str(warning.message) for warning in w]
                assert any(
                    "Coordinate system metadata is missing" in msg
                    for msg in warning_messages
                )
                assert any(
                    "POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED" in msg
                    for msg in warning_messages
                )

            assert len(result) > 0
        finally:
            # Reset to default (strict check)
            pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, True)

    def test_coordinate_system_check_false_with_custom_global_config(self):
        """Test that coordinate_system_check=false uses the correct global config value."""
        df1 = BIO_PD_DF1.copy()
        df2 = BIO_PD_DF2.copy()
        df1.attrs.clear()
        df2.attrs.clear()

        # Set global config to zero-based and disable check
        pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)
        pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, False)

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = pb.overlap(
                    df1,
                    df2,
                    cols1=("contig", "pos_start", "pos_end"),
                    cols2=("contig", "pos_start", "pos_end"),
                    output_type="pandas.DataFrame",
                    suffixes=("_1", "_3"),
                )

                # Check warning mentions 0-based
                warning_messages = [str(warning.message) for warning in w]
                assert any("0-based" in msg for msg in warning_messages)

            assert len(result) > 0
        finally:
            # Reset global config back to defaults
            pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, False)
            pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, True)

    def test_coordinate_system_check_false_no_warning_with_metadata(self):
        """Test that no warning is emitted when metadata is present even with check=false."""
        df1 = BIO_PD_DF1.copy()
        df2 = BIO_PD_DF2.copy()
        df1.attrs["coordinate_system_zero_based"] = False
        df2.attrs["coordinate_system_zero_based"] = False

        # Disable strict check
        pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, False)

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = pb.overlap(
                    df1,
                    df2,
                    cols1=("contig", "pos_start", "pos_end"),
                    cols2=("contig", "pos_start", "pos_end"),
                    output_type="pandas.DataFrame",
                    suffixes=("_1", "_3"),
                )

                # No warning about missing metadata should be emitted
                warning_messages = [str(warning.message) for warning in w]
                assert not any(
                    "Coordinate system metadata is missing" in msg
                    for msg in warning_messages
                )

            assert len(result) > 0
        finally:
            pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, True)

    def test_coordinate_system_check_false_merge_operation(self):
        """Test coordinate_system_check=false works for merge operation."""
        df1 = BIO_PD_DF1.copy()
        df1.attrs.clear()

        # Disable strict check
        pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, False)

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = pb.merge(
                    df1,
                    cols=("contig", "pos_start", "pos_end"),
                    output_type="pandas.DataFrame",
                )

                # Check that a warning was emitted
                assert len(w) >= 1
                warning_messages = [str(warning.message) for warning in w]
                assert any(
                    "Coordinate system metadata is missing" in msg
                    for msg in warning_messages
                )

            assert len(result) > 0
        finally:
            pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, True)

    def test_coordinate_system_check_false_nearest_operation(self):
        """Test coordinate_system_check=false works for nearest operation."""
        df1 = BIO_PD_DF1.copy()
        df2 = BIO_PD_DF2.copy()
        df1.attrs.clear()
        df2.attrs.clear()

        # Disable strict check
        pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, False)

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = pb.nearest(
                    df1,
                    df2,
                    cols1=("contig", "pos_start", "pos_end"),
                    cols2=("contig", "pos_start", "pos_end"),
                    output_type="pandas.DataFrame",
                    suffixes=("_1", "_3"),
                )

                # Check that a warning was emitted
                assert len(w) >= 1
                warning_messages = [str(warning.message) for warning in w]
                assert any(
                    "Coordinate system metadata is missing" in msg
                    for msg in warning_messages
                )

            assert len(result) > 0
        finally:
            pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, True)

    def test_coordinate_system_check_false_count_overlaps_operation(self):
        """Test coordinate_system_check=false works for count_overlaps operation."""
        df1 = BIO_PD_DF1.copy()
        df2 = BIO_PD_DF2.copy()
        df1.attrs.clear()
        df2.attrs.clear()

        # Disable strict check
        pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, False)

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = pb.count_overlaps(
                    df1,
                    df2,
                    cols1=("contig", "pos_start", "pos_end"),
                    cols2=("contig", "pos_start", "pos_end"),
                    output_type="pandas.DataFrame",
                )

                # Check that a warning was emitted
                assert len(w) >= 1
                warning_messages = [str(warning.message) for warning in w]
                assert any(
                    "Coordinate system metadata is missing" in msg
                    for msg in warning_messages
                )

            assert len(result) > 0
        finally:
            pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, True)
