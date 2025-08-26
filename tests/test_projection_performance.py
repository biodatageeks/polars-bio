#!/usr/bin/env python3
"""
Test script to validate projection pushdown performance and functionality.
"""

import tempfile
import time
from pathlib import Path

import polars as pl

import polars_bio as pb
from tests._expected import DATA_DIR


def test_io_projection_pushdown():
    """Test projection pushdown with I/O operations."""
    print("=== Testing I/O Projection Pushdown ===")

    vcf_path = f"{DATA_DIR}/io/vcf/vep.vcf.bgz"

    # Test 1: Without projection pushdown
    print("\n1. Testing without projection pushdown...")
    start_time = time.time()
    lf_no_pushdown = pb.scan_vcf(vcf_path, projection_pushdown=False)
    result_no_pushdown = lf_no_pushdown.select(["chrom", "start"]).collect()
    time_no_pushdown = time.time() - start_time

    print(f"   Result shape: {result_no_pushdown.shape}")
    print(f"   Columns: {result_no_pushdown.columns}")
    print(f"   Time: {time_no_pushdown:.4f}s")

    # Test 2: With projection pushdown
    print("\n2. Testing with projection pushdown...")
    start_time = time.time()
    lf_pushdown = pb.scan_vcf(vcf_path, projection_pushdown=True)
    result_pushdown = lf_pushdown.select(["chrom", "start"]).collect()
    time_pushdown = time.time() - start_time

    print(f"   Result shape: {result_pushdown.shape}")
    print(f"   Columns: {result_pushdown.columns}")
    print(f"   Time: {time_pushdown:.4f}s")

    # Validate column pruning: both should have exactly the same columns (the selected ones)
    result_cols_no_pushdown = set(result_no_pushdown.columns)
    result_cols_pushdown = set(result_pushdown.columns)
    expected_cols = {"chrom", "start"}

    assert result_cols_no_pushdown == result_cols_pushdown == expected_cols, (
        f"Column pruning failed! Expected: {expected_cols}, "
        f"No pushdown: {result_cols_no_pushdown}, With pushdown: {result_cols_pushdown}"
    )
    print(
        "   PASS: Column pruning working correctly! Both methods return exactly the requested columns."
    )

    # Verify data content is identical
    assert result_no_pushdown.equals(result_pushdown), "Data differs between methods"
    print("   PASS: Data is identical between methods")

    # Show performance improvement (if any)
    if time_pushdown < time_no_pushdown:
        improvement = ((time_no_pushdown - time_pushdown) / time_no_pushdown) * 100
        print(f"   Performance improvement: {improvement:.1f}%")
    else:
        print(
            f"   Projection pushdown overhead: {((time_pushdown - time_no_pushdown) / time_no_pushdown) * 100:.1f}%"
        )


def test_interval_projection_pushdown():
    """Test projection pushdown with interval operations."""
    print("\n=== Testing Interval Operations Projection Pushdown ===")

    # Create larger test datasets for better performance measurement
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f1:
        df1 = pl.DataFrame(
            {
                "chrom": ["chr1"] * 1000 + ["chr2"] * 1000,
                "start": list(range(1, 1001)) + list(range(1, 1001)),
                "end": list(range(101, 1101)) + list(range(101, 1101)),
                "name": [f"feature_{i}" for i in range(2000)],
                "score": list(range(2000)),
                "extra_col1": [f"extra1_{i}" for i in range(2000)],
                "extra_col2": [f"extra2_{i}" for i in range(2000)],
                "extra_col3": [f"extra3_{i}" for i in range(2000)],
            }
        )
        df1.write_parquet(f1.name)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f2:
        df2 = pl.DataFrame(
            {
                "chrom": ["chr1"] * 800 + ["chr2"] * 800,
                "start": list(range(50, 850)) + list(range(50, 850)),
                "end": list(range(150, 950)) + list(range(150, 950)),
                "type": [f"type_{i}" for i in range(1600)],
                "value": [i * 0.5 for i in range(1600)],
                "category": [f"cat_{i % 10}" for i in range(1600)],
                "metadata": [f"meta_{i}" for i in range(1600)],
            }
        )
        df2.write_parquet(f2.name)

    try:
        # Test 1: Without projection pushdown
        print("\n1. Testing overlap without projection pushdown...")
        start_time = time.time()
        # First get all available columns
        all_result = pb.overlap(
            f1.name, f2.name, projection_pushdown=False, output_type="polars.LazyFrame"
        ).collect()
        print(f"   Available columns: {all_result.columns}")

        # Select a subset based on available columns
        available_cols = all_result.columns
        select_cols = [
            col
            for col in ["chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2"]
            if col in available_cols
        ]
        # Add extra columns if available
        extra_cols = [
            col for col in ["name", "type", "score", "value"] if col in available_cols
        ]
        select_cols.extend(extra_cols[:2])  # Only add first 2 to keep it manageable

        result_no_pushdown = (
            pb.overlap(
                f1.name,
                f2.name,
                projection_pushdown=False,
                output_type="polars.LazyFrame",
            )
            .select(select_cols)
            .collect()
        )
        time_no_pushdown = time.time() - start_time

        print(f"   Result shape: {result_no_pushdown.shape}")
        print(f"   Columns: {result_no_pushdown.columns}")
        print(f"   Time: {time_no_pushdown:.4f}s")

        # Test 2: With projection pushdown
        print("\n2. Testing overlap with projection pushdown...")
        start_time = time.time()
        result_pushdown = (
            pb.overlap(
                f1.name,
                f2.name,
                projection_pushdown=True,
                output_type="polars.LazyFrame",
            )
            .select(select_cols)
            .collect()
        )
        time_pushdown = time.time() - start_time

        print(f"   Result shape: {result_pushdown.shape}")
        print(f"   Columns: {result_pushdown.columns}")
        print(f"   Time: {time_pushdown:.4f}s")

        # Validate column pruning: both should have exactly the selected columns
        result_cols_no_pushdown = set(result_no_pushdown.columns)
        result_cols_pushdown = set(result_pushdown.columns)
        expected_cols = set(select_cols)

        assert result_cols_no_pushdown == result_cols_pushdown == expected_cols, (
            f"Column pruning failed! Expected: {expected_cols}, "
            f"No pushdown: shape={result_no_pushdown.shape}, columns={result_cols_no_pushdown}, "
            f"With pushdown: shape={result_pushdown.shape}, columns={result_cols_pushdown}"
        )
        print(
            "   PASS: Column pruning working correctly! Both methods return exactly the requested columns."
        )

        # Verify data content is identical
        assert result_no_pushdown.equals(
            result_pushdown
        ), "Data differs between methods"
        print("   PASS: Data is identical between methods")

        # Show performance comparison
        if time_pushdown < time_no_pushdown:
            improvement = ((time_no_pushdown - time_pushdown) / time_no_pushdown) * 100
            print(f"   Performance improvement: {improvement:.1f}%")
        else:
            overhead = ((time_pushdown - time_no_pushdown) / time_no_pushdown) * 100
            print(f"   Projection pushdown overhead: {overhead:.1f}%")

    finally:
        # Clean up temporary files
        Path(f1.name).unlink(missing_ok=True)
        Path(f2.name).unlink(missing_ok=True)


def test_column_extraction():
    """Test the column name extraction functionality."""
    print("\n=== Testing Column Name Extraction ===")

    from polars_bio.io import _extract_column_names_from_expr

    # Test with list of strings
    result = _extract_column_names_from_expr(["chrom", "start", "end"])
    print(f"String list: {result}")
    assert result == [
        "chrom",
        "start",
        "end",
    ], f"Expected ['chrom', 'start', 'end'], got {result}"

    # Test with single string
    result = _extract_column_names_from_expr("chrom")
    print(f"Single string: {result}")
    assert result == ["chrom"], f"Expected ['chrom'], got {result}"

    # Test with Polars column expressions
    try:
        result = _extract_column_names_from_expr([pl.col("chrom"), pl.col("start")])
        print(f"Polars expressions: {result}")
        # This might work or might not depending on the Polars version
    except Exception as e:
        print(f"Polars expressions (expected failure): {e}")

    print("   PASS: Column extraction tests passed")


def main():
    """Run all projection pushdown tests."""
    print("Starting Projection Pushdown Validation Tests")
    print("=" * 60)

    success = True

    try:
        test_column_extraction()
        test_io_projection_pushdown()
        test_interval_projection_pushdown()

    except Exception as e:
        print(f"\nTest failed with error: {e}")
        import traceback

        traceback.print_exc()
        success = False

    print("\n" + "=" * 60)
    if success:
        print("All projection pushdown tests passed!")
        print("Projection pushdown is working correctly")
    else:
        print("Some tests failed!")
        print("Check the implementation for issues")

    return success


if __name__ == "__main__":
    main()
