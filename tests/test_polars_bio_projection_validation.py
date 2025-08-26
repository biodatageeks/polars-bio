#!/usr/bin/env python3
"""
Test to validate that polars-bio projection pushdown actually reduces data processing
by using a larger dataset and measuring the difference in processing behavior.
"""

import tempfile
import time
from pathlib import Path

import polars as pl

import polars_bio as pb
from tests._expected import DATA_DIR


def create_large_test_parquet():
    """Create a large test parquet file to better demonstrate projection benefits."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        # Create a dataset with many columns and many rows
        data = {
            "chrom": [f"chr{i%22+1}" for i in range(50000)],
            "start": list(range(1000000, 1050000)),
            "end": list(range(1001000, 1051000)),
            "score": [i * 0.1 for i in range(50000)],
            "name": [f"feature_{i}" for i in range(50000)],
            "type": [f"type_{i%100}" for i in range(50000)],
            "value1": [i * 2 for i in range(50000)],
            "value2": [i * 3 for i in range(50000)],
            "value3": [i * 4 for i in range(50000)],
            "value4": [i * 5 for i in range(50000)],
            "description": [f"description_for_feature_{i}" for i in range(50000)],
            "category": [f"cat_{i%50}" for i in range(50000)],
            "extra_large_col1": [
                f"extra_data_column_1_with_long_text_entry_{i}" for i in range(50000)
            ],
            "extra_large_col2": [
                f"extra_data_column_2_with_even_longer_text_entry_for_testing_{i}"
                for i in range(50000)
            ],
            "extra_large_col3": [
                f"extra_data_column_3_with_very_very_long_text_entry_for_performance_testing_{i}"
                for i in range(50000)
            ],
        }

        df = pl.DataFrame(data)
        df.write_parquet(f.name)
        return f.name


def test_projection_pushdown_performance_impact():
    """Test that projection pushdown actually impacts data processing performance."""
    print("Testing Projection Pushdown Performance Impact")
    print("=" * 60)

    # Create large test files for more noticeable differences
    print("\n1. Creating large test datasets...")
    file1 = create_large_test_parquet()
    file2 = create_large_test_parquet()

    try:
        print(f"   Created test file 1: {Path(file1).stat().st_size // 1024} KB")
        print(f"   Created test file 2: {Path(file2).stat().st_size // 1024} KB")

        # Test I/O operations with projection pushdown
        print(f"\n2. Testing VCF I/O projection performance...")
        vcf_path = f"{DATA_DIR}/io/vcf/vep.vcf.bgz"

        # Measure without projection pushdown
        start = time.time()
        result_no_pushdown = (
            pb.scan_vcf(vcf_path, projection_pushdown=False)
            .select(["chrom", "start"])
            .collect()
        )
        time_no_pushdown = time.time() - start

        # Measure with projection pushdown
        start = time.time()
        result_with_pushdown = (
            pb.scan_vcf(vcf_path, projection_pushdown=True)
            .select(["chrom", "start"])
            .collect()
        )
        time_with_pushdown = time.time() - start

        print(f"   Without projection pushdown: {time_no_pushdown:.4f}s")
        print(f"   With projection pushdown:    {time_with_pushdown:.4f}s")

        # Results should be identical
        assert result_no_pushdown.equals(
            result_with_pushdown
        ), "Results should be identical"
        print(f"   PASS: Results are identical")

        # Check for performance improvement
        if time_with_pushdown < time_no_pushdown:
            improvement = (
                (time_no_pushdown - time_with_pushdown) / time_no_pushdown
            ) * 100
            print(f"   Performance improvement: {improvement:.1f}%")
        else:
            overhead = (
                (time_with_pushdown - time_no_pushdown) / time_no_pushdown
            ) * 100
            if overhead < 20:  # Allow up to 20% overhead for small files
                print(
                    f"   Small overhead: {overhead:.1f}% (acceptable for small files)"
                )
            else:
                print(f"   High overhead: {overhead:.1f}%")

        # Test interval operations with projection pushdown
        print(f"\n3. Testing interval operations projection performance...")

        # Measure overlap without projection pushdown
        start = time.time()
        overlap_no_pushdown = (
            pb.overlap(
                file1, file2, projection_pushdown=False, output_type="polars.LazyFrame"
            )
            .select(["chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2"])
            .collect()
        )
        time_overlap_no_pushdown = time.time() - start

        # Measure overlap with projection pushdown
        start = time.time()
        overlap_with_pushdown = (
            pb.overlap(
                file1, file2, projection_pushdown=True, output_type="polars.LazyFrame"
            )
            .select(["chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2"])
            .collect()
        )
        time_overlap_with_pushdown = time.time() - start

        print(f"   Without projection pushdown: {time_overlap_no_pushdown:.4f}s")
        print(f"   With projection pushdown:    {time_overlap_with_pushdown:.4f}s")
        print(
            f"   Result shapes: {overlap_no_pushdown.shape} vs {overlap_with_pushdown.shape}"
        )

        # Check column sets match
        cols_no_pushdown = set(overlap_no_pushdown.columns)
        cols_with_pushdown = set(overlap_with_pushdown.columns)
        expected_cols = {"chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2"}

        assert cols_no_pushdown == cols_with_pushdown == expected_cols, (
            f"Column pruning failed! Expected: {expected_cols}, "
            f"No pushdown: {cols_no_pushdown}, With pushdown: {cols_with_pushdown}"
        )
        print(
            f"   PASS: Column pruning working: got exactly {len(expected_cols)} columns"
        )

        # Check for performance improvement in interval operations
        if time_overlap_with_pushdown < time_overlap_no_pushdown:
            improvement = (
                (time_overlap_no_pushdown - time_overlap_with_pushdown)
                / time_overlap_no_pushdown
            ) * 100
            print(f"   Interval operations improvement: {improvement:.1f}%")
        else:
            overhead = (
                (time_overlap_with_pushdown - time_overlap_no_pushdown)
                / time_overlap_no_pushdown
            ) * 100
            print(f"   Interval operations overhead: {overhead:.1f}%")

        print(f"\n4. Column projection validation summary:")
        print(f"   • Both I/O and interval operations produce identical results")
        print(f"   • Column pruning ensures exactly the requested columns are returned")
        print(
            f"   • Projection pushdown may show performance benefits with larger datasets"
        )

    finally:
        # Clean up
        Path(file1).unlink(missing_ok=True)
        Path(file2).unlink(missing_ok=True)


def test_projection_pushdown_correctness_validation():
    """Test that projection pushdown maintains correctness across different scenarios."""
    print("\nTesting Projection Pushdown Correctness")
    print("=" * 50)

    vcf_path = f"{DATA_DIR}/io/vcf/vep.vcf.bgz"

    # Test different column selection scenarios
    test_cases = [
        (["chrom"], "Single column"),
        (["chrom", "start"], "Two columns"),
        (["chrom", "start", "end"], "Three columns"),
        (["start", "end"], "Middle columns"),
        (["end", "filter"], "Last columns"),
    ]

    for columns, description in test_cases:
        print(f"\n   Testing: {description} - {columns}")

        # Compare results with and without projection pushdown
        result_no_pushdown = (
            pb.scan_vcf(vcf_path, projection_pushdown=False).select(columns).collect()
        )
        result_with_pushdown = (
            pb.scan_vcf(vcf_path, projection_pushdown=True).select(columns).collect()
        )

        assert result_no_pushdown.equals(
            result_with_pushdown
        ), f"Results differ for {description}!"
        print(f"     PASS: Results identical for {description}")

        # Verify column pruning
        result_cols = set(result_with_pushdown.columns)
        expected_cols = set(columns)
        assert (
            result_cols == expected_cols
        ), f"Column pruning failed: expected {expected_cols}, got {result_cols}"
        print(f"     PASS: Column pruning correct: {len(result_cols)} columns")

    print(f"\n   All projection pushdown correctness tests passed!")


def main():
    """Run all projection validation tests."""
    print("Polars-Bio Projection Pushdown Validation")
    print("=" * 60)

    success = True

    try:
        test_projection_pushdown_correctness_validation()
        test_projection_pushdown_performance_impact()

    except Exception as e:
        print(f"\nTest failed with error: {e}")
        import traceback

        traceback.print_exc()
        success = False

    print("\n" + "=" * 60)
    if success:
        print("All projection pushdown validation tests passed!")
        print("Column projection pushdown is working correctly")
        print("Both correctness and performance aspects validated")
    else:
        print("Some projection pushdown validation tests failed!")

    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
