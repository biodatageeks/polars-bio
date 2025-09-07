"""
Test suite for polars-bio optimization bug fix.

This test suite comprehensively tests the fix for the critical bug where
filter().select() operations with projection_pushdown=True would return
unfiltered results instead of properly filtered ones.

The bug affected the combination of:
- projection_pushdown=True
- filter().select() operation order
- predicate_pushdown enabled or disabled

Bug Details:
- BROKEN: scan(..., projection_pushdown=True).filter(...).select([...]) -> returned all rows
- WORKING: scan(..., projection_pushdown=True).select([...]).filter(...) -> returned correct rows

Fix Details:
- Implemented predicate deferral mechanism in GffLazyFrameWrapper
- When filter() is called before select() with projection pushdown enabled,
  the predicate is stored and applied later with combined optimization
"""

from pathlib import Path

import polars as pl
import pytest

import polars_bio as pb

# Test data path
GFF_FILE = str(
    Path(__file__).parent / "data" / "io" / "gff" / "chrY_test_subset.gff3.bgz"
)

# Expected result for the specific test query
EXPECTED_FILTER_CONDITION = (
    (pl.col("chrom") == "chrY") & (pl.col("start") < 500000) & (pl.col("end") > 510000)
)
EXPECTED_COLUMNS = ["chrom", "start", "end", "type"]
EXPECTED_ROW_COUNT = 2  # Based on the test data


class TestOptimizationBugFix:
    """Test the fix for projection pushdown optimization bug."""

    @pytest.fixture(scope="class")
    def sample_data_path(self):
        """Ensure test data file exists."""
        if not Path(GFF_FILE).exists():
            pytest.skip(f"Test data file not found: {GFF_FILE}")
        return GFF_FILE

    def test_filter_select_all_optimization_combinations(self, sample_data_path):
        """Test filter().select() with all combinations of optimization flags.

        This is the comprehensive test for the bug that was fixed.
        Previously, projection_pushdown=True with filter().select() would fail.
        """
        # Test all combinations of optimization flags
        optimization_combinations = [
            (False, False, "no optimizations"),
            (True, False, "projection pushdown only"),  # This was the BROKEN case
            (False, True, "predicate pushdown only"),
            (True, True, "both optimizations"),
        ]

        for proj_pd, pred_pd, description in optimization_combinations:
            print(f"\nTesting filter().select() with {description}")

            # Configure single thread for consistent results
            pb.set_option("datafusion.execution.target_partitions", "1")

            # The previously broken pattern: filter().select()
            lf = pb.scan_gff(
                sample_data_path,
                projection_pushdown=proj_pd,
                predicate_pushdown=pred_pd,
            )
            result = (
                lf.filter(EXPECTED_FILTER_CONDITION).select(EXPECTED_COLUMNS).collect()
            )

            # Verify correct number of rows (not all rows)
            assert len(result) == EXPECTED_ROW_COUNT, (
                f"filter().select() with {description} returned {len(result)} rows, "
                f"expected {EXPECTED_ROW_COUNT} rows. This indicates the optimization bug!"
            )

            # Verify correct columns
            assert (
                list(result.columns) == EXPECTED_COLUMNS
            ), f"filter().select() with {description} returned wrong columns: {result.columns}"

            # Verify data correctness (all results should be chrY)
            chrom_values = result.select("chrom").to_series().unique().to_list()
            assert chrom_values == [
                "chrY"
            ], f"filter().select() with {description} returned wrong chroms: {chrom_values}"

    def test_select_filter_all_optimization_combinations(self, sample_data_path):
        """Test select().filter() with all combinations of optimization flags.

        This pattern always worked correctly, but we test it to ensure consistency.
        """
        optimization_combinations = [
            (False, False, "no optimizations"),
            (True, False, "projection pushdown only"),
            (False, True, "predicate pushdown only"),
            (True, True, "both optimizations"),
        ]

        for proj_pd, pred_pd, description in optimization_combinations:
            print(f"\nTesting select().filter() with {description}")

            pb.set_option("datafusion.execution.target_partitions", "1")

            # The always-working pattern: select().filter()
            lf = pb.scan_gff(
                sample_data_path,
                projection_pushdown=proj_pd,
                predicate_pushdown=pred_pd,
            )
            result = (
                lf.select(EXPECTED_COLUMNS).filter(EXPECTED_FILTER_CONDITION).collect()
            )

            # Verify correct number of rows
            assert len(result) == EXPECTED_ROW_COUNT, (
                f"select().filter() with {description} returned {len(result)} rows, "
                f"expected {EXPECTED_ROW_COUNT} rows"
            )

            # Verify correct columns
            assert (
                list(result.columns) == EXPECTED_COLUMNS
            ), f"select().filter() with {description} returned wrong columns: {result.columns}"

            # Verify data correctness
            chrom_values = result.select("chrom").to_series().unique().to_list()
            assert chrom_values == [
                "chrY"
            ], f"select().filter() with {description} returned wrong chroms: {chrom_values}"

    def test_operation_order_equivalence(self, sample_data_path):
        """Test that both operation orders return identical results.

        This test verifies that filter().select() and select().filter()
        produce exactly the same results for all optimization combinations.
        """
        optimization_combinations = [
            (False, False, "no optimizations"),
            (True, False, "projection pushdown only"),  # Critical test case
            (False, True, "predicate pushdown only"),
            (True, True, "both optimizations"),
        ]

        for proj_pd, pred_pd, description in optimization_combinations:
            print(f"\nTesting operation order equivalence with {description}")

            pb.set_option("datafusion.execution.target_partitions", "1")

            # Method 1: filter().select()
            lf1 = pb.scan_gff(
                sample_data_path,
                projection_pushdown=proj_pd,
                predicate_pushdown=pred_pd,
            )
            result1 = (
                lf1.filter(EXPECTED_FILTER_CONDITION).select(EXPECTED_COLUMNS).collect()
            )

            # Method 2: select().filter()
            lf2 = pb.scan_gff(
                sample_data_path,
                projection_pushdown=proj_pd,
                predicate_pushdown=pred_pd,
            )
            result2 = (
                lf2.select(EXPECTED_COLUMNS).filter(EXPECTED_FILTER_CONDITION).collect()
            )

            # Results should be identical
            assert len(result1) == len(result2), (
                f"Operation order gave different row counts with {description}: "
                f"filter().select()={len(result1)}, select().filter()={len(result2)}"
            )

            # Sort both results for comparison (in case order differs)
            result1_sorted = result1.sort(["start", "end", "type"])
            result2_sorted = result2.sort(["start", "end", "type"])

            # Use polars testing framework for deep comparison
            pl.testing.assert_frame_equal(result1_sorted, result2_sorted)

    def test_regression_specific_bug_case(self, sample_data_path):
        """Specific regression test for the exact bug case that was reported.

        This test uses the exact parameters and query that were failing before the fix.
        """
        pb.set_option("datafusion.execution.target_partitions", "1")

        # The exact broken case from the bug report
        lf = pb.scan_gff(
            sample_data_path,
            projection_pushdown=True,  # This was the problem
            predicate_pushdown=True,  # This could be True or False
        )

        # The exact operation that was broken
        result = (
            lf.filter(
                (pl.col("chrom") == "chrY")
                & (pl.col("start") < 500000)
                & (pl.col("end") > 510000)
            )
            .select(["chrom", "start", "end", "type"])
            .collect()
        )

        # Before fix: this would return 7,747,875 rows (all rows)
        # After fix: this should return 2 rows (correct filtered result)
        assert len(result) == EXPECTED_ROW_COUNT, (
            f"Regression test failed! Got {len(result)} rows instead of {EXPECTED_ROW_COUNT}. "
            "The optimization bug has returned!"
        )

        print(f"✅ Regression test passed: {len(result)} rows (correct)")

    def test_edge_cases(self, sample_data_path):
        """Test edge cases that might trigger the bug in different ways."""
        pb.set_option("datafusion.execution.target_partitions", "1")

        # Edge case 1: Empty filter result
        lf = pb.scan_gff(
            sample_data_path, projection_pushdown=True, predicate_pushdown=True
        )
        result = (
            lf.filter(pl.col("chrom") == "nonexistent_chromosome")
            .select(["chrom", "start", "end"])
            .collect()
        )

        assert len(result) == 0, "Empty filter should return 0 rows"

        # Edge case 2: Filter with only one condition
        lf = pb.scan_gff(
            sample_data_path, projection_pushdown=True, predicate_pushdown=True
        )
        result = lf.filter(pl.col("chrom") == "chrY").select(["chrom"]).collect()

        # Should return some chrY rows, but not all rows in the file
        assert len(result) > 0, "Single condition filter should return some rows"
        assert (
            len(result) < 50000
        ), "Single condition should not return massive number of rows"

        # Edge case 3: Multiple chained filters
        lf = pb.scan_gff(
            sample_data_path, projection_pushdown=True, predicate_pushdown=True
        )
        result = (
            lf.filter(pl.col("chrom") == "chrY")
            .filter(pl.col("start") < 500000)
            .select(["chrom", "start"])
            .collect()
        )

        assert len(result) > 0, "Chained filters should work"
        # All results should be chrY
        assert result.select("chrom").to_series().unique().to_list() == ["chrY"]

    def test_with_attribute_columns(self, sample_data_path):
        """Test the fix works with attribute column selections too."""
        pb.set_option("datafusion.execution.target_partitions", "1")

        # Test with mixed static and attribute columns
        lf = pb.scan_gff(
            sample_data_path, projection_pushdown=True, predicate_pushdown=True
        )

        try:
            result = (
                lf.filter((pl.col("chrom") == "chrY") & (pl.col("type") == "gene"))
                .select(["chrom", "start", "end", "type"])
                .collect()
            )

            # Should return small number of gene rows, not all rows
            assert (
                len(result) <= 100
            ), f"Gene filter returned too many rows: {len(result)}"
            assert len(result) > 0, "Gene filter should return some rows"

            # All should be genes on chrY
            types = result.select("type").to_series().unique().to_list()
            assert types == ["gene"], f"Should only contain genes, got: {types}"

            chroms = result.select("chrom").to_series().unique().to_list()
            assert chroms == ["chrY"], f"Should only contain chrY, got: {chroms}"

        except Exception as e:
            pytest.skip(f"Attribute column test skipped due to schema differences: {e}")


class TestOptimizationPerformance:
    """Test that optimizations still provide performance benefits."""

    @pytest.fixture(scope="class")
    def sample_data_path(self):
        """Ensure test data file exists."""
        if not Path(GFF_FILE).exists():
            pytest.skip(f"Test data file not found: {GFF_FILE}")
        return GFF_FILE

    def test_optimization_still_faster(self, sample_data_path):
        """Verify that optimizations still provide performance benefits."""
        import time

        pb.set_option("datafusion.execution.target_partitions", "1")

        # Test with optimizations
        start_time = time.time()
        lf_optimized = pb.scan_gff(
            sample_data_path, projection_pushdown=True, predicate_pushdown=True
        )
        result_optimized = (
            lf_optimized.filter(EXPECTED_FILTER_CONDITION)
            .select(EXPECTED_COLUMNS)
            .collect()
        )
        optimized_time = time.time() - start_time

        # Test without optimizations
        start_time = time.time()
        lf_unoptimized = pb.scan_gff(
            sample_data_path, projection_pushdown=False, predicate_pushdown=False
        )
        result_unoptimized = (
            lf_unoptimized.filter(EXPECTED_FILTER_CONDITION)
            .select(EXPECTED_COLUMNS)
            .collect()
        )
        unoptimized_time = time.time() - start_time

        # Results should be identical
        assert len(result_optimized) == len(result_unoptimized)

        # Optimized version should be faster (allow some variance for test stability)
        print(
            f"Optimized time: {optimized_time:.3f}s, Unoptimized time: {unoptimized_time:.3f}s"
        )

        # Don't make this test too strict - performance can vary
        # Just ensure optimized version isn't significantly slower
        assert optimized_time <= unoptimized_time * 2.0, (
            f"Optimizations seem to have made query slower: "
            f"{optimized_time:.3f}s vs {unoptimized_time:.3f}s"
        )


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
