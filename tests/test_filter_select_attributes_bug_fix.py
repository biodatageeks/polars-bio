"""
Test cases for the filter().select() attributes bug fix.

This test module covers the specific bug where filter().select([..., "ID"])
returns all rows instead of applying the filter correctly when attribute
columns like "ID" are involved.

Bug: https://github.com/wheretrue/polars-bio/issues/filter-select-attributes
"""

from pathlib import Path

import polars as pl
import pytest

import polars_bio as pb


@pytest.fixture(scope="module")
def test_gff_file(tmp_path_factory):
    """Create a test GFF file for attribute filtering tests."""
    tmp_path = tmp_path_factory.mktemp("gff_tests")
    gff_path = tmp_path / "test_filter_attributes.gff3"

    # Create a minimal GFF with various chromosomes and features
    gff_content = """##gff-version 3
chr1	test	gene	1000	2000	.	+	.	ID=GENE001;Name=gene1;Type=protein_coding
chr1	test	transcript	1000	2000	.	+	.	ID=TRANS001;Parent=GENE001;Name=transcript1
chr1	test	exon	1000	1200	.	+	.	ID=EXON001;Parent=TRANS001
chr1	test	exon	1800	2000	.	+	.	ID=EXON002;Parent=TRANS001
chrY	test	gene	386962	511616	.	+	.	ID=GENE_Y1;Name=gene_y1;Type=protein_coding
chrY	test	transcript	387035	511616	.	+	.	ID=TRANS_Y1;Parent=GENE_Y1;Name=transcript_y1
chr2	test	gene	5000	6000	.	-	.	ID=GENE002;Name=gene2;Type=pseudogene
chr2	test	exon	5000	6000	.	-	.	ID=EXON003;Parent=GENE002
chrX	test	gene	100000	200000	.	+	.	ID=GENE_X1;Name=gene_x1;Type=lncRNA
"""

    with open(gff_path, "w") as f:
        f.write(gff_content)

    return str(gff_path)


class TestFilterSelectAttributesBug:
    """Test cases for the filter().select() attributes bug."""

    def test_filter_then_select_with_id_bug_reproduction(self, test_gff_file):
        """Test the exact bug scenario: filter().select([..., "ID"]) returns wrong count."""
        predicate = (
            (pl.col("chrom") == "chrY")
            & (pl.col("start") < 500000)
            & (pl.col("end") > 510000)
        )

        # This was the broken pattern - should return 2 rows, not all rows
        result_with_id = (
            pb.scan_gff(
                test_gff_file, predicate_pushdown=False, projection_pushdown=False
            )
            .filter(predicate)
            .select(["chrom", "start", "end", "ID"])
            .collect()
        )

        assert (
            result_with_id.height == 2
        ), f"Expected 2 rows, got {result_with_id.height}"
        assert all(result_with_id["chrom"] == "chrY"), "All results should be from chrY"
        assert "GENE_Y1" in result_with_id["ID"].to_list(), "Should contain GENE_Y1"
        assert "TRANS_Y1" in result_with_id["ID"].to_list(), "Should contain TRANS_Y1"

    def test_filter_then_select_without_id_works(self, test_gff_file):
        """Test that the same filter works correctly without attribute columns."""
        predicate = (
            (pl.col("chrom") == "chrY")
            & (pl.col("start") < 500000)
            & (pl.col("end") > 510000)
        )

        result_without_id = (
            pb.scan_gff(
                test_gff_file, predicate_pushdown=False, projection_pushdown=False
            )
            .filter(predicate)
            .select(["chrom", "start", "end", "type"])
            .collect()
        )

        assert (
            result_without_id.height == 2
        ), f"Expected 2 rows, got {result_without_id.height}"
        assert all(
            result_without_id["chrom"] == "chrY"
        ), "All results should be from chrY"

    def test_select_then_filter_with_id_works(self, test_gff_file):
        """Test that select().filter() pattern works correctly (was never broken)."""
        predicate = (
            (pl.col("chrom") == "chrY")
            & (pl.col("start") < 500000)
            & (pl.col("end") > 510000)
        )

        result_select_first = (
            pb.scan_gff(
                test_gff_file, predicate_pushdown=False, projection_pushdown=False
            )
            .select(["chrom", "start", "end", "ID"])
            .filter(predicate)
            .collect()
        )

        assert (
            result_select_first.height == 2
        ), f"Expected 2 rows, got {result_select_first.height}"
        assert all(
            result_select_first["chrom"] == "chrY"
        ), "All results should be from chrY"
        assert (
            "GENE_Y1" in result_select_first["ID"].to_list()
        ), "Should contain GENE_Y1"
        assert (
            "TRANS_Y1" in result_select_first["ID"].to_list()
        ), "Should contain TRANS_Y1"

    def test_consistency_between_patterns(self, test_gff_file):
        """Test that filter().select() and select().filter() return identical results."""
        predicate = (
            (pl.col("chrom") == "chrY")
            & (pl.col("start") < 500000)
            & (pl.col("end") > 510000)
        )

        # Pattern 1: filter().select() (was broken)
        result1 = (
            pb.scan_gff(
                test_gff_file, predicate_pushdown=False, projection_pushdown=False
            )
            .filter(predicate)
            .select(["chrom", "start", "end", "ID"])
            .collect()
            .sort("ID")
        )

        # Pattern 2: select().filter() (always worked)
        result2 = (
            pb.scan_gff(
                test_gff_file, predicate_pushdown=False, projection_pushdown=False
            )
            .select(["chrom", "start", "end", "ID"])
            .filter(predicate)
            .collect()
            .sort("ID")
        )

        # They should be identical
        assert result1.equals(result2), "Both patterns should return identical results"
        assert result1.height == 2, "Both should return exactly 2 rows"

    def test_multiple_attribute_columns(self, test_gff_file):
        """Test filter().select() with multiple attribute columns."""
        predicate = pl.col("chrom") == "chr1"

        result = (
            pb.scan_gff(
                test_gff_file, predicate_pushdown=False, projection_pushdown=False
            )
            .filter(predicate)
            .select(["chrom", "start", "end", "ID", "Name", "Type"])
            .collect()
        )

        assert result.height == 4, f"Expected 4 rows for chr1, got {result.height}"
        assert all(result["chrom"] == "chr1"), "All results should be from chr1"

        # Check that attribute extraction worked
        expected_ids = {"GENE001", "TRANS001", "EXON001", "EXON002"}
        actual_ids = set(result["ID"].to_list())
        assert (
            actual_ids == expected_ids
        ), f"Expected IDs {expected_ids}, got {actual_ids}"

    def test_complex_filter_with_attributes(self, test_gff_file):
        """Test complex filters with attribute column selection."""
        predicate = (
            (pl.col("chrom").is_in(["chr1", "chr2"]))
            & (pl.col("start") >= 1000)
            & (pl.col("end") <= 6000)
            & (pl.col("type") == "gene")
        )

        result = (
            pb.scan_gff(
                test_gff_file, predicate_pushdown=False, projection_pushdown=False
            )
            .filter(predicate)
            .select(["chrom", "type", "ID", "Name", "Type"])
            .collect()
        )

        assert result.height == 2, f"Expected 2 gene rows, got {result.height}"

        # Check that we got the correct genes
        expected_ids = {"GENE001", "GENE002"}
        actual_ids = set(result["ID"].to_list())
        assert (
            actual_ids == expected_ids
        ), f"Expected gene IDs {expected_ids}, got {actual_ids}"

    def test_empty_result_filter_with_attributes(self, test_gff_file):
        """Test that filters returning empty results work with attributes."""
        predicate = pl.col("chrom") == "nonexistent_chrom"

        result = (
            pb.scan_gff(
                test_gff_file, predicate_pushdown=False, projection_pushdown=False
            )
            .filter(predicate)
            .select(["chrom", "start", "end", "ID"])
            .collect()
        )

        assert (
            result.height == 0
        ), f"Expected 0 rows for nonexistent chromosome, got {result.height}"
        assert result.columns == [
            "chrom",
            "start",
            "end",
            "ID",
        ], "Should preserve column structure"

    def test_with_predicate_pushdown_enabled(self, test_gff_file):
        """Test that the fix works even with predicate pushdown enabled."""
        predicate = (
            (pl.col("chrom") == "chrY")
            & (pl.col("start") < 500000)
            & (pl.col("end") > 510000)
        )

        result = (
            pb.scan_gff(
                test_gff_file, predicate_pushdown=True, projection_pushdown=False
            )
            .filter(predicate)
            .select(["chrom", "start", "end", "ID"])
            .collect()
        )

        assert (
            result.height == 2
        ), f"Expected 2 rows with predicate pushdown, got {result.height}"
        assert all(result["chrom"] == "chrY"), "All results should be from chrY"

    def test_attributes_column_selection(self, test_gff_file):
        """Test that selecting the raw 'attributes' column also works with filters."""
        predicate = pl.col("chrom") == "chr1"

        result = (
            pb.scan_gff(
                test_gff_file, predicate_pushdown=False, projection_pushdown=False
            )
            .filter(predicate)
            .select(["chrom", "type", "attributes"])
            .collect()
        )

        assert result.height == 4, f"Expected 4 rows for chr1, got {result.height}"
        assert all(result["chrom"] == "chr1"), "All results should be from chr1"
        assert "attributes" in result.columns, "Should include attributes column"


class TestFilterSelectPerformance:
    """Performance-related tests for the filter().select() fix."""

    def test_no_performance_regression(self, test_gff_file):
        """Ensure the fix doesn't cause significant performance regression."""
        import time

        predicate = pl.col("chrom") == "chr1"

        # Measure time for filter().select() pattern
        start_time = time.time()
        for _ in range(10):  # Run multiple times for more stable measurement
            result = (
                pb.scan_gff(
                    test_gff_file, predicate_pushdown=False, projection_pushdown=False
                )
                .filter(predicate)
                .select(["chrom", "start", "end", "ID"])
                .collect()
            )
        end_time = time.time()

        execution_time = end_time - start_time

        # Should complete reasonably quickly (adjust threshold as needed)
        assert (
            execution_time < 5.0
        ), f"Fix should not cause major performance regression: {execution_time}s"
        assert result.height == 4, "Should still return correct results"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
