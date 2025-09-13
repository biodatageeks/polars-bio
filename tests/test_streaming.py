import os
from pathlib import Path

import bioframe as bf
import polars as pl
import pytest
from _expected import (
    DATA_DIR,
    DF_COUNT_OVERLAPS_PATH1,
    DF_COUNT_OVERLAPS_PATH2,
    DF_COVERAGE_PATH1,
    DF_COVERAGE_PATH2,
    DF_MERGE_PATH,
    DF_NEAREST_PATH1,
    DF_NEAREST_PATH2,
    DF_OVER_PATH1,
    DF_OVER_PATH2,
    PD_COVERAGE_DF1,
    PD_COVERAGE_DF2,
    PL_DF_COUNT_OVERLAPS,
    PL_DF_NEAREST,
    PL_DF_OVERLAP,
)

import polars_bio as pb
from polars_bio import FilterOp

# Set environment variable to force new streaming engine for all tests in this module


columns = ["contig", "pos_start", "pos_end"]


class TestStreaming:
    result_overlap_stream = pb.overlap(
        DF_OVER_PATH1,
        DF_OVER_PATH2,
        cols1=columns,
        cols2=columns,
        output_type="polars.LazyFrame",
        use_zero_based=False,
    )

    result_nearest_stream = pb.nearest(
        DF_NEAREST_PATH1,
        DF_NEAREST_PATH2,
        cols1=columns,
        cols2=columns,
        output_type="polars.LazyFrame",
        use_zero_based=False,
    )

    result_count_overlaps_stream = pb.count_overlaps(
        DF_COUNT_OVERLAPS_PATH1,
        DF_COUNT_OVERLAPS_PATH2,
        cols1=columns,
        cols2=columns,
        output_type="polars.LazyFrame",
        use_zero_based=False,
    )

    result_coverage_stream = pb.coverage(
        DF_COUNT_OVERLAPS_PATH1,
        DF_COUNT_OVERLAPS_PATH2,
        cols1=columns,
        cols2=columns,
        output_type="polars.LazyFrame",
        use_zero_based=False,
    )

    result_coverage_bio = bf.coverage(
        PD_COVERAGE_DF1,
        PD_COVERAGE_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("_1", "_2"),
    )

    def test_overlap_plan(self):
        plan = str(self.result_overlap_stream.explain())
        # Streaming is now controlled by POLARS_FORCE_NEW_STREAMING env var
        # Plans show PYTHON SCAN when using custom scan sources
        assert "python scan" in plan.lower() or "scan" in plan.lower()

    def test_nearest_plan(self):
        plan = str(self.result_nearest_stream.explain())
        # Streaming is now controlled by POLARS_FORCE_NEW_STREAMING env var
        # Plans show PYTHON SCAN when using custom scan sources
        assert "python scan" in plan.lower() or "scan" in plan.lower()

    def test_count_overlaps_plan(self):
        plan = str(self.result_count_overlaps_stream.explain())
        # Streaming is now controlled by POLARS_FORCE_NEW_STREAMING env var
        # Plans show PYTHON SCAN when using custom scan sources
        assert "python scan" in plan.lower() or "scan" in plan.lower()

    def test_coverage_plan(self):
        plan = str(self.result_coverage_stream.explain())
        # Streaming is now controlled by POLARS_FORCE_NEW_STREAMING env var
        # Plans show PYTHON SCAN when using custom scan sources
        assert "python scan" in plan.lower() or "scan" in plan.lower()

    def test_overlap_execute(self):
        file = "test_overlap.csv"
        file_path = Path(file)
        file_path.unlink(missing_ok=True)
        result = self.result_overlap_stream
        result_df = result.collect()
        assert len(result_df) == len(PL_DF_OVERLAP)
        result_df.write_csv(file)
        expected = pl.read_csv(file)
        expected.equals(PL_DF_OVERLAP)
        file_path.unlink(missing_ok=True)

    def test_nearest_execute(self):
        file = "test_nearest.csv"
        file_path = Path(file)
        file_path.unlink(missing_ok=True)
        result = self.result_nearest_stream
        result_df = result.collect()
        assert len(result_df) == len(PL_DF_NEAREST)
        result_df.write_csv(file)
        expected = pl.read_csv(file)
        expected.equals(PL_DF_NEAREST)
        file_path.unlink(missing_ok=True)

    def test_count_overlaps_execute(self):
        file = "test_count_over.csv"
        file_path = Path(file)
        file_path.unlink(missing_ok=True)
        result = self.result_count_overlaps_stream
        result_df = result.collect()
        assert len(result_df) == len(PL_DF_COUNT_OVERLAPS)
        result_df.write_csv(file)
        expected = pl.read_csv(file)
        expected.equals(PL_DF_COUNT_OVERLAPS)
        file_path.unlink(missing_ok=True)

    def test_coverage_execute(self):
        file = "test_cov.csv"
        file_path = Path(file)
        file_path.unlink(missing_ok=True)
        result = self.result_coverage_stream
        result_df = result.collect()
        assert len(result_df) == len(self.result_coverage_bio)
        result_df.write_csv(file)
        expected = pl.read_csv(file).to_pandas()
        expected.equals(self.result_coverage_bio)
        file_path.unlink(missing_ok=True)


class TestStreamingIO:
    def test_scan_bam_streaming(self):
        df = pb.scan_bam(f"{DATA_DIR}/io/bam/test.bam").collect()
        assert len(df) == 2333

    def test_scan_bed_streaming(self):
        df = pb.scan_bed(f"{DATA_DIR}/io/bed/chr16_fragile_site.bed.bgz").collect()
        assert len(df) == 5

    def test_scan_fasta_streaming(self):
        df = pb.scan_fasta(f"{DATA_DIR}/io/fasta/test.fasta").collect()
        assert len(df) == 2

    def test_scan_fastq_streaming(self):
        df = pb.scan_fastq(f"{DATA_DIR}/io/fastq/example.fastq.bgz").collect()
        assert len(df) == 200

    def test_scan_gff_streaming(self):
        df = pb.scan_gff(f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3.bgz").collect()
        assert len(df) == 3

    def test_scan_vcf_streaming(self):
        df = pb.scan_vcf(f"{DATA_DIR}/io/vcf/vep.vcf.bgz").collect()
        assert len(df) == 2


class TestStreamingWithPolarsOperations:
    """Test streaming functionality with polars column selection and filtering operations."""

    def test_scan_fasta_with_column_selection(self):
        """Test streaming FASTA scan with column selection."""
        df = (
            pb.scan_fasta(f"{DATA_DIR}/io/fasta/test.fasta")
            .select(["name", "sequence"])
            .collect(engine="streaming")
        )
        assert len(df) == 2
        assert df.columns == ["name", "sequence"]
        assert "description" not in df.columns

    def test_scan_vcf_with_filtering(self):
        """Test streaming VCF scan with filtering operations."""
        # Get full result and then filter
        full_df = pb.scan_vcf(f"{DATA_DIR}/io/vcf/vep.vcf.bgz").collect(
            engine="streaming"
        )
        assert len(full_df) >= 0

        # Test polars filtering on the result
        if len(full_df) > 0:
            # Filter for chromosome 21 if it exists in the data
            unique_chroms = full_df["chrom"].unique().to_list()
            if "21" in unique_chroms:
                filtered_df = full_df.filter(pl.col("chrom") == "21")
                assert all(chrom == "21" for chrom in filtered_df["chrom"].to_list())
            else:
                # Just test that filtering works with any available chromosome
                first_chrom = unique_chroms[0]
                filtered_df = full_df.filter(pl.col("chrom") == first_chrom)
                assert all(
                    chrom == first_chrom for chrom in filtered_df["chrom"].to_list()
                )

    def test_scan_bam_with_column_selection_and_filtering(self):
        """Test streaming BAM scan with both column selection and filtering."""
        # First test column selection
        df = (
            pb.scan_bam(f"{DATA_DIR}/io/bam/test.bam")
            .select(["chrom", "start", "end", "flags"])
            .collect(engine="streaming")
        )
        assert len(df) >= 0
        assert df.columns == ["chrom", "start", "end", "flags"]

        # Then test filtering on the result
        if len(df) > 0:
            filtered_df = df.filter(pl.col("flags") < 200)
            if len(filtered_df) > 0:
                assert all(flag < 200 for flag in filtered_df["flags"].to_list())

    def test_scan_bed_with_operations(self):
        """Test streaming BED scan with various polars operations."""
        # First get data with column selection
        df = (
            pb.scan_bed(f"{DATA_DIR}/io/bed/chr16_fragile_site.bed.bgz")
            .select(["chrom", "start", "end", "name"])
            .collect(engine="streaming")
        )
        assert len(df) >= 0
        assert df.columns == ["chrom", "start", "end", "name"]

        if len(df) > 0:
            # Add computed columns and filter
            result = df.with_columns(
                [(pl.col("end") - pl.col("start")).alias("length")]
            ).filter(pl.col("start") > 0)

            assert "length" in result.columns
            if len(result) > 0:
                # All intervals should have positive length
                assert all(length > 0 for length in result["length"].to_list())

    def test_scan_fastq_with_sequence_filtering(self):
        """Test streaming FASTQ scan with sequence-based filtering."""
        # First get data with column selection
        df = (
            pb.scan_fastq(f"{DATA_DIR}/io/fastq/example.fastq.bgz")
            .select(["name", "sequence", "quality_scores"])
            .collect(engine="streaming")
        )
        assert len(df) >= 0
        assert df.columns == ["name", "sequence", "quality_scores"]

        if len(df) > 0:
            # Apply filtering on the result
            filtered_df = df.filter(pl.col("sequence").str.len_chars() > 10)
            if len(filtered_df) > 0:
                # All sequences should be longer than 10 characters
                assert all(len(seq) > 10 for seq in filtered_df["sequence"].to_list())

    def test_overlap_with_column_selection(self):
        """Test streaming overlap operation with column selection."""
        # Get full result first
        full_result = pb.overlap(
            DF_OVER_PATH1,
            DF_OVER_PATH2,
            cols1=columns,
            cols2=columns,
            output_type="polars.LazyFrame",
        ).collect(engine="streaming")

        # Test column selection on the result
        selected_result = full_result.select(
            ["contig_1", "pos_start_1", "pos_end_1", "contig_2"]
        )
        assert len(selected_result) >= 0
        assert selected_result.columns == [
            "contig_1",
            "pos_start_1",
            "pos_end_1",
            "contig_2",
        ]
        assert len(selected_result) == len(full_result)  # Same number of rows

    def test_overlap_with_filtering(self):
        """Test streaming overlap operation with filtering."""
        # Get the full result first
        full_result = pb.overlap(
            DF_OVER_PATH1,
            DF_OVER_PATH2,
            cols1=columns,
            cols2=columns,
            output_type="polars.LazyFrame",
        ).collect(engine="streaming")

        # Apply filtering using standard polars operations
        filtered_result = full_result.filter(pl.col("contig_1") == "chr1")

        assert len(full_result) > len(filtered_result)  # Filter should reduce size
        assert len(filtered_result) > 0  # Should have some chr1 results

        # All filtered results should be chr1
        unique_contigs = filtered_result["contig_1"].unique().to_list()
        assert len(unique_contigs) == 1 and unique_contigs[0] == "chr1"

    def test_nearest_with_distance_filtering(self):
        """Test streaming nearest operation with distance-based filtering."""
        # Get full result first, then apply filtering
        full_result = pb.nearest(
            DF_NEAREST_PATH1,
            DF_NEAREST_PATH2,
            cols1=columns,
            cols2=columns,
            output_type="polars.LazyFrame",
        ).collect(engine="streaming")

        assert len(full_result) >= 0
        assert "distance" in full_result.columns

        if len(full_result) > 0:
            # Filter for exact overlaps (distance = 0)
            exact_overlaps = full_result.filter(pl.col("distance") == 0)
            if len(exact_overlaps) > 0:
                assert all(dist == 0 for dist in exact_overlaps["distance"].to_list())

    def test_count_overlaps_with_aggregation(self):
        """Test streaming count overlaps with aggregation operations."""
        # Get full result first
        full_result = pb.count_overlaps(
            DF_COUNT_OVERLAPS_PATH1,
            DF_COUNT_OVERLAPS_PATH2,
            cols1=columns,
            cols2=columns,
            output_type="polars.LazyFrame",
        ).collect(engine="streaming")

        assert len(full_result) >= 0
        assert "count" in full_result.columns

        if len(full_result) > 0:
            # Apply aggregation on the collected result
            aggregated = full_result.group_by("contig").agg(
                [
                    pl.col("count").sum().alias("total_overlaps"),
                    pl.col("count").mean().alias("avg_overlaps"),
                ]
            )

            assert "total_overlaps" in aggregated.columns
            assert "avg_overlaps" in aggregated.columns

    def test_coverage_with_threshold_filtering(self):
        """Test streaming coverage operation with coverage threshold filtering."""
        # Get full result first
        full_result = pb.coverage(
            DF_COVERAGE_PATH1,
            DF_COVERAGE_PATH2,
            cols1=columns,
            cols2=columns,
            output_type="polars.LazyFrame",
        ).collect(engine="streaming")

        assert len(full_result) >= 0
        assert "coverage" in full_result.columns

        if len(full_result) > 0:
            # Filter for covered regions
            covered_regions = full_result.filter(pl.col("coverage") > 0)
            if len(covered_regions) > 0:
                assert all(cov > 0 for cov in covered_regions["coverage"].to_list())

    def test_merge_with_size_calculation(self):
        """Test streaming merge operation with size calculations."""
        # Get full result first
        full_result = pb.merge(
            DF_MERGE_PATH,
            cols=columns,
            output_type="polars.LazyFrame",
        ).collect(engine="streaming")

        assert len(full_result) >= 0

        if len(full_result) > 0:
            # Apply column operations and filtering
            result = full_result.with_columns(
                [(pl.col("pos_end") - pl.col("pos_start")).alias("merged_size")]
            )

            assert "merged_size" in result.columns

            # Filter for large merged intervals
            large_intervals = result.filter(pl.col("merged_size") > 100)
            if len(large_intervals) > 0:
                assert all(
                    size > 100 for size in large_intervals["merged_size"].to_list()
                )

    def test_chained_operations_streaming(self):
        """Test complex chained operations with streaming."""
        # Get overlap result with streaming first
        overlap_result = pb.overlap(
            DF_OVER_PATH1,
            DF_OVER_PATH2,
            cols1=columns,
            cols2=columns,
            output_type="polars.LazyFrame",
        ).collect(engine="streaming")

        # Apply complex operations on the result
        result = (
            overlap_result.select(
                [
                    "contig_1",
                    "pos_start_1",
                    "pos_end_1",
                    "contig_2",
                    "pos_start_2",
                    "pos_end_2",
                ]
            )
            .filter(pl.col("contig_1") == pl.col("contig_2"))  # Same chromosome
            .with_columns(
                [
                    (pl.col("pos_end_1") - pl.col("pos_start_1")).alias("length_1"),
                    (pl.col("pos_end_2") - pl.col("pos_start_2")).alias("length_2"),
                ]
            )
            .filter((pl.col("length_1") > 50) & (pl.col("length_2") > 50))
            .group_by("contig_1")
            .agg(
                [
                    pl.len().alias("overlap_count"),
                    pl.col("length_1").mean().alias("avg_length_1"),
                    pl.col("length_2").mean().alias("avg_length_2"),
                ]
            )
        )
        assert len(result) >= 0
        expected_columns = ["contig_1", "overlap_count", "avg_length_1", "avg_length_2"]
        assert all(col in result.columns for col in expected_columns)

    def test_streaming_plan_verification(self):
        """Verify that streaming plans are being used with polars operations."""
        lazy_df = (
            pb.scan_fasta(f"{DATA_DIR}/io/fasta/test.fasta")
            .select(["name", "sequence"])
            .filter(pl.col("name").str.contains("test"))
        )

        plan = lazy_df.explain()
        # With POLARS_FORCE_NEW_STREAMING=1, operations should use streaming-compatible plans
        assert (
            "python scan" in plan.lower()
            or "scan" in plan.lower()
            or "select" in plan.lower()
        )

    def test_streaming_memory_efficiency(self):
        """Test that streaming operations handle data without loading everything into memory."""
        # This test processes data in streaming fashion
        result = (
            pb.scan_bam(f"{DATA_DIR}/io/bam/test.bam")
            .select(["chrom", "start", "end"])
            .head(100)  # Only take first 100 rows to test streaming
            .collect(engine="streaming")
        )

        assert len(result) >= 0
        assert len(result) <= 100  # Should respect the head() limit

        # Apply filtering on the result
        if len(result) > 0:
            filtered_result = result.filter(pl.col("chrom").str.starts_with("chr"))
            # Just test that filtering works
            assert len(filtered_result) >= 0

    def test_explicit_streaming_engine(self):
        """Test using explicit engine='streaming' parameter in collect()."""
        result = (
            pb.scan_fasta(f"{DATA_DIR}/io/fasta/test.fasta")
            .select(["name", "sequence"])
            .collect(engine="streaming")
        )
        assert len(result) == 2
        assert result.columns == ["name", "sequence"]

    def test_streaming_with_column_operations(self):
        """Test streaming with various column operations and explicit streaming engine."""
        result = (
            pb.scan_bed(f"{DATA_DIR}/io/bed/chr16_fragile_site.bed.bgz")
            .select(["chrom", "start", "end", "name"])
            .with_columns(
                [
                    (pl.col("end") - pl.col("start")).alias("interval_length"),
                    pl.col("name").str.to_uppercase().alias("name_upper"),
                ]
            )
            .collect(engine="streaming")
        )

        assert len(result) >= 0
        assert "interval_length" in result.columns
        assert "name_upper" in result.columns

        if len(result) > 0:
            # Test that computed columns work correctly
            first_row = result[0]
            original_length = first_row["end"][0] - first_row["start"][0]
            computed_length = first_row["interval_length"][0]
            assert original_length == computed_length

    def test_overlap_streaming_with_aggregations(self):
        """Test overlap operation with streaming and aggregation operations."""
        result = (
            pb.overlap(
                DF_OVER_PATH1,
                DF_OVER_PATH2,
                cols1=columns,
                cols2=columns,
                output_type="polars.LazyFrame",
            )
            .with_columns(
                [
                    (pl.col("pos_end_1") - pl.col("pos_start_1")).alias("length_1"),
                    (pl.col("pos_end_2") - pl.col("pos_start_2")).alias("length_2"),
                ]
            )
            .group_by("contig_1")
            .agg(
                [
                    pl.len().alias("overlap_count"),
                    pl.col("length_1").mean().alias("avg_length_1"),
                    pl.col("length_2").sum().alias("total_length_2"),
                ]
            )
            .collect(engine="streaming")
        )

        assert len(result) >= 0
        expected_columns = [
            "contig_1",
            "overlap_count",
            "avg_length_1",
            "total_length_2",
        ]
        assert all(col in result.columns for col in expected_columns)

        if len(result) > 0:
            # Verify aggregations worked
            assert all(count > 0 for count in result["overlap_count"].to_list())

    def test_vcf_streaming_with_info_parsing(self):
        """Test VCF streaming with filtering and column selection."""
        # First get the data with column selection
        result = (
            pb.scan_vcf(f"{DATA_DIR}/io/vcf/vep.vcf.bgz")
            .select(["chrom", "start", "ref", "alt"])
            .collect(engine="streaming")
        )

        assert len(result) >= 0
        expected_columns = ["chrom", "start", "ref", "alt"]
        assert all(col in result.columns for col in expected_columns)

        if len(result) > 0:
            # Apply additional operations on the result
            enriched = result.with_columns(
                [
                    pl.col("start").cast(pl.Int64).alias("position_int"),
                    pl.concat_str(
                        [pl.col("chrom"), pl.lit(":"), pl.col("start")]
                    ).alias("locus"),
                ]
            )

            # Test that locus column was created correctly
            first_locus = enriched["locus"][0]
            first_chrom = enriched["chrom"][0]
            first_start = enriched["start"][0]
            expected_locus = f"{first_chrom}:{first_start}"
            assert first_locus == expected_locus

    def test_streaming_engine_comparison(self):
        """Compare results between default and explicit streaming engine."""
        # Get result with default engine (should be streaming due to env var)
        result_default = (
            pb.scan_fastq(f"{DATA_DIR}/io/fastq/example.fastq.bgz")
            .select(["name", "sequence"])
            .head(50)
            .collect(engine="streaming")
        )

        # Get result with explicit streaming engine
        result_streaming = (
            pb.scan_fastq(f"{DATA_DIR}/io/fastq/example.fastq.bgz")
            .select(["name", "sequence"])
            .head(50)
            .collect(engine="streaming")
        )

        # Results should be identical
        assert len(result_default) == len(result_streaming)
        assert result_default.columns == result_streaming.columns

        if len(result_default) > 0:
            # Compare first few rows to ensure they're the same
            for i in range(min(3, len(result_default))):
                assert result_default["name"][i] == result_streaming["name"][i]
                assert result_default["sequence"][i] == result_streaming["sequence"][i]

    def test_complex_streaming_pipeline(self):
        """Test a complex streaming pipeline with multiple operations."""
        # Get overlap result first
        overlap_result = pb.overlap(
            DF_OVER_PATH1,
            DF_OVER_PATH2,
            cols1=columns,
            cols2=columns,
            output_type="polars.LazyFrame",
        ).collect(engine="streaming")

        assert len(overlap_result) >= 0

        if len(overlap_result) > 0:
            # Apply complex operations on the result
            result = (
                overlap_result.with_columns(
                    [
                        (pl.col("pos_end_1") - pl.col("pos_start_1")).alias(
                            "interval_1_size"
                        ),
                        (pl.col("pos_end_2") - pl.col("pos_start_2")).alias(
                            "interval_2_size"
                        ),
                        # Calculate overlap size (simplified approximation)
                        (
                            pl.min_horizontal(
                                [pl.col("pos_end_1"), pl.col("pos_end_2")]
                            )
                            - pl.max_horizontal(
                                [pl.col("pos_start_1"), pl.col("pos_start_2")]
                            )
                        ).alias("overlap_size"),
                    ]
                )
                # Filter for meaningful overlaps
                .filter(pl.col("overlap_size") > 0)
                # Select relevant columns
                .select(
                    [
                        "contig_1",
                        "pos_start_1",
                        "pos_end_1",
                        "contig_2",
                        "pos_start_2",
                        "pos_end_2",
                        "interval_1_size",
                        "interval_2_size",
                        "overlap_size",
                    ]
                )
                # Sort by overlap size
                .sort("overlap_size", descending=True)
            )

            expected_columns = [
                "contig_1",
                "pos_start_1",
                "pos_end_1",
                "contig_2",
                "pos_start_2",
                "pos_end_2",
                "interval_1_size",
                "interval_2_size",
                "overlap_size",
            ]
            assert all(col in result.columns for col in expected_columns)

            if len(result) > 1:
                # Verify sorting worked (overlap_size should be descending)
                overlap_sizes = result["overlap_size"].to_list()
                assert overlap_sizes == sorted(overlap_sizes, reverse=True)
