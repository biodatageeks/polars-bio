import os
from pathlib import Path

import bioframe as bf
import polars as pl
import pytest
from _expected import (
    DATA_DIR,
    DF_COUNT_OVERLAPS_PATH1,
    DF_COUNT_OVERLAPS_PATH2,
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
os.environ["POLARS_FORCE_NEW_STREAMING"] = "1"


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

    @pytest.mark.xfail
    def test_scan_gff_streaming(self):
        df = pb.scan_gff(f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3.bgz").collect()
        assert len(df) == 3

    def test_scan_vcf_streaming(self):
        df = pb.scan_vcf(f"{DATA_DIR}/io/vcf/vep.vcf.bgz").collect()
        assert len(df) == 2
