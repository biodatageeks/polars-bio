"""Integration tests for indexed & parallel reads (datafusion-bio-formats PR #61).

Tests cover:
  A) SQL path: register + pb.sql() with WHERE clauses (works automatically via DataFusion)
  B) Predicate pushdown through scan/read API (predicate_pushdown=True)
"""

import polars as pl
import pytest
from _expected import DATA_DIR

import polars_bio as pb

# ---------------------------------------------------------------------------
# Test data paths
# ---------------------------------------------------------------------------
BAM_PATH = f"{DATA_DIR}/io/bam/multi_chrom.bam"
CRAM_PATH = f"{DATA_DIR}/io/cram/multi_chrom.cram"
VCF_PATH = f"{DATA_DIR}/io/vcf/multi_chrom.vcf.gz"
GFF_PATH = f"{DATA_DIR}/io/gff/multi_chrom.gff3.gz"

# Expected counts (from generate_test_data.py)
BAM_TOTAL = 421
BAM_CHR1 = 160
BAM_CHR2 = 159
BAM_CHRX = 102
BAM_CHR1_CHR2 = BAM_CHR1 + BAM_CHR2  # 319

VCF_TOTAL = 100
VCF_CHR21 = 50
VCF_CHR22 = 50

# GFF counts: 30 genes * 3 features (gene, mRNA, exon) = 90 for chr1
# 20 genes * 3 features = 60 for chr2
GFF_TOTAL = 150
GFF_CHR1 = 90
GFF_CHR2 = 60


# ===========================================================================
# A. SQL path tests (auto-works after dependency bump)
# ===========================================================================
class TestIndexedBAMSQL:
    def test_full_scan(self):
        """Full scan should return all reads."""
        df = pb.read_bam(BAM_PATH)
        assert len(df) == BAM_TOTAL

    def test_single_chrom_filter(self):
        pb.register_bam(BAM_PATH, "idx_bam")
        result = pb.sql("SELECT * FROM idx_bam WHERE chrom = 'chr1'").collect()
        assert len(result) == BAM_CHR1

    def test_multi_chrom_filter(self):
        pb.register_bam(BAM_PATH, "idx_bam_multi")
        result = pb.sql(
            "SELECT * FROM idx_bam_multi WHERE chrom IN ('chr1', 'chr2')"
        ).collect()
        assert len(result) == BAM_CHR1_CHR2

    def test_range_query(self):
        pb.register_bam(BAM_PATH, "idx_bam_range")
        result = pb.sql(
            "SELECT * FROM idx_bam_range WHERE chrom = 'chr1' AND start >= 5000 AND \"end\" <= 50000"
        ).collect()
        assert 0 < len(result) < BAM_CHR1

    def test_combined_genomic_and_record_filter(self):
        pb.register_bam(BAM_PATH, "idx_bam_combined")
        result = pb.sql(
            "SELECT * FROM idx_bam_combined WHERE chrom = 'chr1' AND mapping_quality >= 30"
        ).collect()
        assert 0 < len(result) <= BAM_CHR1


class TestIndexedCRAMSQL:
    def test_full_scan(self):
        df = pb.read_cram(CRAM_PATH)
        assert len(df) == BAM_TOTAL

    def test_single_chrom_filter(self):
        pb.register_cram(CRAM_PATH, "idx_cram")
        result = pb.sql("SELECT * FROM idx_cram WHERE chrom = 'chr1'").collect()
        assert len(result) == BAM_CHR1

    def test_multi_chrom_filter(self):
        pb.register_cram(CRAM_PATH, "idx_cram_multi")
        result = pb.sql(
            "SELECT * FROM idx_cram_multi WHERE chrom IN ('chr1', 'chr2')"
        ).collect()
        assert len(result) == BAM_CHR1_CHR2


class TestIndexedVCFSQL:
    def test_full_scan(self):
        df = pb.read_vcf(VCF_PATH)
        assert len(df) == VCF_TOTAL

    def test_single_chrom_filter(self):
        pb.register_vcf(VCF_PATH, "idx_vcf")
        result = pb.sql("SELECT * FROM idx_vcf WHERE chrom = 'chr21'").collect()
        assert len(result) == VCF_CHR21

    def test_multi_chrom_filter(self):
        pb.register_vcf(VCF_PATH, "idx_vcf_multi")
        result = pb.sql(
            "SELECT * FROM idx_vcf_multi WHERE chrom IN ('chr21', 'chr22')"
        ).collect()
        assert len(result) == VCF_TOTAL


class TestIndexedGFFSQL:
    def test_full_scan(self):
        df = pb.read_gff(GFF_PATH)
        assert len(df) == GFF_TOTAL

    def test_single_chrom_filter(self):
        pb.register_gff(GFF_PATH, "idx_gff")
        result = pb.sql("SELECT * FROM idx_gff WHERE chrom = 'chr1'").collect()
        assert len(result) == GFF_CHR1


# ===========================================================================
# B. Predicate pushdown through scan/read API
# ===========================================================================
class TestIndexedBAMPredicate:
    def test_scan_filter_pushdown(self):
        """scan_bam with predicate_pushdown=True + .filter() uses index."""
        lf = pb.scan_bam(BAM_PATH, predicate_pushdown=True)
        df = lf.filter(pl.col("chrom") == "chr1").collect()
        assert len(df) == BAM_CHR1

    def test_scan_multi_chrom_pushdown(self):
        lf = pb.scan_bam(BAM_PATH, predicate_pushdown=True)
        df = lf.filter(pl.col("chrom").is_in(["chr1", "chr2"])).collect()
        assert len(df) == BAM_CHR1_CHR2

    def test_scan_range_pushdown(self):
        lf = pb.scan_bam(BAM_PATH, predicate_pushdown=True)
        df = lf.filter(
            (pl.col("chrom") == "chr1")
            & (pl.col("start") >= 5000)
            & (pl.col("end") <= 50000)
        ).collect()
        assert 0 < len(df) < BAM_CHR1

    def test_pushdown_vs_no_pushdown_identical(self):
        """Results with and without pushdown should be identical."""
        predicate = pl.col("chrom") == "chr1"
        df_push = (
            pb.scan_bam(BAM_PATH, predicate_pushdown=True).filter(predicate).collect()
        )
        df_no = (
            pb.scan_bam(BAM_PATH, predicate_pushdown=False).filter(predicate).collect()
        )
        assert len(df_push) == len(df_no)
        assert len(df_push) == BAM_CHR1


class TestIndexedVCFPredicate:
    def test_scan_filter_pushdown(self):
        lf = pb.scan_vcf(VCF_PATH, predicate_pushdown=True)
        df = lf.filter(pl.col("chrom") == "chr21").collect()
        assert len(df) == VCF_CHR21

    def test_scan_multi_chrom_pushdown(self):
        lf = pb.scan_vcf(VCF_PATH, predicate_pushdown=True)
        df = lf.filter(pl.col("chrom").is_in(["chr21", "chr22"])).collect()
        assert len(df) == VCF_TOTAL

    def test_pushdown_vs_no_pushdown_identical(self):
        predicate = pl.col("chrom") == "chr21"
        df_push = (
            pb.scan_vcf(VCF_PATH, predicate_pushdown=True).filter(predicate).collect()
        )
        df_no = (
            pb.scan_vcf(VCF_PATH, predicate_pushdown=False).filter(predicate).collect()
        )
        assert len(df_push) == len(df_no)
        assert len(df_push) == VCF_CHR21


class TestIndexedCRAMPredicate:
    def test_scan_filter_pushdown(self):
        lf = pb.scan_cram(CRAM_PATH, predicate_pushdown=True)
        df = lf.filter(pl.col("chrom") == "chr1").collect()
        assert len(df) == BAM_CHR1

    def test_pushdown_vs_no_pushdown_identical(self):
        predicate = pl.col("chrom") == "chr1"
        df_push = (
            pb.scan_cram(CRAM_PATH, predicate_pushdown=True).filter(predicate).collect()
        )
        df_no = (
            pb.scan_cram(CRAM_PATH, predicate_pushdown=False)
            .filter(predicate)
            .collect()
        )
        assert len(df_push) == len(df_no)
        assert len(df_push) == BAM_CHR1


class TestIndexedGFFPredicate:
    def test_scan_filter_pushdown(self):
        lf = pb.scan_gff(GFF_PATH, predicate_pushdown=True)
        df = lf.filter(pl.col("chrom") == "chr1").collect()
        assert len(df) == GFF_CHR1

    def test_pushdown_vs_no_pushdown_identical(self):
        predicate = pl.col("chrom") == "chr1"
        df_push = (
            pb.scan_gff(GFF_PATH, predicate_pushdown=True).filter(predicate).collect()
        )
        df_no = (
            pb.scan_gff(GFF_PATH, predicate_pushdown=False).filter(predicate).collect()
        )
        assert len(df_push) == len(df_no)
        assert len(df_push) == GFF_CHR1
