import pandas as pd
import polars as pl
from _expected import (
    DATA_DIR,
    PD_DF_OVERLAP,
    PD_OVERLAP_DF1,
    PD_OVERLAP_DF2,
    PL_DF1,
    PL_DF2,
    PL_DF_OVERLAP,
)

import polars_bio as pb

# Set coordinate system metadata on test DataFrames (1-based)
PD_OVERLAP_DF1.attrs["coordinate_system_zero_based"] = False
PD_OVERLAP_DF2.attrs["coordinate_system_zero_based"] = False
PL_DF1.config_meta.set(coordinate_system_zero_based=False)
PL_DF2.config_meta.set(coordinate_system_zero_based=False)


def _lazy_with_metadata(df: pl.DataFrame) -> pl.LazyFrame:
    """Create a LazyFrame with coordinate system metadata."""
    lf = df.lazy()
    lf.config_meta.set(coordinate_system_zero_based=False)
    return lf


class TestMemoryCombinations:
    def test_frames(self):
        for df1 in [PD_OVERLAP_DF1, PL_DF1, _lazy_with_metadata(PL_DF1)]:
            for df2 in [PD_OVERLAP_DF2, PL_DF2, _lazy_with_metadata(PL_DF2)]:
                for output_type in [
                    "pandas.DataFrame",
                    "polars.DataFrame",
                    "polars.LazyFrame",
                ]:
                    result = pb.overlap(
                        df1,
                        df2,
                        cols1=("contig", "pos_start", "pos_end"),
                        cols2=("contig", "pos_start", "pos_end"),
                        output_type=output_type,
                    )
                    if output_type == "polars.LazyFrame":
                        result = result.collect()
                    if output_type == "pandas.DataFrame":
                        result = result.sort_values(
                            by=list(result.columns)
                        ).reset_index(drop=True)
                        pd.testing.assert_frame_equal(result, PD_DF_OVERLAP)
                    else:
                        result = result.sort(by=result.columns)
                        assert PL_DF_OVERLAP.equals(result)


class TestIOPathInputs:
    """Verify that pathlib.Path objects are accepted wherever str paths are (issue #190)."""

    def test_read_bam_with_path(self):
        df = pb.read_bam(DATA_DIR / "io" / "bam" / "test.bam")
        assert len(df) == 2333

    def test_scan_bam_with_path(self):
        df = pb.scan_bam(DATA_DIR / "io" / "bam" / "test.bam").collect()
        assert len(df) == 2333

    def test_read_bed_with_path(self):
        df = pb.read_bed(DATA_DIR / "io" / "bed" / "chr16_fragile_site.bed")
        assert len(df) == 5

    def test_scan_bed_with_path(self):
        df = pb.scan_bed(DATA_DIR / "io" / "bed" / "chr16_fragile_site.bed").collect()
        assert len(df) == 5

    def test_read_vcf_with_path(self):
        df = pb.read_vcf(DATA_DIR / "io" / "vcf" / "vep.vcf")
        assert len(df) == 2

    def test_scan_vcf_with_path(self):
        df = pb.scan_vcf(DATA_DIR / "io" / "vcf" / "vep.vcf").collect()
        assert len(df) == 2

    def test_read_fastq_with_path(self):
        df = pb.read_fastq(DATA_DIR / "io" / "fastq" / "example.fastq.bgz")
        assert len(df) == 200

    def test_scan_fastq_with_path(self):
        count = (
            pb.scan_fastq(DATA_DIR / "io" / "fastq" / "example.fastq.bgz")
            .count()
            .collect()["name"][0]
        )
        assert count == 200

    def test_read_fasta_with_path(self):
        df = pb.read_fasta(DATA_DIR / "io" / "fasta" / "test.fasta")
        assert len(df) == 2

    def test_read_gff_with_path(self):
        df = pb.read_gff(DATA_DIR / "io" / "gff" / "gencode.v38.annotation.gff3")
        assert len(df) == 3

    def test_register_bam_with_path(self):
        pb.register_bam(DATA_DIR / "io" / "bam" / "test.bam", "test_bam_path")
        count = pb.sql("select count(*) as cnt from test_bam_path").collect()
        assert count["cnt"][0] == 2333

    def test_register_bed_with_path(self):
        pb.register_bed(
            DATA_DIR / "io" / "bed" / "chr16_fragile_site.bed.bgz", "test_bed_path"
        )
        count = pb.sql("select count(*) as cnt from test_bed_path").collect()
        assert count["cnt"][0] == 5

    def test_register_vcf_with_path(self):
        pb.register_vcf(DATA_DIR / "io" / "vcf" / "vep.vcf", "test_vcf_path")
        count = pb.sql("select count(*) as cnt from test_vcf_path").collect()
        assert count["cnt"][0] == 2

    def test_register_gff_with_path(self):
        pb.register_gff(
            DATA_DIR / "io" / "gff" / "gencode.v38.annotation.gff3.bgz",
            "test_gff_path",
        )
        count = pb.sql("select count(*) as cnt from test_gff_path").collect()
        assert count["cnt"][0] == 3
