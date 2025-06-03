import bioframe as bf
import pandas as pd
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
from polars_bio.polars_bio import FilterOp


class TestMemoryCombinations:
    def test_frames(self):
        for df1 in [PD_OVERLAP_DF1, PL_DF1, PL_DF1.lazy()]:
            for df2 in [PD_OVERLAP_DF2, PL_DF2, PL_DF2.lazy()]:
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
                        overlap_filter=FilterOp.Weak,
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


class TestIOBAM:
    df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam").collect()

    def test_count(self):
        assert len(self.df) == 2333

    def test_fields(self):
        assert self.df["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert self.df["flag"][3] == 1123
        assert self.df["cigar"][4] == "101M"


class TestIOBED:
    df = pb.read_table(f"{DATA_DIR}/io/bed/test.bed", schema="bed12").collect()

    def test_count(self):
        assert len(self.df) == 3

    def test_fields(self):
        assert self.df["chrom"][2] == "chrX"
        assert self.df["strand"][1] == "-"
        assert self.df["end"][2] == 8000


class TestFastq:
    df = pb.read_fastq(f"{DATA_DIR}/io/fastq/test.fastq").collect()

    def test_count(self):
        assert len(self.df) == 2

    def test_fields(self):
        sequences = self.df
        assert sequences["name"][0] == "SEQ_ID_1"
        assert sequences["quality_scores"][0] == "!''*((((***+))%%%++)(%%%%).1***-+*"
        assert sequences["sequence"][1] == "AGTACACTGGT"


class TestFasta:
    df = pb.read_fasta(f"{DATA_DIR}/io/fasta/test.fasta").collect()

    def test_count(self):
        assert len(self.df) == 3

    def test_fields(self):
        sequences = self.df
        assert sequences["id"][1] == "Sequence_2"
        assert sequences["sequence"][2] == "TTAGGCATGCGGCTA"


class TestIOTable:
    file = f"{DATA_DIR}/io/bed/ENCFF001XKR.bed.gz"

    def test_bed9(self):
        df_1 = pb.read_table(self.file, schema="bed9").collect().to_pandas()
        df_1 = df_1.sort_values(by=list(df_1.columns)).reset_index(drop=True)
        df_2 = bf.read_table(self.file, schema="bed9")
        df_2 = df_2.sort_values(by=list(df_2.columns)).reset_index(drop=True)
        pd.testing.assert_frame_equal(df_1, df_2)


class TestIOVCF:
    df_bgz = pb.read_vcf(f"{DATA_DIR}/io/vcf/vep.vcf.bgz").collect()
    df_none = pb.read_vcf(f"{DATA_DIR}/io/vcf/vep.vcf").collect()

    def test_count(self):
        assert len(self.df_none) == 2
        assert len(self.df_bgz) == 2

    def test_fields(self):
        assert self.df_bgz["chrom"][0] == "21" and self.df_none["chrom"][0] == "21"
        assert (
            self.df_bgz["start"][1] == 26965148 and self.df_none["start"][1] == 26965148
        )
        assert self.df_bgz["ref"][0] == "G" and self.df_none["ref"][0] == "G"
