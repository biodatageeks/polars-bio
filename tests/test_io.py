import bioframe as bf
import pandas as pd
import polars as pl
import polars.testing as pl_testing
import pytest
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
                        use_zero_based=False,
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
        assert self.df["flags"][3] == 1123
        assert self.df["cigar"][4] == "101M"
        assert (
            self.df["sequence"][4]
            == "TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACC"
        )
        assert (
            self.df["quality_scores"][4]
            == "CCDACCDCDABBDCDABBDCDABBDCDABBDCD?BBCCDABBCCDABBACDA?BDCAABBDBDA.=?><;CBB2@:;??:D>?5BAC??=DC;=5=?8:76"
        )

    def test_register(self):
        pb.register_bam(f"{DATA_DIR}/io/bam/test.bam", "test_bam")
        count = pb.sql("select count(*) as cnt from test_bam").collect()
        assert count["cnt"][0] == 2333

        projection = pb.sql("select name, flags from test_bam").collect()
        assert projection["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert projection["flags"][3] == 1123


class TestIOBED:
    df = pb.read_table(f"{DATA_DIR}/io/bed/test.bed", schema="bed12").collect()

    def test_count(self):
        assert len(self.df) == 3

    def test_fields(self):
        assert self.df["chrom"][2] == "chrX"
        assert self.df["strand"][1] == "-"
        assert self.df["end"][2] == 8000


class TestFasta:
    fasta_path = f"{DATA_DIR}/io/fasta/test.fasta"

    def test_count(self):
        df = pb.read_fasta(self.fasta_path).collect()
        assert len(df) == 2

    def test_read_fasta(self):

        df = pb.read_fasta(self.fasta_path).collect()
        print("Actual DataFrame:")
        print(df)
        print("Actual Schema:")
        print(df.schema)

        expected_df = pl.DataFrame(
            {
                "name": ["seq1", "seq2"],
                "description": ["First sequence", "Second sequence"],
                "sequence": ["ACTG", "GATTACA"],
            }
        )

        pl_testing.assert_frame_equal(df, expected_df)


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
    df_gz = pb.read_vcf(f"{DATA_DIR}/io/vcf/vep.vcf.gz").collect()
    df_none = pb.read_vcf(f"{DATA_DIR}/io/vcf/vep.vcf").collect()
    df_bgz_wrong_extension = pb.read_vcf(
        f"{DATA_DIR}/io/vcf/wrong_extension.vcf.bgz", compression_type="gz"
    ).collect()
    df_gz_wrong_extension = pb.read_vcf(
        f"{DATA_DIR}/io/vcf/wrong_extension.vcf.gz", compression_type="bgz"
    ).collect()

    def test_count(self):
        assert len(self.df_none) == 2
        assert len(self.df_gz) == 2
        assert len(self.df_bgz) == 2

    def test_compression_override(self):
        assert len(self.df_bgz_wrong_extension) == 2
        assert len(self.df_gz_wrong_extension) == 2

    def test_fields(self):
        assert self.df_bgz["chrom"][0] == "21" and self.df_none["chrom"][0] == "21"
        assert (
            self.df_bgz["start"][1] == 26965148 and self.df_none["start"][1] == 26965148
        )
        assert self.df_bgz["ref"][0] == "G" and self.df_none["ref"][0] == "G"


class TestFastq:
    def test_count(self):
        assert (
            pb.read_fastq(f"{DATA_DIR}/io/fastq/example.fastq.bgz")
            .count()
            .collect()["name"][0]
            == 200
        )
        assert (
            pb.read_fastq(f"{DATA_DIR}/io/fastq/example.fastq.gz")
            .count()
            .collect()["name"][0]
            == 200
        )
        assert (
            pb.read_fastq(f"{DATA_DIR}/io/fastq/example.fastq")
            .count()
            .collect()["name"][0]
            == 200
        )

    def test_compression_override(self):
        assert (
            pb.read_fastq(
                f"{DATA_DIR}/io/fastq/wrong_extension.fastq.gz", compression_type="bgz"
            )
            .count()
            .collect()["name"][0]
            == 200
        )

    def test_fields(self):
        sequences = (
            pb.read_fastq(f"{DATA_DIR}/io/fastq/example.fastq.bgz").limit(5).collect()
        )
        assert sequences["name"][1] == "SRR9130495.2"
        assert (
            sequences["quality_scores"][2]
            == "@@@DDDFFHHHFHBHIIGJIJIIJIIIEHGIGIJJIIGGIIIJIIJIJIIIIIHIJJIIJJIGHGIJJIGGHC=#-#-5?EBEFFFDEEEFEAEDBCCCDC"
        )
        assert (
            sequences["sequence"][3]
            == "GGGAGGCGCCCCGACCGGCCAGGGCGTGAGCCCCAGCCCCAGCGCCATCCTGGAGCGGCGCGACGTGAAGCCAGATGAGGACCTGGCGGGCAAGGCTGGCG"
        )


class TestParallelFastq:
    @pytest.mark.parametrize("partitions", [1, 2, 3, 4])
    def test_read_parallel_fastq(self, partitions):
        pb.set_option("datafusion.execution.target_partitions", str(partitions))
        df = pb.read_fastq(
            f"{DATA_DIR}/io/fastq/sample_parallel.fastq.bgz", parallel=True
        ).collect()
        assert len(df) == 2000

    def test_read_parallel_fastq_with_limit(self):
        lf = pb.read_fastq(
            f"{DATA_DIR}/io/fastq/sample_parallel.fastq.bgz", parallel=True
        ).limit(10)
        print(lf.explain())
        df = lf.collect()
        assert len(df) == 10


class TestIOGFF:
    df_bgz = pb.read_gff(f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3.bgz").collect()
    df_gz = pb.read_gff(f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3.gz").collect()
    df_none = pb.read_gff(f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3").collect()
    df_bgz_wrong_extension = pb.read_gff(
        f"{DATA_DIR}/io/gff/wrong_extension.gff3.gz", compression_type="bgz"
    ).collect()

    def test_count(self):
        assert len(self.df_none) == 3
        assert len(self.df_gz) == 3
        assert len(self.df_bgz) == 3

    def test_compression_override(self):
        assert len(self.df_bgz_wrong_extension) == 3

    def test_fields(self):
        assert self.df_bgz["chrom"][0] == "chr1" and self.df_none["chrom"][0] == "chr1"
        assert self.df_bgz["start"][1] == 11869 and self.df_none["start"][1] == 11869
        assert self.df_bgz["type"][2] == "exon" and self.df_none["type"][2] == "exon"
        assert self.df_bgz["attributes"][0][0] == {
            "tag": "ID",
            "value": "ENSG00000223972.5",
        }

    def test_register_table(self):
        pb.register_gff(
            f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3.bgz", "test_gff3"
        )
        count = pb.sql("select count(*) as cnt from test_gff3").collect()
        assert count["cnt"][0] == 3

    def test_register_gff_unnest(self):
        pb.register_gff(
            f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3.bgz",
            "test_gff3_unnest",
            attr_fields=["ID", "havana_transcript"],
        )
        count = pb.sql(
            "select count(*) as cnt from test_gff3_unnest where `ID` = 'ENSG00000223972.5'"
        ).collect()
        assert count["cnt"][0] == 1

        projection = pb.sql(
            "select `ID`, `havana_transcript` from test_gff3_unnest"
        ).collect()
        assert projection["ID"][0] == "ENSG00000223972.5"
        assert projection["havana_transcript"][0] == None
        assert projection["havana_transcript"][1] == "OTTHUMT00000362751.1"


class TestBED:
    df_bgz = pb.read_bed(f"{DATA_DIR}/io/bed/chr16_fragile_site.bed.bgz").collect()
    df_none = pb.read_bed(f"{DATA_DIR}/io/bed/chr16_fragile_site.bed").collect()

    def test_count(self):
        assert len(self.df_none) == 5
        assert len(self.df_bgz) == 5

    def test_fields(self):
        assert (
            self.df_bgz["chrom"][0] == "chr16" and self.df_none["chrom"][0] == "chr16"
        )
        assert (
            self.df_bgz["start"][1] == 66700001 and self.df_none["start"][1] == 66700001
        )  # example of 1-based for start
        assert (
            self.df_bgz["name"][0] == "FRA16A" and self.df_none["name"][4] == "FRA16E"
        )

    def test_register_table(self):
        pb.register_bed(f"{DATA_DIR}/io/bed/chr16_fragile_site.bed.bgz", "test_bed")
        count = pb.sql("select count(*) as cnt from test_bed").collect()
        assert count["cnt"][0] == 5

        projection = pb.sql("select chrom, start, end, name from test_bed").collect()
        assert projection["chrom"][0] == "chr16"
        assert projection["start"][1] == 66700001  # example of 1-based for start
        assert projection["end"][2] == 63934965
        assert projection["name"][4] == "FRA16E"
