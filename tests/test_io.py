from _expected import DATA_DIR

import polars_bio as pb


class TestIOBAM:
    df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam").collect()

    def test_count(self):
        assert len(self.df) == 2333

    def test_fields(self):
        assert self.df["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert self.df["flag"][3] == 1123
        assert self.df["cigar"][4] == "101M"


class TestIOVCF:
    df = pb.read_vcf(f"{DATA_DIR}/io/vcf/vep.vcf").collect()

    def test_count(self):
        assert len(self.df) == 2

    def test_fields(self):
        assert self.df["chrom"][0] == "21"
        assert self.df["pos"][1] == 26965148
        assert self.df["ref"][0] == "G"


class TestIOBED:
    df = pb.read_bed(f"{DATA_DIR}/io/bed/test.bed").collect()

    def test_count(self):
        assert len(self.df) == 3

    def test_fields(self):
        assert self.df["reference_sequence_name"][2] == "chrX"
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
