import pytest
from _expected import DATA_DIR

import polars_bio as pb


class TestFastq:
    def test_count(self):
        assert (
            pb.scan_fastq(f"{DATA_DIR}/io/fastq/example.fastq.bgz")
            .count()
            .collect()["name"][0]
            == 200
        )
        assert (
            pb.scan_fastq(f"{DATA_DIR}/io/fastq/example.fastq.gz")
            .count()
            .collect()["name"][0]
            == 200
        )
        assert (
            pb.scan_fastq(f"{DATA_DIR}/io/fastq/example.fastq")
            .count()
            .collect()["name"][0]
            == 200
        )

    def test_compression_override(self):
        assert (
            pb.scan_fastq(
                f"{DATA_DIR}/io/fastq/wrong_extension.fastq.gz", compression_type="bgz"
            )
            .count()
            .collect()["name"][0]
            == 200
        )

    def test_fields(self):
        sequences = pb.read_fastq(f"{DATA_DIR}/io/fastq/example.fastq.bgz").limit(5)
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
        )
        assert len(df) == 2000

    def test_read_parallel_fastq_with_limit(self):
        lf = pb.scan_fastq(
            f"{DATA_DIR}/io/fastq/sample_parallel.fastq.bgz", parallel=True
        ).limit(10)
        print(lf.explain())
        df = lf.collect()
        assert len(df) == 10
