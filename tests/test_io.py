from _expected import DATA_DIR

import polars_bio as pb


class TestIOBAM:
    def test_count(self):
        df = pb.read_bam(f"{DATA_DIR}/io/test.bam")
        print(df.collect())
        assert len(df.collect()) == 2333
