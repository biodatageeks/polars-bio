import bioframe as bf
import pandas as pd
from _expected import DATA_DIR

import polars_bio as pb


class TestIOBED:
    df = pb.read_table(f"{DATA_DIR}/io/bed/test.bed", schema="bed12")

    def test_count(self):
        assert len(self.df) == 3

    def test_fields(self):
        assert self.df["chrom"][2] == "chrX"
        assert self.df["strand"][1] == "-"
        assert self.df["end"][2] == 8000


class TestIOTable:
    file = f"{DATA_DIR}/io/bed/ENCFF001XKR.bed.gz"

    def test_bed9(self):
        df_1 = pb.read_table(self.file, schema="bed9").to_pandas()
        df_1 = df_1.sort_values(by=list(df_1.columns)).reset_index(drop=True)
        df_2 = bf.read_table(self.file, schema="bed9")
        df_2 = df_2.sort_values(by=list(df_2.columns)).reset_index(drop=True)
        pd.testing.assert_frame_equal(df_1, df_2)


class TestBED:
    df_bgz = pb.read_bed(f"{DATA_DIR}/io/bed/chr16_fragile_site.bed.bgz")
    df_none = pb.read_bed(f"{DATA_DIR}/io/bed/chr16_fragile_site.bed")

    def test_count(self):
        assert len(self.df_none) == 5
        assert len(self.df_bgz) == 5

    def test_fields(self):
        assert (
            self.df_bgz["chrom"][0] == "chr16" and self.df_none["chrom"][0] == "chr16"
        )
        # 1-based coordinates by default
        assert (
            self.df_bgz["start"][1] == 66700001 and self.df_none["start"][1] == 66700001
        )
        assert (
            self.df_bgz["name"][0] == "FRA16A" and self.df_none["name"][4] == "FRA16E"
        )

    def test_register_table(self):
        pb.register_bed(f"{DATA_DIR}/io/bed/chr16_fragile_site.bed.bgz", "test_bed")
        count = pb.sql("select count(*) as cnt from test_bed").collect()
        assert count["cnt"][0] == 5

        projection = pb.sql("select chrom, start, `end`, name from test_bed").collect()
        assert projection["chrom"][0] == "chr16"
        # Note: register_* functions currently use Rust-side default (0-based)
        # TODO: Update register_* to respect global config
        assert projection["start"][1] == 66700000
        assert projection["end"][2] == 63934964
        assert projection["name"][4] == "FRA16E"
