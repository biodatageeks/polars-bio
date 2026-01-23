import bioframe as bf
import pandas as pd
import polars as pl
from _expected import DATA_DIR

import polars_bio as pb


def _df_with_metadata(df: pl.DataFrame, zero_based: bool = True) -> pl.DataFrame:
    """Add coordinate system metadata to a DataFrame."""
    df.config_meta.set(coordinate_system_zero_based=zero_based)
    return df


def _lf_with_metadata(lf: pl.LazyFrame, zero_based: bool = True) -> pl.LazyFrame:
    """Add coordinate system metadata to a LazyFrame."""
    lf.config_meta.set(coordinate_system_zero_based=zero_based)
    return lf


class TestPolarsExt:
    file = f"{DATA_DIR}/io/bed/ENCFF001XKR.bed.gz"

    def test_sort_bedframe(self):
        df_1_unsorted = (
            pb.scan_table(self.file, schema="bed9").collect().sample(1000, shuffle=True)
        )
        df_2 = df_1_unsorted.to_pandas()
        df_2 = bf.sort_bedframe(df_2)
        df_1 = df_1_unsorted.lazy().pb.sort().collect().to_pandas()
        assert df_1.equals(df_2)
        assert not df_1_unsorted.to_pandas().equals(df_2)

    def test_expand_pad(self):
        df_1 = pb.scan_table(self.file, schema="bed9").collect()
        df_2 = bf.expand(df_1.to_pandas(), pad=1000)
        df_3 = df_1.lazy().pb.expand(pad=1000).collect().to_pandas()
        assert df_2.equals(df_3)

    def test_expand_scale(self):
        df_1 = pb.scan_table(self.file, schema="bed9").collect()
        df_2 = bf.expand(df_1.to_pandas(), scale=1.5)
        df_3 = df_1.lazy().pb.expand(scale=1.5).collect().to_pandas()
        assert df_2.equals(df_3)

    def test_overlap(self):
        cols = ("chrom", "start", "end")
        df_1 = (
            pb.scan_table(self.file, schema="bed9")
            .select(cols)
            .collect()
            .to_pandas()
            .reset_index(drop=True)
        )
        df_2 = (
            pb.scan_table(self.file, schema="bed9")
            .select(cols)
            .collect()
            .to_pandas()
            .reset_index(drop=True)
        )
        df_3 = (
            bf.overlap(df_1, df_2, suffixes=("_1", "_2"))
            .sort_values(by=["chrom_1", "start_1", "end_1"])
            .reset_index(drop=True)
        )
        #
        lf_1 = _lf_with_metadata(pl.DataFrame(df_1).lazy(), zero_based=True)
        lf_2 = _lf_with_metadata(pl.DataFrame(df_2).lazy(), zero_based=True)
        df_4 = (
            lf_1.pb.overlap(lf_2, suffixes=("_1", "_2"))
            .collect()
            .to_pandas()
            .sort_values(by=["chrom_1", "start_1", "end_1"])
            .reset_index(drop=True)
        )
        assert df_3.equals(df_4)

    def test_nearest(self):
        cols = ("chrom", "start", "end")
        df_1 = (
            pb.scan_table(self.file, schema="bed9")
            .select(cols)
            .collect()
            .to_pandas()
            .reset_index(drop=True)
        )
        df_2 = (
            pb.scan_table(self.file, schema="bed9")
            .select(cols)
            .collect()
            .to_pandas()
            .reset_index(drop=True)
        )
        df_3 = (
            bf.closest(df_1, df_2, suffixes=("_1", "_2"))
            .sort_values(by=["chrom_1", "start_1", "end_1"])
            .reset_index(drop=True)
        )

        lf_1 = _lf_with_metadata(pl.DataFrame(df_1).lazy(), zero_based=True)
        lf_2 = _lf_with_metadata(pl.DataFrame(df_2).lazy(), zero_based=True)
        df_4 = (
            lf_1.pb.nearest(lf_2, suffixes=("_1", "_2"))
            .collect()
            .to_pandas()
            .sort_values(by=["chrom_1", "start_1", "end_1"])
            .reset_index(drop=True)
        )
        print(df_3.columns)
        print(df_4.columns)
        pd.testing.assert_frame_equal(df_3, df_4, check_dtype=False)

    def test_merge(self):
        cols = ("chrom", "start", "end")
        df_1 = (
            pb.scan_table(self.file, schema="bed9")
            .select(cols)
            .collect()
            .to_pandas()
            .reset_index(drop=True)
        )

        df_2 = (
            pb.scan_table(self.file, schema="bed9")
            .select(cols)
            .collect()
            .to_pandas()
            .reset_index(drop=True)
        )
        df_3 = (
            bf.merge(df_1, min_dist=None)
            .sort_values(by=["chrom", "start", "end"])
            .reset_index(drop=True)
        )
        #
        lf_1 = _lf_with_metadata(pl.DataFrame(df_1).lazy(), zero_based=True)
        df_4 = (
            lf_1.pb.merge()
            .collect()
            .to_pandas()
            .sort_values(by=["chrom", "start", "end"])
            .reset_index(drop=True)
        )
        print(df_3.columns)
        print(df_4.columns)
        pd.testing.assert_frame_equal(df_3, df_4, check_dtype=False)

    def test_count_overlaps(self):
        cols = ("chrom", "start", "end")
        df_1 = (
            pb.scan_table(self.file, schema="bed9")
            .select(cols)
            .collect()
            .to_pandas()
            .reset_index(drop=True)
        )
        df_2 = (
            pb.scan_table(self.file, schema="bed9")
            .select(cols)
            .collect()
            .to_pandas()
            .reset_index(drop=True)
        )
        df_3 = (
            bf.count_overlaps(
                df_1,
                df_2,
                suffixes=("", "_"),
            )
            .sort_values(by=["chrom", "start", "end"])
            .reset_index(drop=True)
        )
        #
        lf_1 = _lf_with_metadata(pl.DataFrame(df_1).lazy(), zero_based=True)
        lf_2 = _lf_with_metadata(pl.DataFrame(df_2).lazy(), zero_based=True)
        df_4 = (
            lf_1.pb.count_overlaps(lf_2, suffixes=("", "_"), naive_query=False)
            .collect()
            .to_pandas()
            .sort_values(by=["chrom", "start", "end"])
            .reset_index(drop=True)
        )
        print(df_3.columns)
        print(df_4.columns)
        pd.testing.assert_frame_equal(df_3, df_4, check_dtype=False)
