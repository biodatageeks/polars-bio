import bioframe as bf
import pandas as pd
from _expected import BIO_DF_PATH1, BIO_DF_PATH2, BIO_PD_DF1, BIO_PD_DF2
from numpy import int32

import polars_bio as pb

pb.ctx.set_option("datafusion.execution.parquet.schema_force_view_types", "true", False)

# Set coordinate system metadata on pandas DataFrames (0-based for bioframe compatibility)
BIO_PD_DF1.attrs["coordinate_system_zero_based"] = True
BIO_PD_DF2.attrs["coordinate_system_zero_based"] = True


class TestBioframe:
    result_overlap = pb.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
        suffixes=("_1", "_3"),
    )
    result_overlap_lf = (
        pb.overlap(
            BIO_PD_DF1,
            BIO_PD_DF2,
            cols1=("contig", "pos_start", "pos_end"),
            cols2=("contig", "pos_start", "pos_end"),
            output_type="polars.LazyFrame",
            suffixes=("_1", "_3"),
        )
        .collect()
        .to_pandas()
    )

    result_bio_overlap = bf.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("_1", "_3"),
        how="inner",
    )

    result_nearest = pb.nearest(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
    )
    result_bio_nearest = bf.closest(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("_1", "_2"),
    )

    result_count_overlaps = pb.count_overlaps(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
        naive_query=False,
    )

    # For file paths, we need to use polars scan with metadata set.
    import polars as pl

    _df1_parquet = pl.scan_parquet(BIO_DF_PATH1)
    _df1_parquet.config_meta.set(coordinate_system_zero_based=True)
    _df2_parquet = pl.scan_parquet(BIO_DF_PATH2)
    _df2_parquet.config_meta.set(coordinate_system_zero_based=True)
    result_count_overlaps_naive = pb.count_overlaps(
        _df1_parquet,
        _df2_parquet,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        naive_query=True,
    )

    result_bio_count_overlaps = bf.count_overlaps(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("", "_"),
    )

    result_merge = pb.merge(
        BIO_PD_DF1,
        cols=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
    )
    result_merge_lf = pb.merge(
        BIO_PD_DF1,
        cols=("contig", "pos_start", "pos_end"),
        output_type="polars.LazyFrame",
    )
    result_bio_merge = bf.merge(
        BIO_PD_DF1, cols=("contig", "pos_start", "pos_end"), min_dist=None
    ).astype(
        {"pos_start": "int64", "pos_end": "int64"}
    )  # MergeProvider outputs Int64

    def test_overlap_count(self):
        assert len(self.result_overlap) == len(self.result_bio_overlap)
        assert len(self.result_overlap_lf) == len(self.result_bio_overlap)

    def test_overlap_schema_rows(self):
        expected = self.result_bio_overlap.sort_values(
            by=list(self.result_overlap.columns)
        ).reset_index(drop=True)
        result = self.result_overlap.sort_values(
            by=list(self.result_overlap.columns)
        ).reset_index(drop=True)
        result_lf = self.result_overlap_lf.sort_values(
            by=list(self.result_overlap_lf.columns)
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, expected)
        pd.testing.assert_frame_equal(result_lf, expected)

    def test_nearest_count(self):
        assert len(self.result_nearest) == len(self.result_bio_nearest)

    def test_nearest_schema_rows(self):
        # since the find nearest is imprecisely defined (i.e. it can return any overlapping interval) in the case of multiple hits with the same distance
        # we will only compare pos_start_1, pos_end_1 and the distance
        expected = (
            self.result_bio_nearest.sort_values(
                by=list(["contig_1", "pos_start_1", "pos_end_1", "distance"])
            )
            .reset_index(drop=True)
            .astype({"pos_start_2": int32, "pos_end_2": int32, "distance": int})
        )
        expected = expected.drop(columns=["pos_start_2", "pos_end_2"])
        result = self.result_nearest.sort_values(
            by=list(["contig_1", "pos_start_1", "pos_end_1", "distance"])
        ).reset_index(drop=True)
        result = result.drop(columns=["pos_start_2", "pos_end_2"])
        pd.testing.assert_frame_equal(result, expected)

    def test_overlaps_count(self):
        assert len(self.result_count_overlaps) == len(self.result_bio_count_overlaps)
        assert len(self.result_count_overlaps_naive.collect()) == len(
            self.result_bio_count_overlaps
        )

    def test_overlaps_schema_rows(self):
        expected = (
            self.result_bio_count_overlaps.sort_values(
                by=list(self.result_count_overlaps.columns)
            )
            .reset_index(drop=True)
            .astype({"count": int})
        )
        result = self.result_count_overlaps.sort_values(
            by=list(self.result_count_overlaps.columns)
        ).reset_index(drop=True)
        result_naive = (
            self.result_count_overlaps_naive.collect()
            .to_pandas()
            .sort_values(
                by=list(self.result_count_overlaps_naive.collect_schema().names())
            )
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(result, expected)
        pd.testing.assert_frame_equal(result_naive, expected, check_dtype=True)

    def test_merge_count(self):
        assert len(self.result_merge) == len(self.result_bio_merge)
        assert len(self.result_merge_lf.collect()) == len(self.result_bio_merge)

    def test_merge_schema_rows(self):
        expected = self.result_bio_merge.sort_values(
            by=list(self.result_merge.columns)
        ).reset_index(drop=True)
        result = self.result_merge.sort_values(
            by=list(self.result_merge.columns)
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, expected)

    def test_coverage_count(self):
        result = pb.coverage(
            BIO_PD_DF1,
            BIO_PD_DF2,
            cols1=("contig", "pos_start", "pos_end"),
            cols2=("contig", "pos_start", "pos_end"),
            output_type="pandas.DataFrame",
        )
        result_bio = bf.coverage(
            BIO_PD_DF1,
            BIO_PD_DF2,
            cols1=("contig", "pos_start", "pos_end"),
            cols2=("contig", "pos_start", "pos_end"),
            suffixes=("_1", "_2"),
        )
        assert len(result) == len(result_bio)

    def test_coverage_schema_rows(self):
        result = pb.coverage(
            BIO_PD_DF1,
            BIO_PD_DF2,
            cols1=("contig", "pos_start", "pos_end"),
            cols2=("contig", "pos_start", "pos_end"),
            output_type="pandas.DataFrame",
        )
        result_bio = bf.coverage(
            BIO_PD_DF1,
            BIO_PD_DF2,
            cols1=("contig", "pos_start", "pos_end"),
            cols2=("contig", "pos_start", "pos_end"),
            suffixes=("_1", "_2"),
        )
        expected = (
            result_bio.sort_values(by=list(result.columns))
            .reset_index(drop=True)
            .astype({"coverage": "int64"})
        )
        result = result.sort_values(by=list(result.columns)).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, expected)

    # ------------------------------------------------------------------ cluster
    result_cluster = pb.cluster(
        BIO_PD_DF1,
        cols=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
    )
    result_cluster_lf = pb.cluster(
        BIO_PD_DF1,
        cols=("contig", "pos_start", "pos_end"),
        output_type="polars.LazyFrame",
    )
    result_bio_cluster = bf.cluster(
        BIO_PD_DF1, cols=("contig", "pos_start", "pos_end"), min_dist=None
    )

    def test_cluster_count(self):
        assert len(self.result_cluster) == len(self.result_bio_cluster)
        assert len(self.result_cluster_lf.collect()) == len(self.result_bio_cluster)

    def test_cluster_schema_rows(self):
        # Compare cluster boundaries: cluster_start and cluster_end should match
        # bioframe's cluster_start and cluster_end
        result = self.result_cluster.sort_values(
            by=["contig", "pos_start", "pos_end"]
        ).reset_index(drop=True)
        expected = (
            self.result_bio_cluster.sort_values(by=["contig", "pos_start", "pos_end"])
            .reset_index(drop=True)
            .astype(
                {
                    "pos_start": "int64",
                    "pos_end": "int64",
                    "cluster": "int64",
                    "cluster_start": "int64",
                    "cluster_end": "int64",
                }
            )
        )
        pd.testing.assert_frame_equal(result, expected)

    # --------------------------------------------------------------- complement
    # Build a view_df with contig boundaries (min start, max end per contig)
    _view_df = (
        BIO_PD_DF1.groupby("contig")
        .agg({"pos_start": "min", "pos_end": "max"})
        .reset_index()
        .rename(columns={"contig": "chrom", "pos_start": "start", "pos_end": "end"})
    )
    _view_df["name"] = _view_df["chrom"]  # bioframe needs a unique 'name' column
    _view_df.attrs["coordinate_system_zero_based"] = True

    result_complement = pb.complement(
        BIO_PD_DF1,
        view_df=_view_df,
        cols=("contig", "pos_start", "pos_end"),
        view_cols=("chrom", "start", "end"),
        output_type="pandas.DataFrame",
    )
    result_bio_complement = bf.complement(
        BIO_PD_DF1,
        view_df=_view_df,
        cols=("contig", "pos_start", "pos_end"),
        cols_view=("chrom", "start", "end"),
    )[["contig", "pos_start", "pos_end"]].astype(
        {"pos_start": "int64", "pos_end": "int64"}
    )

    def test_complement_count(self):
        assert len(self.result_complement) == len(self.result_bio_complement)

    def test_complement_schema_rows(self):
        expected = self.result_bio_complement.sort_values(
            by=["contig", "pos_start", "pos_end"]
        ).reset_index(drop=True)
        result = self.result_complement.sort_values(
            by=["contig", "pos_start", "pos_end"]
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, expected)

    # ---------------------------------------------------------------- subtract
    result_subtract = pb.subtract(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
    )
    result_bio_subtract = bf.subtract(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
    ).astype({"pos_start": "int64", "pos_end": "int64"})

    def test_subtract_count(self):
        assert len(self.result_subtract) == len(self.result_bio_subtract)

    def test_subtract_schema_rows(self):
        expected = self.result_bio_subtract.sort_values(
            by=["contig", "pos_start", "pos_end"]
        ).reset_index(drop=True)
        result = self.result_subtract.sort_values(
            by=["contig", "pos_start", "pos_end"]
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, expected)
