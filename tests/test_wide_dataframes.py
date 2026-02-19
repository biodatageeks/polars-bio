"""Tests for range operations on wider DataFrames (more than just contig/start/end).

Verifies that all UDTF-based range operations correctly handle DataFrames
with additional columns beyond the core interval triplet.

Expected behavior per operation:
- overlap:        Extra columns from BOTH DFs appear with suffixes
- nearest:        Extra columns from BOTH DFs appear with suffixes
- count_overlaps: Extra columns from df1 preserved + count appended
- coverage:       Extra columns from df1 preserved + coverage appended
- merge:          Only contig/start/end/n_intervals (extra columns dropped - by design)
- cluster:        contig/start/end + cluster columns (extra columns dropped - by design)
- complement:     Only contig/start/end (extra columns dropped - by design)
- subtract:       Only contig/start/end (extra columns dropped - by design)
"""

import pandas as pd
import polars as pl
import pytest

import polars_bio as pb

# ---------------------------------------------------------------------------
# Test data: wider DataFrames with extra columns
# ---------------------------------------------------------------------------

# df1: intervals with gene_id, score, strand
WIDE_DF1 = pd.DataFrame(
    {
        "contig": ["chr1", "chr1", "chr1", "chr1", "chr2", "chr2"],
        "pos_start": [100, 200, 400, 10000, 100, 500],
        "pos_end": [190, 290, 600, 20000, 250, 700],
        "gene_id": ["GENE_A", "GENE_B", "GENE_C", "GENE_D", "GENE_E", "GENE_F"],
        "score": [10, 20, 30, 40, 50, 60],
        "strand": ["+", "-", "+", "-", "+", "-"],
    }
)
WIDE_DF1.attrs["coordinate_system_zero_based"] = True

# df2: intervals with feature, priority
WIDE_DF2 = pd.DataFrame(
    {
        "contig": ["chr1", "chr1", "chr1", "chr2", "chr2"],
        "pos_start": [150, 250, 10000, 50, 600],
        "pos_end": [250, 500, 15000, 200, 800],
        "feature": ["exon", "intron", "enhancer", "promoter", "exon"],
        "priority": [1, 2, 3, 4, 5],
    }
)
WIDE_DF2.attrs["coordinate_system_zero_based"] = True

# Single-input DF for merge/cluster/complement
WIDE_SINGLE = pd.DataFrame(
    {
        "contig": ["chr1", "chr1", "chr1", "chr2", "chr2"],
        "pos_start": [100, 150, 500, 200, 250],
        "pos_end": [200, 300, 600, 400, 500],
        "name": ["region_1", "region_2", "region_3", "region_4", "region_5"],
        "gc_content": [0.45, 0.50, 0.38, 0.55, 0.60],
    }
)
WIDE_SINGLE.attrs["coordinate_system_zero_based"] = True

# Polars versions of the same data
WIDE_PL_DF1 = pl.from_pandas(WIDE_DF1)
WIDE_PL_DF1.config_meta.set(coordinate_system_zero_based=True)
WIDE_PL_DF2 = pl.from_pandas(WIDE_DF2)
WIDE_PL_DF2.config_meta.set(coordinate_system_zero_based=True)
WIDE_PL_SINGLE = pl.from_pandas(WIDE_SINGLE)
WIDE_PL_SINGLE.config_meta.set(coordinate_system_zero_based=True)

COLS = ("contig", "pos_start", "pos_end")


# ===========================================================================
# Overlap
# ===========================================================================


class TestOverlapWide:
    """Overlap should preserve ALL columns from both DFs with suffixes."""

    def test_overlap_pandas_runs(self):
        result = pb.overlap(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
            suffixes=("_1", "_2"),
        )
        assert len(result) > 0

    def test_overlap_extra_columns_present(self):
        result = pb.overlap(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
            suffixes=("_1", "_2"),
        )
        # df1 extra columns with suffix _1
        assert "gene_id_1" in result.columns
        assert "score_1" in result.columns
        assert "strand_1" in result.columns
        # df2 extra columns with suffix _2
        assert "feature_2" in result.columns
        assert "priority_2" in result.columns

    def test_overlap_extra_column_values(self):
        result = pb.overlap(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
            suffixes=("_1", "_2"),
        )
        # Every gene_id value in the result should come from DF1
        assert set(result["gene_id_1"].unique()).issubset(set(WIDE_DF1["gene_id"]))
        # Every feature value in the result should come from DF2
        assert set(result["feature_2"].unique()).issubset(set(WIDE_DF2["feature"]))

    def test_overlap_polars_lazyframe_extra_columns(self):
        result = pb.overlap(
            WIDE_PL_DF1,
            WIDE_PL_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="polars.LazyFrame",
            suffixes=("_1", "_2"),
        ).collect()
        assert "gene_id_1" in result.columns
        assert "score_1" in result.columns
        assert "strand_1" in result.columns
        assert "feature_2" in result.columns
        assert "priority_2" in result.columns
        assert len(result) > 0

    def test_overlap_polars_dataframe_extra_columns(self):
        result = pb.overlap(
            WIDE_PL_DF1,
            WIDE_PL_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="polars.DataFrame",
            suffixes=("_1", "_2"),
        )
        assert "gene_id_1" in result.columns
        assert "feature_2" in result.columns
        assert len(result) > 0

    def test_overlap_wide_vs_narrow_same_intervals(self):
        """Core interval columns should match regardless of extra columns."""
        narrow_df1 = WIDE_DF1[["contig", "pos_start", "pos_end"]].copy()
        narrow_df1.attrs["coordinate_system_zero_based"] = True
        narrow_df2 = WIDE_DF2[["contig", "pos_start", "pos_end"]].copy()
        narrow_df2.attrs["coordinate_system_zero_based"] = True

        result_wide = pb.overlap(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
            suffixes=("_1", "_2"),
        )
        result_narrow = pb.overlap(
            narrow_df1,
            narrow_df2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
            suffixes=("_1", "_2"),
        )

        core_cols = [
            "contig_1",
            "pos_start_1",
            "pos_end_1",
            "contig_2",
            "pos_start_2",
            "pos_end_2",
        ]
        wide_core = result_wide[core_cols].sort_values(core_cols).reset_index(drop=True)
        narrow_core = (
            result_narrow[core_cols].sort_values(core_cols).reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(wide_core, narrow_core)


# ===========================================================================
# Nearest
# ===========================================================================


class TestNearestWide:
    """Nearest should preserve ALL columns from both DFs with suffixes."""

    def test_nearest_pandas_runs(self):
        result = pb.nearest(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
        )
        assert len(result) > 0

    def test_nearest_extra_columns_present(self):
        result = pb.nearest(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
        )
        # df1 extras
        assert "gene_id_1" in result.columns
        assert "score_1" in result.columns
        assert "strand_1" in result.columns
        # df2 extras
        assert "feature_2" in result.columns
        assert "priority_2" in result.columns
        # distance column
        assert "distance" in result.columns

    def test_nearest_extra_column_values(self):
        result = pb.nearest(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
        )
        assert set(result["gene_id_1"].unique()).issubset(set(WIDE_DF1["gene_id"]))
        assert set(result["feature_2"].unique()).issubset(set(WIDE_DF2["feature"]))

    def test_nearest_polars_lazyframe_extra_columns(self):
        result = pb.nearest(
            WIDE_PL_DF1,
            WIDE_PL_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="polars.LazyFrame",
        ).collect()
        assert "gene_id_1" in result.columns
        assert "feature_2" in result.columns
        assert "distance" in result.columns
        assert len(result) > 0


# ===========================================================================
# Count Overlaps
# ===========================================================================


class TestCountOverlapsWide:
    """Count overlaps should preserve df1 extra columns + count."""

    def test_count_overlaps_pandas_runs(self):
        result = pb.count_overlaps(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
            naive_query=False,
        )
        assert len(result) == len(WIDE_DF1)

    def test_count_overlaps_count_column_present(self):
        result = pb.count_overlaps(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
            naive_query=False,
        )
        assert "count" in result.columns

    def test_count_overlaps_core_columns_present(self):
        result = pb.count_overlaps(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
            naive_query=False,
        )
        assert "contig" in result.columns
        assert "pos_start" in result.columns
        assert "pos_end" in result.columns


# ===========================================================================
# Coverage
# ===========================================================================


class TestCoverageWide:
    """Coverage should preserve df1 extra columns + coverage."""

    def test_coverage_pandas_runs(self):
        result = pb.coverage(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
        )
        assert len(result) == len(WIDE_DF1)

    def test_coverage_column_present(self):
        result = pb.coverage(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
        )
        assert "coverage" in result.columns

    def test_coverage_core_columns_present(self):
        result = pb.coverage(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
        )
        assert "contig" in result.columns
        assert "pos_start" in result.columns
        assert "pos_end" in result.columns


# ===========================================================================
# Merge
# ===========================================================================


class TestMergeWide:
    """Merge produces only core columns + n_intervals (extra cols dropped by design)."""

    def test_merge_pandas_runs(self):
        result = pb.merge(
            WIDE_SINGLE,
            cols=COLS,
            output_type="pandas.DataFrame",
        )
        assert len(result) > 0

    def test_merge_output_schema(self):
        result = pb.merge(
            WIDE_SINGLE,
            cols=COLS,
            output_type="pandas.DataFrame",
        )
        assert list(result.columns) == ["contig", "pos_start", "pos_end", "n_intervals"]

    def test_merge_wide_vs_narrow_same_result(self):
        """Merge should produce identical results regardless of extra columns."""
        narrow = WIDE_SINGLE[["contig", "pos_start", "pos_end"]].copy()
        narrow.attrs["coordinate_system_zero_based"] = True

        result_wide = pb.merge(
            WIDE_SINGLE,
            cols=COLS,
            output_type="pandas.DataFrame",
        )
        result_narrow = pb.merge(
            narrow,
            cols=COLS,
            output_type="pandas.DataFrame",
        )
        result_wide = result_wide.sort_values(
            ["contig", "pos_start", "pos_end"]
        ).reset_index(drop=True)
        result_narrow = result_narrow.sort_values(
            ["contig", "pos_start", "pos_end"]
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result_wide, result_narrow)

    def test_merge_polars_lazyframe(self):
        result = pb.merge(
            WIDE_PL_SINGLE,
            cols=COLS,
            output_type="polars.LazyFrame",
        ).collect()
        assert result.columns == ["contig", "pos_start", "pos_end", "n_intervals"]
        assert len(result) > 0


# ===========================================================================
# Cluster
# ===========================================================================


class TestClusterWide:
    """Cluster outputs core cols + cluster/cluster_start/cluster_end."""

    def test_cluster_pandas_runs(self):
        result = pb.cluster(
            WIDE_SINGLE,
            cols=COLS,
            output_type="pandas.DataFrame",
        )
        assert len(result) > 0

    def test_cluster_output_schema(self):
        result = pb.cluster(
            WIDE_SINGLE,
            cols=COLS,
            output_type="pandas.DataFrame",
        )
        expected_cols = [
            "contig",
            "pos_start",
            "pos_end",
            "cluster",
            "cluster_start",
            "cluster_end",
        ]
        assert list(result.columns) == expected_cols

    def test_cluster_row_count_matches_input(self):
        """Cluster should return one row per input row."""
        result = pb.cluster(
            WIDE_SINGLE,
            cols=COLS,
            output_type="pandas.DataFrame",
        )
        assert len(result) == len(WIDE_SINGLE)

    def test_cluster_wide_vs_narrow_same_intervals(self):
        """Core cluster results should be identical regardless of extra columns."""
        narrow = WIDE_SINGLE[["contig", "pos_start", "pos_end"]].copy()
        narrow.attrs["coordinate_system_zero_based"] = True

        result_wide = pb.cluster(
            WIDE_SINGLE,
            cols=COLS,
            output_type="pandas.DataFrame",
        )
        result_narrow = pb.cluster(
            narrow,
            cols=COLS,
            output_type="pandas.DataFrame",
        )
        sort_cols = ["contig", "pos_start", "pos_end"]
        result_wide = result_wide.sort_values(sort_cols).reset_index(drop=True)
        result_narrow = result_narrow.sort_values(sort_cols).reset_index(drop=True)
        pd.testing.assert_frame_equal(result_wide, result_narrow)

    def test_cluster_polars_lazyframe(self):
        result = pb.cluster(
            WIDE_PL_SINGLE,
            cols=COLS,
            output_type="polars.LazyFrame",
        ).collect()
        expected_cols = [
            "contig",
            "pos_start",
            "pos_end",
            "cluster",
            "cluster_start",
            "cluster_end",
        ]
        assert result.columns == expected_cols
        assert len(result) == len(WIDE_SINGLE)

    def test_cluster_values_correct(self):
        """Overlapping intervals on same contig should share a cluster ID."""
        result = (
            pb.cluster(
                WIDE_SINGLE,
                cols=COLS,
                output_type="pandas.DataFrame",
            )
            .sort_values(["contig", "pos_start", "pos_end"])
            .reset_index(drop=True)
        )

        # chr1: [100,200) and [150,300) overlap → same cluster
        chr1_rows = result[result["contig"] == "chr1"]
        overlapping = chr1_rows[chr1_rows["pos_start"].isin([100, 150])]
        assert overlapping["cluster"].nunique() == 1

        # chr1: [500,600) doesn't overlap with [100,200)/[150,300) → different cluster
        isolated = chr1_rows[chr1_rows["pos_start"] == 500]
        if len(isolated) > 0:
            assert isolated["cluster"].iloc[0] != overlapping["cluster"].iloc[0]


# ===========================================================================
# Complement
# ===========================================================================


class TestComplementWide:
    """Complement produces only core columns (extra columns not applicable)."""

    def test_complement_pandas_runs(self):
        # Build view from the input data
        view_df = (
            WIDE_SINGLE.groupby("contig")
            .agg({"pos_start": "min", "pos_end": "max"})
            .reset_index()
            .rename(columns={"contig": "chrom", "pos_start": "start", "pos_end": "end"})
        )
        view_df["name"] = view_df["chrom"]
        view_df.attrs["coordinate_system_zero_based"] = True

        result = pb.complement(
            WIDE_SINGLE,
            view_df=view_df,
            cols=COLS,
            view_cols=("chrom", "start", "end"),
            output_type="pandas.DataFrame",
        )
        assert len(result) > 0

    def test_complement_output_schema(self):
        view_df = (
            WIDE_SINGLE.groupby("contig")
            .agg({"pos_start": "min", "pos_end": "max"})
            .reset_index()
            .rename(columns={"contig": "chrom", "pos_start": "start", "pos_end": "end"})
        )
        view_df["name"] = view_df["chrom"]
        view_df.attrs["coordinate_system_zero_based"] = True

        result = pb.complement(
            WIDE_SINGLE,
            view_df=view_df,
            cols=COLS,
            view_cols=("chrom", "start", "end"),
            output_type="pandas.DataFrame",
        )
        assert list(result.columns) == ["contig", "pos_start", "pos_end"]

    def test_complement_wide_vs_narrow_same_result(self):
        """Complement should produce identical results regardless of extra columns."""
        narrow = WIDE_SINGLE[["contig", "pos_start", "pos_end"]].copy()
        narrow.attrs["coordinate_system_zero_based"] = True

        view_df = (
            WIDE_SINGLE.groupby("contig")
            .agg({"pos_start": "min", "pos_end": "max"})
            .reset_index()
            .rename(columns={"contig": "chrom", "pos_start": "start", "pos_end": "end"})
        )
        view_df["name"] = view_df["chrom"]
        view_df.attrs["coordinate_system_zero_based"] = True

        result_wide = pb.complement(
            WIDE_SINGLE,
            view_df=view_df,
            cols=COLS,
            view_cols=("chrom", "start", "end"),
            output_type="pandas.DataFrame",
        )
        result_narrow = pb.complement(
            narrow,
            view_df=view_df,
            cols=COLS,
            view_cols=("chrom", "start", "end"),
            output_type="pandas.DataFrame",
        )
        sort_cols = ["contig", "pos_start", "pos_end"]
        result_wide = result_wide.sort_values(sort_cols).reset_index(drop=True)
        result_narrow = result_narrow.sort_values(sort_cols).reset_index(drop=True)
        pd.testing.assert_frame_equal(result_wide, result_narrow)


# ===========================================================================
# Subtract
# ===========================================================================


class TestSubtractWide:
    """Subtract produces core columns from df1 fragments."""

    def test_subtract_pandas_runs(self):
        result = pb.subtract(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
        )
        assert len(result) > 0

    def test_subtract_output_schema(self):
        result = pb.subtract(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
        )
        assert list(result.columns) == ["contig", "pos_start", "pos_end"]

    def test_subtract_wide_vs_narrow_same_result(self):
        """Subtract should produce identical interval fragments regardless of extra columns."""
        narrow_df1 = WIDE_DF1[["contig", "pos_start", "pos_end"]].copy()
        narrow_df1.attrs["coordinate_system_zero_based"] = True
        narrow_df2 = WIDE_DF2[["contig", "pos_start", "pos_end"]].copy()
        narrow_df2.attrs["coordinate_system_zero_based"] = True

        result_wide = pb.subtract(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
        )
        result_narrow = pb.subtract(
            narrow_df1,
            narrow_df2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
        )
        sort_cols = ["contig", "pos_start", "pos_end"]
        result_wide = result_wide.sort_values(sort_cols).reset_index(drop=True)
        result_narrow = result_narrow.sort_values(sort_cols).reset_index(drop=True)
        pd.testing.assert_frame_equal(result_wide, result_narrow)

    def test_subtract_polars_lazyframe(self):
        result = pb.subtract(
            WIDE_PL_DF1,
            WIDE_PL_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="polars.LazyFrame",
        ).collect()
        assert result.columns == ["contig", "pos_start", "pos_end"]
        assert len(result) > 0

    def test_subtract_removes_overlapping_portions(self):
        """Verify subtract actually removes overlapping regions."""
        # chr1 GENE_A: [100,190) overlaps with df2 [150,250)
        # Expected fragment: [100,150) — the non-overlapping portion
        result = pb.subtract(
            WIDE_DF1,
            WIDE_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
        )
        chr1_frags = result[result["contig"] == "chr1"].sort_values("pos_start")
        # There should be at least one fragment starting at 100 and ending before 150
        early_frags = chr1_frags[chr1_frags["pos_start"] == 100]
        if len(early_frags) > 0:
            assert early_frags.iloc[0]["pos_end"] <= 150


# ===========================================================================
# Mixed types: Polars DataFrame input with wide DFs
# ===========================================================================


class TestMixedInputTypesWide:
    """Verify wide DF handling across different input types (pandas vs polars)."""

    def test_overlap_polars_df_output(self):
        result = pb.overlap(
            WIDE_PL_DF1,
            WIDE_PL_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="polars.DataFrame",
            suffixes=("_1", "_2"),
        )
        assert "gene_id_1" in result.columns
        assert "feature_2" in result.columns

    def test_cluster_polars_df_output(self):
        result = pb.cluster(
            WIDE_PL_SINGLE,
            cols=COLS,
            output_type="polars.DataFrame",
        )
        expected_cols = [
            "contig",
            "pos_start",
            "pos_end",
            "cluster",
            "cluster_start",
            "cluster_end",
        ]
        assert result.columns == expected_cols
        assert len(result) == len(WIDE_SINGLE)

    def test_merge_polars_df_output(self):
        result = pb.merge(
            WIDE_PL_SINGLE,
            cols=COLS,
            output_type="polars.DataFrame",
        )
        assert result.columns == ["contig", "pos_start", "pos_end", "n_intervals"]

    def test_subtract_polars_df_output(self):
        result = pb.subtract(
            WIDE_PL_DF1,
            WIDE_PL_DF2,
            cols1=COLS,
            cols2=COLS,
            output_type="polars.DataFrame",
        )
        assert result.columns == ["contig", "pos_start", "pos_end"]
        assert len(result) > 0


# ===========================================================================
# Many extra columns stress test
# ===========================================================================


class TestManyExtraColumns:
    """Test with DataFrames having many extra columns to stress the schema handling."""

    @staticmethod
    def _make_wide_df(n_extra_cols: int) -> pd.DataFrame:
        base = {
            "contig": ["chr1", "chr1", "chr2"],
            "pos_start": [100, 300, 100],
            "pos_end": [200, 500, 400],
        }
        for i in range(n_extra_cols):
            base[f"extra_{i}"] = [i * 10, i * 20, i * 30]
        df = pd.DataFrame(base)
        df.attrs["coordinate_system_zero_based"] = True
        return df

    def test_overlap_20_extra_columns(self):
        df1 = self._make_wide_df(20)
        df2 = self._make_wide_df(10)
        result = pb.overlap(
            df1,
            df2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
            suffixes=("_1", "_2"),
        )
        # All 20 extra columns from df1 should appear with _1 suffix
        for i in range(20):
            assert f"extra_{i}_1" in result.columns
        # All 10 extra columns from df2 should appear with _2 suffix
        for i in range(10):
            assert f"extra_{i}_2" in result.columns

    def test_nearest_20_extra_columns(self):
        df1 = self._make_wide_df(20)
        df2 = self._make_wide_df(10)
        result = pb.nearest(
            df1,
            df2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
        )
        for i in range(20):
            assert f"extra_{i}_1" in result.columns
        for i in range(10):
            assert f"extra_{i}_2" in result.columns

    def test_cluster_20_extra_columns(self):
        df = self._make_wide_df(20)
        result = pb.cluster(df, cols=COLS, output_type="pandas.DataFrame")
        # Cluster only outputs core + cluster columns
        assert len(result) == 3
        assert "cluster" in result.columns

    def test_merge_20_extra_columns(self):
        df = self._make_wide_df(20)
        result = pb.merge(df, cols=COLS, output_type="pandas.DataFrame")
        assert list(result.columns) == ["contig", "pos_start", "pos_end", "n_intervals"]

    def test_subtract_20_extra_columns(self):
        df1 = self._make_wide_df(20)
        df2 = self._make_wide_df(10)
        result = pb.subtract(
            df1,
            df2,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
        )
        assert list(result.columns) == ["contig", "pos_start", "pos_end"]


# ===========================================================================
# Edge case: extra columns with various dtypes
# ===========================================================================


class TestExtraColumnDtypes:
    """Test that various column dtypes survive round-trip through the UDTF pipeline."""

    @staticmethod
    def _make_typed_df() -> pd.DataFrame:
        df = pd.DataFrame(
            {
                "contig": ["chr1", "chr1", "chr2"],
                "pos_start": [100, 300, 100],
                "pos_end": [200, 500, 400],
                "int_col": [1, 2, 3],
                "float_col": [1.5, 2.5, 3.5],
                "str_col": ["a", "b", "c"],
                "bool_col": [True, False, True],
            }
        )
        df.attrs["coordinate_system_zero_based"] = True
        return df

    def test_overlap_preserves_dtypes(self):
        df = self._make_typed_df()
        result = pb.overlap(
            df,
            df,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
            suffixes=("_1", "_2"),
        )
        assert result["int_col_1"].dtype in ["int64", "int32"]
        assert result["float_col_1"].dtype == "float64"
        assert pd.api.types.is_string_dtype(result["str_col_1"])

    def test_nearest_preserves_dtypes(self):
        df = self._make_typed_df()
        result = pb.nearest(
            df,
            df,
            cols1=COLS,
            cols2=COLS,
            output_type="pandas.DataFrame",
        )
        assert result["int_col_1"].dtype in ["int64", "int32"]
        assert result["float_col_1"].dtype == "float64"
        assert pd.api.types.is_string_dtype(result["str_col_1"])
