import tracemalloc
from shutil import copytree

import polars as pl
import pytest
from _expected import DATA_DIR

import polars_bio as pb
from polars_bio._metadata import get_coordinate_system

VEP_CACHE_DIR = DATA_DIR / "io" / "vep_cache"
VARIATION_CACHE = str(VEP_CACHE_DIR / "variation_non_tabix")
TRANSCRIPT_CACHE = str(VEP_CACHE_DIR / "transcript_storable")
REGULATORY_CACHE = str(VEP_CACHE_DIR / "regulatory_storable")


class TestVepCacheIO:
    def test_scan_vep_cache_returns_lazyframe(self):
        lf = pb.scan_vep_cache(VARIATION_CACHE, entity="variation")

        assert isinstance(lf, pl.LazyFrame)
        assert "PYTHON SCAN" in lf.explain()

    def test_scan_vep_cache_minimal_memory_usage(self):
        tracemalloc.start()
        snapshot1 = tracemalloc.take_snapshot()

        pb.scan_vep_cache(VARIATION_CACHE, entity="variation")

        snapshot2 = tracemalloc.take_snapshot()
        mem_diff = sum(
            stat.size_diff for stat in snapshot2.compare_to(snapshot1, "lineno")
        )
        mem_mb = mem_diff / (1024 * 1024)
        tracemalloc.stop()

        assert (
            mem_mb < 5
        ), f"scan_vep_cache used {mem_mb:.2f} MB, likely materializing data"

    def test_scan_vep_cache_collect_streams_batches(self):
        df = (
            pb.scan_vep_cache(VARIATION_CACHE, entity="variation")
            .select(["chrom", "start", "end", "variation_name"])
            .limit(2)
            .collect()
        )

        assert len(df) > 0
        assert {"chrom", "start", "end", "variation_name"}.issubset(df.columns)

    def test_read_vep_cache_variation(self):
        df = pb.read_vep_cache(VARIATION_CACHE, entity="variation")
        assert len(df) > 0
        assert "variation_name" in df.columns

    def test_vep_cache_coordinate_system_metadata(self):
        lf_one = pb.scan_vep_cache(
            VARIATION_CACHE, entity="variation", use_zero_based=False
        )
        lf_zero = pb.scan_vep_cache(
            VARIATION_CACHE, entity="variation", use_zero_based=True
        )
        assert get_coordinate_system(lf_one) is False
        assert get_coordinate_system(lf_zero) is True

        df_zero = pb.read_vep_cache(
            VARIATION_CACHE, entity="variation", use_zero_based=True
        )
        assert get_coordinate_system(df_zero) is True

    def test_scan_vep_cache_transcript_entity(self):
        df = pb.scan_vep_cache(TRANSCRIPT_CACHE, entity="transcript").limit(1).collect()
        assert "stable_id" in df.columns
        assert "raw_object_json" in df.columns

    def test_scan_vep_cache_regulatory_entity(self):
        df = (
            pb.scan_vep_cache(REGULATORY_CACHE, entity="regulatory_feature")
            .limit(1)
            .collect()
        )
        assert "feature_type" in df.columns
        assert "cell_types" in df.columns

    def test_scan_vep_cache_motif_entity(self):
        df = (
            pb.scan_vep_cache(REGULATORY_CACHE, entity="motif_feature")
            .limit(1)
            .collect()
        )
        assert len(df) > 0
        assert "binding_matrix" in df.columns
        assert "overlapping_regulatory_feature" in df.columns

    def test_register_vep_cache_sql(self):
        pb.register_vep_cache(
            VARIATION_CACHE, entity="variation", name="vep_cache_variation_test"
        )
        df = pb.sql(
            'SELECT chrom, start, "end" FROM vep_cache_variation_test LIMIT 2'
        ).collect()
        assert len(df) > 0
        assert list(df.columns) == ["chrom", "start", "end"]

    def test_scan_vep_cache_invalid_entity_raises(self):
        with pytest.raises(ValueError, match="Unsupported entity"):
            pb.scan_vep_cache(VARIATION_CACHE, entity="unknown")

    def test_scan_vep_cache_count_with_numeric_table_name(self, tmp_path):
        numeric_cache = tmp_path / "115_vep_cache"
        copytree(VARIATION_CACHE, numeric_cache)

        out = (
            pb.scan_vep_cache(str(numeric_cache), entity="variation").count().collect()
        )

        assert out.height == 1
        assert out.width > 0
