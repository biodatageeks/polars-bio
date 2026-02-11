import polars as pl
import pytest
from _expected import DATA_DIR

import polars_bio as pb

PAIRS_DIR = f"{DATA_DIR}/io/pairs"
PLAIN_FILE = f"{PAIRS_DIR}/test.pairs"
INDEXED_FILE = f"{PAIRS_DIR}/multi_chrom.pairs.gz"


class TestPairsPlain:
    """Tests using the plain text .pairs file (4 rows)."""

    def test_count(self):
        df = pb.read_pairs(PLAIN_FILE)
        assert len(df) == 4

    def test_columns(self):
        df = pb.read_pairs(PLAIN_FILE)
        expected_cols = {"readID", "chr1", "pos1", "chr2", "pos2", "strand1", "strand2"}
        assert set(df.columns) == expected_cols

    def test_fields(self):
        df = pb.read_pairs(PLAIN_FILE)
        assert df["readID"][0] == "EAS139:136:FC706VJ:2:2104:23462:197393"
        assert df["chr1"][0] == "chr1"
        assert df["pos1"][0] == 10000
        assert df["chr2"][0] == "chr1"
        assert df["pos2"][0] == 20000
        assert df["strand1"][0] == "+"
        assert df["strand2"][0] == "+"
        # Check last row
        assert df["chr2"][3] == "chr3"
        assert df["strand2"][3] == "-"

    def test_scan(self):
        lf = pb.scan_pairs(PLAIN_FILE)
        df = lf.collect()
        assert len(df) == 4

    def test_register_and_sql(self):
        pb.register_pairs(PLAIN_FILE, "test_pairs_plain")
        count = pb.sql("SELECT count(*) AS cnt FROM test_pairs_plain").collect()
        assert count["cnt"][0] == 4

    def test_projection_pushdown(self):
        pb.register_pairs(PLAIN_FILE, "test_pairs_proj")
        result = pb.sql("SELECT chr1, pos1, chr2, pos2 FROM test_pairs_proj").collect()
        assert set(result.columns) == {"chr1", "pos1", "chr2", "pos2"}
        assert len(result) == 4

    def test_predicate_filter(self):
        df = pb.scan_pairs(PLAIN_FILE).filter(pl.col("chr2") == "chr2").collect()
        assert len(df) == 1
        assert df["readID"][0] == "EAS139:136:FC706VJ:2:2342:15343:9863"


class TestPairsIndexed:
    """Tests using the bgzipped + tabix-indexed .pairs.gz file (30 rows)."""

    def test_indexed_full_scan(self):
        df = pb.read_pairs(INDEXED_FILE)
        assert len(df) == 30

    def test_indexed_single_chr1_filter(self):
        pb.register_pairs(INDEXED_FILE, "pairs_idx")
        result = pb.sql("SELECT * FROM pairs_idx WHERE chr1 = 'chr1'").collect()
        assert len(result) == 10

    def test_indexed_multi_chr1_filter(self):
        pb.register_pairs(INDEXED_FILE, "pairs_idx_multi")
        result = pb.sql(
            "SELECT * FROM pairs_idx_multi WHERE chr1 IN ('chr1', 'chr2')"
        ).collect()
        assert len(result) == 20

    def test_indexed_range_query(self):
        pb.register_pairs(INDEXED_FILE, "pairs_idx_range")
        result = pb.sql(
            "SELECT * FROM pairs_idx_range WHERE chr1 = 'chr1' AND pos1 >= 5000 AND pos1 <= 50000"
        ).collect()
        # chr1 rows with pos1 in [5000, 50000]: 5000, 10000, 15000, 30000, 40000, 50000
        assert len(result) == 6

    def test_indexed_chr2_residual_filter(self):
        pb.register_pairs(INDEXED_FILE, "pairs_idx_res")
        result = pb.sql(
            "SELECT * FROM pairs_idx_res WHERE chr1 = 'chr1' AND chr2 = 'chr2'"
        ).collect()
        # chr1 rows where chr2='chr2': read_04(pos1=15000), read_06(pos1=40000), read_09(pos1=80000)
        assert len(result) == 3

    def test_scan_predicate_pushdown(self):
        df = pb.scan_pairs(INDEXED_FILE).filter(pl.col("chr1") == "chr1").collect()
        assert len(df) == 10

    def test_pushdown_vs_no_pushdown_identical(self):
        df_pushdown = (
            pb.scan_pairs(INDEXED_FILE).filter(pl.col("chr1") == "chr1").collect()
        )
        df_no_pushdown = pb.read_pairs(INDEXED_FILE).filter(pl.col("chr1") == "chr1")
        assert len(df_pushdown) == len(df_no_pushdown)
        assert (
            df_pushdown["readID"].sort().to_list()
            == df_no_pushdown["readID"].sort().to_list()
        )
