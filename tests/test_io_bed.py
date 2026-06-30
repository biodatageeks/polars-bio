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
        # 0-based half-open coordinates match the on-disk BED record exactly
        # (chr16  63917502  63934965). See issue #413: the end must NOT be
        # decremented in 0-based mode.
        assert projection["start"][1] == 66700000
        assert projection["end"][2] == 63934965
        assert projection["name"][4] == "FRA16E"


class TestBEDCoordinateSystem:
    """Regression tests for BED coordinate-system semantics (issue #413).

    A BED file stores intervals as 0-based half-open ``[start, end)``. The
    on-disk records in ``test.bed`` are::

        chr1  1000  2000
        chr2  1500  2500
        chrX  5000  8000

    Therefore the reader must produce:

    * ``use_zero_based=True``  -> ``(start, end)``     unchanged (0-based half-open)
    * ``use_zero_based=False`` -> ``(start + 1, end)``  (1-based closed)

    The ``end`` coordinate is identical in both systems because a 1-based
    *inclusive* end equals a 0-based *exclusive* end. Issue #413 reported that
    ``use_zero_based=True`` incorrectly emitted ``end - 1`` (a 0-based *closed*
    interval). The pre-existing value test only compared ``start``, so the
    ``end`` regression went undetected.
    """

    bed_path = f"{DATA_DIR}/io/bed/test.bed"

    # On-disk 0-based half-open coordinates (the source of truth).
    file_starts = [1000, 1500, 5000]
    file_ends = [2000, 2500, 8000]

    def _sorted(self, df):
        df = df.to_pandas().sort_values("start").reset_index(drop=True)
        return df["start"].tolist(), df["end"].tolist()

    def test_read_bed_zero_based_is_half_open(self):
        starts, ends = self._sorted(pb.read_bed(self.bed_path, use_zero_based=True))
        assert starts == self.file_starts, "0-based start must equal the file start"
        # The crux of #413: end must NOT be decremented.
        assert ends == self.file_ends, "0-based half-open end must equal the file end"

    def test_read_bed_one_based_is_closed(self):
        starts, ends = self._sorted(pb.read_bed(self.bed_path, use_zero_based=False))
        assert starts == [s + 1 for s in self.file_starts], "1-based start = file + 1"
        assert ends == self.file_ends, "1-based closed end must equal the file end"

    def test_scan_bed_zero_based_is_half_open(self):
        starts, ends = self._sorted(
            pb.scan_bed(self.bed_path, use_zero_based=True).collect()
        )
        assert starts == self.file_starts
        assert ends == self.file_ends, "scan_bed 0-based end must equal the file end"

    def test_read_bed_end_identical_across_coordinate_systems(self):
        _, ends_zero = self._sorted(pb.read_bed(self.bed_path, use_zero_based=True))
        _, ends_one = self._sorted(pb.read_bed(self.bed_path, use_zero_based=False))
        # Only the start differs by 1 between systems; the end is invariant.
        assert ends_zero == ends_one == self.file_ends

    def test_read_bed_preserves_interval_length(self):
        z_starts, z_ends = self._sorted(pb.read_bed(self.bed_path, use_zero_based=True))
        o_starts, o_ends = self._sorted(
            pb.read_bed(self.bed_path, use_zero_based=False)
        )
        for i in range(len(self.file_starts)):
            span = self.file_ends[i] - self.file_starts[i]
            assert z_ends[i] - z_starts[i] == span, "half-open length mismatch"
            assert o_ends[i] - o_starts[i] + 1 == span, "closed length mismatch"
