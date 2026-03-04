"""Tests that replicate bcftools pipelines using pb.sql().

Mimics this three-step bcftools pipeline:

    bcftools view --samples-file samples_rand_2000.txt input.vcf.gz |
    bcftools filter --exclude 'QUAL<20 || AVG(FORMAT/GQ)<15 || AVG(FORMAT/DP)<15 || AVG(FORMAT/DP)>150' |
    bcftools filter --exclude 'FORMAT/GQ<10 | FORMAT/DP<10 | FORMAT/DP>200' --set-GTs '.' \
        -o final.vcf.gz --write-index=tbi

Test data:
  - tests/data/io/vcf/bcftools/multisample_3202.vcf.gz  (100 variants, 3202 samples)
  - tests/data/io/vcf/bcftools/samples_rand_2000.txt     (2000 sample IDs)
  - tests/data/io/vcf/bcftools/bcftools_expected.vcf.gz   (precomputed bcftools output)
"""

from pathlib import Path

import polars as pl
import pytest

import polars_bio as pb

DATA_DIR = Path(__file__).parent / "data"
BCFTOOLS_DIR = DATA_DIR / "io" / "vcf" / "bcftools"
INPUT_VCF = str(BCFTOOLS_DIR / "multisample_3202.vcf.gz")
SAMPLES_FILE = str(BCFTOOLS_DIR / "samples_rand_2000.txt")
EXPECTED_VCF = str(BCFTOOLS_DIR / "bcftools_expected.vcf.gz")

FILTER_SQL = """\
SELECT chrom, start, "end", id, ref, alt, qual, filter, \
  named_struct(\
    'GT', vcf_set_gts(genotypes."GT", \
      list_and(list_and(list_gte(genotypes."GQ", 10), list_gte(genotypes."DP", 10)), \
               list_lte(genotypes."DP", 200))), \
    'GQ', genotypes."GQ", \
    'DP', genotypes."DP"\
  ) AS genotypes \
FROM vcf_table \
WHERE qual >= 20 \
  AND list_avg(genotypes."GQ") >= 15.0 \
  AND list_avg(genotypes."DP") >= 15.0 \
  AND list_avg(genotypes."DP") <= 150.0"""


def _read_samples(path: str) -> list[str]:
    return Path(path).read_text().strip().splitlines()


@pytest.fixture(scope="module")
def selected_samples():
    return _read_samples(SAMPLES_FILE)


@pytest.fixture(scope="module")
def registered_table(selected_samples):
    """Register input VCF with sample subset and FORMAT fields."""
    table_name = "vcf_table"
    pb.register_vcf(
        INPUT_VCF,
        name=table_name,
        info_fields=[],
        format_fields=["GT", "DP", "GQ"],
        samples=selected_samples,
    )
    return table_name


@pytest.fixture(scope="module")
def result_df(registered_table):
    """Execute the bcftools-equivalent SQL query."""
    lf = pb.sql(FILTER_SQL)
    return lf.collect()


@pytest.fixture(scope="module")
def expected_df(selected_samples):
    """Read precomputed bcftools expected output."""
    pb.register_vcf(
        EXPECTED_VCF,
        name="bcftools_expected",
        info_fields=[],
        format_fields=["GT", "DP", "GQ"],
    )
    lf = pb.sql("SELECT * FROM bcftools_expected")
    return lf.collect()


class TestBcftoolsFilterPipeline:
    """Verify pb.sql replicates the three-step bcftools pipeline."""

    def test_row_count_matches(self, result_df, expected_df):
        """Same number of variants pass the filter."""
        assert result_df.shape[0] == expected_df.shape[0]

    def test_sample_count(self, result_df, selected_samples):
        """Genotype lists have correct sample count (2000)."""
        gt_list = result_df["genotypes"].struct.field("GT").to_list()[0]
        assert len(gt_list) == len(selected_samples)

    def test_positions_match(self, result_df, expected_df):
        """Variant positions match between pb.sql and bcftools."""
        result_pos = result_df.select("chrom", "start").sort("chrom", "start")
        expected_pos = expected_df.select("chrom", "start").sort("chrom", "start")
        assert result_pos.equals(expected_pos)

    def test_qual_filter(self, result_df):
        """All variants have QUAL >= 20."""
        assert result_df.filter(pl.col("qual") < 20).shape[0] == 0

    def test_avg_gq_filter(self, result_df):
        """All variants have avg(GQ) >= 15."""
        lf = pb.sql(
            'SELECT list_avg(genotypes."GQ") AS avg_gq FROM vcf_table '
            "WHERE qual >= 20 "
            'AND list_avg(genotypes."GQ") >= 15.0 '
            'AND list_avg(genotypes."DP") >= 15.0 '
            'AND list_avg(genotypes."DP") <= 150.0'
        )
        df = lf.collect()
        assert df.filter(pl.col("avg_gq") < 15.0).shape[0] == 0

    def test_avg_dp_filter(self, result_df):
        """All variants have 15 <= avg(DP) <= 150."""
        lf = pb.sql(
            'SELECT list_avg(genotypes."DP") AS avg_dp FROM vcf_table '
            "WHERE qual >= 20 "
            'AND list_avg(genotypes."GQ") >= 15.0 '
            'AND list_avg(genotypes."DP") >= 15.0 '
            'AND list_avg(genotypes."DP") <= 150.0'
        )
        df = lf.collect()
        assert df.filter(pl.col("avg_dp") < 15.0).shape[0] == 0
        assert df.filter(pl.col("avg_dp") > 150.0).shape[0] == 0

    def test_set_gts_masking(self, result_df, expected_df):
        """GT masking matches bcftools --set-GTs '.' behavior.

        Count ./. per variant and verify totals match.
        """
        result_gts = result_df["genotypes"].struct.field("GT").to_list()
        expected_gts = expected_df["genotypes"].struct.field("GT").to_list()

        result_missing = [row.count("./.") for row in result_gts]
        expected_missing = [row.count("./.") for row in expected_gts]
        assert result_missing == expected_missing

    def test_gt_values_match(self, result_df, expected_df):
        """All GT values match between pb.sql and bcftools per variant."""
        result_gts = result_df["genotypes"].struct.field("GT").to_list()
        expected_gts = expected_df["genotypes"].struct.field("GT").to_list()

        for i, (r_gt, e_gt) in enumerate(zip(result_gts, expected_gts)):
            assert r_gt == e_gt, f"GT mismatch at variant index {i}"

    def test_dp_values_preserved(self, result_df, expected_df):
        """DP values are unchanged (not masked, only GT is masked)."""
        result_dp = result_df["genotypes"].struct.field("DP").to_list()
        expected_dp = expected_df["genotypes"].struct.field("DP").to_list()

        for i, (r_dp, e_dp) in enumerate(zip(result_dp, expected_dp)):
            assert r_dp == e_dp, f"DP mismatch at variant index {i}"

    def test_gq_values_preserved(self, result_df, expected_df):
        """GQ values are unchanged."""
        result_gq = result_df["genotypes"].struct.field("GQ").to_list()
        expected_gq = expected_df["genotypes"].struct.field("GQ").to_list()

        for i, (r_gq, e_gq) in enumerate(zip(result_gq, expected_gq)):
            assert r_gq == e_gq, f"GQ mismatch at variant index {i}"


class TestBcftoolsSinkRoundtrip:
    """Verify the full pipeline writes a valid VCF via sink_vcf."""

    def test_sink_vcf_roundtrip(self, registered_table, selected_samples, tmp_path):
        """Write pb.sql result to VCF and re-read it."""
        output_path = tmp_path / "bcftools_result.vcf.gz"

        lf = pb.sql(FILTER_SQL)
        pb.sink_vcf(lf, str(output_path))
        assert output_path.exists()

        pb.register_vcf(
            str(output_path),
            name="bcftools_roundtrip",
            info_fields=[],
            format_fields=["GT", "DP", "GQ"],
        )
        roundtrip_df = pb.sql("SELECT * FROM bcftools_roundtrip").collect()
        assert roundtrip_df.shape[0] == 100

    def test_sink_vcf_content_matches_bcftools(
        self, registered_table, expected_df, tmp_path
    ):
        """Re-read sink_vcf output and verify GT/DP/GQ match bcftools expected."""
        output_path = tmp_path / "bcftools_content_check.vcf.gz"

        lf = pb.sql(FILTER_SQL)
        pb.sink_vcf(lf, str(output_path))

        table = f"sink_content_{tmp_path.name.replace('-', '_')}"
        pb.register_vcf(
            str(output_path),
            name=table,
            info_fields=[],
            format_fields=["GT", "DP", "GQ"],
        )
        written_df = pb.sql(f"SELECT * FROM {table}").collect()

        # Row count
        assert written_df.shape[0] == expected_df.shape[0]

        # GT values (including ./. masking) match bcftools
        written_gts = written_df["genotypes"].struct.field("GT").to_list()
        expected_gts = expected_df["genotypes"].struct.field("GT").to_list()
        for i, (w, e) in enumerate(zip(written_gts, expected_gts)):
            assert w == e, f"GT mismatch at variant {i} after sink_vcf roundtrip"

        # DP values preserved through write
        written_dp = written_df["genotypes"].struct.field("DP").to_list()
        expected_dp = expected_df["genotypes"].struct.field("DP").to_list()
        for i, (w, e) in enumerate(zip(written_dp, expected_dp)):
            assert w == e, f"DP mismatch at variant {i} after sink_vcf roundtrip"

        # GQ values preserved through write
        written_gq = written_df["genotypes"].struct.field("GQ").to_list()
        expected_gq = expected_df["genotypes"].struct.field("GQ").to_list()
        for i, (w, e) in enumerate(zip(written_gq, expected_gq)):
            assert w == e, f"GQ mismatch at variant {i} after sink_vcf roundtrip"

    def test_sink_vcf_preserves_sample_names(
        self, registered_table, selected_samples, tmp_path
    ):
        """sink_vcf output header has the correct 2000 sample names."""
        output_path = tmp_path / "bcftools_samples.vcf"

        lf = pb.sql(FILTER_SQL)
        pb.sink_vcf(lf, str(output_path))

        with open(output_path, "rt") as f:
            for line in f:
                if line.startswith("#CHROM"):
                    header_cols = line.strip().split("\t")
                    sample_names = header_cols[9:]
                    assert len(sample_names) == len(selected_samples)
                    assert sample_names == selected_samples
                    break
