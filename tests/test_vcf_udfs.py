"""Tests for VCF scalar UDFs and long view registration.

UDFs tested: list_avg, list_gte, list_lte, list_and, vcf_set_gts, vcf_an, vcf_ac, vcf_af
Long view: register_vcf_long_view creates {table}_long view.

Test data: tests/data/io/vcf/multisample.vcf
  - 3 variants, 3 samples (NA12878, NA12879, NA12880)
  - FORMAT: GT, DP, GQ
  - Row 0: GQ=[99,95,85]  DP=[25,30,20]  GT=[0/1,1/1,0/0]
  - Row 1: GQ=[90,88,99]  DP=[22,28,35]  GT=[0/0,0/1,1/1]
  - Row 2: GQ=[75,92,80]  DP=[18,32,27]  GT=[1/1,0/0,0/1]
"""

import polars as pl
import pytest

import polars_bio as pb

VCF_PATH = "tests/data/io/vcf/multisample.vcf"
TABLE_NAME = "vcf_udf_test"


@pytest.fixture(autouse=True)
def _register_vcf_table():
    """Register multisample VCF with FORMAT fields for all tests."""
    pb.register_vcf(
        VCF_PATH,
        name=TABLE_NAME,
        format_fields=["GT", "DP", "GQ"],
    )


# =============================================================================
# UDF tests
# =============================================================================


def test_list_avg():
    """list_avg(genotypes."GQ") returns correct Float64 average per row."""
    df = pb.sql(
        f'SELECT list_avg(genotypes."GQ") AS avg_gq FROM {TABLE_NAME}'
    ).collect()

    assert df.shape == (3, 1)
    avg_gq = df["avg_gq"].to_list()

    # Row 0: (99+95+85)/3 = 93.0
    assert abs(avg_gq[0] - 93.0) < 0.01
    # Row 1: (90+88+99)/3 = 92.333...
    assert abs(avg_gq[1] - 92.333) < 0.01
    # Row 2: (75+92+80)/3 = 82.333...
    assert abs(avg_gq[2] - 82.333) < 0.01


def test_list_avg_dp():
    """list_avg also works on DP field."""
    df = pb.sql(
        f'SELECT list_avg(genotypes."DP") AS avg_dp FROM {TABLE_NAME}'
    ).collect()

    avg_dp = df["avg_dp"].to_list()
    # Row 0: (25+30+20)/3 = 25.0
    assert abs(avg_dp[0] - 25.0) < 0.01


def test_list_gte():
    """list_gte(genotypes."DP", 25) returns List<Boolean> with correct element-wise results."""
    df = pb.sql(
        f'SELECT list_gte(genotypes."DP", 25) AS dp_gte_25 FROM {TABLE_NAME}'
    ).collect()

    assert df.shape == (3, 1)
    vals = df["dp_gte_25"].to_list()

    # Row 0: DP=[25,30,20] >= 25 → [true, true, false]
    assert vals[0] == [True, True, False]
    # Row 1: DP=[22,28,35] >= 25 → [false, true, true]
    assert vals[1] == [False, True, True]
    # Row 2: DP=[18,32,27] >= 25 → [false, true, true]
    assert vals[2] == [False, True, True]


def test_list_lte():
    """list_lte(genotypes."GQ", 90) returns correct element-wise results."""
    df = pb.sql(
        f'SELECT list_lte(genotypes."GQ", 90) AS gq_lte_90 FROM {TABLE_NAME}'
    ).collect()

    vals = df["gq_lte_90"].to_list()

    # Row 0: GQ=[99,95,85] <= 90 → [false, false, true]
    assert vals[0] == [False, False, True]
    # Row 1: GQ=[90,88,99] <= 90 → [true, true, false]
    assert vals[1] == [True, True, False]
    # Row 2: GQ=[75,92,80] <= 90 → [true, false, true]
    assert vals[2] == [True, False, True]


def test_list_and():
    """list_and combines two boolean lists element-wise."""
    df = pb.sql(
        f"""SELECT list_and(
            list_gte(genotypes."DP", 25),
            list_gte(genotypes."GQ", 90)
        ) AS combined FROM {TABLE_NAME}"""
    ).collect()

    vals = df["combined"].to_list()

    # Row 0: DP>=25=[T,T,F] AND GQ>=90=[T,T,F] → [T,T,F]
    assert vals[0] == [True, True, False]
    # Row 1: DP>=25=[F,T,T] AND GQ>=90=[T,F,T] → [F,F,T]
    assert vals[1] == [False, False, True]
    # Row 2: DP>=25=[F,T,T] AND GQ>=90=[F,T,F] → [F,T,F]
    assert vals[2] == [False, True, False]


def test_vcf_set_gts():
    """vcf_set_gts masks GT values where mask is false; default replacement is './.'."""
    df = pb.sql(
        f"""SELECT vcf_set_gts(
            genotypes."GT",
            list_gte(genotypes."GQ", 90)
        ) AS masked_gt FROM {TABLE_NAME}"""
    ).collect()

    vals = df["masked_gt"].to_list()

    # Row 0: GQ>=90=[T,T,F] → keep GT where true, mask with "./." where false
    # GT=[0/1, 1/1, 0/0] → [0/1, 1/1, ./.]
    assert vals[0][0] == "0/1"
    assert vals[0][1] == "1/1"
    assert vals[0][2] == "./."

    # Row 1: GQ>=90=[T,F,T] → GT=[0/0, 0/1, 1/1] → [0/0, ./., 1/1]
    assert vals[1][0] == "0/0"
    assert vals[1][1] == "./."
    assert vals[1][2] == "1/1"


def test_vcf_set_gts_custom_replacement():
    """vcf_set_gts supports explicit custom replacement (bcftools-compatible '.')."""
    df = pb.sql(
        f"""SELECT vcf_set_gts(
            genotypes."GT",
            list_gte(genotypes."GQ", 90),
            '.'
        ) AS masked_gt FROM {TABLE_NAME}"""
    ).collect()

    vals = df["masked_gt"].to_list()
    assert vals[0][2] == "."
    assert vals[1][1] == "."


def test_vcf_allele_stats_udfs():
    """vcf_an/vcf_ac/vcf_af compute allele statistics from GT lists."""
    df = pb.sql(
        f"""SELECT
            vcf_an(genotypes."GT") AS an,
            vcf_ac(genotypes."GT") AS ac,
            vcf_af(genotypes."GT") AS af
        FROM {TABLE_NAME}
        ORDER BY chrom, start"""
    ).collect()

    # Row 0 GT=[0/1,1/1,0/0] -> AN=6, AC=[3], AF=[0.5]
    assert df["an"].to_list()[0] == 6
    assert df["ac"].to_list()[0] == [3]
    assert abs(df["af"].to_list()[0][0] - 0.5) < 1e-9

    # Row 1 GT=[0/0,0/1,1/1] -> AN=6, AC=[3], AF=[0.5]
    assert df["an"].to_list()[1] == 6
    assert df["ac"].to_list()[1] == [3]
    assert abs(df["af"].to_list()[1][0] - 0.5) < 1e-9


def test_bcftools_equivalent_query():
    """Full bcftools-equivalent pipeline as single SQL query.

    Equivalent to:
      bcftools filter --exclude 'QUAL<30 || AVG(FORMAT/GQ)<85'
      bcftools filter --exclude 'FORMAT/GQ<90 | FORMAT/DP<25' --set-GTs '.'
    """
    df = pb.sql(
        f"""SELECT chrom, start, "end", "ref", alt, qual,
            named_struct(
                'GT', vcf_set_gts(genotypes."GT",
                    list_and(
                        list_gte(genotypes."GQ", 90),
                        list_gte(genotypes."DP", 25)
                    )),
                'GQ', genotypes."GQ",
                'DP', genotypes."DP"
            ) AS genotypes
        FROM {TABLE_NAME}
        WHERE qual >= 30
          AND list_avg(genotypes."GQ") >= 85"""
    ).collect()

    # Row 2 avg(GQ)=(75+92+80)/3≈82.3 < 85, so 2 rows pass
    assert df.shape[0] == 2
    # Standard columns present
    assert "chrom" in df.columns
    assert "genotypes" in df.columns


# =============================================================================
# Long view tests
# =============================================================================

LONG_TABLE = "vcf_long_test"


@pytest.fixture(scope="module")
def long_view():
    """Register a long view on top of the UDF test table (once per module)."""
    pb.register_vcf(
        VCF_PATH,
        name=LONG_TABLE,
        format_fields=["GT", "DP", "GQ"],
    )
    view_name = pb.register_vcf_long_view(LONG_TABLE)
    return view_name


def test_long_view_registration(long_view):
    """register_vcf_long_view creates {table}_long view."""
    assert long_view == f"{LONG_TABLE}_long"


def test_long_view_schema(long_view):
    """Long view has sample_id column and flat FORMAT columns."""
    df = pb.sql(f"SELECT * FROM {long_view}").collect()

    assert "sample_id" in df.columns
    # Row count = variants × samples = 3 × 3 = 9
    assert df.shape[0] == 9


def test_long_view_values(long_view):
    """GT values match per-sample expectations in long view."""
    df = pb.sql(
        f'SELECT sample_id, "GT" FROM {long_view} ORDER BY chrom, start, sample_id'
    ).collect()

    # Filter to first variant (start=10000 in 1-based)
    na12878_rows = df.filter(pl.col("sample_id") == "NA12878")
    gt_vals = na12878_rows["GT"].to_list()
    assert gt_vals[0] == "0/1"
