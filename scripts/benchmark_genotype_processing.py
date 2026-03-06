"""Benchmark: Compare explode, Polars list, and SQL approaches for VCF genotype processing.

Each approach runs in a separate subprocess for accurate peak RSS measurement.
Results are saved as parquet files for cross-approach equivalence checking.
"""

import json
import os
import resource
import subprocess
import sys
import tempfile
import time
from pathlib import Path

DATA_DIR = Path("/Users/mwiewior/research/data/polars-bio")

HEAD_100_VCF = str(
    DATA_DIR
    / "20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr_12_22_X.recalibrated_variants.exome.head_100.vcf.gz"
)
FULL_VCF = str(
    DATA_DIR
    / "20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr_12_22_X.recalibrated_variants.exome.vcf.gz"
)

SAMPLES_FILE = DATA_DIR / "samples_rand_2000.txt"

# Thresholds
QUAL_MIN = 20
AVG_GQ_MIN = 15
AVG_DP_MIN = 15
AVG_DP_MAX = 150
SAMPLE_GQ_MIN = 10
SAMPLE_DP_MIN = 10
SAMPLE_DP_MAX = 200
CALC_DS_MIN_GQ = 10


# ---------------------------------------------------------------------------
# Worker: runs a single approach in its own process, saves result to parquet
# ---------------------------------------------------------------------------
WORKER_SCRIPT = r'''
import gc, json, resource, sys, time
from pathlib import Path

import polars as pl
import polars_bio as pb

approach = sys.argv[1]
input_vcf = sys.argv[2]
output_vcf = sys.argv[3]

DATA_DIR = Path("/Users/mwiewior/research/data/polars-bio")
samples = [s.strip() for s in (DATA_DIR / "samples_rand_2000.txt").read_text().splitlines() if s.strip()]

QUAL_MIN, AVG_GQ_MIN, AVG_DP_MIN, AVG_DP_MAX = 20, 15, 15, 150
SAMPLE_GQ_MIN, SAMPLE_DP_MIN, SAMPLE_DP_MAX, CALC_DS_MIN_GQ = 10, 10, 200, 10

pb.set_option("datafusion.execution.target_partitions", "1")


def variant_filter(lf):
    return (
        lf.with_columns(
            pl.col("genotypes").struct.field("GQ").cast(pl.List(pl.Float64)).list.mean().alias("_avg_gq"),
            pl.col("genotypes").struct.field("DP").cast(pl.List(pl.Float64)).list.mean().alias("_avg_dp"),
        )
        .filter(
            (pl.col("qual") >= QUAL_MIN)
            & (pl.col("_avg_gq") >= AVG_GQ_MIN)
            & (pl.col("_avg_dp") >= AVG_DP_MIN)
            & (pl.col("_avg_dp") <= AVG_DP_MAX)
        )
        .drop(["_avg_gq", "_avg_dp"])
    )


def run_explode():
    # Import only the function, not the module top-level (which has hardcoded paths)
    import importlib.util
    spec = importlib.util.spec_from_file_location("_explode_mod", "/private/tmp/with_pl_and_ds_3_2_explode.py")
    # We can't import the module directly (top-level side effects), so inline the function
    lf = pb.scan_vcf(input_vcf, format_fields=["GT", "GQ", "DP", "PL"], samples=samples)
    lf = variant_filter(lf)

    # --- inline process_genotypes_via_explode ---
    non_genotype_cols = [c for c in lf.collect_schema().names() if c != "genotypes"]
    lf_indexed = (
        lf.with_row_index("_row_idx")
        .with_columns([
            pl.col("genotypes").struct.field("GT").alias("_GT"),
            pl.col("genotypes").struct.field("GQ").alias("_GQ"),
            pl.col("genotypes").struct.field("DP").alias("_DP"),
            pl.col("genotypes").struct.field("PL").alias("_PL"),
        ])
        .drop("genotypes")
    )
    lf_exploded = lf_indexed.explode(["_GT", "_GQ", "_DP", "_PL"])

    gt_raw = pl.col("_GT"); gq_raw = pl.col("_GQ"); dp_raw = pl.col("_DP"); pl_field = pl.col("_PL")
    gt = pl.when(gt_raw == "0").then(pl.lit("0/0")).when(gt_raw == "1").then(pl.lit("1/1")).when(gt_raw == "2").then(pl.lit("2/2")).otherwise(gt_raw)
    gq = gq_raw.cast(pl.Float64); dp = dp_raw.cast(pl.Float64)
    good = gq.is_not_null() & dp.is_not_null() & (gq >= SAMPLE_GQ_MIN) & (dp >= SAMPLE_DP_MIN) & (dp <= SAMPLE_DP_MAX)
    pl_len = pl_field.list.len(); pl_valid = pl_field.is_not_null() & (pl_len >= 3)
    orig_pl_0 = pl_field.list.get(0, null_on_oob=True).cast(pl.Float64)
    orig_pl_1 = pl_field.list.get(1, null_on_oob=True).cast(pl.Float64)
    orig_pl_2 = pl_field.list.get(2, null_on_oob=True).cast(pl.Float64)
    is_gt_hom_ref = (gt == "0/0") | (gt == "0|0")
    is_pl_all_zero = pl_valid & orig_pl_0.is_not_null() & (orig_pl_0 == 0) & (orig_pl_1 == 0) & (orig_pl_2 == 0)
    gq_for_calc = gq.fill_null(0.0)
    gq_sufficient_for_calc = gq.is_not_null() & (gq >= CALC_DS_MIN_GQ)
    needs_pl_correction = is_gt_hom_ref & is_pl_all_zero & gq_sufficient_for_calc
    p_wrong = pl.lit(10.0).pow(-gq_for_calc / 10.0)
    discriminant = pl.lit(1.0) + pl.lit(4.0) * p_wrong
    x = (pl.lit(-1.0) + discriminant.sqrt()) / pl.lit(2.0)
    l_het_corr = x; l_alt_corr = x * x; l_ref_corr = pl.lit(1.0) - l_het_corr - l_alt_corr
    pl_ref_corr = pl.when(l_ref_corr > 0).then((-pl.lit(10.0) * l_ref_corr.log(base=10)).round(0).clip(0, 255)).otherwise(pl.lit(255.0))
    pl_het_corr = pl.when(l_het_corr > 0).then((-pl.lit(10.0) * l_het_corr.log(base=10)).round(0).clip(0, 255)).otherwise(pl.lit(255.0))
    pl_alt_corr = pl.when(l_alt_corr > 0).then((-pl.lit(10.0) * l_alt_corr.log(base=10)).round(0).clip(0, 255)).otherwise(pl.lit(255.0))
    final_pl_0 = pl.when(needs_pl_correction).then(pl_ref_corr).otherwise(orig_pl_0.fill_null(0))
    final_pl_1 = pl.when(needs_pl_correction).then(pl_het_corr).otherwise(orig_pl_1.fill_null(0))
    final_pl_2 = pl.when(needs_pl_correction).then(pl_alt_corr).otherwise(orig_pl_2.fill_null(0))
    l_ref = pl.when(final_pl_0 < 255).then(pl.lit(10.0).pow(-final_pl_0 / 10.0)).otherwise(pl.lit(0.0))
    l_het = pl.when(final_pl_1 < 255).then(pl.lit(10.0).pow(-final_pl_1 / 10.0)).otherwise(pl.lit(0.0))
    l_alt = pl.when(final_pl_2 < 255).then(pl.lit(10.0).pow(-final_pl_2 / 10.0)).otherwise(pl.lit(0.0))
    l_sum = l_ref + l_het + l_alt
    dosage_raw = pl.when(l_sum > 0).then((l_het / l_sum) + pl.lit(2.0) * (l_alt / l_sum)).otherwise(pl.lit(0.0))
    dosage_cleaned = pl.when((dosage_raw > 0) & (dosage_raw < 0.0001)).then(pl.lit(0.0)).otherwise(dosage_raw)
    final_ds = pl.when(pl_valid & gq_sufficient_for_calc).then(dosage_cleaned).otherwise(pl.lit(None).cast(pl.Float64))
    final_gt = pl.when(~good).then(pl.lit("./.")).otherwise(gt)
    output_pl = pl.when(pl_valid).then(
        pl.concat_list([final_pl_0.cast(pl.Int32), final_pl_1.cast(pl.Int32), final_pl_2.cast(pl.Int32)])
    ).otherwise(pl_field)
    lf_processed = lf_exploded.with_columns([
        final_gt.alias("_GT_out"), gq_raw.alias("_GQ_out"), dp_raw.alias("_DP_out"),
        output_pl.alias("_PL_out"), final_ds.cast(pl.Float32).alias("_DS_out"),
    ])
    lf_reaggregated = (
        lf_processed.group_by("_row_idx", maintain_order=True)
        .agg([*[pl.col(c).first() for c in non_genotype_cols],
              pl.col("_GT_out"), pl.col("_GQ_out"), pl.col("_DP_out"), pl.col("_PL_out"), pl.col("_DS_out")])
        .with_columns(pl.struct([
            pl.col("_GT_out").alias("GT"), pl.col("_GQ_out").alias("GQ"),
            pl.col("_DP_out").alias("DP"), pl.col("_PL_out").alias("PL"),
            pl.col("_DS_out").alias("DSG"),
        ]).alias("genotypes"))
        .select([*non_genotype_cols, "genotypes"])
    )
    lf = lf_reaggregated
    from polars_bio._metadata import set_vcf_metadata
    set_vcf_metadata(lf, sample_names=samples, format_fields={
        "DSG": {"number": "1", "type": "Float", "description": "Dosage"}})
    pb.sink_vcf(lf, output_vcf)


def run_polars_list():
    sys.path.insert(0, "/private/tmp")
    from with_pl_and_ds_polars_list import process_genotypes_polars_list
    lf = pb.scan_vcf(input_vcf, format_fields=["GT", "GQ", "DP", "PL"], samples=samples)
    lf = variant_filter(lf)
    lf = process_genotypes_polars_list(lf)
    from polars_bio._metadata import set_vcf_metadata
    set_vcf_metadata(lf, sample_names=samples, format_fields={
        "DSG": {"number": "1", "type": "Float", "description": "Dosage"}})
    pb.sink_vcf(lf, output_vcf)


def run_sql():
    table_name = f"vcf_bench_{int(time.time() * 1000)}"
    pb.register_vcf(input_vcf, name=table_name, info_fields=[],
                    format_fields=["GT", "GQ", "DP", "PL"], samples=samples)
    sql = f"""SELECT chrom, start, "end", id, ref, alt, qual, filter,
        vcf_process_genotypes(
            genotypes."GT", genotypes."GQ", genotypes."DP", genotypes."PL",
            {SAMPLE_GQ_MIN}, {SAMPLE_DP_MIN}, {SAMPLE_DP_MAX}, {CALC_DS_MIN_GQ}
        ) AS genotypes
    FROM {table_name}
    WHERE qual >= {QUAL_MIN}
      AND list_avg(genotypes."GQ") >= {AVG_GQ_MIN}.0
      AND list_avg(genotypes."DP") >= {AVG_DP_MIN}.0
      AND list_avg(genotypes."DP") <= {AVG_DP_MAX}.0"""
    lf = pb.sql(sql)
    from polars_bio._metadata import set_vcf_metadata
    set_vcf_metadata(lf, sample_names=samples, format_fields={
        "DSG": {"number": "1", "type": "Float", "description": "Dosage"}})
    pb.sink_vcf(lf, output_vcf)


gc.collect()
t0 = time.perf_counter()

if approach == "explode":
    run_explode()
elif approach == "polars_list":
    run_polars_list()
elif approach == "sql":
    run_sql()
else:
    raise ValueError(f"Unknown approach: {approach}")

wall = time.perf_counter() - t0
peak_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)

# Count rows from the output file (just count data lines, skip headers)
import gzip
rows = 0
opener = gzip.open if output_vcf.endswith('.gz') else open
with opener(output_vcf, 'rt') as f:
    for line in f:
        if not line.startswith('#'):
            rows += 1

# Print metrics as JSON to stdout
print(json.dumps({"wall": wall, "peak_rss_mb": peak_rss_mb, "rows": rows}))
'''


def run_approach(approach: str, input_vcf: str, output_vcf: str) -> dict:
    """Run a single approach in a subprocess and return metrics."""
    env = os.environ.copy()
    env.pop("CONDA_PREFIX", None)
    env["POLARS_MAX_THREADS"] = "1"
    result = subprocess.run(
        [sys.executable, "-c", WORKER_SCRIPT, approach, input_vcf, output_vcf],
        capture_output=True,
        text=True,
        env=env,
        timeout=600,
        cwd="/private/tmp",
    )
    if result.returncode != 0:
        print(f"  [{approach}] FAILED:\n{result.stderr[-2000:]}", file=sys.stderr)
        return None
    # Parse the last JSON line from stdout (skip progress bars on stderr)
    for line in reversed(result.stdout.strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            return json.loads(line)
    print(
        f"  [{approach}] No JSON output:\nstdout={result.stdout[-500:]}",
        file=sys.stderr,
    )
    return None


def check_equivalence(vcf_files: dict):
    """Load VCF outputs and compare all fields across approaches."""
    import polars as pl

    import polars_bio as pb

    dfs = {}
    for name, path in vcf_files.items():
        tbl = f"_equiv_{name.replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_')}_{int(time.time()*1000)}"
        pb.register_vcf(
            path,
            name=tbl,
            info_fields=[],
            format_fields=["GT", "GQ", "DP", "PL"],
            chunk_size=8,
            concurrent_fetches=1,
        )
        dfs[name] = pb.sql(f"SELECT * FROM {tbl}").collect()
    names = list(dfs.keys())
    baseline_name = names[0]
    df_base = dfs[baseline_name]
    n = df_base.shape[0]
    issues = []

    for other_name in names[1:]:
        df_other = dfs[other_name]
        if df_other.shape[0] != n:
            issues.append(
                f"Row count: {baseline_name}={n} vs {other_name}={df_other.shape[0]}"
            )
            continue

        # Exact fields: GT, GQ, DP, PL
        for field in ["GT", "GQ", "DP", "PL"]:
            base_vals = df_base["genotypes"].struct.field(field).to_list()
            other_vals = df_other["genotypes"].struct.field(field).to_list()
            for i in range(n):
                if base_vals[i] != other_vals[i]:
                    issues.append(
                        f"{field} mismatch row {i}: {baseline_name} vs {other_name}"
                    )
                    break

    if issues:
        return "FAIL", issues
    return "PASS", ["GT exact, GQ exact, DP exact, PL exact"]


def benchmark(input_vcf, label, skip_explode=False):
    approaches = [
        ("Explode (baseline)", "explode"),
        ("Polars-list", "polars_list"),
        ("SQL", "sql"),
    ]
    if skip_explode:
        approaches = [a for a in approaches if a[1] != "explode"]

    results = []
    vcf_files = {}

    with tempfile.TemporaryDirectory(prefix="bench_") as tmpdir:
        for display_name, approach_id in approaches:
            vcf_path = os.path.join(tmpdir, f"{approach_id}.vcf.gz")
            print(f"  Running {display_name}...", end="", flush=True)
            metrics = run_approach(approach_id, input_vcf, vcf_path)
            if metrics is None:
                print(" FAILED")
                continue
            print(f" {metrics['wall']:.2f}s")
            results.append(
                (display_name, metrics["wall"], metrics["peak_rss_mb"], metrics["rows"])
            )
            vcf_files[display_name] = vcf_path

        baseline_time = next(
            (r[1] for r in results if r[0] == "Explode (baseline)"), None
        )

        print(f"\n=== Benchmark: {label} ===")
        print(
            f"{'Approach':<22} {'Wall (s)':>10} {'Peak RSS (MB)':>14} {'Rows':>8} {'Speedup':>8}"
        )
        print("-" * 66)
        for name, wall, rss, rows in results:
            speedup = f"{baseline_time / wall:.1f}x" if baseline_time else "-"
            print(f"{name:<22} {wall:>10.2f} {rss:>14.0f} {rows:>8} {speedup:>8}")

        # Equivalence
        if len(vcf_files) >= 2:
            print("  Checking equivalence...", end="", flush=True)
            status, details = check_equivalence(vcf_files)
            print(f" done")
            print(f"\nEquivalence: {status} ({'; '.join(details)})")


if __name__ == "__main__":
    use_full = "--full" in sys.argv
    skip_explode = "--skip-explode" in sys.argv

    if not use_full and Path(HEAD_100_VCF).exists():
        benchmark(
            HEAD_100_VCF,
            "head_100 (100 variants x 2000 samples)",
            skip_explode=skip_explode,
        )

    if use_full and Path(FULL_VCF).exists():
        benchmark(
            FULL_VCF, "full (131K variants x 2000 samples)", skip_explode=skip_explode
        )
