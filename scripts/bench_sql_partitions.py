"""Benchmark SQL approach with 1,2,4,8,12 partitions using pure Rust path."""

import gzip
import json
import os
import subprocess
import sys
import tempfile

VENV_PYTHON = "/Users/mwiewior/research/git/polars-bio/.venv/bin/python"
DATA_DIR = "/Users/mwiewior/research/data/polars-bio"
HEAD_VCF = f"{DATA_DIR}/20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr_12_22_X.recalibrated_variants.exome.head_100.vcf.gz"
FULL_VCF = f"{DATA_DIR}/20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr_12_22_X.recalibrated_variants.exome.vcf.gz"

WORKER = r'''
import gc, gzip, json, resource, sys, time
from pathlib import Path
import polars_bio as pb
from polars_bio.polars_bio import py_write_from_sql, WriteOptions, VcfWriteOptions, OutputFormat
from polars_bio.context import ctx

input_vcf = sys.argv[1]
output_vcf = sys.argv[2]
target_partitions = sys.argv[3]

DATA_DIR = Path("/Users/mwiewior/research/data/polars-bio")
samples = [s.strip() for s in (DATA_DIR / "samples_rand_2000.txt").read_text().splitlines() if s.strip()]

pb.set_option("datafusion.execution.target_partitions", target_partitions)

table_name = f"vcf_bench_{int(time.time() * 1000)}"
pb.register_vcf(input_vcf, name=table_name, info_fields=[],
                format_fields=["GT", "GQ", "DP", "PL"], samples=samples)

sql = f"""SELECT chrom, start, "end", id, ref, alt, qual, filter,
    vcf_process_genotypes(
        genotypes."GT", genotypes."GQ", genotypes."DP", genotypes."PL",
        10, 10, 200, 10
    ) AS genotypes
FROM {table_name}
WHERE qual >= 20
  AND list_avg(genotypes."GQ") >= 15.0
  AND list_avg(genotypes."DP") >= 15.0
  AND list_avg(genotypes."DP") <= 150.0"""

format_meta = {"DSG": {"number": "1", "type": "Float", "description": "Dosage"}}
wo = WriteOptions(
    vcf_write_options=VcfWriteOptions(
        zero_based=True,
        format_fields_metadata=json.dumps(format_meta),
        sample_names=json.dumps(samples),
    )
)

gc.collect()
t0 = time.perf_counter()
row_count = py_write_from_sql(ctx, sql, output_vcf, OutputFormat.Vcf, wo)
wall = time.perf_counter() - t0
peak_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)

print(json.dumps({"wall": wall, "peak_rss_mb": peak_rss_mb, "rows": row_count}))
'''

input_vcf = sys.argv[1] if len(sys.argv) > 1 else HEAD_VCF
label = "head_100" if "head_100" in input_vcf else "full"

print(f"\n=== SQL Partition Benchmark (pure Rust): {label} ===\n")
print(
    f"{'Partitions':<12} {'Wall (s)':>10} {'Peak RSS (MB)':>15} {'Rows':>10} {'Speedup':>10}"
)
print("-" * 62)

baseline_wall = None
output_files = {}

for partitions in [1, 2, 4, 8, 12]:
    out_path = tempfile.mktemp(suffix=f"_{partitions}p.vcf.gz")
    env = os.environ.copy()
    env["POLARS_MAX_THREADS"] = "1"

    result = subprocess.run(
        [VENV_PYTHON, "-c", WORKER, input_vcf, out_path, str(partitions)],
        capture_output=True,
        text=True,
        env=env,
        timeout=600,
    )

    if result.returncode != 0:
        print(f"{partitions:<12} FAILED: {result.stderr[-300:]}")
        continue

    metrics = json.loads(result.stdout.strip().split("\n")[-1])
    wall, rss, rows = metrics["wall"], metrics["peak_rss_mb"], metrics["rows"]

    if baseline_wall is None:
        baseline_wall = wall

    speedup = baseline_wall / wall if wall > 0 else 0
    print(f"{partitions:<12} {wall:>10.1f} {rss:>15.0f} {rows:>10} {speedup:>9.1f}x")
    output_files[partitions] = out_path

# Equivalence check
print("\n=== Equivalence Check vs 1-partition baseline ===\n")

import polars as pl

import polars_bio as pb

pb.set_option("datafusion.execution.target_partitions", "1")

baseline_path = output_files.get(1)
if not baseline_path:
    print("No baseline (1 partition) to compare against")
    sys.exit(1)

base_tbl = f"base_{id(baseline_path)}"
pb.register_vcf(
    baseline_path,
    name=base_tbl,
    info_fields=[],
    format_fields=["GT", "GQ", "DP", "PL", "DSG"],
)
df_base = pb.sql(f"SELECT * FROM {base_tbl}").collect().sort(["chrom", "start"])

all_pass = True
for partitions in [2, 4, 8, 12]:
    path = output_files.get(partitions)
    if not path:
        continue

    tbl = f"cmp_{partitions}_{id(path)}"
    pb.register_vcf(
        path, name=tbl, info_fields=[], format_fields=["GT", "GQ", "DP", "PL", "DSG"]
    )
    df_cmp = pb.sql(f"SELECT * FROM {tbl}").collect().sort(["chrom", "start"])

    if df_base.height != df_cmp.height:
        print(
            f"  {partitions}p: FAIL - row count {df_cmp.height} vs baseline {df_base.height}"
        )
        all_pass = False
        continue

    mismatches = []
    for col in ["chrom", "start", "qual"]:
        if not df_base[col].equals(df_cmp[col]):
            mismatches.append(col)

    for field in ["GT", "GQ", "DP", "PL"]:
        if (
            not df_base["genotypes"]
            .struct.field(field)
            .equals(df_cmp["genotypes"].struct.field(field))
        ):
            mismatches.append(field)

    if mismatches:
        print(f"  {partitions}p: FAIL - mismatches in {mismatches}")
        all_pass = False
    else:
        print(f"  {partitions}p: PASS (rows={df_cmp.height}, GT/GQ/DP/PL exact match)")

for p in output_files.values():
    os.unlink(p)

print(f"\nOverall: {'PASS' if all_pass else 'FAIL'}")
