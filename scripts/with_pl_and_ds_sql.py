"""Approach B: Process VCF genotypes using pure DataFusion SQL with Rust UDFs."""

from pathlib import Path

import polars_bio as pb
from polars_bio._metadata import set_vcf_metadata

DATA_DIR = Path("/Users/mwiewior/research/data/polars-bio")
# input_vcf = str(DATA_DIR / "20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr_12_22_X.recalibrated_variants.exome.vcf.gz")
input_vcf = str(
    DATA_DIR
    / "20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr_12_22_X.recalibrated_variants.exome.head_100.vcf.gz"
)
output_vcf = "/private/tmp/output_sql.vcf.gz"

samples = [
    s.strip()
    for s in (DATA_DIR / "samples_rand_2000.txt").read_text().splitlines()
    if s.strip()
]

pb.set_option("datafusion.execution.target_partitions", "1")

TABLE_NAME = "vcf_data"
pb.register_vcf(
    input_vcf,
    name=TABLE_NAME,
    info_fields=[],
    format_fields=["GT", "GQ", "DP", "PL"],
    samples=samples,
)

SQL = f"""\
SELECT chrom, start, "end", id, ref, alt, qual, filter,
    vcf_process_genotypes(
        genotypes."GT", genotypes."GQ", genotypes."DP", genotypes."PL",
        10, 10, 200, 10
    ) AS genotypes
FROM {TABLE_NAME}
WHERE qual >= 20
  AND list_avg(genotypes."GQ") >= 15.0
  AND list_avg(genotypes."DP") >= 15.0
  AND list_avg(genotypes."DP") <= 150.0"""

lf = pb.sql(SQL)
set_vcf_metadata(lf, sample_names=samples)
pb.sink_vcf(lf, output_vcf)
print(f"Wrote: {output_vcf}")
