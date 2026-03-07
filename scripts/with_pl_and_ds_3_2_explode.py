from pathlib import Path

import polars as pl

import polars_bio as pb
from polars_bio._metadata import set_vcf_metadata

input_vcf = "20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr_12_22_X.recalibrated_variants.exome.vcf.gz"
# input_vcf = "20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr_12_22_X.recalibrated_variants.exome.head_100.vcf.gz"
output_vcf = "output_pb_with_ds_3_full.vcf.gz"
# input_vcf = "head_106667_tail_6.vcf"
# output_vcf = "output_head_106667_tail_6.vcf"

# Thresholds
qual_min = 20
avg_gq_min = 15
avg_dp_min = 15
avg_dp_max = 150
sample_gq_min = 10
sample_dp_min = 10
sample_dp_max = 200
calc_ds_min_gq = 10  # Separate threshold for DS calculation and PL correction

samples = [
    s.strip()
    for s in Path("samples_rand_2000.txt").read_text().splitlines()
    if s.strip()
]

pb.set_option("datafusion.execution.target_partitions", "1")


def process_genotypes_via_explode(
    lf: pl.LazyFrame,
    sample_gq_min: float,
    sample_dp_min: float,
    sample_dp_max: float,
    calc_ds_min_gq: float,
) -> pl.LazyFrame:
    """
    Process genotypes by exploding, transforming, and re-aggregating.

    This approach:
    1. Adds a row index for grouping
    2. Unnests genotypes struct to get individual field lists as columns
    3. Explodes all genotype field lists (GT, GQ, DP, PL) together
    4. Processes each sample (now a row)
    5. Groups by row index to re-aggregate into lists
    """
    # Save non-genotype columns for later
    non_genotype_cols = [c for c in lf.collect_schema().names() if c != "genotypes"]

    # Add row index and unnest genotypes
    lf_indexed = (
        lf.with_row_index("_row_idx")
        .with_columns(
            [
                pl.col("genotypes").struct.field("GT").alias("_GT"),
                pl.col("genotypes").struct.field("GQ").alias("_GQ"),
                pl.col("genotypes").struct.field("DP").alias("_DP"),
                pl.col("genotypes").struct.field("PL").alias("_PL"),
            ]
        )
        .drop("genotypes")
    )

    # Explode all genotype fields together
    lf_exploded = lf_indexed.explode(["_GT", "_GQ", "_DP", "_PL"])

    # Process each sample
    gt_raw = pl.col("_GT")
    gq_raw = pl.col("_GQ")
    dp_raw = pl.col("_DP")
    pl_field = pl.col("_PL")

    # Normalize single-digit GT
    gt = (
        pl.when(gt_raw == "0")
        .then(pl.lit("0/0"))
        .when(gt_raw == "1")
        .then(pl.lit("1/1"))
        .when(gt_raw == "2")
        .then(pl.lit("2/2"))
        .otherwise(gt_raw)
    )

    gq = gq_raw.cast(pl.Float64)
    dp = dp_raw.cast(pl.Float64)

    good = (
        gq.is_not_null()
        & dp.is_not_null()
        & (gq >= sample_gq_min)
        & (dp >= sample_dp_min)
        & (dp <= sample_dp_max)
    )

    # PL validation
    pl_len = pl_field.list.len()
    pl_valid = pl_field.is_not_null() & (pl_len >= 3)

    # Use null_on_oob=True to safely handle short/empty PL lists
    orig_pl_0 = pl_field.list.get(0, null_on_oob=True).cast(pl.Float64)
    orig_pl_1 = pl_field.list.get(1, null_on_oob=True).cast(pl.Float64)
    orig_pl_2 = pl_field.list.get(2, null_on_oob=True).cast(pl.Float64)

    # PL correction conditions
    is_gt_hom_ref = (gt == "0/0") | (gt == "0|0")
    is_pl_all_zero = (
        pl_valid
        & orig_pl_0.is_not_null()
        & (orig_pl_0 == 0)
        & (orig_pl_1 == 0)
        & (orig_pl_2 == 0)
    )
    gq_for_calc = gq.fill_null(0.0)
    gq_sufficient_for_calc = gq.is_not_null() & (gq >= calc_ds_min_gq)
    needs_pl_correction = is_gt_hom_ref & is_pl_all_zero & gq_sufficient_for_calc

    # Corrected PL calculation
    p_wrong = pl.lit(10.0).pow(-gq_for_calc / 10.0)
    discriminant = pl.lit(1.0) + pl.lit(4.0) * p_wrong
    x = (pl.lit(-1.0) + discriminant.sqrt()) / pl.lit(2.0)

    l_het_corr = x
    l_alt_corr = x * x
    l_ref_corr = pl.lit(1.0) - l_het_corr - l_alt_corr

    pl_ref_corr = (
        pl.when(l_ref_corr > 0)
        .then((-pl.lit(10.0) * l_ref_corr.log(base=10)).round(0).clip(0, 255))
        .otherwise(pl.lit(255.0))
    )

    pl_het_corr = (
        pl.when(l_het_corr > 0)
        .then((-pl.lit(10.0) * l_het_corr.log(base=10)).round(0).clip(0, 255))
        .otherwise(pl.lit(255.0))
    )

    pl_alt_corr = (
        pl.when(l_alt_corr > 0)
        .then((-pl.lit(10.0) * l_alt_corr.log(base=10)).round(0).clip(0, 255))
        .otherwise(pl.lit(255.0))
    )

    final_pl_0 = (
        pl.when(needs_pl_correction).then(pl_ref_corr).otherwise(orig_pl_0.fill_null(0))
    )
    final_pl_1 = (
        pl.when(needs_pl_correction).then(pl_het_corr).otherwise(orig_pl_1.fill_null(0))
    )
    final_pl_2 = (
        pl.when(needs_pl_correction).then(pl_alt_corr).otherwise(orig_pl_2.fill_null(0))
    )

    # Calculate DS from final PL
    l_ref = (
        pl.when(final_pl_0 < 255)
        .then(pl.lit(10.0).pow(-final_pl_0 / 10.0))
        .otherwise(pl.lit(0.0))
    )
    l_het = (
        pl.when(final_pl_1 < 255)
        .then(pl.lit(10.0).pow(-final_pl_1 / 10.0))
        .otherwise(pl.lit(0.0))
    )
    l_alt = (
        pl.when(final_pl_2 < 255)
        .then(pl.lit(10.0).pow(-final_pl_2 / 10.0))
        .otherwise(pl.lit(0.0))
    )
    l_sum = l_ref + l_het + l_alt

    dosage_raw = (
        pl.when(l_sum > 0)
        .then((l_het / l_sum) + pl.lit(2.0) * (l_alt / l_sum))
        .otherwise(pl.lit(0.0))
    )
    dosage_cleaned = (
        pl.when((dosage_raw > 0) & (dosage_raw < 0.0001))
        .then(pl.lit(0.0))
        .otherwise(dosage_raw)
    )
    final_ds = (
        pl.when(pl_valid & gq_sufficient_for_calc)
        .then(dosage_cleaned)
        .otherwise(pl.lit(None).cast(pl.Float64))
    )

    final_gt = pl.when(~good).then(pl.lit("./.")).otherwise(gt)

    output_pl = (
        pl.when(pl_valid)
        .then(
            pl.concat_list(
                [
                    final_pl_0.cast(pl.Int32),
                    final_pl_1.cast(pl.Int32),
                    final_pl_2.cast(pl.Int32),
                ]
            )
        )
        .otherwise(pl_field)
    )

    lf_processed = lf_exploded.with_columns(
        [
            final_gt.alias("_GT_out"),
            gq_raw.alias("_GQ_out"),
            dp_raw.alias("_DP_out"),
            output_pl.alias("_PL_out"),
            final_ds.cast(pl.Float32).alias("_DS_out"),
        ]
    )

    # Re-aggregate by row index
    lf_reaggregated = (
        lf_processed.group_by("_row_idx", maintain_order=True)
        .agg(
            [
                *[pl.col(c).first() for c in non_genotype_cols],
                pl.col("_GT_out"),
                pl.col("_GQ_out"),
                pl.col("_DP_out"),
                pl.col("_PL_out"),
                pl.col("_DS_out"),
            ]
        )
        .with_columns(
            pl.struct(
                [
                    pl.col("_GT_out").alias("GT"),
                    pl.col("_GQ_out").alias("GQ"),
                    pl.col("_DP_out").alias("DP"),
                    pl.col("_PL_out").alias("PL"),
                    pl.col("_DS_out").alias("DS"),
                ]
            ).alias("genotypes")
        )
        .select([*non_genotype_cols, "genotypes"])
    )

    return lf_reaggregated


lf = (
    # Sample filtering is done via scan_vcf samples parameter
    pb.scan_vcf(input_vcf, samples=samples)
    .with_columns(
        # Calculate averages directly from the column-oriented schema
        pl.col("genotypes")
        .struct.field("GQ")
        .cast(pl.List(pl.Float64))
        .list.mean()
        .alias("_avg_gq"),
        pl.col("genotypes")
        .struct.field("DP")
        .cast(pl.List(pl.Float64))
        .list.mean()
        .alias("_avg_dp"),
    )
    .filter(
        (pl.col("qual") >= qual_min)
        & (pl.col("_avg_gq") >= avg_gq_min)
        & (pl.col("_avg_dp") >= avg_dp_min)
        & (pl.col("_avg_dp") <= avg_dp_max)
    )
    .drop(["_avg_gq", "_avg_dp"])
)

# Process genotypes via explode/aggregate approach
lf = process_genotypes_via_explode(
    lf,
    sample_gq_min=sample_gq_min,
    sample_dp_min=sample_dp_min,
    sample_dp_max=sample_dp_max,
    calc_ds_min_gq=calc_ds_min_gq,
)

# Set metadata with updated sample names
set_vcf_metadata(lf, sample_names=samples)

pb.sink_vcf(lf, output_vcf)
print(f"Wrote: {output_vcf}")
