"""Approach A: Process VCF genotypes using Polars list ops + map_batches (no explode)."""

from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

import polars_bio as pb
from polars_bio._metadata import set_vcf_metadata

DATA_DIR = Path("/Users/mwiewior/research/data/polars-bio")
# input_vcf = str(DATA_DIR / "20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr_12_22_X.recalibrated_variants.exome.vcf.gz")
input_vcf = str(
    DATA_DIR
    / "20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr_12_22_X.recalibrated_variants.exome.head_100.vcf.gz"
)
output_vcf = "/private/tmp/output_polars_list.vcf.gz"

# Thresholds
qual_min = 20
avg_gq_min = 15
avg_dp_min = 15
avg_dp_max = 150
sample_gq_min = 10
sample_dp_min = 10
sample_dp_max = 200
calc_ds_min_gq = 10

samples = [
    s.strip()
    for s in (DATA_DIR / "samples_rand_2000.txt").read_text().splitlines()
    if s.strip()
]

pb.set_option("datafusion.execution.target_partitions", "1")


def process_genotypes_arrow(struct_series: pl.Series) -> pl.Series:
    """Process genotypes via PyArrow compute on struct of lists.

    Input struct fields: _gt_norm (List<Utf8>), _GQ (List<Int32>),
                         _DP (List<Int32>), _pl0/_pl1/_pl2 (List<Float64>)
    Returns struct with GT, GQ, DP, PL (List<List<Int32>>), DS (List<Float32>)
    """
    import math

    arrow_struct = struct_series.to_arrow()
    n = len(arrow_struct)

    gt_col = arrow_struct.field("_gt_norm")
    gq_col = arrow_struct.field("_GQ")
    dp_col = arrow_struct.field("_DP")
    pl0_col = arrow_struct.field("_pl0")
    pl1_col = arrow_struct.field("_pl1")
    pl2_col = arrow_struct.field("_pl2")

    # Build output lists
    gt_out_offsets = [0]
    gt_out_values = []
    gq_out_offsets = [0]
    gq_out_values = []
    dp_out_offsets = [0]
    dp_out_values = []
    pl_outer_offsets = [0]
    pl_inner_offsets = [0]
    pl_values = []
    pl_sample_valid = []  # track per-sample PL validity for null passthrough
    ds_out_offsets = [0]
    ds_out_values = []
    ds_out_valid = []

    for row in range(n):
        gt_list = gt_col[row].as_py()
        gq_list = gq_col[row].as_py()
        dp_list = dp_col[row].as_py()
        pl0_list = pl0_col[row].as_py()
        pl1_list = pl1_col[row].as_py()
        pl2_list = pl2_col[row].as_py()

        if gt_list is None:
            gt_out_offsets.append(gt_out_offsets[-1])
            gq_out_offsets.append(gq_out_offsets[-1])
            dp_out_offsets.append(dp_out_offsets[-1])
            pl_outer_offsets.append(pl_outer_offsets[-1])
            ds_out_offsets.append(ds_out_offsets[-1])
            continue

        nsamples = len(gt_list)
        for s in range(nsamples):
            gt_raw = gt_list[s]
            gq_val = gq_list[s] if gq_list else None
            dp_val = dp_list[s] if dp_list else None
            p0 = pl0_list[s] if pl0_list else None
            p1 = pl1_list[s] if pl1_list else None
            p2 = pl2_list[s] if pl2_list else None

            pl_valid = p0 is not None and p1 is not None and p2 is not None

            # Quality mask
            good = (
                gq_val is not None
                and dp_val is not None
                and gq_val >= sample_gq_min
                and dp_val >= sample_dp_min
                and dp_val <= sample_dp_max
            )

            # GT masking
            gt_final = gt_raw if good else "./."

            # PL correction
            is_hom_ref = gt_raw in ("0/0", "0|0")
            gq_sufficient = gq_val is not None and gq_val >= calc_ds_min_gq
            is_pl_all_zero = pl_valid and p0 == 0 and p1 == 0 and p2 == 0
            needs_correction = is_hom_ref and is_pl_all_zero and gq_sufficient

            if needs_correction:
                gq_f = float(gq_val or 0)
                p_wrong = 10.0 ** (-gq_f / 10.0)
                discriminant = 1.0 + 4.0 * p_wrong
                x = (-1.0 + math.sqrt(discriminant)) / 2.0
                l_het = x
                l_alt = x * x
                l_ref = 1.0 - l_het - l_alt
                fp0 = round(-10.0 * math.log10(l_ref), 0) if l_ref > 0 else 255.0
                fp1 = round(-10.0 * math.log10(l_het), 0) if l_het > 0 else 255.0
                fp2 = round(-10.0 * math.log10(l_alt), 0) if l_alt > 0 else 255.0
                fp0 = max(0, min(255, fp0))
                fp1 = max(0, min(255, fp1))
                fp2 = max(0, min(255, fp2))
            else:
                fp0 = p0 if p0 is not None else 0.0
                fp1 = p1 if p1 is not None else 0.0
                fp2 = p2 if p2 is not None else 0.0

            # DS
            if pl_valid and gq_sufficient:
                lr = 10.0 ** (-fp0 / 10.0) if fp0 < 255 else 0.0
                lh = 10.0 ** (-fp1 / 10.0) if fp1 < 255 else 0.0
                la = 10.0 ** (-fp2 / 10.0) if fp2 < 255 else 0.0
                ls = lr + lh + la
                if ls > 0:
                    dosage = (lh / ls) + 2.0 * (la / ls)
                    if 0 < dosage < 0.0001:
                        dosage = 0.0
                    ds_val = dosage
                else:
                    ds_val = 0.0
                ds_out_values.append(ds_val)
                ds_out_valid.append(True)
            else:
                ds_out_values.append(0.0)
                ds_out_valid.append(False)

            gt_out_values.append(gt_final)
            gq_out_values.append(gq_val)
            dp_out_values.append(dp_val)

            # PL triplet (null when original PL was null/invalid)
            if pl_valid:
                pl_values.extend([int(fp0), int(fp1), int(fp2)])
                pl_inner_offsets.append(pl_inner_offsets[-1] + 3)
                pl_sample_valid.append(True)
            else:
                # Null inner list: offset stays same, mark invalid
                pl_inner_offsets.append(pl_inner_offsets[-1])
                pl_sample_valid.append(False)

        gt_out_offsets.append(gt_out_offsets[-1] + nsamples)
        gq_out_offsets.append(gq_out_offsets[-1] + nsamples)
        dp_out_offsets.append(dp_out_offsets[-1] + nsamples)
        pl_outer_offsets.append(pl_outer_offsets[-1] + nsamples)
        ds_out_offsets.append(ds_out_offsets[-1] + nsamples)

    # Build Arrow arrays
    gt_arrow = pa.ListArray.from_arrays(
        pa.array(gt_out_offsets, type=pa.int32()),
        pa.array(gt_out_values, type=pa.utf8()),
    )
    gq_arrow = pa.ListArray.from_arrays(
        pa.array(gq_out_offsets, type=pa.int32()),
        pa.array(gq_out_values, type=pa.int32()),
    )
    dp_arrow = pa.ListArray.from_arrays(
        pa.array(dp_out_offsets, type=pa.int32()),
        pa.array(dp_out_values, type=pa.int32()),
    )
    pl_inner_validity = pa.array(pl_sample_valid, type=pa.bool_())
    pl_inner_mask = pc.invert(pl_inner_validity)  # True where null
    pl_inner = pa.ListArray.from_arrays(
        pa.array(pl_inner_offsets, type=pa.int32()),
        pa.array(pl_values, type=pa.int32()),
        mask=pl_inner_mask,
    )
    pl_arrow = pa.ListArray.from_arrays(
        pa.array(pl_outer_offsets, type=pa.int32()),
        pl_inner,
    )
    ds_validity = pa.array(ds_out_valid, type=pa.bool_())
    ds_vals = pa.array(ds_out_values, type=pa.float32())
    ds_masked = pc.if_else(ds_validity, ds_vals, pa.scalar(None, type=pa.float32()))
    ds_arrow = pa.ListArray.from_arrays(
        pa.array(ds_out_offsets, type=pa.int32()),
        ds_masked,
    )

    result_struct = pa.StructArray.from_arrays(
        [gt_arrow, gq_arrow, dp_arrow, pl_arrow, ds_arrow],
        names=["GT", "GQ", "DP", "PL", "DSG"],
    )
    return pl.Series("genotypes", result_struct)


def process_genotypes_polars_list(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Process genotypes using list.eval for normalization, map_batches for cross-list ops."""
    # Extract fields from genotypes struct
    lf = lf.with_columns(
        pl.col("genotypes").struct.field("GT").alias("_GT_raw"),
        pl.col("genotypes").struct.field("GQ").alias("_GQ"),
        pl.col("genotypes").struct.field("DP").alias("_DP"),
        pl.col("genotypes").struct.field("PL").alias("_PL_raw"),
    ).drop("genotypes")

    # GT normalization via list.eval
    lf = lf.with_columns(
        pl.col("_GT_raw")
        .list.eval(
            pl.when(pl.element() == "0")
            .then(pl.lit("0/0"))
            .when(pl.element() == "1")
            .then(pl.lit("1/1"))
            .when(pl.element() == "2")
            .then(pl.lit("2/2"))
            .otherwise(pl.element())
        )
        .alias("_gt_norm")
    )

    # PL decomposition: extract pl0, pl1, pl2 from nested List<List<Int32>>
    lf = lf.with_columns(
        pl.col("_PL_raw")
        .list.eval(pl.element().list.get(0, null_on_oob=True).cast(pl.Float64))
        .alias("_pl0"),
        pl.col("_PL_raw")
        .list.eval(pl.element().list.get(1, null_on_oob=True).cast(pl.Float64))
        .alias("_pl1"),
        pl.col("_PL_raw")
        .list.eval(pl.element().list.get(2, null_on_oob=True).cast(pl.Float64))
        .alias("_pl2"),
    )

    # Single map_batches for cross-list operations
    non_genotype_cols = [
        c
        for c in lf.collect_schema().names()
        if c
        not in ("_GT_raw", "_GQ", "_DP", "_PL_raw", "_gt_norm", "_pl0", "_pl1", "_pl2")
    ]

    lf = lf.with_columns(
        pl.struct(["_gt_norm", "_GQ", "_DP", "_pl0", "_pl1", "_pl2"])
        .map_batches(
            process_genotypes_arrow,
            return_dtype=pl.Struct(
                {
                    "GT": pl.List(pl.Utf8),
                    "GQ": pl.List(pl.Int32),
                    "DP": pl.List(pl.Int32),
                    "PL": pl.List(pl.List(pl.Int32)),
                    "DSG": pl.List(pl.Float32),
                }
            ),
        )
        .alias("genotypes")
    ).select([*non_genotype_cols, "genotypes"])

    return lf


# Main pipeline
lf = (
    pb.scan_vcf(input_vcf, format_fields=["GT", "GQ", "DP", "PL"], samples=samples)
    .with_columns(
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

lf = process_genotypes_polars_list(lf)
set_vcf_metadata(lf, sample_names=samples)
pb.sink_vcf(lf, output_vcf)
print(f"Wrote: {output_vcf}")
