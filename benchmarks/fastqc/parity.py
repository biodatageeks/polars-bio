"""Parse FastQC/RastQC outputs into polars-bio's tidy schema and compare.

FastQC (s-andrews) is the correctness oracle. Run references with --nogroup
for exact per-position parity.
"""

import subprocess
import tempfile
from pathlib import Path

import polars as pl

_FASTQC_MODULE = {
    "Basic Statistics": "basic_stats",
    "Per base sequence quality": "per_base_quality",
    "Per sequence GC content": "per_seq_gc",
    "Sequence Duplication Levels": "dup_levels",
    "Per tile sequence quality": "per_tile_quality",
    "Kmer Content": "kmer_content",
}

# Per-metric absolute tolerance (0 == exact match required).
TOLERANCES = {
    ("basic_stats", "n_seq"): 0.0,
    ("basic_stats", "total_bases"): 0.0,
    ("basic_stats", "gc_pct"): 0.5,
    ("per_base_quality", "mean"): 0.1,
    ("per_base_quality", "median"): 0.0,
    ("per_seq_gc", "count"): 0.0,
    ("dup_levels", "pct_dup"): 0.5,
    ("kmer_content", "count"): 0.0,
    ("kmer_content", "max_position"): 0.0,
    ("kmer_content", "obs_exp_max"): 1e-2,  # FastQC prints float32 obs/exp
    ("kmer_content", "pvalue"): 1e-3,
}
DEFAULT_TOL = 0.1


def run_fastqc(fastq: str, outdir: str) -> str:
    """Run FastQC with --nogroup and return the path to fastqc_data.txt."""
    subprocess.run(
        ["fastqc", "--extract", "--nogroup", "-o", outdir, fastq],
        check=True,
        capture_output=True,
    )
    stem = Path(fastq).name
    for suffix in (".fastq.gz", ".fastq", ".fq.gz", ".fq"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    return str(Path(outdir) / f"{stem}_fastqc" / "fastqc_data.txt")


def parse_fastqc_data(path: str) -> pl.DataFrame:
    """Parse fastqc_data.txt >>Module ... >>END_MODULE sections to tidy rows."""
    rows = []
    module = None
    header = None
    for line in Path(path).read_text().splitlines():
        if line.startswith(">>END_MODULE"):
            module, header = None, None
            continue
        if line.startswith(">>"):
            name = line[2:].rsplit("\t", 1)[0].strip()
            module = _FASTQC_MODULE.get(name)
            header = None
            continue
        if module is None:
            continue
        if line.startswith("#"):
            header = line[1:].split("\t")
            continue
        _emit(rows, module, header, line.split("\t"))
    return pl.DataFrame(
        rows,
        schema={
            "module": pl.Utf8,
            "label": pl.Utf8,
            "position": pl.Int32,
            "metric": pl.Utf8,
            "value": pl.Float64,
        },
    )


def _emit(rows, module, header, parts):
    if module == "basic_stats":
        key = {"Total Sequences": "n_seq", "%GC": "gc_pct"}.get(parts[0])
        if key:
            rows.append(
                dict(
                    module=module,
                    label=None,
                    position=None,
                    metric=key,
                    value=float(parts[1]),
                )
            )
    elif module == "per_base_quality":
        pos = int(parts[0].split("-")[0])
        for metric, idx in [
            ("mean", 1),
            ("median", 2),
            ("q1", 3),
            ("q3", 4),
            ("p10", 5),
            ("p90", 6),
        ]:
            rows.append(
                dict(
                    module=module,
                    label=None,
                    position=pos,
                    metric=metric,
                    value=float(parts[idx]),
                )
            )
    elif module == "per_seq_gc":
        rows.append(
            dict(
                module=module,
                label=None,
                position=int(float(parts[0])),
                metric="count",
                value=float(parts[1]),
            )
        )
    elif module == "dup_levels":
        # FastQC 0.12.1: "<level>\t<percentage of total>" (2 columns).
        if parts[0].startswith("#") or len(parts) < 2:
            return
        rows.append(
            dict(
                module=module,
                label=parts[0],
                position=None,
                metric="pct",
                value=float(parts[1]),
            )
        )
    elif module == "per_tile_quality":
        # FastQC 0.12.1: "#Tile\tBase\tMean" (Mean is the deviation from the
        # per-position average of tile means).
        if len(parts) < 3:
            return
        rows.append(
            dict(
                module=module,
                label=parts[0],  # tile as string
                position=int(parts[1]),
                metric="mean",
                value=float(parts[2]),
            )
        )
    elif module == "kmer_content":
        # FastQC 0.12.1: "#Sequence\tCount\tPValue\tObs/Exp Max\tMax Obs/Exp Position"
        if len(parts) < 5:
            return
        kmer = parts[0]
        for metric, idx in [
            ("count", 1),
            ("pvalue", 2),
            ("obs_exp_max", 3),
            ("max_position", 4),
        ]:
            rows.append(
                dict(
                    module=module,
                    label=kmer,
                    position=None,
                    metric=metric,
                    value=float(parts[idx]),
                )
            )


def pb_tidy(fastq: str, modules) -> pl.DataFrame:
    import polars_bio as pb

    return (
        pb.fastqc(fastq, modules=modules)
        .tidy.collect()
        .select("module", "label", "position", "metric", "value")
    )


def parity_report(pb_df: pl.DataFrame, ref_df: pl.DataFrame) -> pl.DataFrame:
    keys = ["module", "label", "position", "metric"]
    # join_nulls=True: label/position are null for scalar & positional metrics;
    # without it every such row would silently drop (null != null in joins),
    # yielding a false "0 mismatches" pass.
    joined = pb_df.join(ref_df, on=keys, how="inner", suffix="_ref", nulls_equal=True)

    def verdict(row):
        tol = TOLERANCES.get((row["module"], row["metric"]), DEFAULT_TOL)
        diff = abs((row["value"] or 0.0) - (row["value_ref"] or 0.0))
        return "exact" if diff == 0 else ("within_tol" if diff <= tol else "mismatch")

    joined = joined.with_columns(
        pl.struct(["module", "metric", "value", "value_ref"])
        .map_elements(verdict, return_dtype=pl.Utf8)
        .alias("verdict")
    )
    return joined.group_by("module", "verdict").len().sort("module", "verdict")


if __name__ == "__main__":
    import sys

    fastq = sys.argv[1]
    with tempfile.TemporaryDirectory() as d:
        ref = parse_fastqc_data(run_fastqc(fastq, d))
        got = pb_tidy(fastq, None)
        print(parity_report(got, ref))
