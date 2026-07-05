#!/usr/bin/env python3
"""Parse FastQC/RastQC fastqc_data.txt + polars-bio tidy into comparable scalar
metrics and print a drift table vs the FastQC golden."""
import glob
import subprocess
import sys


def parse_fqc(path):
    """Extract comparable scalars from a fastqc_data.txt (FastQC or RastQC)."""
    d = {}
    mod = None
    psq = {}  # per_seq_quality: phred -> count
    pbq1 = None  # per_base_quality position-1 mean
    kmers = []  # (seq, obsexp)
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith(">>END_MODULE"):
                mod = None
                continue
            if line.startswith(">>"):
                mod = line[2:].rsplit("\t", 1)[0].strip()
                continue
            if not mod or line.startswith("#"):
                # capture the dedup header line (starts with #)
                if mod == "Sequence Duplication Levels" and line.startswith(
                    "#Total Deduplicated"
                ):
                    d["dedup_pct"] = float(line.split("\t")[1])
                continue
            c = line.split("\t")
            if mod == "Basic Statistics":
                if line.startswith("Total Sequences"):
                    d["total_seq"] = int(c[1])
                if line.startswith("%GC"):
                    d["gc_pct"] = float(c[1])
            elif mod == "Per sequence quality scores":
                psq[int(c[0])] = float(c[1])
            elif mod == "Per base sequence quality" and pbq1 is None:
                pbq1 = float(c[1])  # first data row Mean
            elif mod == "Kmer Content":
                kmers.append((c[0], float(c[3])))
    if psq:
        mode = max(psq, key=psq.get)
        d["psq_mode"] = mode
        d["psq_mode_frac"] = round(100 * psq[mode] / sum(psq.values()), 1)
    if pbq1 is not None:
        d["pbq_pos1_mean"] = round(pbq1, 3)
    d["kmer_n"] = len(kmers)
    if kmers:
        top = max(kmers, key=lambda x: x[1])
        d["kmer_top"] = f"{top[0]} ({top[1]:.1f})"
    return d


def parse_pb(part):
    import polars as pl

    import polars_bio as pb

    pb.set_option("datafusion.execution.target_partitions", str(part))
    t = pb.fastqc("f.fastq.gz").tidy.collect()
    d = {}
    g = lambda m, me, pos=None: t.filter(
        (pl.col("module") == m)
        & (pl.col("metric") == me)
        & ((pl.col("position") == pos) if pos is not None else True)
    )
    d["total_seq"] = int(g("basic_stats", "n_seq")["value"][0])
    d["gc_pct"] = float(g("basic_stats", "gc_pct")["value"][0])
    psq = g("per_seq_quality", "count")
    if psq.height:
        row = psq.sort("value", descending=True).row(0, named=True)
        d["psq_mode"] = int(row["position"])
        tot = psq["value"].sum()
        d["psq_mode_frac"] = round(100 * row["value"] / tot, 1)
    pbq1 = g("per_base_quality", "mean", 1)
    if pbq1.height:
        d["pbq_pos1_mean"] = round(float(pbq1["value"][0]), 3)
    dd = g("dup_levels", "total_dedup_pct")
    if dd.height:
        d["dedup_pct"] = round(float(dd["value"][0]), 2)
    km = t.filter((pl.col("module") == "kmer_content") & (pl.col("metric") == "count"))
    d["kmer_n"] = km.height
    if km.height:
        oe = t.filter(
            (pl.col("module") == "kmer_content") & (pl.col("metric") == "obs_exp_max")
        )
        j = km.join(oe, on="label", suffix="_oe")
        top = j.sort("value_oe", descending=True).row(0, named=True)
        d["kmer_top"] = f"{top['label']} ({top['value_oe']:.1f})"
    return d


gold = parse_fqc(glob.glob("fqc_gold/*_fastqc/fastqc_data.txt")[0])
rq1 = parse_fqc(glob.glob("rq1/*_fastqc/fastqc_data.txt")[0])
rq4 = parse_fqc(glob.glob("rq4/*_fastqc/fastqc_data.txt")[0])
pb1 = parse_pb(1)
pb4 = parse_pb(4)

pb_stable = pb1 == pb4
print(f"polars-bio p1==p4 : {pb_stable}")
rq_stable = rq1 == rq4
print(f"RastQC t1==t4     : {rq_stable}\n")

rows = [
    ("Total sequences", "total_seq"),
    ("%GC", "gc_pct"),
    ("per_base_quality pos1 mean", "pbq_pos1_mean"),
    ("per_seq_quality mode (phred)", "psq_mode"),
    ("per_seq_quality mode %reads", "psq_mode_frac"),
    ("Total Deduplicated %", "dedup_pct"),
    ("# enriched kmers", "kmer_n"),
    ("top kmer (obs/exp)", "kmer_top"),
]


def cell(v, ref):
    if v is None:
        return "-"
    mark = "" if v == ref else "  ←DRIFT"
    return f"{v}{mark}"


w = 30
print(
    f"{'metric':<{w}} | {'FastQC (golden)':>18} | {'polars-bio':>14} | {'RastQC t=1':>16} | {'RastQC t=4':>16}"
)
print("-" * (w + 74))
for label, key in rows:
    gv = gold.get(key)
    print(
        f"{label:<{w}} | {str(gv):>18} | {cell(pb1.get(key), gv):>14} | "
        f"{cell(rq1.get(key), gv):>16} | {cell(rq4.get(key), gv):>16}"
    )
