#!/usr/bin/env python3
"""Full 12-module drift: max |tool - FastQC golden| across ALL data points per
module, for polars-bio (p1) and RastQC (t1, t4). Also reports thread self-
consistency and structural key mismatches (rows a tool reports that FastQC
doesn't, or vice versa)."""
import csv
import glob
import math
from collections import defaultdict

DISPLAY = {
    "Basic Statistics": "basic_stats",
    "Per base sequence quality": "per_base_quality",
    "Per tile sequence quality": "per_tile_quality",
    "Per sequence quality scores": "per_seq_quality",
    "Per base sequence content": "per_base_content",
    "Per sequence GC content": "per_seq_gc",
    "Per base N content": "per_base_n",
    "Sequence Length Distribution": "seq_length",
    "Sequence Duplication Levels": "dup_levels",
    "Overrepresented sequences": "overrepresented",
    "Adapter Content": "adapter_content",
    "Kmer Content": "kmer_content",
}
ORDER = list(DISPLAY.values())


def _int0(s):
    return int(s.split("-")[0])


def parse_fqc(path):
    """fastqc_data.txt (FastQC or RastQC) -> {module: {key: float}}."""
    out = defaultdict(dict)
    mod = None
    hdr = {}
    for line in open(path):
        line = line.rstrip("\n")
        if line.startswith(">>END_MODULE"):
            mod = None
            continue
        if line.startswith(">>"):
            name = line[2:].rsplit("\t", 1)[0].strip()
            mod = DISPLAY.get(name)
            continue
        if mod is None:
            continue
        if line.startswith("#"):
            if mod == "dup_levels" and line.startswith("#Total Deduplicated"):
                out[mod][("__dedup__",)] = float(line.split("\t")[1])
            else:
                hdr[mod] = line[1:].split("\t")[1:]
            continue
        c = line.split("\t")
        if mod == "basic_stats":
            if c[0] == "Total Sequences":
                out[mod][("total_seq",)] = float(c[1])
            elif c[0] == "%GC":
                out[mod][("gc_floor",)] = float(math.floor(float(c[1])))
        elif mod == "per_base_quality":
            p = _int0(c[0])
            for j, st in enumerate(["mean", "median", "q1", "q3", "p10", "p90"]):
                out[mod][(p, st)] = float(c[j + 1])
        elif mod == "per_tile_quality":
            out[mod][(int(c[0]), _int0(c[1]))] = float(c[2])
        elif mod == "per_seq_quality":
            out[mod][(int(c[0]),)] = float(c[1])
        elif mod == "per_base_content":
            p = _int0(c[0])
            for j, b in enumerate(hdr.get(mod, ["G", "A", "T", "C"])):
                out[mod][(p, b)] = float(c[j + 1])
        elif mod == "per_seq_gc":
            out[mod][(int(float(c[0])),)] = float(c[1])
        elif mod == "per_base_n":
            out[mod][(_int0(c[0]),)] = float(c[1])
        elif mod == "seq_length":
            out[mod][(_int0(c[0]),)] = float(c[1])
        elif mod == "dup_levels":
            out[mod][(c[0],)] = float(c[1])  # % of deduplicated
        elif mod == "overrepresented":
            out[mod][(c[0],)] = float(c[1])  # count
        elif mod == "adapter_content":
            p = _int0(c[0])
            for j, nm in enumerate(hdr.get(mod, [])):
                out[mod][(p, nm)] = float(c[j + 1])
        elif mod == "kmer_content":
            out[mod][(c[0],)] = float(c[3])  # obs/exp max
    return out


def parse_pb(path):
    """polars-bio tidy CSV -> {module: {key: float}} in the same key space."""
    out = defaultdict(dict)
    for r in csv.DictReader(open(path)):
        m, lab, pos, me, val = (
            r["module"],
            r["label"],
            r["position"],
            r["metric"],
            r["value"],
        )
        if val == "" or val is None:
            continue
        v = float(val)
        p = int(float(pos)) if pos not in ("", None) else None
        if m == "basic_stats":
            if me == "n_seq":
                out[m][("total_seq",)] = v
            elif me == "gc_pct":
                out[m][("gc_floor",)] = float(math.floor(v))
        elif m == "per_base_quality":
            out[m][(p, me)] = v
        elif m == "per_tile_quality":
            out[m][(int(lab), p)] = v
        elif m == "per_seq_quality" and me == "count":
            out[m][(p,)] = v
        elif m == "per_base_content":
            out[m][(p, me)] = v
        elif m == "per_seq_gc" and me == "count":
            out[m][(p,)] = v
        elif m == "per_base_n" and me == "pct":
            out[m][(p,)] = v
        elif m == "seq_length" and me == "count":
            out[m][(p,)] = v
        elif m == "dup_levels":
            if me == "total_dedup_pct":
                out[m][("__dedup__",)] = v
            elif me == "pct":
                out[m][(lab,)] = v
        elif m == "overrepresented" and me == "count":
            out[m][(lab,)] = v
        elif m == "adapter_content" and me == "pct":
            out[m][(p, lab)] = v
        elif m == "kmer_content" and me == "obs_exp_max":
            out[m][(lab,)] = v
    return out


def compare(gold, tool, mod):
    g, t = gold.get(mod, {}), tool.get(mod, {})
    shared = set(g) & set(t)
    maxdev = max((abs(g[k] - t[k]) for k in shared), default=float("nan"))
    only_g = len(set(g) - set(t))
    only_t = len(set(t) - set(g))
    return maxdev, only_g, only_t, len(g)


gold = parse_fqc(glob.glob("fqc_gold/*_fastqc/fastqc_data.txt")[0])
rq1 = parse_fqc(glob.glob("rq1/*_fastqc/fastqc_data.txt")[0])
rq4 = parse_fqc(glob.glob("rq4/*_fastqc/fastqc_data.txt")[0])
pb1 = parse_pb("pb1.csv")
pb4 = parse_pb("pb4.csv")


def stable(a, b):
    mods = set(a) | set(b)
    return all(a.get(m) == b.get(m) for m in mods)


print(f"polars-bio p1==p4 : {stable(pb1, pb4)}")
print(f"RastQC    t1==t4  : {stable(rq1, rq4)}\n")


def fmt(md, og, ot):
    if md != md:  # nan
        return "n/a"
    s = f"{md:.4g}"
    if og or ot:
        s += f" [Δkeys +{ot}/-{og}]"
    return s


hdr = f"{'module':<20} | {'#pts':>4} | {'polars-bio':>12} | {'RastQC t=1':>22} | {'RastQC t=4':>22}"
print(hdr)
print("-" * len(hdr))
for m in ORDER:
    pmd, pg, pt, npts = compare(gold, pb1, m)
    r1 = compare(gold, rq1, m)
    r4 = compare(gold, rq4, m)
    print(
        f"{m:<20} | {npts:>4} | {fmt(pmd, pg, pt):>12} | {fmt(r1[0], r1[1], r1[2]):>22} | {fmt(r4[0], r4[1], r4[2]):>22}"
    )
print("\n(value = max |tool - FastQC golden| over all data points in the module;")
print(
    " 0 = bit-exact. [Δkeys +x/-y] = x rows the tool reports that FastQC doesn't, y it omits.)"
)
