#!/usr/bin/env python3
"""Fair per-base comparison: RastQC only reports grouped positions and has no
--nogroup. So re-bin the full-resolution FastQC(--nogroup) and polars-bio
per-position values into RastQC's exact bins, then compare like-for-like across
the whole read. Reports max |tool - FastQC| per per-base module."""
import csv
import glob
from collections import defaultdict

MODS = {
    "Per base sequence quality": ("per_base_quality", 1),  # col idx of Mean
    "Per base sequence content": ("per_base_content", None),
    "Per base N content": ("per_base_n", 1),
}


def ranges_and_vals(path, modname, colidx):
    """From a fastqc_data.txt: RastQC's (lo,hi) bins -> value (Mean col), and for
    per_base_content a dict of (lo,hi)->{base:pct}."""
    mod = None
    hdr = None
    out = {}
    for line in open(path):
        line = line.rstrip("\n")
        if line.startswith(">>END_MODULE"):
            mod = None
            continue
        if line.startswith(">>"):
            mod = line[2:].rsplit("\t", 1)[0].strip()
            continue
        if mod != modname:
            continue
        if line.startswith("#"):
            hdr = line[1:].split("\t")[1:]
            continue
        c = line.split("\t")
        p = c[0]
        lo = int(p.split("-")[0])
        hi = int(p.split("-")[-1])
        if colidx is not None:
            out[(lo, hi)] = float(c[colidx])
        else:  # per_base_content: all bases by header name
            out[(lo, hi)] = {b: float(c[j + 1]) for j, b in enumerate(hdr)}
    return out


def perpos_fqc(path, modname, colidx):
    """FastQC --nogroup per-position values (pos -> value or {base:pct})."""
    return {lo: v for (lo, hi), v in ranges_and_vals(path, modname, colidx).items()}


def perpos_pb(csvpath, module, metric_is_base):
    d = defaultdict(dict) if metric_is_base else {}
    for r in csv.DictReader(open(csvpath)):
        if r["module"] != module or r["value"] == "":
            continue
        pos = int(float(r["position"]))
        if metric_is_base:
            d[pos][r["metric"]] = float(r["value"])
        elif r["metric"] in ("mean", "pct"):
            d[pos] = float(r["value"])
    return d


def rebin(perpos, bins):
    out = {}
    for lo, hi in bins:
        vals = [perpos[p] for p in range(lo, hi + 1) if p in perpos]
        if vals:
            out[(lo, hi)] = sum(vals) / len(vals)
    return out


def rebin_content(perpos, bins, base):
    out = {}
    for lo, hi in bins:
        vals = [
            perpos[p][base]
            for p in range(lo, hi + 1)
            if p in perpos and base in perpos[p]
        ]
        if vals:
            out[(lo, hi)] = sum(vals) / len(vals)
    return out


G = glob.glob("fqc_gold/*_fastqc/fastqc_data.txt")[0]
RQ1 = glob.glob("rq1/*_fastqc/fastqc_data.txt")[0]
RQ4 = glob.glob("rq4/*_fastqc/fastqc_data.txt")[0]

print(
    f"{'per-base module (re-binned to RastQC bins)':<42} | {'polars-bio':>10} | {'RastQC t=1':>10} | {'RastQC t=4':>10}"
)
print("-" * 82)

# --- Per base sequence quality: Mean ---
for disp, (mod, col) in [
    ("Per base sequence quality (Mean)", ("Per base sequence quality", 1)),
    ("Per base N content", ("Per base N content", 1)),
]:
    rq1 = (
        ranges_and_vals(RQ1, disp.split(" (")[0] if "(" in disp else disp, col)
        if False
        else ranges_and_vals(RQ1, mod, col)
    )
    rq4 = ranges_and_vals(RQ4, mod, col)
    bins = list(rq1.keys())
    fq_pp = perpos_fqc(G, mod, col)
    pb_pp = perpos_pb("pb1.csv", MODS[mod][0], metric_is_base=False)
    fq_b = rebin(fq_pp, bins)
    pb_b = rebin(pb_pp, bins)
    md_pb = max(abs(pb_b[k] - fq_b[k]) for k in bins if k in pb_b and k in fq_b)
    md_r1 = max(abs(rq1[k] - fq_b[k]) for k in bins if k in fq_b)
    md_r4 = max(abs(rq4[k] - fq_b[k]) for k in bins if k in fq_b)
    print(f"{disp:<42} | {md_pb:>10.4f} | {md_r1:>10.4f} | {md_r4:>10.4f}")

# --- Per base sequence content: max over bases ---
mod = "Per base sequence content"
rq1 = ranges_and_vals(RQ1, mod, None)
rq4 = ranges_and_vals(RQ4, mod, None)
bins = list(rq1.keys())
fq_pp = perpos_fqc(G, mod, None)
pb_pp = perpos_pb("pb1.csv", "per_base_content", metric_is_base=True)
bases = ["G", "A", "T", "C"]
md_pb = md_r1 = md_r4 = 0.0
for b in bases:
    fq_b = rebin_content(fq_pp, bins, b)
    pb_b = rebin_content(pb_pp, bins, b)
    for k in bins:
        if k in fq_b:
            if k in pb_b:
                md_pb = max(md_pb, abs(pb_b[k] - fq_b[k]))
            if b in rq1[k]:
                md_r1 = max(md_r1, abs(rq1[k][b] - fq_b[k]))
            if b in rq4[k]:
                md_r4 = max(md_r4, abs(rq4[k][b] - fq_b[k]))
print(
    f"{'Per base sequence content (max over GATC)':<42} | {md_pb:>10.4f} | {md_r1:>10.4f} | {md_r4:>10.4f}"
)
print(
    "\n(max |tool - FastQC| in the module's units: phred for quality, % for content/N,"
)
print(
    " after re-binning the high-resolution FastQC/polars-bio data into RastQC's exact bins.)"
)
