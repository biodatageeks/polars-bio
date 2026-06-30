#!/usr/bin/env python
"""
GFF/GTF predicate & projection pushdown regression harness.

Validates polars_bio's `scan_gtf`/`scan_gff` pushdown against a real GENCODE
annotation file, three ways, for every (predicate, projection) case:

  A = polars_bio, pushdown ON   (predicate_pushdown=projection_pushdown=True)
  B = polars_bio, pushdown OFF  (both False)  -> client-side filter/select only
  C = oxbow ground truth        (independent reader: read-all, filter in Polars)

A case PASSES iff A == C and B == C as row sets (row order is also checked).
This is the core correctness contract of the pushdown work (issue #396 / PR
#407): pushdown is a pure optimization layered on top of a client-side filter
that is the *source of truth*, so a translation bug may cost performance but
must never change results. Comparing against oxbow (a separate Rust/Arrow
GFF/GTF reader) guards against a bug that is wrong in the *same* way on both
the ON and OFF polars_bio paths.

It also times A vs B so you can see the optimization actually firing: a
selective predicate should be markedly faster with pushdown on.

Usage:
    python bench_pushdown.py [gtf] [gff3]          # default: gtf
    POLARS_BIO_GENCODE_DIR=/data python bench_pushdown.py gtf gff3

Exit code is non-zero if any case fails, so this doubles as a CI check.
Data files are downloaded once into the data dir if absent.
"""

import os
import subprocess
import sys
import time
from pathlib import Path

import oxbow
import polars as pl

import polars_bio as pb

# GENCODE v50 (human). Pin the release so the row counts/results are stable.
RELEASE = "release_50"
BASE_URL = f"https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/{RELEASE}"
FILES = {
    "gtf": "gencode.v50.annotation.gtf.gz",
    "gff3": "gencode.v50.annotation.gff3.gz",
}

DATA_DIR = Path(
    os.environ.get(
        "POLARS_BIO_GENCODE_DIR",
        Path.home() / ".cache" / "polars-bio-gencode",
    )
).expanduser()

ATTRS = ["gene_id", "gene_type", "gene_name"]
CORE = ["chrom", "source", "type", "start", "end", "score", "strand", "phase"]


def ensure_file(kind: str) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / FILES[kind]
    if path.exists() and path.stat().st_size > 0:
        return path
    url = f"{BASE_URL}/{FILES[kind]}"
    print(f"downloading {url} -> {path} ...", flush=True)
    # curl is universally available on CI runners and is resumable.
    subprocess.run(["curl", "-fsSL", "-o", str(path), url], check=True)
    return path


def load_oxbow(kind: str) -> pl.DataFrame:
    """Independent ground truth, normalized to polars_bio's column names/types."""
    src = str(ensure_file(kind))
    defs = [(a, "String") for a in ATTRS]
    reader = oxbow.from_gtf if kind == "gtf" else oxbow.from_gff
    gf = reader(src, compression="gzip", attribute_defs=defs)
    df = gf.pl().rename({"seqid": "chrom", "frame": "phase"})
    df = df.unnest("attributes").select(CORE + ATTRS)
    return df.with_columns(
        pl.col("start").cast(pl.UInt32),
        pl.col("end").cast(pl.UInt32),
        pl.col("score").cast(pl.Float32),
        pl.col("phase").cast(pl.UInt32),
    )


def scan(kind: str, attr_fields, *, pushdown: bool):
    src = str(ensure_file(kind))
    fn = pb.scan_gtf if kind == "gtf" else pb.scan_gff
    return fn(
        src,
        attr_fields=attr_fields,
        predicate_pushdown=pushdown,
        projection_pushdown=pushdown,
    )


def run_case(kind, gt, name, pred, proj, attr_fields) -> bool:
    proj = proj or (CORE + (attr_fields or []))
    c = gt.filter(pred).select(proj) if pred is not None else gt.select(proj)

    la = scan(kind, attr_fields, pushdown=True)
    if pred is not None:
        la = la.filter(pred)
    t = time.perf_counter()
    a = la.select(proj).collect()
    ta = time.perf_counter() - t

    lb = scan(kind, attr_fields, pushdown=False)
    if pred is not None:
        lb = lb.filter(pred)
    t = time.perf_counter()
    b = lb.select(proj).collect()
    tb = time.perf_counter() - t

    # Compare by OUTPUT column names (proj may alias/compute, e.g. start+1->s1),
    # so we sort each already-projected frame by its own columns rather than
    # re-applying proj. Order-independent for the set check; order also verified.
    cols = c.columns

    def srt(df):
        return df.select(cols).sort(cols, nulls_last=True)

    c_n = srt(c)
    ac = srt(a).equals(c_n)
    bc = srt(b).equals(c_n)
    order_ok = a.select(cols).equals(c.select(cols)) if ac else None
    ok = ac and bc
    speed = (tb / ta) if ta > 0 else float("nan")
    print(
        f"[{'PASS' if ok else 'FAIL'}] {kind} {name:42s} "
        f"rows={a.height:>9,} A=C:{ac} B=C:{bc} order:{order_ok} "
        f"| ON {ta:6.2f}s OFF {tb:6.2f}s ({speed:4.1f}x)",
        flush=True,
    )
    if not ok:
        print(f"        A.h={a.height} B.h={b.height} C.h={c.height}", flush=True)
    return ok


def cases():
    """Each case exercises a distinct translator branch or a known failure mode.

    Keep the client-side rows (str.contains, string ordering, is_in([...,None]))
    and the #396 attribute cases: those are exactly the predicates the translator
    must DECLINE to push and reapply client-side, and they are the ones most
    likely to silently regress.
    """
    c = pl.col
    return [
        ("chrom == chr7", c("chrom") == "chr7", None, None),
        ("type == gene", c("type") == "gene", None, None),
        ("start >= 1_000_000", c("start") >= 1_000_000, None, None),
        (
            "chrom==chr1 & type==gene",
            (c("chrom") == "chr1") & (c("type") == "gene"),
            None,
            None,
        ),
        (
            "chrom.is_in([chr1,chrX,chrM])",
            c("chrom").is_in(["chr1", "chrX", "chrM"]),
            None,
            None,
        ),
        (
            "exon & start>50k & end<200k",
            (c("type") == "exon") & (c("start") > 50000) & (c("end") < 200000),
            None,
            None,
        ),
        (
            "type==gene | type==transcript",
            (c("type") == "gene") | (c("type") == "transcript"),
            None,
            None,
        ),
        ("NOT type==exon", ~(c("type") == "exon"), None, None),
        (
            "chrom.str.contains(chr1) [client]",
            c("chrom").str.contains("chr1"),
            None,
            None,
        ),
        ("chrom < chr2 [client ordering]", c("chrom") < "chr2", None, None),
        (
            "is_in([chr1,None]) [null-safe]",
            c("chrom").is_in(["chr1", None]),
            None,
            None,
        ),
        ("score > nan [non-finite]", c("score") > float("nan"), None, None),
        ("proj-only [chrom,start,end]", None, ["chrom", "start", "end"], None),
        ("proj alias chrom->c", None, [c("chrom").alias("c")], None),
        ("proj computed (start+1)->s1", None, [(c("start") + 1).alias("s1")], None),
        (
            "attr gene_type==protein_coding",
            c("gene_type") == "protein_coding",
            ["chrom", "start", "end", "type", "gene_id", "gene_type"],
            ATTRS,
        ),
        (
            "#396 gene_name.contains(MIR)->gene_id",
            c("gene_name").str.contains("MIR"),
            ["chrom", "start", "end", "gene_id", "gene_name"],
            ATTRS,
        ),
        ("aliased attr gene_id->g", None, [c("gene_id").alias("g")], ATTRS),
    ]


def main() -> int:
    kinds = [k for k in sys.argv[1:] if not k.startswith("-")] or ["gtf"]
    total_fail = 0
    for kind in kinds:
        print(f"\n===== {kind.upper()} =====", flush=True)
        t = time.perf_counter()
        gt = load_oxbow(kind)
        print(
            f"oxbow ground truth: {gt.height:,} rows in {time.perf_counter()-t:.1f}s",
            flush=True,
        )
        cs = cases()
        npass = sum(run_case(kind, gt, *case) for case in cs)
        total_fail += len(cs) - npass
        print(f"--- {kind}: {npass}/{len(cs)} cases PASS ---", flush=True)
    print(f"\n==== {'ALL PASS' if total_fail == 0 else f'{total_fail} FAILED'} ====")
    return 1 if total_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
