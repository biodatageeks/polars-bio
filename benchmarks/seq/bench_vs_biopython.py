"""polars-bio `seq` operations against Biopython: correctness first, then speed.

Every benchmark first checks that polars-bio and Biopython produce the same
result on the same input, and only then reports timings. A mismatch stops the
run. Biopython runs single-threaded in Python loops; polars-bio runs as one
lazy query (Polars streaming engine) at 1 thread and at all threads.

Two kinds of benchmark:
- operation: the data is loaded into memory first (Python lists for
  Biopython, a Polars DataFrame for polars-bio), so only the operation is
  timed;
- end to end: both sides read the same gzip FASTQ file and filter it.

    python benchmarks/seq/bench_vs_biopython.py \\
        --cds  Homo_sapiens.GRCh38.cds.all.fa.gz \\
        --fastq ERR5897746_1.fastq.gz \\
        [--cds-limit 50000] [--reads-limit 1000000] [--repeats 3]

The 1-thread polars-bio timings come from a child process started with
POLARS_MAX_THREADS=1, because Polars reads it once, at import.
"""

import argparse
import gzip
import json
import math
import os
import platform
import subprocess
import sys
import time


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--cds", required=True, help="gzip FASTA of coding sequences")
    p.add_argument("--fastq", required=True, help="gzip FASTQ of reads")
    p.add_argument("--cds-limit", type=int, default=50_000)
    p.add_argument("--reads-limit", type=int, default=1_000_000)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--polars-only", action="store_true", help=argparse.SUPPRESS)
    return p.parse_args(argv)


def best_of(fn, repeats):
    """(best wall seconds, last result)."""
    best, result = math.inf, None
    for _ in range(repeats):
        t = time.perf_counter()
        result = fn()
        best = min(best, time.perf_counter() - t)
    return best, result


# --- inputs --------------------------------------------------------------------


def read_fasta(path, limit):
    from Bio.SeqIO.FastaIO import SimpleFastaParser

    out = []
    with gzip.open(path, "rt") as fh:
        for _, seq in SimpleFastaParser(fh):
            out.append(seq)
            if len(out) == limit:
                break
    return out


def read_fastq(path, limit):
    from Bio.SeqIO.QualityIO import FastqGeneralIterator

    seqs, quals = [], []
    with gzip.open(path, "rt") as fh:
        for _, seq, qual in FastqGeneralIterator(fh):
            seqs.append(seq)
            quals.append(qual)
            if len(seqs) == limit:
                break
    return seqs, quals


# --- Biopython implementations ----------------------------------------------------

CDS_CHECKS = {
    "First codon": "no_start_codon",
    "Sequence length": "length_not_multiple_of_3",
    "Final codon": "no_stop_codon",
    "Extra in frame stop codon": "internal_stop_codon",
}


def bio_cds(seqs):
    """Seq.translate(cds=True) per sequence: (proteins, statuses)."""
    from Bio.Data.CodonTable import TranslationError
    from Bio.Seq import Seq

    proteins, statuses = [], []
    for s in seqs:
        try:
            proteins.append(str(Seq(s).translate(cds=True)))
            statuses.append(None)
        except TranslationError as e:
            message = str(e)
            reason = next(
                (v for k, v in CDS_CHECKS.items() if message.startswith(k)), None
            )
            statuses.append(reason or "invalid_codon")
    return proteins, statuses


def bio_translate(seqs):
    from Bio.Seq import Seq

    out = []
    for s in seqs:
        x = Seq(s)
        out.append(str(x[: len(x) // 3 * 3].translate()))
    return out


def bio_orfs(seqs, min_len):
    """Complete ATG ORFs on both strands, after the Biopython tutorial recipe."""
    from Bio.Seq import Seq

    found = []
    for seq in seqs:
        n, rows = len(seq), []
        for strand, nuc in ((1, Seq(seq)), (-1, Seq(seq).reverse_complement())):
            for frame in range(3):
                length = 3 * ((n - frame) // 3)
                protein = str(nuc[frame : frame + length].translate())
                aa_start = 0
                for segment in protein.split("*")[:-1]:
                    m = segment.find("M")
                    if m != -1 and len(segment) - m >= min_len:
                        start = frame + 3 * (aa_start + m)
                        end = frame + 3 * (aa_start + len(segment) + 1)
                        if strand == -1:
                            start, end = n - end, n - start
                        rows.append((start, end, strand))
                    aa_start += len(segment) + 1
        found.append(sorted(rows))
    return found


def bio_motif(seqs, pattern):
    from Bio.SeqUtils import nt_search

    return [nt_search(s, pattern)[1:] for s in seqs]


def bio_weight(seqs):
    from Bio.SeqUtils import molecular_weight

    out = []
    for s in seqs:
        try:
            out.append(molecular_weight(s))
        except ValueError:
            out.append(None)
    return out


def bio_gc_skew(seqs, window):
    from Bio.SeqUtils import GC_skew

    return [GC_skew(s, window) for s in seqs]


def bio_transcribe(seqs):
    from Bio.Seq import Seq

    return [str(Seq(s).transcribe()) for s in seqs]


def bio_expected_errors(quals):
    table = [10 ** (-q / 10) for q in range(94)]
    return [sum(table[ord(c) - 33] for c in q) for q in quals]


def bio_maxee_file_seqio(path, maxee):
    """The usual Biopython loop: SeqIO.parse with per-base Phred lists."""
    from Bio import SeqIO

    kept = 0
    with gzip.open(path, "rt") as fh:
        for rec in SeqIO.parse(fh, "fastq"):
            ee = sum(10 ** (-q / 10) for q in rec.letter_annotations["phred_quality"])
            kept += ee <= maxee
    return kept


def bio_maxee_file_fast(path, maxee):
    """The fastest Biopython loop: FastqGeneralIterator and a lookup table."""
    from Bio.SeqIO.QualityIO import FastqGeneralIterator

    table = [10 ** (-q / 10) for q in range(94)]
    kept = 0
    with gzip.open(path, "rt") as fh:
        for _, _, qual in FastqGeneralIterator(fh):
            kept += sum(table[ord(c) - 33] for c in qual) <= maxee
    return kept


# --- polars-bio ------------------------------------------------------------------


def polars_benchmarks(args, cds, seqs, quals):
    """name -> (best seconds, result) for every polars-bio benchmark."""
    import polars as pl

    import polars_bio as pb

    pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)
    cds_df = pl.DataFrame({"s": cds})
    reads = pl.DataFrame({"s": seqs, "q": quals})
    s, q = pl.col("s"), pl.col("q")

    def run(df, *exprs):
        return lambda: df.lazy().select(*exprs).collect(engine="streaming")

    jobs = {
        "translate_cds": run(
            cds_df,
            s.seq.cds_status().alias("status"),
            pl.when(s.seq.cds_status().is_null())
            .then(s)
            .seq.translate(cds=True, ambiguous="biopython")
            .alias("protein"),
        ),
        "translate": run(cds_df, s.seq.translate(ambiguous="biopython")),
        "orfs": run(
            cds_df.head(args.orf_limit), s.seq.orfs(min_len=30, start_codons="ATG")
        ),
        "find_motif": run(reads, s.seq.find_motif("GANTC")),
        "molecular_weight": run(reads, s.seq.molecular_weight()),
        "gc_skew": run(cds_df, s.seq.gc_skew(100)),
        "transcribe": run(reads, s.seq.transcribe()),
        "expected_errors": run(reads, q.seq.expected_errors()),
        "maxee_file": lambda: pb.scan_fastq(args.fastq)
        .filter(pl.col("quality_scores").seq.expected_errors() <= 1.0)
        .select(pl.len())
        .collect(engine="streaming")
        .item(),
    }
    return {name: best_of(fn, args.repeats) for name, fn in jobs.items()}


# --- checks ----------------------------------------------------------------------


def check(name, ok, detail=""):
    if not ok:
        sys.exit(f"MISMATCH in {name}: {detail}")
    print(f"  ok  {name}", file=sys.stderr)


def compare(results, bio, args):
    """Assert that every polars-bio result equals Biopython's."""
    proteins, statuses = bio["translate_cds"]
    df = results["translate_cds"][1]
    got_status = df["status"].to_list()
    # The kernels translate a codon with an unknown byte to X where Biopython
    # raises; Ensembl CDSs contain none, which the check below confirms.
    check("translate_cds status", got_status == statuses, "statuses differ")
    check(
        "translate_cds protein",
        df["protein"].drop_nulls().to_list() == proteins,
        "proteins differ",
    )
    check("translate", results["translate"][1]["s"].to_list() == bio["translate"])
    got_orfs = [
        sorted((o["start"], o["end"], 1 if o["strand"] == "+" else -1) for o in row)
        for row in results["orfs"][1]["s"].to_list()
    ]
    check("orfs", got_orfs == bio["orfs"], "ORFs differ")
    check("find_motif", results["find_motif"][1]["s"].to_list() == bio["find_motif"])
    got_w = results["molecular_weight"][1]["s"].to_list()
    check(
        "molecular_weight",
        all(
            (g is None and w is None)
            or (g is not None and w is not None and math.isclose(g, w, rel_tol=1e-12))
            for g, w in zip(got_w, bio["molecular_weight"])
        ),
    )
    check("gc_skew", results["gc_skew"][1]["s"].to_list() == bio["gc_skew"])
    check("transcribe", results["transcribe"][1]["s"].to_list() == bio["transcribe"])
    got_ee = results["expected_errors"][1]["q"].to_list()
    check(
        "expected_errors",
        all(
            math.isclose(g, w, rel_tol=1e-9, abs_tol=1e-12)
            for g, w in zip(got_ee, bio["expected_errors"])
        ),
    )
    check(
        "maxee_file",
        results["maxee_file"][1] == bio["maxee_file_fast"] == bio["maxee_file_seqio"],
        f"{results['maxee_file'][1]} vs {bio['maxee_file_fast']} vs {bio['maxee_file_seqio']}",
    )


ROWS = [
    ("translate_cds", "CDS translate(cds=True) + cds_status", "cds", ["translate_cds"]),
    ("translate", "translate, B/Z/J letters", "cds", ["translate"]),
    ("orfs", "ORFs, both strands, min 30 aa", "orf", ["orfs"]),
    ("find_motif", "IUPAC motif GANTC (nt_search)", "reads", ["find_motif"]),
    ("molecular_weight", "molecular_weight (DNA)", "reads", ["molecular_weight"]),
    ("gc_skew", "GC_skew, window 100", "cds", ["gc_skew"]),
    ("transcribe", "transcribe", "reads", ["transcribe"]),
    ("expected_errors", "expected errors per read", "reads", ["expected_errors"]),
    (
        "maxee_file",
        "FASTQ.gz → EE ≤ 1 filter (end to end)",
        "file",
        ["maxee_file_seqio", "maxee_file_fast"],
    ),
]


def main():
    args = parse_args()
    args.orf_limit = max(1, args.cds_limit // 5)
    cds = read_fasta(args.cds, args.cds_limit)
    seqs, quals = read_fastq(args.fastq, args.reads_limit)

    if args.polars_only:
        results = polars_benchmarks(args, cds, seqs, quals)
        print(json.dumps({k: v[0] for k, v in results.items()}))
        return

    print("polars-bio (all threads) ...", file=sys.stderr)
    results = polars_benchmarks(args, cds, seqs, quals)
    print("polars-bio (1 thread, child process) ...", file=sys.stderr)
    child = subprocess.run(
        [sys.executable, *sys.argv, "--polars-only"],
        env={**os.environ, "POLARS_MAX_THREADS": "1"},
        capture_output=True,
        text=True,
        check=True,
    )
    one_thread = json.loads(child.stdout.strip().splitlines()[-1])

    print("Biopython ...", file=sys.stderr)
    bio_jobs = {
        "translate_cds": lambda: bio_cds(cds),
        "translate": lambda: bio_translate(cds),
        "orfs": lambda: bio_orfs(cds[: args.orf_limit], 30),
        "find_motif": lambda: bio_motif(seqs, "GANTC"),
        "molecular_weight": lambda: bio_weight(seqs),
        "gc_skew": lambda: bio_gc_skew(cds, 100),
        "transcribe": lambda: bio_transcribe(seqs),
        "expected_errors": lambda: bio_expected_errors(quals),
        "maxee_file_seqio": lambda: bio_maxee_file_seqio(args.fastq, 1.0),
        "maxee_file_fast": lambda: bio_maxee_file_fast(args.fastq, 1.0),
    }
    bio_times, bio = {}, {}
    for name, fn in bio_jobs.items():
        bio_times[name], bio[name] = best_of(
            fn, 1 if name.startswith("maxee") else args.repeats
        )
        print(f"  {name}: {bio_times[name]:.2f} s", file=sys.stderr)

    print("Checking results ...", file=sys.stderr)
    compare(results, bio, args)

    import Bio
    import polars as pl

    import polars_bio as pb

    sizes = {
        "cds": f"{len(cds):,} CDS, {sum(map(len, cds)) / 1e6:.0f} Mbp",
        "orf": f"{args.orf_limit:,} CDS, {sum(map(len, cds[: args.orf_limit])) / 1e6:.0f} Mbp",
        "reads": f"{len(seqs):,} reads × {sum(map(len, seqs)) / len(seqs):.0f} bp",
        "file": os.path.basename(args.fastq),
    }
    print(
        f"\npolars-bio {pb.__version__}, Polars {pl.__version__}, Biopython {Bio.__version__}, "
        f"Python {platform.python_version()}, {platform.machine()}, {os.cpu_count()} cores. "
        f"Best of {args.repeats} (end-to-end Biopython: 1 run). All outputs checked equal.\n"
    )
    print(
        "| Operation | Input | Biopython | polars-bio 1 thread | polars-bio all threads | Speed-up (1 thread / all) |"
    )
    print("|---|---|---:|---:|---:|---:|")
    for key, label, size, bio_keys in ROWS:
        for bio_key in bio_keys:
            b = bio_times[bio_key]
            one, many = one_thread[key], results[key][0]
            suffix = {
                "maxee_file_seqio": " — SeqIO.parse",
                "maxee_file_fast": " — FastqGeneralIterator",
            }.get(bio_key, "")
            print(
                f"| {label}{suffix} | {sizes[size]} | {b:.2f} s | {one:.3f} s | {many:.3f} s "
                f"| {b / one:.0f}× / {b / many:.0f}× |"
            )


if __name__ == "__main__":
    main()
