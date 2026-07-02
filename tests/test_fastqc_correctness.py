"""Independent-computation correctness gate (pure Python, no external tools).

Recomputes the raw arithmetic of each module straight from the FASTQ text and
asserts our engine matches exactly. This is what proved us right and RastQC
wrong on per-base means, and it covers the raw-count modules (duplication,
totals) that FastQC transforms for display.
"""

from collections import Counter

import polars_bio as pb

FASTQ = "tests/data/io/fastq/example.fastq"


def _reads():
    lines = open(FASTQ).read().splitlines()
    seqs = [lines[i] for i in range(1, len(lines), 4)]
    quals = [lines[i] for i in range(3, len(lines), 4)]
    return seqs, quals


def test_basic_stats_exact():
    seqs, _ = _reads()
    gc = sum(c in "GCgc" for s in seqs for c in s)
    atgc = sum(c in "ACGTacgt" for s in seqs for c in s)  # FastQC excludes N
    total = sum(len(s) for s in seqs)
    bs = pb.fastqc(FASTQ, modules=["basic_stats"]).basic_stats.collect()
    m = dict(zip(bs["metric"], bs["value"]))
    assert m["n_seq"] == len(seqs)
    assert m["total_bases"] == total  # full length, incl N
    assert m["min_len"] == min(len(s) for s in seqs)
    assert m["max_len"] == max(len(s) for s in seqs)
    assert abs(m["gc_pct"] - gc / atgc * 100) < 1e-9  # %GC over A/T/G/C only


def test_per_base_mean_exact():
    seqs, quals = _reads()
    maxlen = max(len(q) for q in quals)
    sums = [0] * maxlen
    ns = [0] * maxlen
    for q in quals:
        for i, ch in enumerate(q):
            sums[i] += ord(ch) - 33
            ns[i] += 1
    pbq = pb.fastqc(FASTQ, modules=["per_base_quality"]).per_base_quality.collect()
    got = dict(zip(pbq["position"], pbq["mean"]))
    for i in range(maxlen):
        expected = sums[i] / ns[i]
        assert abs(got[i + 1] - expected) < 1e-9, f"pos {i + 1}"


def test_duplication_dedup_pct_exact():
    seqs, _ = _reads()
    # FastQC keys on the first 50 bases; "Total Deduplicated Percentage" is the
    # fraction of distinct sequences remaining (identity correction < 100k).
    keys = [s[:50] for s in seqs]
    counts = Counter(keys)
    expected_dedup_pct = len(counts) / len(keys) * 100
    tidy = pb.fastqc(FASTQ, modules=["dup_levels"]).tidy.collect()
    dedup = tidy.filter(
        (tidy["module"] == "dup_levels") & (tidy["metric"] == "total_dedup_pct")
    )["value"][0]
    assert abs(dedup - expected_dedup_pct) < 1e-9
