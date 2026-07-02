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
    total = sum(len(s) for s in seqs)
    bs = pb.fastqc(FASTQ, modules=["basic_stats"]).basic_stats.collect()
    m = dict(zip(bs["metric"], bs["value"]))
    assert m["n_seq"] == len(seqs)
    assert m["total_bases"] == total
    assert m["min_len"] == min(len(s) for s in seqs)
    assert m["max_len"] == max(len(s) for s in seqs)
    assert abs(m["gc_pct"] - gc / total * 100) < 1e-9


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


def test_duplication_pct_dup_exact():
    seqs, _ = _reads()
    # Our key is the first 50 bases, uppercased (KEY_PREFIX=50).
    keys = [s[:50].upper() for s in seqs]
    counts = Counter(keys)
    distinct = len(counts)
    total = len(keys)
    expected_pct_dup = (total - distinct) / total * 100
    tidy = pb.fastqc(FASTQ, modules=["dup_levels"]).tidy.collect()
    pct_dup = tidy.filter(
        (tidy["module"] == "dup_levels") & (tidy["metric"] == "pct_dup")
    )["value"][0]
    assert abs(pct_dup - expected_pct_dup) < 1e-9
