import pytest

import polars_bio as pb

DATA = "tests/data/io/fastq"
FASTQ = f"{DATA}/example.fastq"
# Same 200 reads in three encodings.
FASTQ_GZ = f"{DATA}/example.fastq.gz"
FASTQ_BGZ = f"{DATA}/example.fastq.bgz"
# Splittable BGZF (has a .gzi index) — exercises real parallel accumulate+merge.
PARALLEL_BGZ = f"{DATA}/sample_parallel.fastq.bgz"
# Robustness fixtures (unindexed BGZF, multi-member gzip).
NOINDEX_BGZ = f"{DATA}/sample_no_index.fastq.bgz"
MULTIMEMBER_GZ = f"{DATA}/multimember_pigz.fastq.gz"

EXPECTED_MODULES = [
    "basic_stats",
    "per_base_quality",
    "per_seq_quality",
    "per_base_content",
    "per_seq_gc",
    "per_base_n",
    "seq_length",
    "overrepresented",
    "adapter_content",
    "dup_levels",
    "per_tile_quality",
    "kmer_content",
]


@pytest.fixture(autouse=True)
def _restore_target_partitions():
    """Keep target_partitions changes from leaking into the wider suite."""
    key = "datafusion.execution.target_partitions"
    original = pb.get_option(key)
    yield
    pb.set_option(key, original if original is not None else "1")


def _sorted_tidy(path: str) -> "pl.DataFrame":  # noqa: F821
    return (
        pb.fastqc(path).tidy.collect().sort(["module", "label", "position", "metric"])
    )


def test_tidy_schema_and_all_modules():
    qc = pb.fastqc(FASTQ)
    tidy = qc.tidy.collect()
    assert tidy.columns == [
        "module",
        "label",
        "position",
        "metric",
        "value",
        "value_str",
    ]
    assert set(tidy["module"].unique()) == set(EXPECTED_MODULES)


def test_basic_stats_values():
    qc = pb.fastqc(FASTQ, modules=["basic_stats"])
    bs = qc.basic_stats.collect()
    metrics = dict(zip(bs["metric"], bs["value"]))
    assert metrics["n_seq"] > 0
    assert metrics["max_len"] >= metrics["min_len"]
    assert 0.0 <= metrics["gc_pct"] <= 100.0


def test_per_base_quality_shape():
    qc = pb.fastqc(FASTQ, modules=["per_base_quality"])
    pbq = qc.per_base_quality.collect()
    assert {"position", "mean", "median", "q1", "q3", "p10", "p90"}.issubset(
        pbq.columns
    )
    assert pbq["position"].min() == 1


def test_multiple_property_access_single_pass():
    # One fastqc() result must serve every per-module view without re-running
    # (regression: the tidy table was previously deregistered after the first
    # collect, breaking the second property access).
    qc = pb.fastqc(FASTQ)
    assert qc.basic_stats.collect().height > 0
    assert qc.per_base_quality.collect().height > 0
    assert qc.per_seq_gc.collect().height > 0
    assert qc.dup_levels.collect().height > 0
    assert qc.summary().collect().height == len(EXPECTED_MODULES)
    assert qc.tidy.collect().height > 0


def test_new_module_properties_shapes():
    qc = pb.fastqc(FASTQ)
    assert {"quality", "count"} == set(qc.per_seq_quality.collect().columns)
    pbc = qc.per_base_content.collect()
    assert {"position", "G", "A", "T", "C"}.issubset(pbc.columns)
    assert pbc["position"].min() == 1
    assert {"position", "n_pct"} == set(qc.per_base_n.collect().columns)
    sl = qc.seq_length.collect()
    assert {"length", "count"} == set(sl.columns)
    assert sl["count"].sum() == 200  # all reads accounted for
    over = qc.overrepresented.collect()
    assert {"sequence", "count", "pct", "possible_source"} == set(over.columns)


def test_non_computed_module_raises():
    qc = pb.fastqc(FASTQ, modules=["basic_stats"])
    with pytest.raises(KeyError):
        _ = qc.per_base_quality


def test_unknown_module_raises():
    with pytest.raises(ValueError):
        pb.fastqc(FASTQ, modules=["not_a_module"])


def test_summary_has_status_per_module():
    qc = pb.fastqc(FASTQ)
    summary = qc.summary().collect()
    assert set(summary["module"]) == set(EXPECTED_MODULES)
    assert summary["status"].is_in(["PASS", "WARN", "FAIL"]).all()


def test_sql_udtf():
    df = pb.sql(f"SELECT * FROM fastqc('{FASTQ}') WHERE metric = 'status'").collect()
    assert "module" in df.columns
    assert df.height == len(EXPECTED_MODULES)


def _n_seq_at(n_parts: int) -> float:
    pb.set_option("datafusion.execution.target_partitions", str(n_parts))
    # PARALLEL_BGZ is a splittable BGZF (has a .gzi), so n_parts>1 genuinely
    # fans the scan across partitions and exercises the accumulate+merge path.
    bs = pb.fastqc(PARALLEL_BGZ, modules=["basic_stats"]).basic_stats.collect()
    return float(dict(zip(bs["metric"], bs["value"]))["n_seq"])


@pytest.mark.parametrize("n_parts", [1, 4])
def test_partition_merge_invariant(n_parts):
    expected = _n_seq_at(1)
    assert _n_seq_at(n_parts) == pytest.approx(expected)


@pytest.mark.parametrize("compressed", [FASTQ_GZ, FASTQ_BGZ])
def test_format_invariance_plain_vs_compressed(compressed):
    # gzip / BGZF decoding must yield byte-identical QC to the plain file
    # (same 200 reads, same order -> identical integer histograms -> identical
    # finalized floats).
    assert _sorted_tidy(compressed).equals(_sorted_tidy(FASTQ))


@pytest.mark.parametrize("n_parts", [1, 2, 4, 8])
def test_bgzf_partition_invariance_full_tidy(n_parts):
    # On a SPLITTABLE BGZF file the entire tidy result (histograms, quantiles,
    # duplication levels, GC bins) must be identical regardless of partition
    # count -- this is the real end-to-end parallel accumulate+merge path that
    # the plain-FASTQ test cannot reach (plain FASTQ is single-partition).
    pb.set_option("datafusion.execution.target_partitions", "1")
    expected = _sorted_tidy(PARALLEL_BGZ)
    pb.set_option("datafusion.execution.target_partitions", str(n_parts))
    assert _sorted_tidy(PARALLEL_BGZ).equals(expected)


@pytest.mark.parametrize(
    "path,expected_n_seq",
    [
        (NOINDEX_BGZ, 2000),
        (MULTIMEMBER_GZ, 2000),
    ],
)
def test_reads_robust_compressed_inputs(path, expected_n_seq):
    # Unindexed BGZF and multi-member gzip must parse cleanly (the class of
    # inputs where other readers -- e.g. RastQC 0.1.0 -- fail on the header).
    bs = pb.fastqc(path, modules=["basic_stats"]).basic_stats.collect()
    metrics = dict(zip(bs["metric"], bs["value"]))
    assert metrics["n_seq"] == expected_n_seq
    assert metrics["min_len"] > 0
