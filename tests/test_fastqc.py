import pytest

import polars_bio as pb

FASTQ = "tests/data/io/fastq/example.fastq"


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
    assert set(tidy["module"].unique()) == {
        "basic_stats",
        "per_base_quality",
        "per_seq_gc",
        "dup_levels",
    }


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
    assert set(summary["module"]) == {
        "basic_stats",
        "per_base_quality",
        "per_seq_gc",
        "dup_levels",
    }
    assert summary["status"].is_in(["PASS", "WARN", "FAIL"]).all()


def test_sql_udtf():
    df = pb.sql(f"SELECT * FROM fastqc('{FASTQ}') WHERE metric = 'status'").collect()
    assert "module" in df.columns
    assert df.height == 4


def _n_seq_at(n_parts: int) -> float:
    pb.set_option("datafusion.execution.target_partitions", str(n_parts))
    bs = pb.fastqc(FASTQ, modules=["basic_stats"]).basic_stats.collect()
    return float(dict(zip(bs["metric"], bs["value"]))["n_seq"])


@pytest.mark.parametrize("n_parts", [1, 4])
def test_partition_merge_invariant(n_parts):
    expected = _n_seq_at(1)
    assert _n_seq_at(n_parts) == pytest.approx(expected)
