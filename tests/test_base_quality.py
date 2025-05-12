import polars as pl
from polars_bio.quality.base_quality import base_quality

def test_base_quality_on_sample_fastq():
    df = pl.read_parquet("tests/data/base_quality/example.parquet")
    metrics = base_quality(df, streaming=True, target_partitions=4)
    assert "position" in metrics.columns
    assert metrics["average"].mean() > 0
