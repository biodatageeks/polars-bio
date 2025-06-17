import pandas as pd
import polars as pl
import polars_bio as pb
import pytest

from _expected import DATA_DIR


@pytest.fixture
def test_fastq_file():
    return f"{DATA_DIR}/base_quality/example.fastq"


@pytest.fixture
def fastq_df(test_fastq_file):
    return pb.read_fastq(test_fastq_file)


def test_base_sequence_quality_structure(fastq_df):
    df = pb.base_sequence_quality(fastq_df, "quality_scores")

    expected_columns = {"pos", "avg", "lower", "q1", "median", "q3", "upper"}
    actual_columns = set(df.columns)
    
    assert all(col in actual_columns for col in expected_columns), f"Missing column in returned DataFrame"
    assert len(df) == 101, f"Expected 100 rows, got {len(df)}"


def test_base_sequence_quality_pos_zero(fastq_df):
    expected_row = [0, 30.135, 26.5, 31.0, 33.0, 34.0, 38.5]

    df = pb.base_sequence_quality(fastq_df, "quality_scores")
    df = df.filter(pl.col("pos") == 0)
    assert len(df) == 1

    row = df.row(0)
    assert row is not None

    assert all(
        pytest.approx(expected_value) == value
        for expected_value, value in zip(expected_row, row)
    ), "Calculated stats for row are incorrect"


def test_input_table_name(fastq_df):
    result = pb.base_sequence_quality("example", "quality_scores")
    assert isinstance(result, pl.DataFrame)


def test_input_polars_dataframe(fastq_df):
    polars_df = fastq_df.collect()
    result = pb.base_sequence_quality(polars_df, "quality_scores")
    assert isinstance(result, pl.DataFrame)


def test_input_pandas_dataframe(fastq_df):
    polars_df = fastq_df.collect()
    pandas_df = polars_df.to_pandas()
    result = pb.base_sequence_quality(pandas_df, "quality_scores")
    assert isinstance(result, pl.DataFrame)


def test_output_default_is_polars(fastq_df):
    polars_df = fastq_df.collect()
    result = pb.base_sequence_quality(polars_df, "quality_scores")
    assert isinstance(result, pl.DataFrame)


def test_output_type_pandas(fastq_df):
    polars_df = fastq_df.collect()
    result = pb.base_sequence_quality(
        polars_df, "quality_scores", output_type="pandas.DataFrame"
    )
    assert isinstance(result, pd.DataFrame)
