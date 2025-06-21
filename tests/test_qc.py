import json
import pytest
from polars_bio import qc_operations
import polars_bio
from _expected import (
    EXPECTED_MEAN_QUALITY_PATH,
    EXPECTED_MEAN_QUALITY_HIST_PATH,
    DATA_DIR,
)

@pytest.fixture
def sample_df():
    # Wczytaj dane testowe FASTQ
    fastq_file_path = DATA_DIR / "io/fastq/example.fastq"
    return polars_bio.read_fastq(str(fastq_file_path)).collect()

def test_sequence_quality_score(sample_df):
    actual_df = qc_operations.sequence_quality_score(sample_df, output_type="polars.DataFrame")
    expected_values = [int(line.strip()) for line in open(EXPECTED_MEAN_QUALITY_PATH) if line.strip().isdigit()]

    # Konwersja do listy wartości z df
    actual_values = actual_df["mean_c"].to_list()

    assert actual_values == expected_values, f"Differences found in mean quality scores:\n{actual_values} != {expected_values}"

def test_sequence_quality_score_histogram(sample_df):
    actual_df = qc_operations.sequence_quality_score_histogram(sample_df, output_type="polars.DataFrame")
    
    # Wczytaj dane JSON
    with open(EXPECTED_MEAN_QUALITY_HIST_PATH) as f:
        expected_json = json.load(f)["data"]["values"]

    # Przekształcenie JSON do słownika: {score: count}
    expected_dict = {int(row["score"]): row["count"] for row in expected_json}

    # Konwersja dataframe do słownika: {bin_start: count}
    actual_dict = {
        int(row["bin_start"]): row["count"]
        for row in actual_df.to_dicts()
    }

    assert actual_dict == expected_dict, f"Differences found in histogram data:\n{actual_dict} != {expected_dict}"
