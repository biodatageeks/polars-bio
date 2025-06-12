import time
import polars as pl
import pandas as pd
from quality_op import cacl_base_seq_quality

def test_with_polars():
    df = pl.DataFrame({
        "quality_scores": ["!!!", ">>>", "###"]
    })
    start = time.time()
    result = cacl_base_seq_quality(df)
    end = time.time()
    print("Test z Polars DF zakończony")
    print(result)
    print(f"Czas wykonania: {end - start:.4f} s")

def test_with_pandas():
    df = pd.DataFrame({
        "quality_scores": ["!!!", ">>>", "###"]
    })
    start = time.time()
    result = cacl_base_seq_quality(df, output_type="pandas.DataFrame")
    end = time.time()
    print("Test z Pandas DF zakończony")
    print(result)
    print(f"Czas wykonania: {end - start:.4f} s")

def test_with_file():
    filepath = "data/example.fastq"
    start = time.time()
    result = cacl_base_seq_quality(filepath)
    end = time.time()
    print("Test z plikiem FASTQ zakończony")
    print(result)
    print(f"Czas wykonania: {end - start:.4f} s")

if __name__ == "__main__":
    test_with_polars()
    test_with_pandas()
    # test_with_file()  # odkomentuj jak masz example.fastq


def test_with_invalid_input_type():
    try:
        cacl_base_seq_quality(12345)
    except TypeError as e:
        print("Działa - TypeError:", e)

def test_with_invalid_output_type():
    try:
        df = pl.DataFrame({"quality_scores": ["!!!"]})
        cacl_base_seq_quality(df, output_type="wrong_type")
    except ValueError as e:
        print("Działa - ValueError:", e)

import timeit

execution_time = timeit.timeit(
    "cacl_base_seq_quality(df)", 
    setup="from quality_op import cacl_base_seq_quality; import polars as pl; df = pl.DataFrame({'quality_scores': ['!!!', '###', '%%%']})",
    number=10
)
print(f"Średni czas dla 10 wykonań: {execution_time / 10:.5f} s")
