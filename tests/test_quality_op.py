# Podmieńcie ścieżkę do pliku min_example.fastq na odpowiednią u was bo dałem bezpośrednio swoją
import polars_bio as pb
import polars as pl
import pytest
import time

# Poniższe expected dane wpisałem jako pierwsze 6 wierszy z wyników opartych na pliku "min_example.fastq"
EXPECTED_ROWS = [
    {"position": 0, "min_score": 2.0, "max_score": 31.0, "median_score": 2.0, "q1_score": 2.0, "q3_score": 31.0, "sample_count": 3},
    {"position": 1, "min_score": 16.0, "max_score": 31.0, "median_score": 19.0, "q1_score": 16.0, "q3_score": 31.0, "sample_count": 3},
    {"position": 2, "min_score": 28.0, "max_score": 33.0, "median_score": 31.0, "q1_score": 28.0, "q3_score": 33.0, "sample_count": 3},
    {"position": 3, "min_score": 35.0, "max_score": 35.0, "median_score": 35.0, "q1_score": 35.0, "q3_score": 35.0, "sample_count": 3},
    {"position": 4, "min_score": 35.0, "max_score": 37.0, "median_score": 35.0, "q1_score": 35.0, "q3_score": 37.0, "sample_count": 3},
    {"position": 5, "min_score": 35.0, "max_score": 35.0, "median_score": 35.0, "q1_score": 35.0, "q3_score": 35.0, "sample_count": 3},
]

# Test czy funkcja działa identycznie niezależnie od wejścia (ścieżka do pliku lub DataFrame)
def test_cacl_base_seq_quality_equivalence():
    path = r"..\benchmark\src\min_example.fastq"
    fastq_df = pb.read_fastq(path)  # Dane wczytane jako DataFrame

    result_from_path = pb.cacl_base_seq_quality(path, output_type="polars.DataFrame")   # Oblicz jako ścieżka do pliku
    result_from_df = pb.cacl_base_seq_quality(fastq_df, output_type="polars.DataFrame")    # Oblicz jako DataFrame

    # Sprawdzenie, czy wyniki są identyczne
    assert result_from_path.equals(result_from_df)

# Test czy konkretne wartości z pierwszych 3 wierszy (0, 1, 2) są zgodne z expected 
def test_cacl_base_seq_quality_expected_values():
    path = r"..\benchmark\src\min_example.fastq"
    result = pb.cacl_base_seq_quality(path, output_type="polars.DataFrame")

    for i, expected in enumerate(EXPECTED_ROWS):
        row = result[i]
        for key, expected_value in expected.items():
            actual_value = row[key].item()  # lub row[key][0]
            assert actual_value == expected_value, f"Błąd w {key} dla pozycji {i}: oczekiwano {expected_value}, otrzymano {actual_value}"


# Sprawdzenie wydajności funkcji cacl_base_seq_quality, potem mogę dodać porównanie do fastq-rs
def test_performance_comparison():
    path = r"..\benchmark\src\min_example.fastq"

    start1 = time.perf_counter()
    pb.cacl_base_seq_quality(path, output_type="polars.DataFrame")
    duration1 = time.perf_counter() - start1

    # start2 = time.perf_counter()
    # pb.fastq_rs_cacl_base_seq_quality(path)
    # duration2 = time.perf_counter() - start2

    print(f"Polars-bio: {duration1:.6f} sekundy")
    # print(f"fastq-rs  : {duration2:.6f} sekundy")

    # assert duration1 < duration2 * 3, "Polars-bio działa wolniej względem fastq-rs"
