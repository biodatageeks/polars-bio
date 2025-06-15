# Podmieńcie ścieżkę do pliku min_example.fastq na odpowiednią u was bo dałem bezpośrednio swoją
import polars_bio as pb
import polars as pl
import time
import pytest
import subprocess
import os




# Poniższe expected dane wpisałem jako pierwsze 6 wierszy z wyników opartych na pliku "min_example.fastq"
EXPECTED_ROWS = [
    {"position": 0, "min": 2.0, "max": 31.0, "median": 2.0, "q1": 2.0, "q3": 16.5, "sample_count": 3},
    {"position": 1, "min": 16.0, "max": 31.0, "median": 19.0, "q1": 17.5, "q3": 25.0, "sample_count": 3},
    {"position": 2, "min": 28.0, "max": 33.0, "median": 31.0, "q1": 29.5, "q3": 32.0, "sample_count": 3},
    {"position": 3, "min": 35.0, "max": 35.0, "median": 35.0, "q1": 35.0, "q3": 35.0, "sample_count": 3},
    {"position": 4, "min": 35.0, "max": 37.0, "median": 35.0, "q1": 35.0, "q3": 36.0, "sample_count": 3},
    {"position": 5, "min": 35.0, "max": 35.0, "median": 35.0, "q1": 35.0, "q3": 35.0, "sample_count": 3},
]

# Test czy funkcja działa identycznie niezależnie od wejścia (ścieżka do pliku lub DataFrame)
def test_cacl_base_seq_quality_equivalence():
    path = "min_example.fastq"
    fastq_df = pb.read_fastq(path)  # Dane wczytane jako DataFrame

    result_from_path = pb.cacl_base_seq_quality(path, output_type="polars.DataFrame")   # Oblicz jako ścieżka do pliku
    result_from_df = pb.cacl_base_seq_quality(fastq_df, output_type="polars.DataFrame")    # Oblicz jako DataFrame

    # Sprawdzenie, czy wyniki są identyczne
    assert result_from_path.equals(result_from_df)

# Test czy konkretne wartości z pierwszych 3 wierszy (0, 1, 2) są zgodne z expected 
def test_cacl_base_seq_quality_expected_values():
    path = "min_example.fastq"
    result = pb.cacl_base_seq_quality(path, output_type="polars.DataFrame")

    for i, expected in enumerate(EXPECTED_ROWS):
        row = result[i]
        for key, expected_value in expected.items():
            actual_value = row[key].item()  # lub row[key][0]
            assert actual_value == expected_value, f"Błąd w {key} dla pozycji {i}: oczekiwano {expected_value}, otrzymano {actual_value}"


# Sprawdzenie wydajności funkcji cacl_base_seq_quality, potem mogę dodać porównanie do fastq-rs

def test_performance_comparison():
    TEST_DIR = os.path.dirname(__file__)
    fastq_path   = os.path.join(TEST_DIR, "min_example.fastq")
    summary_path = os.path.join(TEST_DIR, "summary.txt")

    # nasza funkcja
    start1 = time.perf_counter()
    pb.cacl_base_seq_quality(fastq_path, output_type="polars.DataFrame")
    duration1 = time.perf_counter() - start1

    # fqc
    start2 = time.perf_counter()
    proc = subprocess.run(
        [r"C:\Users\piotr\.cargo\bin\fqc.exe", "-q", fastq_path, "-s", summary_path],
        capture_output=True,
        check=False
    )
    duration2 = time.perf_counter() - start2

    # jeżeli exit code >1 – traktujemy jako błąd
    if proc.returncode not in (0, 1):
        pytest.fail(f"fqc.exe zakończył się błędem (exit {proc.returncode}):\n{proc.stderr.decode()}")

    print(f"Polars-bio: {duration1:.4f} s")
    print(f"fastqc-rs CLI: {duration2:.4f} s")

    # dopuszczalny margines (×10)
    # assert duration1 < duration2 , (
    #     f"Polars-bio działa znacznie wolniej ({duration1:.4f}s) niż fastqc-rs ({duration2:.4f}s)"
    # )
