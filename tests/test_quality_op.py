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
    path = "min_example.fastq"

    # Pomiar czasu dla naszej funkcji cacl_base_seq_quality
    start1 = time.perf_counter()
    pb.cacl_base_seq_quality(path, output_type="polars.DataFrame")
    duration1 = time.perf_counter() - start1

    # Pomiar czasu dla fqc (wzięte z repo fastqc-rs), uruchamiam jako subprocess i mierzę czas 
    start2 = time.perf_counter()

    fqc_path = r"C:\Users\piotr\.cargo\bin\fqc.exe"  # pełna ścieżka do fqc
    summary_path = os.path.join(os.path.dirname(__file__), "summary.txt")
    subprocess.run([fqc_path, "-q", path, "-s", summary_path], capture_output=True, check=True)    
    duration2 = time.perf_counter() - start2

    print(f"Polars-bio: {duration1:.4f} s")
    print(f"fastqc-rs CLI: {duration2:.4f} s")

    # Sprawdzenie, żcz polars_bio nie jest za wolny
    # x4 to dopuszczalny margines jaki wziąłem, tyle razy wolniej może działać nasza funkcja żeby przeszła test
    assert duration1 < duration2 * 10, ( 
        f"Polars-bio działa znacznie wolniej ({duration1:.4f}s) niż fastqc-rs ({duration2:.4f}s)"
    )
