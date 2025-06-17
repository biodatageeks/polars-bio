#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Skrypt do testowania zgodności i wydajności analizy k-merów
między własną implementacją a fastqc-rs
"""

import json
import time
import os
import subprocess
import tempfile
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from polars_bio.io import read_fastq
from polars_bio.kmer_analysis import kmer_count, visualize_kmers


SMALL_FASTQ_PATH = "tests/data/io/fastq/example.fastq"
LARGE_FASTQ_PATH = "tests/data/io/fastq/ERR194147.fastq"  # Można zmienić na inny większy plik
OUTPUT_DIR = "benchmark_results"
FASTQC_RS_OUTPUT = "tests/data/io/fastq/output_big.json"
FASTQC_RS_OUTPUT_K3 = "tests/data/io/fastq/output.json"

# Upewnij się że katalog na wyniki istnieje
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# ==================== SEKCJA 1: POMOCNICZE FUNKCJE ====================

def run_fastqc_rs(fastq_path, k, output_json=None):
    """
    Uruchamia narzędzie fastqc-rs do zliczania k-merów.

    Argumenty:
        fastq_path (str): Ścieżka do pliku FASTQ
        k (int): Długość k-merów
        output_json (str, opcjonalnie): Ścieżka do pliku wyjściowego JSON

    Zwraca:
        dict: Słownik z wynikami lub None w przypadku błędu
    """
    if output_json is None:
        fd, output_json = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        temp_file = True
    else:
        temp_file = False

    cmd = [
        "fqc",
        "-k", str(k),
        "-q", fastq_path
    ]

    try:
        start_time = time.time()
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        execution_time = time.time() - start_time

        with open(output_json) as f:
            data = json.load(f)

        if temp_file:
            os.unlink(output_json)

        return data["values"], execution_time
    except Exception as e:
        print(f"Błąd fastqc-rs: {e}")
        return None, 0

def load_fastqc_rs_results(result_path):
    """
    Wczytuje wyniki fastqc-rs z pliku JSON.

    Argumenty:
        result_path (str): Ścieżka do pliku JSON z wynikami

    Zwraca:
        list: Lista wyników lub None w przypadku błędu
    """
    try:
        with open(result_path) as f:
            return json.load(f)["values"]
    except Exception as e:
        print(f"Błąd podczas wczytywania wyników fastqc-rs: {e}")
        return None

# ==================== SEKCJA 2: TEST ZGODNOŚCI ====================

def compare_results(kmers_polars, fastqc_rs_results):
    """
    Porównuje wyniki analizy k-merów między implementacją Polars a fastqc-rs.

    Argumenty:
        kmers_polars (pd.DataFrame): Wyniki z własnej implementacji
        fastqc_rs_results (list): Wyniki z fastqc-rs

    Zwraca:
        bool: True jeśli wyniki są zgodne, False w przeciwnym razie
    """
    fastqc_dict = {item["k_mer"]: item["count"] for item in fastqc_rs_results}
    polars_dict = {row["kmer"]: row["count"] for _, row in kmers_polars.iterrows()}

    # Sprawdź czy liczba k-merów się zgadza
    if len(fastqc_dict) != len(polars_dict):
        print(f"Różna liczba unikalnych k-merów: fastqc-rs={len(fastqc_dict)}, polars={len(polars_dict)}")
        return False

    # Sprawdź czy wszystkie k-mery mają te same wartości
    mismatches = []
    for kmer, count in polars_dict.items():
        if kmer in fastqc_dict:
            if count != fastqc_dict[kmer]:
                mismatches.append((kmer, count, fastqc_dict[kmer]))
        else:
            mismatches.append((kmer, count, "brak"))

    if mismatches:
        print(f"Znaleziono {len(mismatches)} różnic w zliczeniach k-merów:")
        for kmer, polars_count, fastqc_count in mismatches[:10]:  # Pokaż tylko pierwsze 10 różnic
            print(f"  k-mer: {kmer}, polars: {polars_count}, fastqc-rs: {fastqc_count}")
        if len(mismatches) > 10:
            print(f"  ... i {len(mismatches) - 10} więcej różnic")
        return False

    return True

def test_compatibility(k=3, fastq_path=SMALL_FASTQ_PATH, use_cached=True):
    """
    Przeprowadza test zgodności między własną implementacją a fastqc-rs.

    Argumenty:
        k (int): Długość k-merów
        fastq_path (str): Ścieżka do pliku FASTQ
        use_cached (bool): Czy używać zapisanych wyników fastqc-rs

    Zwraca:
        tuple: (bool, pd.DataFrame, list) - Wynik testu, wyniki własne, wyniki fastqc-rs
    """
    print(f"\n=== Test zgodności dla k={k}, plik={Path(fastq_path).name} ===")

    # Wczytaj dane i wykonaj analizę własną implementacją
    df = read_fastq(fastq_path)
    print("Wczytano dane FASTQ")

    start_time = time.time()
    kmers_polars = kmer_count(k=k, df=df)
    polars_time = time.time() - start_time
    print(f"Analiza k-merów własną implementacją: {polars_time:.4f}s")

    # Wczytaj lub wygeneruj wyniki fastqc-rs
    if k == 3 and use_cached and os.path.exists(FASTQC_RS_OUTPUT_K3) and fastq_path == SMALL_FASTQ_PATH:
        fastqc_rs_results = load_fastqc_rs_results(FASTQC_RS_OUTPUT_K3)
        fastqc_time = 0  # Nie mierzymy czasu dla zapisanych wyników
        print("Wczytano zapisane wyniki fastqc-rs")
    else:
        fastqc_rs_results, fastqc_time = run_fastqc_rs(fastq_path, k)
        print(f"Analiza k-merów fastqc-rs: {fastqc_time:.4f}s")

    if not fastqc_rs_results:
        print("Nie udało się uzyskać wyników z fastqc-rs")
        return False, kmers_polars, None

    # Porównaj wyniki
    is_compatible = compare_results(kmers_polars, fastqc_rs_results)
    if is_compatible:
        print("✓ Wyniki są zgodne!")
    else:
        print("✗ Wyniki NIE są zgodne!")

    # Zwróć wyniki i czasy
    return is_compatible, kmers_polars, fastqc_rs_results, polars_time, fastqc_time

# ==================== SEKCJA 3: TEST WYDAJNOŚCI ====================

def performance_test(fastq_paths=None, k_values=None, include_fastqc_rs=True):
    """
    Przeprowadza test wydajności dla różnych wartości k i plików.

    Argumenty:
        fastq_paths (list): Lista ścieżek do plików FASTQ
        k_values (list): Lista wartości k
        include_fastqc_rs (bool): Czy uwzględnić fastqc-rs w testach

    Zwraca:
        pd.DataFrame: DataFrame z wynikami testów
    """
    print("\n=== Test wydajności ===")

    fastq_paths = fastq_paths or [SMALL_FASTQ_PATH, LARGE_FASTQ_PATH]
    k_values = k_values or [3, 5]

    results = []

    for path in fastq_paths:
        path_name = Path(path).name
        print(f"\nTestowanie pliku: {path_name}")

        df = read_fastq(path)

        for k in k_values:
            print(f"  k={k}")

            # Test własnej implementacji
            start_time = time.time()
            kmer_count(k=k, df=df)
            polars_time = time.time() - start_time

            results.append({
                'implementation': 'polars-bio',
                'file': path_name,
                'k': k,
                'time': polars_time
            })

            # Test fastqc-rs
            if include_fastqc_rs:
                _, fastqc_time = run_fastqc_rs(path, k)
                if fastqc_time > 0:
                    results.append({
                        'implementation': 'fastqc-rs',
                        'file': path_name,
                        'k': k,
                        'time': fastqc_time
                    })

    return pd.DataFrame(results)

# ==================== SEKCJA 4: WIZUALIZACJA WYNIKÓW ====================

def visualize_compatibility_results(polars_results, fastqc_results, k, top_n=20, save_path=None):
    """
    Wizualizuje porównanie wyników k-merów z obu implementacji.

    Argumenty:
        polars_results (pd.DataFrame): Wyniki z własnej implementacji
        fastqc_results (list): Wyniki z fastqc-rs
        k (int): Długość k-merów
        top_n (int): Liczba najczęstszych k-merów do pokazania
        save_path (str): Ścieżka do zapisania wykresu
    """
    # Przekształć wyniki fastqc-rs na DataFrame
    fastqc_df = pd.DataFrame(fastqc_results).rename(columns={'k_mer': 'kmer'})

    # Wybierz top_n najczęstszych k-merów
    polars_top = polars_results.sort_values(by='count', ascending=False).head(top_n)
    fastqc_top = fastqc_df.sort_values(by='count', ascending=False).head(top_n)

    # Stwórz wykres
    fig, axes = plt.subplots(2, 1, figsize=(12, 10))
    fig.suptitle(f'Porównanie najczęstszych {top_n} k-merów (k={k})', fontsize=16)

    # Wykres dla własnej implementacji
    axes[0].barh(polars_top['kmer'][::-1], polars_top['count'][::-1], color='royalblue')
    axes[0].set_title('polars-bio')
    axes[0].set_xlabel('Liczba wystąpień')
    axes[0].grid(axis='x', linestyle='--', alpha=0.6)

    # Wykres dla fastqc-rs
    axes[1].barh(fastqc_top['kmer'][::-1], fastqc_top['count'][::-1], color='firebrick')
    axes[1].set_title('fastqc-rs')
    axes[1].set_xlabel('Liczba wystąpień')
    axes[1].grid(axis='x', linestyle='--', alpha=0.6)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path)
    else:
        plt.show()

def visualize_performance_results(results, save_path=None):
    """
    Wizualizuje wyniki testów wydajności.

    Argumenty:
        results (pd.DataFrame): DataFrame z wynikami testów
        save_path (str): Ścieżka do zapisania wykresu
    """
    # Stwórz wykres czasu wykonania dla różnych wartości k
    plt.figure(figsize=(14, 8))

    sns.set_style("whitegrid")

    g = sns.catplot(
        data=results,
        x="k",
        y="time",
        hue="implementation",
        col="file",
        kind="bar",
        palette=["royalblue", "firebrick"],
        height=5,
        aspect=1.2
    )
    g.set_axis_labels("Wartość k", "Czas wykonania (s)")
    g.fig.suptitle("Porównanie wydajności dla różnych wartości k", y=1.05, fontsize=16)

    if save_path:
        plt.savefig(f"{save_path}_k_comparison.png")
    else:
        plt.show()

# ==================== SEKCJA 5: GŁÓWNA FUNKCJA ====================

def main():
    compatible, polars_results, fastqc_results, polars_time, fastqc_time = test_compatibility(k=3, use_cached=True)
    if compatible and fastqc_results:
        print("\nGeneruję wykres porównawczy dla k=3...")
        visualize_compatibility_results(
            polars_results,
            fastqc_results,
            k=3,
            top_n=20,
            save_path=os.path.join(OUTPUT_DIR, "comparison_k3.png")
        )

    print(f"\nCzas wykonania dla k=3:")
    print(f"  polars-bio: {polars_time:.4f}s")
    print(f"  fastqc-rs:  {fastqc_time:.4f}s")


    print("\nRozpoczynam testy wydajności...")
    performance_results = performance_test(
        fastq_paths=[SMALL_FASTQ_PATH],
        k_values=[3, 5],
        include_fastqc_rs=True
    )

    # Zapisz wyniki wydajności do CSV
    results_path = os.path.join(OUTPUT_DIR, "performance_results.csv")
    performance_results.to_csv(results_path, index=False)
    print(f"Zapisano wyniki wydajności do {results_path}")

    print("\nGeneruję wykresy wydajności...")
    visualize_performance_results(
        performance_results,
        save_path=os.path.join(OUTPUT_DIR, "performance")
    )


if __name__ == "__main__":
    main()
