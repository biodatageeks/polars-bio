#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
LARGE_FASTQ_PATH = "tests/data/io/fastq/ERR194147.fastq"  
OUTPUT_DIR = "benchmark_results"
FASTQC_RS_OUTPUT = "tests/data/io/fastq/output_big.json"
FASTQC_RS_OUTPUT_K3 = "tests/data/io/fastq/output.json"


if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# ==================== SEKCJA 1: POMOCNICZE FUNKCJE ====================

def run_fastqc_rs(fastq_path, k, output_json=None):

    try:
        start_time = time.time()
        subprocess.run(
            ["fqc", "-q", fastq_path, "-k", str(k)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True
        )
        execution_time = time.time() - start_time

        return  execution_time
    except Exception as e:
        print(f"Błąd fastqc-rs: {e}")
        return None, 0

def load_fastqc_rs_results(result_path):

    try:
        with open(result_path) as f:
            return json.load(f)["values"]
    except Exception as e:
        print(f"Błąd podczas wczytywania wyników fastqc-rs: {e}")
        return None


# ==================== SEKCJA 3: TEST WYDAJNOŚCI ====================

def performance_test(fastq_paths=None, k_values=None, include_fastqc_rs=True):

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
                fastqc_time = run_fastqc_rs(path, k)
                print(fastqc_time)
                if fastqc_time > 0:
                    results.append({
                        'implementation': 'fastqc-rs',
                        'file': path_name,
                        'k': k,
                        'time': fastqc_time
                    })

    return pd.DataFrame(results)


def visualize_performance_results(results, save_path=None):

    sns.set_style("whitegrid")

    files = results['file'].unique()

    fig, axes = plt.subplots(1, len(files), figsize=(14, 6), sharey=False)

    if len(files) == 1:
        axes = [axes]

    for i, file in enumerate(files):
        file_data = results[results['file'] == file]

        sns.barplot(
            data=file_data,
            x="k",
            y="time",
            hue="implementation",
            palette=["royalblue", "firebrick"],
            ax=axes[i]
        )

        axes[i].set_title(file)
        axes[i].set_xlabel("Wartość k")
        axes[i].set_ylabel("Czas wykonania (s)")

    plt.tight_layout()
    fig.suptitle("Porównanie wydajności dla różnych wartości k", y=1.02, fontsize=16)

    handles, labels = axes[-1].get_legend_handles_labels()
    [ax.get_legend().remove() for ax in axes if ax.get_legend()]
    fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, 0),
               ncol=2, frameon=True, title="Implementation")

    if save_path:
        plt.savefig(f"{save_path}_k_comparison.png", bbox_inches='tight')
    else:
        plt.show()

# ==================== SEKCJA 5: GŁÓWNA FUNKCJA ====================

def get_kmer_results(fastq_path, k):

    df = read_fastq(fastq_path)
    kmer_df = kmer_count(k=k, df=df)
    return kmer_df


def main():
    print("\nRozpoczynam testy wydajności...")
    performance_results = performance_test(
        fastq_paths=[SMALL_FASTQ_PATH, LARGE_FASTQ_PATH],
        k_values=[3],
        include_fastqc_rs=True
    )

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
