import json
import matplotlib.pyplot as plt
import pandas as pd
import polars as pl


import polars_bio
from polars_bio.polars_bio import (
    py_kmer_count,
)

# Możesz podać seed i katalog, jeśli masz takie wymagania



def kmer_count(k, df):
    """
    Count k-mers in a Polars DataFrame.

    Parameters:
        k: The size of the k-mers.
        df: Polars DataFrame or LazyFrame containing the sequences.

    Returns:
        Pandas DataFrame with k-mer counts.
    """

    assert isinstance(df, (pl.DataFrame, pl.LazyFrame)), "df must be Polars DataFrame or LazyFrame"
    assert isinstance(k, int) and k > 0, "k must be a positive integer"

    arrow_reader = (
        df.to_arrow()
        if isinstance(df, pl.DataFrame)
        else df.collect().to_arrow().to_reader()
    )

    # Uruchomienie liczenia k-merów
    ctx = polars_bio.BioSessionContext(seed="seed", catalog_dir=".")
    result = py_kmer_count(ctx, k, arrow_reader).collect()

    # Odczyt JSON-a z kolumny kmer_counts
    json_str = result[0]["kmer_counts"][0].as_py()
    json_data = json.loads(json_str)

    # Konwersja do Pandas
    rows = [{"kmer": kmer, "count": count} for kmer, count in json_data.items()]
    return pd.DataFrame(rows)


def visualize_kmers(df, top_n=None):
    """
    Visualize k-mer count result as a horizontal bar chart.

    Parameters:
        df: Pandas DataFrame with 'kmer' and 'count' columns.
        top_n: Number of top k-mers to visualize.
    """
    assert isinstance(df, pd.DataFrame), "df must be a Pandas DataFrame"
    assert "kmer" in df.columns and "count" in df.columns, "DataFrame must contain 'kmer' and 'count' columns"

    df = df.sort_values(by='count', ascending=False)
    if top_n:
        df = df.head(top_n)

    df = df[::-1].reset_index(drop=True)

    plt.figure(figsize=(10, max(6, 0.3 * len(df))))
    bars = plt.barh(range(len(df)), df['count'], color='steelblue', edgecolor='black')
    plt.yticks(range(len(df)), df['kmer'])

    for i, bar in enumerate(bars):
        width = bar.get_width()
        plt.text(width + 0.5, bar.get_y() + bar.get_height() / 2,
                 str(int(width)), va='center', fontsize=9)

    plt.xlabel('count')
    plt.ylabel('k-mer')
    plt.title('k-mer quantities')
    plt.grid(axis='x', linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.show()
