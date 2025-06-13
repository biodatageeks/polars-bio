import polars as pl

from polars_bio.kmer_analysis import kmer_count, visualize_kmers


# Przykładowe dane DNA
sequences = ["ACGTACGT", "TGCATGCA", "ACGTAC", "NNNN", "ACGT"]
df = pl.DataFrame({"sequence": sequences})

# Liczenie k-merów (k=3)
result_df = kmer_count(3, df)

# Wizualizacja top 10
visualize_kmers(result_df, top_n=10)
