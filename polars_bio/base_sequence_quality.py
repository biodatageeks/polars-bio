from polars_bio import (
    compute_quality_stats
)

fastq_file = 'example.fastq'

df = compute_quality_stats(fastq_file, partitions=4)

print(df.head())
df.to_csv("base_quality_stats.csv", index=False)