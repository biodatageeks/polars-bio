## ADDED Requirements

### Requirement: Sequence Statistics

polars-bio SHALL provide `pb.seq_stats(source)`. `source` SHALL be a FASTA or
FASTQ path, a list of such paths, or a frame with a `sequence` column and an
optional `quality_scores` column. The function SHALL return one row per path,
or a single row for a frame, with these columns:
`file`, `format`, `num_seqs`, `sum_len`, `min_len`, `avg_len`, `max_len`,
`q1`, `q2`, `q3`, `sum_gap`, `n50`, `n50_num`, `q20_pct`, `q30_pct`,
`avg_qual`, `gc_pct` and `sum_n`. For FASTA input, or a frame without quality
scores, the quality columns SHALL be null.

#### Scenario: Parity with seqkit stats
- **WHEN** `pb.seq_stats(path)` runs on each file of the parity corpus (short-read FASTQ, long-read FASTQ, multi-record FASTA, gzip and BGZF variants)
- **THEN** the integer columns SHALL equal the output of `seqkit stats -a -T` exactly, and the floating-point columns SHALL equal it to seqkit's printed precision

#### Scenario: Multiple files
- **WHEN** `pb.seq_stats(["a.fastq.gz", "b.fastq.gz"])` is called
- **THEN** the result SHALL have two rows whose `file` values identify each input

#### Scenario: Frame input
- **WHEN** `pb.seq_stats(lf)` is called on a LazyFrame filtered from `pb.scan_fastq(path)`
- **THEN** the statistics SHALL describe only the rows that pass the filter

#### Scenario: Empty input
- **WHEN** `pb.seq_stats` runs on an empty file or a frame with zero rows
- **THEN** it SHALL return one row with `num_seqs = 0`, `sum_len = 0` and null values for every length and quality statistic

### Requirement: Bounded-Memory Statistics

`pb.seq_stats` SHALL compute its result in a single streaming pass, and its
memory SHALL grow with the number of distinct sequence lengths, not with the
number of sequences.

#### Scenario: Large short-read file
- **WHEN** `pb.seq_stats` runs on a FASTQ with tens of millions of fixed-length reads
- **THEN** peak memory SHALL stay within a small constant factor of a plain `scan_fastq(...).select(pl.len())` streaming pass over the same file
