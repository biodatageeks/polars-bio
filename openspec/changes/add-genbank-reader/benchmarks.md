# GenBank reader benchmarks

A release build of polars-bio 0.38.0 with the GenBank provider was tested on an Apple M3 Max using Python 3.12, Polars 1.44.2, Biopython 1.88, gb-io 0.4.0 and microbiorust 0.1.6. Inputs were local, uncompressed GenBank files. Each invocation ran in a fresh process with one Polars worker and one DataFusion target partition unless the table specifies four partitions. File system caches were warm.

## Inputs

| RefSeq assembly | Downloaded gzip SHA-256 | Uncompressed bytes |
| --- | --- | ---: |
| [GCF_000009045.1_ASM904v1](https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/009/045/GCF_000009045.1_ASM904v1/GCF_000009045.1_ASM904v1_genomic.gbff.gz) | `cf958f2c986289f4edc7c94b7cd6f625a4a4af16a3f0f67620fc9de67e670c0e` | 13,416,020 |
| [GCF_000005845.2_ASM584v2](https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/005/845/GCF_000005845.2_ASM584v2/GCF_000005845.2_ASM584v2_genomic.gbff.gz) | `cbcc40a9859312cbcdbeb3df484ee471dfe354c5cd8f28056b48431353b28384` | 11,450,954 |
| [GCF_000203835.1_ASM20383v1](https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/203/835/GCF_000203835.1_ASM20383v1/GCF_000203835.1_ASM20383v1_genomic.gbff.gz) | `dd1aa84547eb603adf3e7352166d01e533c719d45767be6d40cecdfd231d775c` | 22,692,374 |

The combined input concatenates the three decompressed files in table order. It contains five LOCUS records, occupies 47,559,348 bytes and has SHA-256 `6e0ecb979e16a7eccd7ca3adf38d6fdb6aa8c50dcd5a422b4d72a6ef8ce9d5b2`. The assembly versions identify the sequences. Check the gzip digests when reproducing the run because RefSeq can revise annotations.

## Workloads

- **FAA** writes annotated CDS translations to a temporary FASTA file with locus-tag headers. Entries without translations are skipped.
- **Narrow** materializes record identifier, feature type and locus tag in a Polars DataFrame.
- **Wide** adds gene, product, protein ID and translation. Each qualifier uses its first value. Wrapped qualifier text is unfolded before comparison.
- **Full** materializes the polars-bio default feature view, including all qualifier values and location parts. Its comparable fields are extracted after timing.

The timer covers file opening, partition discovery, parsing and materialization. Peak RSS is for the whole process through materialization. An order-independent digest of output rows includes duplicates. polars-bio, Biopython and gb-io produced identical FAA, narrow and wide outputs for all four inputs. The full view matched the wide digest on its comparable fields.

## Combined input

Median of three fresh processes. Throughput uses uncompressed input size.

| Workload | Reader | Partitions | Rows | Seconds | MiB/s | Peak RSS (MiB) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| faa | polars-bio | 1 | 16,533 | 0.109 | 415.8 | 215.7 |
| narrow | polars-bio | 1 | 34,935 | 0.091 | 497.3 | 198.6 |
| wide | polars-bio | 1 | 34,935 | 0.111 | 408.7 | 214.7 |
| faa | biopython | 1 | 16,533 | 1.065 | 42.6 | 190.9 |
| narrow | biopython | 1 | 34,935 | 1.103 | 41.1 | 208.8 |
| wide | biopython | 1 | 34,935 | 1.114 | 40.7 | 236.8 |
| faa | gb-io | 1 | 16,533 | 0.166 | 273.4 | 113.1 |
| narrow | gb-io | 1 | 34,935 | 0.155 | 293.2 | 116.9 |
| wide | gb-io | 1 | 34,935 | 0.218 | 207.8 | 155.7 |
| full | polars-bio | 1 | 34,935 | 0.134 | 338.3 | 233.0 |
| full | polars-bio | 4 | 34,935 | 0.109 | 416.2 | 239.1 |

The microbiorust FAA adapter returned 16,769 entries on the combined input. The three agreeing readers returned 16,533. Its Python feature API groups entries by locus tag and does not expose an equivalent all-feature table. Its timings are excluded from the equivalent-output comparison.

## Per-assembly results

Single-run elapsed seconds, with one partition.

| Assembly | Workload | polars-bio | Biopython | gb-io | Rows |
| --- | --- | ---: | ---: | ---: | ---: |
| bsubtilis | faa | 0.048 | 0.323 | 0.053 | 4,237 |
| bsubtilis | narrow | 0.039 | 0.331 | 0.044 | 9,074 |
| bsubtilis | wide | 0.044 | 0.332 | 0.068 | 9,074 |
| ecoli | faa | 0.041 | 0.264 | 0.043 | 4,300 |
| ecoli | narrow | 0.035 | 0.268 | 0.038 | 9,285 |
| ecoli | wide | 0.040 | 0.265 | 0.059 | 9,285 |
| scoelicolor | faa | 0.059 | 0.462 | 0.074 | 7,996 |
| scoelicolor | narrow | 0.050 | 0.468 | 0.064 | 16,576 |
| scoelicolor | wide | 0.057 | 0.474 | 0.088 | 16,576 |

## Reproduction

Run from a checkout with the release extension installed and the comparison packages above. Repeat per input and workload. Each invocation prints elapsed time, throughput, peak RSS, row count and SHA-256.

```sh
PYTHONPATH=. POLARS_MAX_THREADS=1 python benchmark-scripts/genbank.py polars-bio faa multi.gbk
PYTHONPATH=. POLARS_MAX_THREADS=1 python benchmark-scripts/genbank.py biopython narrow multi.gbk
PYTHONPATH=. POLARS_MAX_THREADS=1 python benchmark-scripts/genbank.py gb-io wide multi.gbk
PYTHONPATH=. POLARS_MAX_THREADS=1 python benchmark-scripts/genbank.py polars-bio full multi.gbk --threads 4
PYTHONPATH=. POLARS_MAX_THREADS=1 python benchmark-scripts/genbank.py microbiorust faa multi.gbk
```

## Parallel feature scans

On the same M3 Max, an eightfold concatenation of the five-record input produced a 362.8 MiB plain file with 40 LOCUS records and 279,480 features. The large input repeats annotations and has nearly ideal byte splits at 2, 4 and 8 partitions. The original five-record file and the single-record E. coli assembly provide controls for uneven and unsplittable inputs.

Each cell is median wall time in seconds over three warm-cache runs. Each run used a fresh Python process with one Polars worker. The target partition count was set with `pb.set_option("datafusion.execution.target_partitions", str(n))`. Timing covered public scan construction and collection, including partition discovery, decompression and materialization. The run order was shuffled in each round. The full view included all qualifiers, locations and parts. All partitions and compression modes had the same row count and order-independent checksum across all returned columns for each dataset and projection. A separate DataFusion EXPLAIN checked the physical scan partition count.

| Encoding | 1 requested | 2 requested | 4 requested | 8 requested | 1-to-8 speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| Plain | 0.968 s | 0.775 s | 0.422 s | 0.265 s | 3.65× |
| BGZF with GZI | 1.029 s | 0.506 s | 0.267 s | 0.160 s | 6.42× |
| BGZF without GZI | 0.993 s | 0.590 s | 0.351 s | 0.249 s | 3.99× |
| Ordinary gzip | 1.624 s | 1.623 s | 1.635 s | 1.625 s | 1.00× |

Plain and BGZF used the requested number of physical partitions. The two BGZF inputs had identical compressed bytes. A GZI supplies block offsets that the unindexed reader otherwise discovers during planning. Ordinary gzip used one physical decoding stream at every setting.

For a narrow plain-file scan of record ID, feature type and locus tag, elapsed time fell from 0.629 s to 0.211 s (2.98×) at eight partitions.

| Plain input, full view | Requested → physical partitions | Median elapsed |
| --- | --- | ---: |
| Original five records | 1 → 1 | 0.133 s |
| Original five records | 4 → 4 | 0.107 s |
| Original five records | 8 → 4 | 0.129 s |
| Single LOCUS | 1 → 1 | 0.043 s |
| Single LOCUS | 8 → 1 | 0.057 s |

The reader adjusts candidate splits to complete record boundaries. Several candidate offsets can map to the same boundary. One large chromosome therefore yields one read partition even when its feature table spans many Arrow batches.

To recreate the larger input and compare targets using the benchmark driver, start from the `multi.gbk` described above. The driver reports on-disk bytes per second, so use its elapsed seconds when comparing compressed encodings.

```sh
for i in 1 2 3 4 5 6 7 8; do cat multi.gbk; done > mixed.gbk
bgzip -@ 4 -i -I mixed.bgz.gzi -c mixed.gbk > mixed.bgz
cp mixed.bgz unindexed.bgz
gzip -4 -c mixed.gbk > mixed.gz
for n in 1 2 4 8; do
    PYTHONPATH=. POLARS_MAX_THREADS=1 python benchmark-scripts/genbank.py polars-bio full mixed.bgz --threads "$n"
done
```

## Biopython comparison on the 40-record input

Both readers materialized record ID, feature type, locus tag, gene, product, protein ID and translation as seven string columns in a Polars DataFrame. Three fresh-process runs per configuration used a warm file cache on the same Apple M3 Max. The benchmark includes reading, qualifier extraction and DataFrame construction. Each process used one Polars worker. All 27 outputs contained 279,480 rows and had the same order-independent SHA-256 digest, including duplicate rows.

| Input | Bio.SeqIO, 1 process | Bio.SeqIO, 8 processes | polars-bio, 1 partition | polars-bio, 8 partitions |
| --- | ---: | ---: | ---: | ---: |
| Plain | 9.372 s | 1.719 s | 0.788 s | 0.239 s |
| BGZF with GZI | 10.450 s | — | 0.857 s | 0.140 s |
| Ordinary gzip | 10.362 s | — | 1.438 s | — |

On plain input, polars-bio was 11.9× faster in the single-reader comparison and 7.2× faster in the eight-way comparison. Times are medians. The Biopython multiprocessing adapter discovers complete record ranges, spawns a fresh pool for each scan, calls `Bio.SeqIO.parse` in each process and concatenates the returned per-worker frames. Its timed region includes boundary discovery, process startup, result transfer and pool shutdown. The serial compressed baseline uses `gzip.open` with `Bio.SeqIO.parse`. The eight-process Biopython measurement covers plain input. Ordinary gzip uses one stream in polars-bio.

The inputs can be recreated with the commands in the parallel feature scans section. Run each plain-file comparison with the benchmark driver.

```sh
for reader in biopython polars-bio; do
    for n in 1 8; do
        PYTHONPATH=. POLARS_MAX_THREADS=1 python benchmark-scripts/genbank.py "$reader" wide mixed.gbk --threads "$n"
    done
done
```
