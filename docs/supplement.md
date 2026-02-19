## Benchmark setup

### Code and  benchmarking scenarios
[Repository](https://github.com/biodatageeks/polars-bio-bench)


### Operating systems and hardware configurations

#### macOS

- cpu architecture: `arm64`
- cpu name: `Apple M3 Max`
- cpu cores: `16`
- memory: `64 GB`
- kernel: `Darwin Kernel Version 24.2.0: Fri Dec  6 19:02:12 PST 2024; root:xnu-11215.61.5~2/RELEASE_ARM64_T6031`
- system: `Darwin`
- os-release: `macOS-15.2-arm64-arm-64bit`
- python: `3.12.4`
- polars-bio: `0.8.3`




#### Linux
[c3-standard-22](https://gcloud-compute.com/c3-standard-22.html) machine was used for benchmarking.

- cpu architecture: `x86_64`
- cpu name: `Intel(R) Xeon(R) Platinum 8481C CPU @ 2.70GHz`
- cpu cores: `22`
- memory: `88 GB`
- kernel: `Linux-6.8.0-1025-gcp-x86_64-with-glibc2.35`
- system: `Linux`
- os-release: `#27~22.04.1-Ubuntu SMP Mon Feb 24 16:42:24 UTC 2025`
- python: `3.12.8`
- polars-bio: `0.8.3`

### Software

- [Bioframe](https://github.com/open2c/bioframe)-0.7.2
- [PyRanges0](https://github.com/pyranges/pyranges)-0.0.132
- [PyRanges1](https://github.com/pyranges/pyranges_1.x)-[e634a11](https://github.com/mwiewior/pyranges1/commit/e634a110e7c00d7c5458d69d5e39bec41d23a2fe)
- [pybedtools](https://github.com/daler/pybedtools)-0.10.0
- [PyGenomics](https://gitlab.com/gtamazian/pygenomics)-0.1.1
- [GenomicRanges](https://github.com/BiocPy/GenomicRanges)-0.5.0


### Data
#### Real dataset
The [AIList](https://github.com/databio/AIList) dataset after transcoding into the Parquet file format (with the Snappy compression) was used for benchmarking.
This dataset was published with the AIList paper:

Jianglin Feng , Aakrosh Ratan , Nathan C Sheffield, *Augmented Interval List: a novel data structure for efficient genomic interval search*, Bioinformatics 2019.


| Dataset# | Name             | Size(x1000) | Description                                                                                 |
|:---------|:-----------------|:------------|---------------------------------------------------------------------------------------------|
| 0        | chainRn4         | 2,351       | [Source](https://hgdownload.soe.ucsc.edu/goldenPath/hg19/database/chainRn4.txt.gz)          |
| 1        | fBrain           | 199         | [Source](https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSM595923)                      |
| 2        | exons            | 439         | Dataset used in the BEDTools tutorial.                                                      |
| 3        | chainOrnAna1     | 1,957       | [Source](https://hgdownload.soe.ucsc.edu/goldenPath/hg19/database/chainOrnAna1.txt.gz)      |
| 4        | chainVicPac2     | 7,684       | [Source](https://hgdownload.soe.ucsc.edu/goldenPath/hg19/database/chainVicPac2.txt.gz)      |
| 5        | chainXenTro3Link | 50,981      | [Source](https://hgdownload.soe.ucsc.edu/goldenPath/hg19/database/chainXenTro3Link.txt.gz)  |
| 6        | chainMonDom5Link | 128,187     | [Source](https://hgdownload.soe.ucsc.edu/goldenPath/hg19/database/chainMonDom5Link.txt.gz)  |
| 7        | ex-anno          | 1,194       | Dataset contains GenCode annotations with ~1.2 million lines, mixing all types of features. |
| 8        | ex-rna           | 9,945       | Dataset contains ~10 million direct-RNA mappings.                                           |

Source: [AIList Github](https://github.com/databio/AIList?tab=readme-ov-file#test-results)


| Rank | Dataset 1        | Dataset 2        | # of overlaps   |
  |------|------------------|------------------|-----------------|
| 1    | chainMonDom5Link | chainXenTro3Link | 416,157,506,000 |
| 2    | chainMonDom5Link | chainVicPac2     | 248,984,248,721 |
| 3    | chainVicPac2     | chainXenTro3Link | 117,131,343,532 |
| 4    | chainMonDom5Link | chainOrnAna1     | 52,992,648,116  |
| 5    | chainMonDom5Link | chainRn4         | 27,741,145,443  |
| 6    | chainXenTro3Link | chainOrnAna1     | 26,405,758,645  |
| 7    | chainRn4         | chainXenTro3Link | 18,432,254,632  |
| 8    | chainVicPac2     | chainOrnAna1     | 6,864,638,705   |
| 9    | chainMonDom5Link | ex-rna           | 4,349,989,219   |
| 10   | chainRn4         | chainVicPac2     | 3,892,115,928   |
| 11   | ex-rna           | chainXenTro3Link | 1,830,555,949   |
| ---  | ---------------- | ---------------- | --------------- |
| 12   | chainRn4         | chainOrnAna1     | 1,086,692,495   |
| 13   | ex-rna           | ex-anno          | 307,184,634     |
| 14   | ex-rna           | chainVicPac2     | 227,832,153     |
| 15   | ex-rna           | chainRn4         | 164,196,784     |
| 16   | chainMonDom5Link | exons            | 116,300,901     |
| 17   | ex-rna           | chainOrnAna1     | 109,300,082     |
| 18   | chainXenTro3Link | exons            | 52,395,369      |
| 19   | ex-rna           | exons            | 36,411,474      |
| 20   | chainMonDom5Link | ex-anno          | 33,966,070      |
| 21   | chainXenTro3Link | ex-anno          | 13,693,852      |
| 22   | chainVicPac2     | exons            | 10,566,462      |
| 23   | ex-rna           | fBrain           | 8,385,799       |
| 24   | chainVicPac2     | ex-anno          | 5,745,319       |
| 25   | chainOrnAna1     | ex-anno          | 4,408,383       |
| 26   | chainOrnAna1     | exons            | 3,255,513       |
| 27   | chainRn4         | ex-anno          | 2,761,621       |
| 28   | chainRn4         | exons            | 2,633,098       |
| 29   | chainMonDom5Link | fBrain           | 2,380,147       |
| 30   | fBrain           | chainXenTro3Link | 625,718         |
| 31   | fBrain           | chainOrnAna1     | 398,738         |
| 32   | fBrain           | chainVicPac2     | 357,564         |
| 33   | chainRn4         | fBrain           | 320,955         |
| 34   | ex-anno          | exons            | 273,500         |
| 35   | fBrain           | ex-anno          | 73,437          |
| 36   | fBrain           | exons            | 54,246          |


Source: Calculated with polars-bio (using 0-based coordinates) in streaming mode.


All Parquet files from this dataset shared the same schema:
```sql
  contig STRING
  pos_start INT32
  pos_end INT32
```

#### Synthetic dataset
Randomly generated intervals (100-10,000,000) inspired by [bioframe](http://bioframe.readthedocs.io/en/latest/guide-performance.html) performance analysis.
Generated with [generate_dataset.py](https://github.com/biodatageeks/polars-bio-bench/blob/bioframe-data-generator/src/generate_dataset.py)
```shell
poetry run python src/generate_dataset.py
```
All Parquet files from this dataset shared the same schema:
```sql
  contig STRING
  pos_start INT64
  pos_end INT64
```

!!! note
    Test datasets in the *Parquet* format can be downloaded from:

    * single thread benchmarks
        * [databio.zip](https://drive.google.com/uc?id=1lctmude31mSAh9fWjI60K1bDrbeDPGfm)
        * [random_intervals_20250622_221714-1p.zip](https://drive.google.com/uc?id=1qCkSozLN20B2l6EiYYGqwthZk3_RzYZW)
    * parallel benchmarks (partitioned)
        * [databio-8p.zip](https://drive.google.com/uc?id=1Sj7nTB5gCUq9nbeQOg4zzS4tKO37M5Nd)
        * [random_intervals_20250622_221714-8p.zip](https://drive.google.com/uc?id=1ZvpNAdNFck7XgExJnJm-dwhbBJyW9VAw)


#### Overlap summary

| Test case | polars_bio<sup>1</sup> - # of overlaps | bioframe<sup>2</sup> - # of overlaps | pyranges0 - # of overlaps | pyranges1 - # of overlaps |
|:----------|:---------------------------------------|--------------------------------------|---------------------------|---------------------------|
| 1-2       | 54,246                                 | 54,246                               | 54,246                    | 54,246                    |
| 8-7       | 307,184,634                            | 307,184,634                          | 307,184,634               | 307,184,634               |
| 100       | 781                                    | 781                                  | 781                       | 781                       |
| 1000      | 8,859                                  | 8,859                                | 8,859                     | 8,859                     |
| 10000     | 90,236                                 | 90,236                               | 90,236                    | 90,236                    |
| 100000    | 902,553                                | 902,553                              | 902,553                   | 902,553                   |
| 1000000   | 9,007,817                              | 9,007,817                            | 9,007,817                 | 9,007,817                 |
| 10000000  | 90,005,371                             | 90,005,371                           | 90,005,371                | 90,005,371                |


<sup>1</sup> bioframe and pyranges are zero-based. In polars-bio >= 0.19.0, coordinate system is managed via DataFrame metadata. Use `pb.scan_*(..., use_zero_based=True)` to read data in 0-based coordinates.

<sup>2</sup> bioframe `how` parameter is set to `inner` (`left` by default)


### Summary statistics
![summary-results.png](assets/summary-results.png)

### Single-thread results
Results for `overlap`, `nearest`, `count-overlaps`, and `coverage` operations with single-thread performance on `apple-m3-max` and `gcp-linux` platforms.

!!! note
    Please note that in case of `pyranges0` we were unable to compute the results of *coverage* and *count-overlaps* operations for macOS and Linux in the synthetic benchmark, so the results are not presented here.


```python exec="true"
import pandas as pd
BRANCH="bioframe-data-generator"
BASE_URL=f"https://raw.githubusercontent.com/biodatageeks/polars-bio-bench/refs/heads/{BRANCH}/results/paper/"
test_datasets = ["1-2", "8-7", "100-1p", "1000-1p", "10000-1p", "100000-1p", "1000000-1p", "10000000-1p"]
test_operations = ["overlap", "nearest", "count-overlaps", "coverage"]
test_platforms = ["apple-m3-max", "gcp-linux"]


for p in test_platforms:
    print(f"#### {p}")
    for d in test_datasets:
        print(f"##### {d}")
        for o in test_operations:
            print(f"###### {o}")
            file_path = f"{BASE_URL}/{p}/{d}/{o}_{d}.csv"
            try:
                print(pd.read_csv(file_path).to_markdown(index=False, disable_numparse=True))
            except:
                pass
            print("\n")


```
### Parallel performance
Results for parallel operations with 1, 2, 4, 6 and 8 threads.

```python exec="true"
import pandas as pd
BRANCH="bioframe-data-generator"
BASE_URL=f"https://raw.githubusercontent.com/biodatageeks/polars-bio-bench/refs/heads/{BRANCH}/results/paper/"
test_platforms = ["apple-m3-max", "gcp-linux"]
parallel_test_datasets=["8-7-8p", "1000000-8p", "10000000-8p"]
test_operations = ["overlap", "nearest", "count-overlaps", "coverage"]
for p in test_platforms:
    print(f"#### {p}")
    for d in parallel_test_datasets:
        print(f"##### {d}")
        for o in test_operations:
            print(f"###### {o}")
            file_path = f"{BASE_URL}/{p}/{d}-parallel/{o}_{d}.csv"
            try:
                print(pd.read_csv(file_path).to_markdown(index=False, disable_numparse=True))
            except:
                pass
            print("\n")

```
### End to end tests
Results for an end-to-end test with calculating overlaps, nearest, coverage and count overlaps and saving results to a CSV file.

!!! note
    Please note that in case of `pyranges0` we were unable to export the results of *coverage* and *count-overlaps* operations to a CSV file, so the results are not presented here.

```python exec="true"
import pandas as pd
import logging
BRANCH="bioframe-data-generator"
BASE_URL=f"https://raw.githubusercontent.com/biodatageeks/polars-bio-bench/refs/heads/{BRANCH}/results/paper/"
e2e_tests = ["e2e-overlap-csv", "e2e-nearest-csv", "e2e-coverage-csv", "e2e-count-overlaps-csv"]
test_platforms = ["apple-m3-max", "gcp-linux"]
test_datasets = ["1-2", "8-7", "100-1p", "10000000-1p"]
for p in test_platforms:
    print(f"#### {p}")
    for d in test_datasets:
        print("#####", d)
        for o in e2e_tests:
            print(f"###### {o}")
            file_path = f"{BASE_URL}/{p}/{o}/{o}_{d}.csv"
            try:
                print(pd.read_csv(file_path).to_markdown(index=False, disable_numparse=True))
                print("\n")
            except:
                logging.warn(f"File not found: {file_path}\n")
```

##### Comparison of the output schemas and data types

`polars-bio` tries to preserve the output schema of the `bioframe` package, `pyranges` uses its own internal representation that can be converted to a Pandas dataframe. It is also worth mentioning that `pyranges` always uses `int64` for start/end positions representation (*polars-bio* and *bioframe* determine it adaptively based on the input file formats/DataFrames datatypes used. *polars-bio* does not support interval operations on chromosomes longer than **2Gp**([issue](https://github.com/biodatageeks/polars-bio/issues/169))). However, in the analyzed test case (`8-7`) input/output data structures have similar memory requirements.
Please compare the following schema and memory size estimates of the input and output DataFrames for `8-7` test case:
```python
import bioframe as bf
import polars_bio as pb
import pandas as pd
import polars as pl
import pyranges0 as pr0


DATA_DIR="/Users/mwiewior/research/polars-bio-benchmarking/data/"
df_1 = f"{DATA_DIR}/ex-anno/*.parquet"
df_2 = f"{DATA_DIR}/ex-rna/*.parquet"
df1 = pd.read_parquet(df_1.replace("*.parquet", ""))
df2 = pd.read_parquet(df_2.replace("*.parquet", ""))
cols = ["contig", "pos_start", "pos_end"]

def df2pr0(df):
    return pr0.PyRanges(
        chromosomes=df.contig,
        starts=df.pos_start,
        ends=df.pos_end,
    )
```

###### Input datasets sizes and schemas

```python
df1.info()
```
```shell
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 1194285 entries, 0 to 1194284
Data columns (total 3 columns):
#   Column     Non-Null Count    Dtype
---  ------     --------------    -----
0   contig     1194285 non-null  object
1   pos_start  1194285 non-null  int32
2   pos_end    1194285 non-null  int32
dtypes: int32(2), object(1)
memory usage: 18.2+ MB
```

```python
df2.info()
```
```shell
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 9944559 entries, 0 to 9944558
Data columns (total 3 columns):
 #   Column     Dtype
---  ------     -----
 0   contig     object
 1   pos_start  int32
 2   pos_end    int32
dtypes: int32(2), object(1)
memory usage: 151.7+ MB
```
###### polars-bio output DataFrames schema and memory used (Polars and Pandas)
```python
# Note: In polars-bio >= 0.19.0, coordinate system is read from DataFrame metadata
# Set metadata on DataFrames before range operations:
df_1.config_meta.set(coordinate_system_zero_based=True)
df_2.config_meta.set(coordinate_system_zero_based=True)
df_pb = pb.overlap(df_1, df_2, cols1=cols, cols2=cols)
df_pb.count().collect()
```

```shell
307184634
```

```python
df_pb.collect_schema()
```
```shell
Schema([('contig_1', String),
        ('pos_start_1', Int32),
        ('pos_end_1', Int32),
        ('contig_2', String),
        ('pos_start_2', Int32),
        ('pos_end_2', Int32)])
```

```python
df_pb.collect().estimated_size("mb")
```
```shell
7360.232946395874
```

```python
df_pb.collect().to_pandas().info()
```

```shell
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 307184634 entries, 0 to 307184633
Data columns (total 6 columns):
 #   Column       Dtype
---  ------       -----
 0   contig_1     object
 1   pos_start_1  int32
 2   pos_end_1    int32
 3   contig_2     object
 4   pos_start_2  int32
 5   pos_end_2    int32
dtypes: int32(4), object(2)
memory usage: 9.2+ GB

```



###### bioframe output DataFrame schema and memory used (Pandas)
```python
df_bf = bf.overlap(df1, df2, cols1=cols, cols2=cols, how="inner")
len(df_bf)
```
```shell
307184634
```

```python
df_bf.info()
```

```shell
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 307184634 entries, 0 to 307184633
Data columns (total 6 columns):
 #   Column      Dtype
---  ------      -----
 0   contig      object
 1   pos_start   int32
 2   pos_end     int32
 3   contig_     object
 4   pos_start_  int32
 5   pos_end_    int32
dtypes: int32(4), object(2)
memory usage: 9.2+ GB
```

###### pyranges0 output DataFrame schema and memory used (Pandas)
```python
df_pr0_1 = df2pr0(df1)
df_pr0_2 = df2pr0(df2)
```

```python
df_pr0_1.df.info()
```

```shell
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 1194285 entries, 0 to 1194284
Data columns (total 3 columns):
 #   Column      Non-Null Count    Dtype
---  ------      --------------    -----
 0   Chromosome  1194285 non-null  category
 1   Start       1194285 non-null  int64
 2   End         1194285 non-null  int64
dtypes: category(1), int64(2)
memory usage: 19.4 MB
```

```python
df_pr0_2.df.info()
```

```shell
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 9944559 entries, 0 to 9944558
Data columns (total 3 columns):
 #   Column      Dtype
---  ------      -----
 0   Chromosome  category
 1   Start       int64
 2   End         int64
dtypes: category(1), int64(2)
memory usage: 161.2 MB
```

```python
df_pr0 = df_pr0_1.join(df_pr0_2)
len(df_pr0)
```
```shell
307184634
```

```python
df_pr0.df.info()
```

```shell
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 307184634 entries, 0 to 307184633
Data columns (total 5 columns):
 #   Column      Dtype
---  ------      -----
 0   Chromosome  category
 1   Start       int64
 2   End         int64
 3   Start_b     int64
 4   End_b       int64
dtypes: category(1), int64(4)
memory usage: 9.4 GB
```

!!! Note
    Please note that `pyranges` unlike *bioframe* and *polars-bio* returns only one chromosome column but uses `int64` data types for encoding start and end positions even if input datasets use `int32`.

