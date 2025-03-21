
## Supplemental material
This document provides additional information about the benchmarking setup, data, and results that were presented in the manuscript.

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

[AIList](https://github.com/databio/AIList) dataset was used for benchmarking.

|Dataset#  | Name            | Size(x1000) | Non-flatness |
|:---------|:----------------|:------------|:-------------|
|0         | chainRn4        | 2,351       | 6            |
|1         | fBrain          | 199         | 1            |
|2         | exons           | 439         | 2            |
|3         | chainOrnAna1    | 1,957       | 6            |
|4         | chainVicPac2    | 7,684       | 8            |
|5         | chainXenTro3Link| 50,981      | 7            |
|6         | chainMonDom5Link| 128,187     | 7            |
|7         | ex-anno         | 1,194       | 2            |
|8         | ex-rna          | 9,945       | 7            |

!!! note
    Test dataset in *Parquet* format can be downloaded from:

    * [databio.zip](https://drive.google.com/file/d/1lctmude31mSAh9fWjI60K1bDrbeDPGfm/view?usp=sharing)


### Single thread results
Results for `overlap`, `nearest`, `count-overlaps`, and `coverage` operations with single-thread performance on `apple-m3-max` and `gcp-linux` platforms.
```python exec="true"
import pandas as pd

BRANCH="master"
BASE_URL=f"https://raw.githubusercontent.com/biodatageeks/polars-bio-bench/refs/heads/{BRANCH}/results/paper/"
test_datasets = ["1-2", "8-7"]
test_operations = ["overlap", "nearest", "count-overlaps", "coverage"]
test_platforms = ["apple-m3-max", "gcp-linux"]


for p in test_platforms:
    print(f"#### {p}")
    for d in test_datasets:
        print(f"#### {d}")
        for o in test_operations:
            print(f"##### {o}")
            file_path = f"{BASE_URL}/{p}/{d}/{o}_{d}.csv"
            print(pd.read_csv(file_path).to_markdown(index=False, disable_numparse=True))
            print("\n")


```
### Parallel performance
Results for parallel operations with 1, 2, 4, 6 and 8 threads.
```python exec="true"
import pandas as pd
BRANCH="master"
BASE_URL=f"https://raw.githubusercontent.com/biodatageeks/polars-bio-bench/refs/heads/{BRANCH}/results/paper/"
test_platforms = ["apple-m3-max", "gcp-linux"]
parallel_test_datasets=["8-7"]
test_operations = ["overlap", "nearest", "count-overlaps", "coverage"]
for p in test_platforms:
    print(f"#### {p}")
    for d in parallel_test_datasets:
        print(f"#### {d}")
        for o in test_operations:
            print(f"##### {o}")
            file_path = f"{BASE_URL}/{p}/{d}-parallel/{o}_{d}.csv"
            print(pd.read_csv(file_path).to_markdown(index=False, disable_numparse=True))
            print("\n")

```
### End to end tests
Results for an end-to-end test with calculating overlaps and saving results to a CSV file.
```python exec="true"
import pandas as pd
BRANCH="master"
BASE_URL=f"https://raw.githubusercontent.com/biodatageeks/polars-bio-bench/refs/heads/{BRANCH}/results/paper/"
e2e_tests = ["e2e-overlap-csv"]
test_platforms = ["apple-m3-max", "gcp-linux"]
test_datasets = ["1-2", "8-7"]
for p in test_platforms:
    print(f"### {p}")
    for d in test_datasets:
        print("####", d)
        for o in e2e_tests:
            print(f"##### {o}")
            file_path = f"{BASE_URL}/{p}/{o}/{o}_{d}.csv"
            print(pd.read_csv(file_path).to_markdown(index=False, disable_numparse=True))
            print("\n")
```

#### Memory profiles

```python exec="1" html="1"
from io import StringIO
import pandas as pd
import matplotlib.pyplot as plt

BRANCH="master"
BASE_URL=f"https://raw.githubusercontent.com/biodatageeks/polars-bio-bench/refs/heads/{BRANCH}/results/paper/"
e2e_tests = ["e2e-overlap-csv"]
test_platforms = ["apple-m3-max", "gcp-linux"]
test_datasets = ["1-2", "8-7"]
tools = ["polars_bio", "polars_bio_streaming", "bioframe", "pyranges0", "pyranges1"]
for p in test_platforms:
    # print(f"### {p}")
    for d in test_datasets:
        # print("####", d)
        for o in e2e_tests:
            for t in tools:
                # print(f"##### {o}")
                url = f"{BASE_URL}/{p}/{o}/memory_profile/{d}/{t}_{d}.dat"
                df = pd.read_csv(url, sep='\s+', header=None,skiprows=1, names=["Type", "Memory", "Timestamp"])
                df["Time_sec"] = df["Timestamp"] - df["Timestamp"].iloc[0]

                # Create figure and axis
                fig, ax = plt.subplots(figsize=(10, 5))

                # Plot the data (without error bars)
                ax.plot(df["Time_sec"], df["Memory"], marker='x', color='black')
                ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray')

                # Add dashed lines to mark the peak memory usage
                max_memory = df["Memory"].max()
                time_at_max = df.loc[df["Memory"].idxmax(), "Time_sec"]
                ax.axhline(y=max_memory, color='red', linestyle='dashed')
                ax.axvline(x=time_at_max, color='red', linestyle='dashed')

                # Add labels and title
                ax.set_xlabel("Time (seconds)", fontsize=12)
                ax.set_ylabel("Memory used (MiB)", fontsize=12)
                ax.set_title(f"Memory usage profile for {t} on {p} with {d} dataset", fontsize=14)
                buffer = StringIO()
                plt.savefig(buffer, format="svg")
                print(buffer.getvalue())
                print("\n")


# Read the data:
# The file has three columns: a label ("MEM"), memory usage, and a timestamp.

```