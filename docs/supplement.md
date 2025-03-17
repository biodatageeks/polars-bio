
## Benchmark setup

### Operating System, CPU, and Memory

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

* [Tools and versions](performance.md#test-libraries)
* [Datasets](performance.md#test-datasets)

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