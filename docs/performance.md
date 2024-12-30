

## Results summary

## Navigation
- [Binary operations](#binary-operations)
- [Parallel operations](#parallel-execution-and-scalability)
## Benchmarks
### Test datasets
[AIList](https://github.com/databio/AIList) dataset was used for benchmarking.

|Dataset#  | Name            |size(x1000) |non-flatness |
|:---------|:----------------|:-----------|:------------|
|0         | chainRn4        |2,351       |6            |
|1         | fBrain          |199         |1            |
|2         | exons           |439         |2            |
|3         | chainOrnAna1    |1,957       |6            |
|4         | chainVicPac2    |7,684       |8            |
|5         | chainXenTro3Link|50,981      |7            |
|6         | chainMonDom5Link|128,187     |7            |
|7         | ex-anno         |1,194       |2            |
|8         | ex-rna          |9,945       |7            |

!!! note
    Test dataset in *Parquet* format can be downloaded from:

    * for [single-threaded](https://drive.google.com/file/d/1lctmude31mSAh9fWjI60K1bDrbeDPGfm/view?usp=sharing) tests
    * for [parallel](https://drive.google.com/file/d/1Sj7nTB5gCUq9nbeQOg4zzS4tKO37M5Nd/view?usp=sharing) tests (8 partitions per dataset)

### Test libraries

- [Bioframe](https://github.com/open2c/bioframe)-0.7.2
- [PyRanges0](https://github.com/pyranges/pyranges)-0.0.132
- [PyRanges1](https://github.com/pyranges/pyranges_1.x)-master
- [pybedtools](https://github.com/daler/pybedtools)-0.10.0
- [PyGenomics](https://gitlab.com/gtamazian/pygenomics)-0.1.1
- [GenomicRanges](https://github.com/BiocPy/GenomicRanges)-0.5.0

## Binary operations


### Apple Silicon

- cpu architecture: `arm64`
- cpu name: `Apple M3 Max`
- cpu cores: `16`
- memory: `64 GB`
- kernel: `Darwin Kernel Version 24.2.0: Fri Dec  6 19:02:12 PST 2024; root:xnu-11215.61.5~2/RELEASE_ARM64_T6031`
- system: `Darwin`
- os-release: `macOS-15.2-arm64-arm-64bit`
- python: `3.12.4`
- polars-bio: `0.3.0`

#### Overlap operation
#### S-size, output < 1,000,000
##### S-size (1-2) - output: 54,246

| Library       |  Min (s) |  Max (s) | Mean (s) | Speedup   |
|---------------|----------|----------|----------|-----------|
| bioframe      | 0.100738 | 0.101541 | 0.101119 | 0.25x     |
| polars_bio    | 0.032156 | 0.035501 | 0.033394 | **0.77x** |
| pyranges0     | 0.024100 | 0.028271 | 0.025589 | **1.00x** |
| pyranges1     | 0.053770 | 0.054647 | 0.054121 | 0.47x     |
| pybedtools0   | 0.281969 | 0.283385 | 0.282857 | 0.09x     |
| pygenomics    | 1.424975 | 1.436369 | 1.430531 | 0.02x     |
| genomicranges | 0.972717 | 0.979013 | 0.975761 | 0.03x     |

##### S-size (2-7), output: 273,500

| Library       |  Min (s) |  Max (s) | Mean (s) | Speedup   |
|---------------|----------|----------|----------|-----------|
| bioframe      | 0.298039 | 0.309271 | 0.302905 | 0.30x     |
| polars_bio    | 0.089324 | 0.092200 | 0.090332 | **1.00x** |
| pyranges0     | 0.096478 | 0.103456 | 0.101023 | **0.89x** |
| pyranges1     | 0.195621 | 0.198025 | 0.197146 | 0.46x     |
| pybedtools0   | 1.004577 | 1.013097 | 1.007701 | 0.09x     |
| pygenomics    | 4.264575 | 4.275965 | 4.269055 | 0.02x     |
| genomicranges | 2.919675 | 2.926785 | 2.923549 | 0.03x     |


##### S-size (1-0) - output: 320,955

| Library       |  Min (s) |  Max (s) | Mean (s) | Speedup   |
|---------------|----------|----------|----------|-----------|
| bioframe      | 0.521093 | 0.549674 | 0.534084 | 0.28x     |
| polars_bio    | 0.135411 | 0.168570 | 0.147222 | **1.00x** |
| pyranges0     | 0.271539 | 0.282081 | 0.276298 | **0.53x** |
| pyranges1     | 0.418972 | 0.426373 | 0.422060 | 0.35x     |
| pybedtools0   | 1.258828 | 1.269215 | 1.264674 | 0.12x     |
| pygenomics    | 7.877381 | 7.908531 | 7.894108 | 0.02x     |
| genomicranges | 4.222082 | 4.266592 | 4.244865 | 0.03x     |





#### M-size, 1,000,000 < output  < 100,000,000


##### M-size (7-0), output: 2,761,621

| Library       |  Min (s) |  Max (s) | Mean (s) | Speedup   |
|---------------|----------|----------|----------|-----------|
| bioframe      | 0.935141 | 0.990710 | 0.970345 | 0.22x     |
| polars_bio    | 0.213880 | 0.220151 | 0.216288 | **1.00x** |
| pyranges0     | 0.408637 | 0.434262 | 0.422380 | **0.51x** |
| pyranges1     | 1.130004 | 1.162724 | 1.147292 | 0.19x     |
| pybedtools0   | 6.415976 | 6.467304 | 6.444268 | 0.03x     |
| pygenomics    | 9.588232 | 9.705035 | 9.653368 | 0.02x     |
| genomicranges | 9.017886 | 9.058916 | 9.033964 | 0.02x     |



##### M-size (7-3), output: 4,408,383

| Library       |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|---------------|-----------|-----------|-----------|-----------|
| bioframe      |  0.954765 |  0.969307 |  0.959704 | 0.21x     |
| polars_bio    |  0.198607 |  0.208906 |  0.203033 | **1.00x** |
| pyranges0     |  0.425277 |  0.430527 |  0.428594 | **0.47x** |
| pyranges1     |  1.190621 |  1.199869 |  1.194593 | 0.17x     |
| pybedtools0   |  9.403818 |  9.491574 |  9.453402 | 0.02x     |
| pygenomics    |  8.638968 |  8.662197 |  8.647764 | 0.02x     |
| genomicranges | 10.514233 | 10.556004 | 10.540377 | 0.02x     |


##### L-size (0-8), output: 164,196,784
| Library       |    Min (s) |    Max (s) |   Mean (s) | Speedup   |
|---------------|------------|------------|------------|-----------|
| bioframe      |  15.630508 |  16.719793 |  16.080009 | 0.19x     |
| polars_bio    |   2.882900 |   3.135100 |   2.997755 | **1.00x** |
| pyranges0     |   9.276095 |  10.158109 |   9.761880 | **0.31x** |
| pyranges1     |  13.076820 |  13.510234 |  13.329948 | 0.22x     |
| pybedtools0   | 322.922915 | 335.123071 | 329.659142 | 0.01x     |
| pygenomics    | 128.849536 | 132.109689 | 130.089096 | 0.02x     |
| genomicranges | 234.237435 | 239.315157 | 236.504565 | 0.01x     |


##### L-size (4-8), output: 227,832,153

| Library       |    Min (s) |    Max (s) |   Mean (s) | Speedup   |
|---------------|------------|------------|------------|-----------|
| bioframe      |  22.911206 |  23.118100 |  23.030572 | 0.16x     |
| polars_bio    |   3.541325 |   3.937760 |   3.684317 | **1.00x** |
| pyranges0     |  13.035069 |  13.510203 |  13.225005 | **0.28x** |
| pyranges1     |  20.924921 |  21.657297 |  21.398281 | 0.17x     |
| pybedtools0   | 505.897157 | 521.239276 | 511.310686 | 0.01x     |
| pygenomics    | 159.883847 | 160.942329 | 160.306970 | 0.02x     |
| genomicranges | 322.217280 | 322.490391 | 322.371662 | 0.01x     |


##### L-size (7-8), output: 307, 184,634

| Library       |    Min (s) |    Max (s) |   Mean (s) | Speedup   |
|---------------|------------|------------|------------|-----------|
| bioframe      |  29.128664 |  29.993182 |  29.518215 | 0.12x     |
| polars_bio    |   3.260438 |   3.897260 |   3.489278 | **1.00x** |
| pyranges0     |  16.615283 |  16.983202 |  16.753369 | **0.21x** |
| pyranges1     |  44.154733 |  44.496357 |  44.379647 | 0.08x     |
| pybedtools0   | 555.480532 | 559.947421 | 556.986772 | 0.01x     |
| pygenomics    | 156.724420 | 157.321514 | 156.935424 | 0.02x     |
| genomicranges | 416.095573 | 417.284236 | 416.700000 | 0.01x     |


#### XL-size, output > 1,000,000,000

##### XL-size (3-0), output: 1,086,692,495

| Library    |    Min (s) |    Max (s) |   Mean (s) | Speedup   |
|------------|------------|------------|------------|-----------|
| bioframe   | 124.244987 | 126.569689 | 125.435831 | 0.12x     |
| polars_bio |  12.650240 |  15.858913 |  14.776997 | **1.00x** |
| pyranges0  |  85.652054 |  94.383934 |  88.712706 | **0.17x** |
| pyranges1  |  92.802026 |  94.400313 |  93.447716 | 0.16x     |


##### XL-size (0-4), output: xxx

##### XL-size (0-5), output: xxx

### AMD
- cpu architecture: `x86_64`
- cpu name: `AMD EPYC 9B14`
- cpu cores: `4`
- memory: `63 GB`
- kernel: `#22~22.04.1-Ubuntu SMP Mon Dec  9 20:42:57 UTC 2024`
- system: `Linux`
- os-release: `Linux-6.8.0-1020-gcp-x86_64-with-glibc2.35`
- python: `3.12.8`
- polars-bio: `0.3.0`

#### Overlap operation
#### S-size, output < 1,000,000
##### S-size (1-2) - output: 54,246

| Library       |  Min (s) |  Max (s) | Mean (s) | Speedup   |
|---------------|----------|----------|----------|-----------|
| bioframe      | 0.094509 | 0.095311 | 0.094797 | 0.61x     |
| polars_bio    | 0.058527 | 0.066444 | 0.061503 | **0.95x** |
| pyranges0     | 0.057583 | 0.059461 | 0.058245 | **1.00x** |
| pyranges1     | 0.098868 | 0.107992 | 0.101964 | 0.57x     |
| pybedtools0   | 0.382701 | 0.384930 | 0.383619 | 0.15x     |
| pygenomics    | 2.335400 | 2.340616 | 2.338876 | 0.02x     |
| genomicranges | 1.648289 | 1.663941 | 1.657652 | 0.04x     |


##### S-size (2-7), output: 273,500

| Library       |  Min (s) |  Max (s) | Mean (s) | Speedup   |
|---------------|----------|----------|----------|-----------|
| bioframe      | 0.273727 | 0.275239 | 0.274383 | 0.60x     |
| polars_bio    | 0.161882 | 0.164253 | 0.163334 | **1.00x** |
| pyranges0     | 0.169721 | 0.171931 | 0.170678 | **0.96x** |
| pyranges1     | 0.304432 | 0.323747 | 0.311284 | 0.52x     |
| pybedtools0   | 1.477541 | 1.478301 | 1.477841 | 0.11x     |
| pygenomics    | 6.929725 | 6.932875 | 6.931662 | 0.02x     |
| genomicranges | 5.096514 | 5.105638 | 5.100280 | 0.03x     |

##### S-size (1-0) - output: 320,955

| Library       |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|---------------|-----------|-----------|-----------|-----------|
| bioframe      |  0.457869 |  0.460473 |  0.459397 | 0.55x     |
| polars_bio    |  0.251083 |  0.252582 |  0.251673 | **1.00x** |
| pyranges0     |  0.365083 |  0.376212 |  0.369148 | **0.68x** |
| pyranges1     |  0.593858 |  0.605304 |  0.600537 | 0.42x     |
| pybedtools0   |  1.834958 |  1.858740 |  1.844379 | 0.14x     |
| pygenomics    | 12.730241 | 12.771149 | 12.756920 | 0.02x     |
| genomicranges |  7.090998 |  7.121029 |  7.107298 | 0.04x     |


##### M-size (7-0), output: 2,761,621

| Library       |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|---------------|-----------|-----------|-----------|-----------|
| bioframe      |  0.873343 |  0.875288 |  0.874457 | 0.50x     |
| polars_bio    |  0.420260 |  0.450565 |  0.433827 | **1.00x** |
| pyranges0     |  0.559251 |  0.564516 |  0.561273 | **0.77x** |
| pyranges1     |  1.876350 |  1.888463 |  1.880867 | 0.23x     |
| pybedtools0   | 10.379844 | 10.430488 | 10.404292 | 0.04x     |
| pygenomics    | 15.553783 | 15.567857 | 15.562953 | 0.03x     |
| genomicranges | 15.517461 | 15.548186 | 15.535206 | 0.03x     |



##### M-size (7-3), output: 4,408,383

| Library       |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|---------------|-----------|-----------|-----------|-----------|
| bioframe      |  1.022998 |  1.028002 |  1.024980 | 0.40x     |
| polars_bio    |  0.397203 |  0.426743 |  0.412704 | **1.00x** |
| pyranges0     |  0.590809 |  0.602570 |  0.594928 | **0.69x** |
| pyranges1     |  2.027123 |  2.074861 |  2.045372 | 0.20x     |
| pybedtools0   | 15.957823 | 16.006681 | 15.988963 | 0.03x     |
| pygenomics    | 13.983596 | 13.994300 | 13.990662 | 0.03x     |
| genomicranges | 18.602139 | 18.625446 | 18.615777 | 0.02x     |

##### L-size (0-8), output: 164,196,784

| Library       |    Min (s) |    Max (s) |   Mean (s) | Speedup   |
|---------------|------------|------------|------------|-----------|
| bioframe      |  21.459718 |  21.516023 |  21.480410 | 0.29x     |
| polars_bio    |   5.713430 |   6.952107 |   6.129996 | **1.00x** |
| pyranges0     |  15.898455 |  16.227408 |  16.011707 | **0.38x** |
| pyranges1     |  21.721230 |  22.272518 |  21.917855 | 0.28x     |
| pybedtools0   | 575.612739 | 578.021023 | 577.165597 | 0.01x     |
| pygenomics    | 244.510614 | 245.508453 | 245.063967 | 0.03x     |
| genomicranges | 440.650408 | 440.737924 | 440.706206 | 0.01x     |


##### L-size (4-8), output: 227,832,153

| Library    |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|------------|-----------|-----------|-----------|-----------|
| bioframe   | 29.460466 | 29.864740 | 29.633731 | 0.34x     |
| polars_bio |  9.731893 | 10.180046 |  9.968996 | **1.00x** |
| pyranges0  | 21.637592 | 22.724399 | 22.011753 | **0.45x** |
| pyranges1  | 37.035666 | 37.531010 | 37.218867 | 0.27x     |

##### L-size (7-8), output: 307, 184,634

| Library    |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|------------|-----------|-----------|-----------|-----------|
| bioframe   | 38.547761 | 38.593432 | 38.573512 | 0.18x     |
| polars_bio |  6.356472 |  8.204682 |  6.980182 | **1.00x** |
| pyranges0  | 28.664496 | 28.878972 | 28.751498 | **0.24x** |
| pyranges1  | 80.373241 | 80.871479 | 80.546908 | 0.09x     |


### Intel

- cpu architecture: `x86_64`
- cpu name: `INTEL(R) XEON(R) PLATINUM 8581C CPU @ 2.30GHz`
- cpu cores: `4`
- memory: `61 GB`
- kernel: `#27~22.04.1-Ubuntu SMP Tue Jul 16 23:03:39 UTC 2024`
- system: `Linux`
- os-release: `Linux-6.5.0-1025-gcp-x86_64-with-glibc2.35`
- python: `3.12.8`
- polars-bio: `0.3.0`

#### Overlap operation
#### S-size, output < 1,000,000
##### S-size (1-2) - output: 54,246

| Library       |  Min (s) |  Max (s) | Mean (s) | Speedup   |
|---------------|----------|----------|----------|-----------|
| bioframe      | 0.080274 | 0.083350 | 0.082125 | 0.67x     |
| polars_bio    | 0.051923 | 0.060853 | 0.055115 | **1.00x** |
| pyranges0     | 0.057737 | 0.063692 | 0.060233 | **0.92x** |
| pyranges1     | 0.092273 | 0.104232 | 0.096598 | 0.57x     |
| pybedtools0   | 0.342928 | 0.350446 | 0.345739 | 0.16x     |
| pygenomics    | 1.933479 | 1.980263 | 1.958915 | 0.03x     |
| genomicranges | 1.317808 | 1.365975 | 1.345268 | 0.04x     |

##### S-size (2-7), output: 273,500

| Library       |  Min (s) |  Max (s) | Mean (s) | Speedup   |
|---------------|----------|----------|----------|-----------|
| bioframe      | 0.242910 | 0.250233 | 0.246872 | 0.59x     |
| polars_bio    | 0.142933 | 0.151324 | 0.146654 | **1.00x** |
| pyranges0     | 0.181919 | 0.184524 | 0.183063 | **0.80x** |
| pyranges1     | 0.303359 | 0.305036 | 0.304166 | 0.48x     |
| pybedtools0   | 1.303765 | 1.318575 | 1.310322 | 0.11x     |
| pygenomics    | 5.744573 | 5.917737 | 5.816145 | 0.03x     |
| genomicranges | 4.202981 | 4.298941 | 4.243175 | 0.03x     |


##### S-size (1-0) - output: 320,955

| Library       |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|---------------|-----------|-----------|-----------|-----------|
| bioframe      |  0.421461 |  0.449266 |  0.434152 | 0.53x     |
| polars_bio    |  0.228252 |  0.233000 |  0.230004 | **1.00x** |
| pyranges0     |  0.383663 |  0.401601 |  0.391000 | **0.59x** |
| pyranges1     |  0.563753 |  0.575554 |  0.570290 | 0.40x     |
| pybedtools0   |  1.617740 |  1.643310 |  1.631340 | 0.14x     |
| pygenomics    | 10.491757 | 10.753130 | 10.636810 | 0.02x     |
| genomicranges |  5.806456 |  5.880285 |  5.851234 | 0.04x     |


##### M-size (7-0), output: 2,761,621

| Library       |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|---------------|-----------|-----------|-----------|-----------|
| bioframe      |  0.900843 |  0.928098 |  0.917930 | 0.43x     |
| polars_bio    |  0.380828 |  0.408791 |  0.390157 | **1.00x** |
| pyranges0     |  0.580401 |  0.607483 |  0.595004 | **0.66x** |
| pyranges1     |  1.697365 |  1.705109 |  1.699965 | 0.23x     |
| pybedtools0   |  9.120270 |  9.384526 |  9.211789 | 0.04x     |
| pygenomics    | 13.123205 | 13.179993 | 13.160740 | 0.03x     |
| genomicranges | 13.230635 | 13.690668 | 13.472020 | 0.03x     |


##### M-size (7-3), output: 4,408,383

| Library       |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|---------------|-----------|-----------|-----------|-----------|
| bioframe      |  1.137155 |  1.142985 |  1.140749 | 0.35x     |
| polars_bio    |  0.382198 |  0.411443 |  0.396179 | **1.00x** |
| pyranges0     |  0.650236 |  0.675971 |  0.659619 | **0.60x** |
| pyranges1     |  1.818395 |  1.841851 |  1.826528 | 0.22x     |
| pybedtools0   | 14.588216 | 14.666769 | 14.621019 | 0.03x     |
| pygenomics    | 11.975859 | 12.196851 | 12.121281 | 0.03x     |
| genomicranges | 15.640415 | 15.839974 | 15.736289 | 0.03x     |


##### L-size (0-8), output: 164,196,784

| Library    |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|------------|-----------|-----------|-----------|-----------|
| bioframe   | 28.818453 | 28.956365 | 28.884398 | 0.21x     |
| polars_bio |  5.904987 |  6.562457 |  6.145784 | **1.00x** |
| pyranges0  | 22.664353 | 22.997717 | 22.806512 | **0.27x** |
| pyranges1  | 24.446387 | 24.804753 | 24.613135 | 0.25x     |


##### L-size (4-8), output: 227,832,153

| Library    |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|------------|-----------|-----------|-----------|-----------|
| bioframe   | 39.868340 | 40.109302 | 39.951601 | 0.25x     |
| polars_bio |  9.736690 | 10.277895 | 10.021107 | **1.00x** |
| pyranges0  | 31.146222 | 31.290984 | 31.208499 | **0.32x** |
| pyranges1  | 39.407547 | 40.279563 | 39.843926 | 0.25x     |


##### L-size (7-8), output: 307, 184,634

| Library    |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|------------|-----------|-----------|-----------|-----------|
| bioframe   | 51.923368 | 52.840132 | 52.354141 | 0.14x     |
| polars_bio |  6.604371 |  7.975253 |  7.151908 | **1.00x** |
| pyranges0  | 41.702499 | 42.557826 | 42.027393 | **0.17x** |
| pyranges1  | 73.713501 | 76.161131 | 74.770918 | 0.10x     |


### Google Axion
[//]: # (## Benchmarking)

[//]: # (polars-bio significantly outperforms other libraries in terms of speed and memory usage.)

[//]: # (It was benchmarked against following libraries:)

[//]: # ()
[//]: # ()
[//]: # ()
[//]: # ()
[//]: # ()
[//]: # (## Results)

[//]: # (### Overlap operation)

[//]: # (![results-overlap-0.1.1.png]&#40;assets/results-overlap-0.1.1.png&#41;)

[//]: # ()
[//]: # (### Nearest interval operation)

[//]: # (![results-nearest-0.1.1.png]&#40;assets/results-nearest-0.1.1.png&#41;)

#### Nearest (closest) operation

### Parallel execution and scalability

#### Intel

- cpu architecture: `x86_64`
- cpu name: `INTEL(R) XEON(R) PLATINUM 8581C CPU @ 2.30GHz`
- cpu cores: `16`
- memory: `118 GB`
- kernel: `#27~22.04.1-Ubuntu SMP Tue Jul 16 23:03:39 UTC 2024`
- system: `Linux`
- os-release: `Linux-6.5.0-1025-gcp-x86_64-with-glibc2.35`
- python: `3.12.8`
- polars-bio: `0.3.0`

#### 0-8 (input: 2,350,965 and 9,944,559,  output: 164,196,784)

##### Apple Silicon
| Library       |  Min (s) |  Max (s) | Mean (s) | Speedup   |
|---------------|----------|----------|----------|-----------|
| pyranges0-1   | 9.331440 | 9.399316 | 9.358115 | 0.31x     |
| polars_bio-1  | 2.810053 | 3.163260 | 2.935647 | **1.00x** |
| polars_bio-2  | 1.353191 | 1.422477 | 1.376621 | 2.13x     |
| polars_bio-4  | 1.020456 | 1.029563 | 1.024929 | 2.86x     |
| polars_bio-8  | 0.734393 | 0.738268 | 0.735762 | **3.99x** |



##### Intel
| Library       |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|---------------|-----------|-----------|-----------|-----------|
| pyranges0-1   | 22.856168 | 23.086879 | 22.958235 | 0.27x     |
| polars_bio-1  |  5.935124 |  6.694116 |  6.203911 | **1.00x** |
| polars_bio-2  |  3.763082 |  3.913454 |  3.815991 | 1.63x     |
| polars_bio-4  |  2.331916 |  2.358274 |  2.342218 | 2.65x     |
| polars_bio-8  |  1.317331 |  1.326317 |  1.322318 | **4.69x** |



#### 2-5 (input: 438,694 and 50,980,975,  output: 52,395,369)

##### Apple Silicon
| Library       |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|---------------|-----------|-----------|-----------|-----------|
| pyranges0-1   | 11.836572 | 12.033881 | 11.943536 | 0.41x     |
| polars_bio-1  |  4.878542 |  4.944363 |  4.912092 | **1.00x** |
| polars_bio-2  |  3.109014 |  3.113733 |  3.111639 | 1.58x     |
| polars_bio-4  |  1.928374 |  1.944733 |  1.935807 | 2.54x     |
| polars_bio-8  |  1.319147 |  1.334540 |  1.324507 | 3.71x     |
| polars_bio-16 |  0.751453 |  0.758128 |  0.754517 | **6.51x** |


#### 2-6 (input: 438,694 and 128,186,542, output: 116,300,901)

| Library       |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|---------------|-----------|-----------|-----------|-----------|
| pyranges0-1   | 29.674772 | 31.891295 | 30.546541 | 0.37x     |
| polars_bio-1  | 11.379310 | 11.423765 | 11.399042 | **1.00x** |
| polars_bio-2  |  7.134765 |  7.209546 |  7.163538 | 1.59x     |
| polars_bio-4  |  4.409859 |  4.462592 |  4.429911 | 2.57x     |
| polars_bio-8  |  3.069381 |  3.080261 |  3.073801 | 3.71x     |
| polars_bio-16 |  1.698058 |  1.736596 |  1.717683 | **6.64x** |


#### 3-0 (input:  1,956,864 and 2,350,965, output: 1,086,692,495

##### Apple Silicon

| Library      |   Min (s) |   Max (s) |  Mean (s) | Speedup   |
|--------------|-----------|-----------|-----------|-----------|
| pyranges0-1  | 86.613871 | 86.613871 | 86.613871 | 0.14x     |
| polars_bio-1 | 12.626873 | 19.909944 | 17.401360 | **1.00x** |
| polars_bio-2 | 10.837240 | 15.490195 | 12.717995 | 1.37x     |
| polars_bio-4 |  7.708758 |  7.817039 |  7.754055 | 2.24x     |
| polars_bio-8 |  6.023458 |  6.521387 |  6.295188 | **2.76x** |



##### Intel
| Library      |    Min (s) |    Max (s) |   Mean (s) | Speedup   |
|--------------|------------|------------|------------|-----------|
| pyranges0-1  | 158.193622 | 159.014103 | 158.563798 | 0.38x     |
| polars_bio-1 | 35.225662 | 35.821574 | 35.573672 | **1.00x** |
| polars_bio-2 | 24.591723 | 25.029197 | 24.797555 | 1.43x     |
| polars_bio-4 | 16.198270 | 16.867106 | 16.497054 | 2.16x     |
| polars_bio-8 | 11.666194 | 11.735179 | 11.699761 | **3.04x** |


### Native, Pandas, Polars performance comparison

## How to run the benchmarks
```bash
poetry env use python3.12
poetry update
poetry shell
RUSTFLAGS="-Ctarget-cpu=native" maturin develop --release  -m Cargo.toml
python benchmark/src/bench_overlap.py
```