# BigWig / BigBed validation & benchmarks

Validation of the polars-bio BBI readers (`scan_bigwig` / `scan_bigbed`) against
[pyBigWig](https://github.com/deeptools/pyBigWig) (libBigWig), including the
predicate-pushdown fix ([datafusion-bio-formats#205], tag `v1.8.3`).

## Integration tests (run in CI)

| File | Needs pyBigWig | Covers |
|------|----------------|--------|
| `tests/test_io_bbi.py` | no | API, schema, autoSQL/rest, coordinate conversion, pushdown == client-side, register/SQL path |
| `tests/test_io_bbi_streaming.py` | no | lazy scan, `limit` pushdown across batch boundaries, streaming vs in-memory engine equality, streaming aggregation, region pushdown unclipped |
| `tests/test_io_bbi_parity.py` | yes (`importorskip`) | row-for-row equality vs pyBigWig (bigWig intervals, bigBed `rest`), pushdown-region parity |

Fixtures are committed under `tests/data/io/bbi/`. `large_signal.bw` (95 KB,
20,000 intervals on chr1 + 5,000 on chr2) is generated with pyBigWig so the
streaming/limit tests cross the 8,192-row BBI batch boundary.

## Reference datasets (large, external)

Used for the manual validation and benchmarks below (not downloaded in CI):

- **bigBed** — ENCODE `ENCFF001JBR.bigBed` (16 MB, RnaElements BED6+3, 21 chroms, 602,461 entries)
  `https://www.encodeproject.org/files/ENCFF001JBR/@@download/ENCFF001JBR.bigBed`
- **bigWig** — GEO `GSM7256643` fold-change-over-control, GRCh38 (546 MB, 135 chroms, 98,391,029 intervals)
  `https://ftp.ncbi.nlm.nih.gov/geo/samples/GSM7256nnn/GSM7256643/suppl/GSM7256643_ENCFF713VEX_fold_change_over_control_GRCh38.bigWig`

## Correctness results (vs pyBigWig)

| Dataset | Rows | Row count | Full DataFrame equality |
|---------|------|-----------|-------------------------|
| bigBed `ENCFF001JBR` | 602,461 | match | **exact** `(chrom, start, end, rest)` |
| bigWig `GSM7256643`  | 98,391,029 | match | **exact** `(start, end, value:f32)`, all 135 chromosomes |

Pushdown regression on real data (`chr1:1,000,000–1,010,000`): predicate-pushdown
result equals the client-side filter, and the interval straddling the upper
bound is returned with its **true, unclipped** end (max end `1,010,060` >
`1,010,000`).

## Performance (Apple M3 Max, polars-bio release `target-cpu=native`, warm cache, median of repeated runs)

| Operation | pyBigWig | polars-bio | |
|-----------|---------:|-----------:|---|
| bigWig full read (98.4 M rows) | 12,937 ms | **3,763 ms** | polars-bio **3.4×** |
| bigWig read chr1 (7.5 M rows)  | 960 ms | **302 ms** | polars-bio **3.2×** |
| bigWig region chr1:1M–1.01M    | **0.2 ms** | 3.7 ms | pyBigWig 18× |
| bigWig mean signal chr1 (exact, length-weighted) | 272 ms | 284 ms | tie (values equal, Δ=2e-16) |
| bigBed full read (602 K rows)  | 143 ms | **95 ms** | polars-bio **1.5×** |
| bigBed region chr1:1M–5M       | **0.1 ms** | 3.3 ms | pyBigWig 38× |

**Summary:** polars-bio wins on bulk / whole-genome / per-chromosome reads
(columnar Arrow, no per-row Python-object overhead) and ties on aggregation;
pyBigWig wins on tiny random-access region lookups (persistent C handle, no
per-query plan/registration overhead). Streaming aggregation over all 98.4 M
intervals runs in ~241 MB RSS (out-of-core), without materializing the result.

[datafusion-bio-formats#205]: https://github.com/biodatageeks/datafusion-bio-formats/pull/205
