# Parallel processing

polars-bio runs in parallel on top of [Apache DataFusion](https://datafusion.apache.org/). A single global setting — `datafusion.execution.target_partitions` — controls the degree of parallelism for **both** sides of a pipeline: reading input files **and** running interval/range operations.

## Parallel engine 🏎️

Set the degree of parallelism with the `datafusion.execution.target_partitions` option, e.g.:

```python
import polars_bio as pb
pb.set_option("datafusion.execution.target_partitions", "8")
```

!!! tip
    1. The default value is **1** (parallel execution disabled).
    2. `datafusion.execution.target_partitions` is a **global** setting and affects **all** operations in the current session — both reading files and running range operations.
    3. Check [available strategies](../performance.md#parallel-execution-and-scalability) for optimal performance.
    4. See the other configuration settings in the Apache DataFusion [documentation](https://datafusion.apache.org/user-guide/configs.html).

The same setting drives both:

- **Input reads** — files are split into partitions that are decoded in parallel (index-based region splitting, parallel BGZF decoding). See [Parallel reads & partitioning](reading.md#parallel-reads-partitioning) for the per-format behavior.
- **Range operations** — `overlap`, `nearest`, `count_overlaps`, `coverage`, and the other [genomic operations](operations.md) process partitions across threads.

Raising `target_partitions` allows more parallel work when the reader and input
support it. It does not guarantee more source partitions or a speedup, and it can
increase memory use. See [alignment and structure scaling limits](reading.md#alignment-and-structure-scaling-limits)
for format-specific constraints.

## Limiting memory

By default the engine's memory pool is unbounded. Set `datafusion.runtime.memory_limit` to cap it, so memory-hungry operators such as sorts and grouped aggregates spill to disk instead of growing without bound (or fail with `ResourcesExhausted` when they cannot spill):

```python
import polars_bio as pb
pb.set_option("datafusion.runtime.memory_limit", "4G")
# Optional: where spill files go (defaults to the OS temp directory)
pb.set_option("datafusion.runtime.temp_directory", "/scratch/tmp")
```

!!! note
    1. The limit covers DataFusion operators (SQL, interval operations, aggregates). It does not cover the final Polars `DataFrame` built by `.collect()`.
    2. `pb.get_option()` and `information_schema.df_settings` report the active value. To remove the limit use `pb.sql("RESET datafusion.runtime.memory_limit")`: `"unlimited"` is not accepted as a value, and `"0M"` means a zero-byte limit, not "no limit".
    3. Other `datafusion.runtime.*` options (`max_temp_directory_size`, `metadata_cache_limit`, `list_files_cache_limit`, `list_files_cache_ttl`) are accepted too.
