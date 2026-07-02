# DataFrames support

polars-bio range operations accept a **file path**, a Polars `DataFrame`/`LazyFrame`, or a Pandas `DataFrame` as input, and can return any of them via `output_type`. How you construct the input DataFrame — its **backend** and column **dtypes** — has a real performance impact, because polars-bio exchanges data with the engine through Apache Arrow and avoids dtype conversion when the input is already Arrow-backed.

| Input path | How to construct | Notes |
|------------|------------------|-------|
| Direct Parquet (DataFusion) | pass the path: `pb.overlap("data.parquet", …)` | Fastest on large inputs; no Python DataFrame is materialized |
| Polars lazy | `pl.scan_parquet("data.parquet")` | Recommended; effectively ties with direct Parquet on large inputs |
| Polars eager | `pl.read_parquet("data.parquet")` | Zero-copy Arrow exchange |
| Pandas (Arrow dtypes) | `pd.read_parquet("data.parquet", engine="pyarrow", dtype_backend="pyarrow")` | Near-Polars performance; often the fastest path on small inputs |
| Pandas (default) | `pd.read_parquet("data.parquet")` | Slowest — NumPy-backed dtypes add conversion overhead |

!!! tip "Pandas with Arrow dtypes ≈ Polars"
    If you must work with Pandas, load it with `dtype_backend="pyarrow"` (and `engine="pyarrow"`).
    Arrow-backed Pandas reaches performance **on par with Polars** — and is often the fastest path
    on smaller interval workloads — whereas default NumPy-backed Pandas is consistently the slowest.
    See the
    [benchmark setup](../blog/posts/dataframe-paths-benchmark-2026-04.md#benchmark-setup) and
    [full results](../blog/posts/dataframe-paths-benchmark-2026-04.md) for the numbers.
