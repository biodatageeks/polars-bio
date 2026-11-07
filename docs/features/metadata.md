# Frame metadata

polars-bio attaches coordinate conventions and source information to ordinary
Polars DataFrames and LazyFrames. Importing `polars_bio` initializes the
`config_meta` namespace and a bounded set of propagation methods. No custom
frame class or transform callback is required.

## Start with the interval readers

Use polars-bio's CSV and Parquet readers for genomic interval tables:

```python
import polars as pl
import polars_bio as pb

lf = pb.scan_parquet("intervals.parquet", zero_based=True)
# CSV equivalent: pb.scan_csv("intervals.csv", zero_based=True)
result = lf.filter(pl.col("start") > 100).select("chrom", "start", "end").collect()
```

`zero_based` declares the input convention: `True` for 0-based half-open intervals,
`False` for 1-based closed intervals. Optional `use_zero_based` converts the output
to the requested convention. Readers validate coordinate types, normalize column
names when requested, and attach coordinate/source metadata. Eager loading uses
`pb.read_csv()` and `pb.read_parquet()` with the same declarations.

Existing Polars frames can be tagged explicitly:

```python
lf = pl.scan_parquet("intervals.parquet")
lf.config_meta.set(coordinate_system_zero_based=True)
```

Manual tagging does not validate or convert coordinates. Plain Polars readers
remain appropriate for tables unrelated to genomic intervals.

## Read and update metadata

The built-in keys are `coordinate_system_zero_based`, `source_format`,
`source_path`, and `source_header`. The header is a JSON string containing the
complete available source information, including VCF samples/layout or alignment
tag types. Use `pb.get_metadata(frame)` for the parsed source/header view.

```python
snapshot = lf.config_meta.get_metadata()
lf.config_meta.set(source_path="corrected-source.parquet")
```

`set()` updates entries and returns `None`; do not assign its return value to a
frame. `get_metadata()` returns a shallow snapshot. Changing a top-level snapshot
entry does not change the frame. Extra nested mutable user values remain shared;
the built-in boolean/string values do not have this issue.

## Supported transformations

Metadata is copied through these operations without changing their normal types
or executing a lazy query solely to copy metadata:

| Frames | Methods |
| --- | --- |
| DataFrame and LazyFrame | `filter`, `select`, `with_columns`, `drop`, `rename`, `sort`, `slice`, `head`, `tail`, `limit`, `clone`, `cast`, `unique`, `reverse`, `explode` |
| DataFrame | `lazy`, indexing when it returns a DataFrame |
| LazyFrame | `collect`, including `engine="streaming"` |

Series and scalar results do not receive frame metadata. Grouping also supports
ordinary `group_by(...).agg(...)`, `len`, grouped `head`/`tail`, and
`having(...).agg(...)` on eager and lazy frames:

```python
spans = lf.group_by("chrom").agg(
    pl.col("start").min(),
    pl.col("end").max(),
)
```

A grouped query snapshots metadata when `group_by()` is called. Later retagging
of the source does not change that snapshot. Separate results own separate
top-level dictionaries.

Copied metadata describes the source and declared coordinate convention. It does
not prove that an arbitrary transformation produces valid intervals. A count-only
summary has no start/end columns; an expression that converts coordinates needs
the correct new tag. Prefer reader-level `use_zero_based=` for coordinate
conversion. Headers are source provenance, not automatically rewritten output
schema descriptions.

## Combining frames

Generic multi-input methods clear all result metadata, even when input tags agree:

| Frames | Methods |
| --- | --- |
| DataFrame and LazyFrame | `join`, `join_asof`, `join_where`, `update`, `merge_sorted` |
| DataFrame | `vstack`, `hstack`, `extend` |
| LazyFrame (Polars 1 only) | `with_context` |

Successful in-place combinations clear the receiver's metadata. Other inputs
retain their metadata. After checking and normalizing a combined result, explicitly
set its convention before using it as interval data. polars-bio range operations
such as `pb.overlap()` continue to validate inputs and tag their outputs themselves.

Module functions such as `pl.concat()` and `pl.collect_all()`, custom `map_groups`
callbacks, rolling/dynamic groups, group iteration, serialization, and unlisted
methods have no propagation guarantee. Native batch streams are unchanged; their
individual batches do not acquire an automatic metadata guarantee.

Missing metadata follows existing coordinate-validation rules. Strict mode rejects
unknown conventions; lenient mode can warn and assume the global convention when
both inputs are unknown. An explicitly tagged input paired with an unknown input
is rejected in either mode. These checks cannot detect an incorrect but present tag.

## Parquet persistence

```python
lf.config_meta.write_parquet("tagged.parquet", compression="snappy")
restored = pb.scan_parquet("tagged.parquet", zero_based=True)
```

The compatibility writer retains JSON under `polars_plugin_meta` in both Parquet
file metadata and the encoded Arrow schema. Options use the legacy PyArrow
vocabulary and default Snappy compression. A verified set of common options with
scalar schemas uses native Polars writing. Other options and nested/categorical
schemas fall back to PyArrow, which materializes a LazyFrame. This preserves
Arrow-schema metadata that native writing can otherwise drop for those schemas.
Ordinary `write_parquet()`/`sink_parquet()` calls do not automatically
serialize `config_meta`.

polars-bio readers still require the explicit source-coordinate declaration.
They do not automatically restore the embedded original source header or give
embedded coordinate tags precedence over that declaration.

## Migration from polars-config-meta

polars-bio no longer requires or imports the external distribution. Remove direct
`import polars_config_meta` statements and restart existing interpreters. The
distribution can remain installed for other environments, but initializing both
providers in one process is unsupported and produces a conflict error at owned
metadata boundaries. No import hook prevents another package from replacing APIs.

The compatibility surface is DataFrame/LazyFrame `config_meta.set()`,
`get_metadata()`, and `write_parquet()`. Series metadata, arbitrary namespace
forwarding, `merge`, `update`, `clear_metadata`, `ConfigMetaOpts`, diagnostics, and
standalone upstream metadata-reader imports are not provided. Replace mutation
of the old live metadata dictionary with `set()`.

The implementation wraps exactly 57 class/method pairs process-wide on Polars 1:
33 frame transformations, 10 grouping boundaries, and 14 invalidating operations.
On Polars 2.0/2.1, which removed `with_context`, the manifest has 56 entries.
polars-bio supports Polars 1.x from 1.40.0, 2.0.x, and 2.1.x; newer minor
versions remain excluded until their IO, streaming, and metadata behavior is
validated.
Untagged Polars frames pass through those wrappers too. Compatibility tests cover the
supported Polars versions; this is not native Polars propagation for every API.
