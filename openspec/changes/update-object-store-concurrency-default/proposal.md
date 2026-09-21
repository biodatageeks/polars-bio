# Change: Default to 8 concurrent object-store fetches

GitHub issue: https://github.com/biodatageeks/polars-bio/issues/459#issuecomment-5665668661
Upstream: https://github.com/biodatageeks/datafusion-bio-formats/pull/253

## Why

A whole-file scan from S3 (`pb.scan_a3m("s3://openfold/.../bfd_uniclust_hits.a3m")`)
took twice as long as `aws s3 cp` on the reporter's link. The root cause
(fixed upstream in datafusion-bio-formats#253) was that the S3 backend always
streamed the object over one connection. With that fix, `concurrent_fetches`
controls how many ranged requests are in flight for S3 as it already did for
GCS and HTTP — but polars-bio's `read_*`, `scan_*` and `describe_*` functions
default `concurrent_fetches=1`, which keeps the single sequential request, so
users only benefit if they know to pass `concurrent_fetches=8`.

The default is also inconsistent today: every `register_*` function in
`polars_bio/sql.py` already defaults to `concurrent_fetches=8` (documented as
"optimized for large scale operations"), except `register_fasta`, while the 32
`read_*`/`scan_*`/`describe_*` functions in `polars_bio/io.py` default to 1.
The same file therefore downloads at a different speed depending on whether it
is registered for SQL or scanned.

Measured on the 89 MB public A3M (fast link; the mechanism, not the absolute
number, is what matters): one sequential GET 4.8–29.5 s across runs, 8 × 8 MiB
concurrent ranged GETs 3.1–3.9 s (the `aws s3 cp` time), 8 MiB chunks with
concurrency 1: 8–11 s, slower than a single stream.

## What Changes

- `concurrent_fetches` defaults to `8` in every public `read_*`, `scan_*`,
  `describe_*` and `register_*` function that exposes it (33 signatures:
  32 in `polars_bio/io.py` plus `register_fasta` in `polars_bio/sql.py`).
  `chunk_size` stays at 8 MiB for reads/scans/describes and `register_fasta`,
  or 64 MiB for other registration functions (up to 64 or 512 MiB in flight).
- Docstrings describe the option uniformly: it applies to S3, GCS and HTTP;
  `1` disables parallel fetching. S3 whole-object reads then use one
  sequential request; HTTP/GCS may still use chunks and a HEAD preflight.
  GET-only HTTP compatibility depends on the reader path, not this value.
- `docs/features/cloud.md` marks S3 concurrent requests as supported and
  gains a short tuning section.
- No signature is removed. The new default can add a size preflight and
  increase requests and memory use. Explicit concurrency values are preserved.
- The lazy-scan object-storage fallback also defaults to 8 while preserving
  explicitly stored options.

## Ordering

All `datafusion-bio-format-*` dependencies now pin merged upstream revision
`fd17754c55c63394717967c18b7a45cf8aeb48ee` (datafusion-bio-formats#253).
This includes S3 concurrent ranged reads and the refused-HEAD fallback in
the shared full-object stream helper. This PR no longer depends on #460
merging first. Return to a release tag once one includes this revision.

## Impact

- Affected specs: `object-storage-io` (new capability)
- Affected code: `polars_bio/io.py`, `polars_bio/sql.py`,
  `docs/features/cloud.md`, `CHANGELOG.md`, `tests/test_object_store_defaults.py`,
  `tests/test_object_store_requests.py`, `Cargo.toml`, `Cargo.lock`
- Not changed: the Rust `ObjectStorageOptions` default (`concurrent_fetches=None`,
  core falls back to 1); Python always passes an explicit value. The internal
  `_describe_variant`, `describe_bgen` and `describe_pgen` header reads keep a
  hard-coded `concurrent_fetches=1` because they read only a header.
- Not changed: `chunk_size`. `read_*`/`scan_*` default to 8 MiB and
  `register_fasta` to 8 MiB and other `register_*` functions to 64 MiB, so a `register_*` call on the default now has up to
  512 MiB in flight per stream on S3 (GCS already behaved this way). Unifying
  `chunk_size` is a follow-up; 8 MiB × 8 measured best on the reference file.
- Six functions whose docstrings defer to their `read_*` counterpart
  (`scan_bcf`, `scan_bgen`, `scan_pgen`, `read_bgen_matrix`,
  `register_bigwig`, `register_bigbed`) get the new default but no new
  docstring line; the referenced function carries the description.
