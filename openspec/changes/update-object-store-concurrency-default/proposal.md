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
  `chunk_size` stays at 8 MiB, so at most 64 MiB is in flight per stream.
- Docstrings describe the option uniformly: it applies to S3, GCS and HTTP;
  `1` selects one sequential request, which is also the choice for backends
  that refuse HEAD (pre-signed GET-only URLs).
- `docs/features/cloud.md` marks S3 concurrent requests as supported and
  gains a short tuning section.
- Behaviour change, not breaking: no signature is removed and every caller
  that passed `concurrent_fetches` explicitly is unaffected. Callers on the
  default now issue up to 8 parallel ranged requests plus one HEAD per object
  read instead of one GET.

## Ordering

The S3 benefit needs the `datafusion-bio-format-*` pin to include
datafusion-bio-formats#253 (carried by polars-bio#460). On the current pin,
the new default only changes GCS and HTTP reads, which already honoured it;
S3 keeps one stream until that pin lands. Merge after #460 or repin here.

## Impact

- Affected specs: `object-storage-io` (new capability)
- Affected code: `polars_bio/io.py`, `polars_bio/sql.py`,
  `docs/features/cloud.md`, `CHANGELOG.md`, `tests/test_object_store_defaults.py`
- Not changed: the Rust `ObjectStorageOptions` default (`concurrent_fetches=None`,
  core falls back to 1); Python always passes an explicit value. The internal
  `_describe_variant`, `describe_bgen` and `describe_pgen` header reads keep a
  hard-coded `concurrent_fetches=1` because they read only a header.
- Not changed: `chunk_size`. `read_*`/`scan_*` default to 8 MiB and
  `register_*` to 64 MiB, so a `register_*` call on the default now has up to
  512 MiB in flight per stream on S3 (GCS already behaved this way). Unifying
  `chunk_size` is a follow-up; 8 MiB × 8 measured best on the reference file.
- Six functions whose docstrings defer to their `read_*` counterpart
  (`scan_bcf`, `scan_bgen`, `scan_pgen`, `read_bgen_matrix`,
  `register_bigwig`, `register_bigbed`) get the new default but no new
  docstring line; the referenced function carries the description.
