# Change: Expose the parsed PVAR sidecar cache

## Why

After `refactor-pgen-companion-memory-model` every `scan_pgen`,
`describe_pgen`, `register_pgen`, and `read_pgen_matrix` call re-parses the
PVAR (~5 s and ~4.5 GB resident on the PGS Catalog 1000 Genomes panel).
Upstream adds a memory-mappable sidecar under the same change id; polars-bio
needs to let callers choose the cache mode and location.

## What Changes

- Add `companion_cache` (`"off"`, `"read_only"`, `"read_write"`; default
  `"read_only"`) and `cache_dir` to `PgenReadOptions` and to every PGEN entry
  point, forwarded like the companion caps.
- Document the sidecar, its key, and the memory effect in `reading.md`.

## Impact

- Affected specs: `pgen`.
- Affected code: `src/option.rs`, `src/scan.rs`, `polars_bio/io.py`,
  `polars_bio/sql.py`, `docs/features/reading.md`, `tests/test_pgen_io.py`.
- Depends on the upstream release carrying `add-pgen-companion-sidecar-cache`.
