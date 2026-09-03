## Context

The size gate lives in the upstream provider's `PgenReadOptions` defaults
(`max_companion_bytes` 512 MiB, `max_decompressed_companion_bytes` 1 GiB,
`max_variants` 100M, `max_samples` 10M). polars-bio's `native_pgen_options`
(src/scan.rs:534) forwards only `max_range_gap`, `max_range_bytes`, and
`batch_soft_byte_limit` and takes the rest from `Default`. The caps guard an
eager materialization whose measured cost is 5–10× the decoded PVAR text, so
the memory model has to change before the caps can move. That work is
upstream; this change carries the API surface and the dependency bump. See
the upstream `design.md` under the same change id for the streaming and
columnar-table decisions.

## Goals / Non-Goals

- Goals: the published 1000G panels open with default arguments; every PGEN
  entry point can raise or lower the companion caps; a limit error tells the
  caller which argument to change.
- Non-Goals: caching parsed companions across calls; changing the PGEN
  schema; exposing `max_samples`, `max_header_bytes`, or `max_record_bytes`,
  which no reported fileset approaches.

## Decisions

- **Decision: expose three caps, not all six.** `max_companion_bytes`,
  `max_decompressed_companion_bytes`, and `max_variants` are the ones a real
  fileset can hit; the others are header and record sanity bounds. They follow
  the existing pattern: `Optional[int]`, `None` keeps the provider default,
  forwarded through `PgenReadOptions` and `native_pgen_options` with
  `unwrap_or(defaults.x)`.
- **Decision: `describe_pgen` gets the caps too.** It opens the fileset like
  a scan and was the first call in the report to fail, so it must accept the
  same controls as the other entry points, as `pgi_path` already does.
- **Decision: matrix positions cross the boundary as a NumPy array.**
  `PyPgenMatrixReader::positions` returns `Vec<u64>`, which PyO3 converts to
  a `list` of Python integers (~36 B each) before `np.asarray` copies it
  again. It will fill a caller-allocated `int64` array through the same
  buffer path `read_into` already uses, so the transfer costs 8 B per row
  and no Python objects. `PgenMatrix.positions` keeps its dtype and shape.
- **Decision: tests assert forwarding and the error, not the panel.** The
  panel is 9 GiB and cannot be a fixture. Tests use `_captured_pgen_options`
  to prove the values reach the options object, lower a cap below the oracle
  fixture's size to prove the error names the argument, and confirm a raised
  cap does not change content. The real-panel run is recorded in the change
  tasks and the PR, not in CI.

## Risks / Trade-offs

- The fix depends on an upstream release; until the tag lands, polars-bio
  verifies through a local `[patch]` that must not be committed.
- `read_pgen_matrix` still has no variant selection, so the full panel's
  dense output (224 GiB int8, 896 GiB float32) cannot be requested; that is
  the separate change `add-pgen-matrix-variant-selection`. Repeated opens
  re-parse the companion; that is `add-pgen-companion-sidecar-cache`.
- Resident memory on the full panel is still ~4.5–5 GB for the variant table
  at 75–85M rows; documented in `reading.md` alongside the caps.

## Migration Plan

Additive keyword arguments and a dependency bump; minor release. No caller
changes required.
