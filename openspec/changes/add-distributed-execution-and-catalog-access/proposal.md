# Change: Distributed execution and catalog-governed storage access

GitHub issue: https://github.com/biodatageeks/polars-bio/issues/529

Analysis date: 2026-11-28. Sources read (nothing was compiled or run):
datafusion-distributed `ba63b52` (6.0.0+1) and tag `v5.0.0`,
datafusion-bio-formats `v1.17.0`, datafusion-bio-functions `v0.27.0`,
Lakekeeper `fee1729` (0.15.0), iceberg-rust `28970e0` (0.10.1).

## Why

polars-bio runs every query in one process. Two limits follow from that.

1. **Scale.** Decoding BAM, CRAM, VCF and FASTQ is CPU-bound. A cohort of
   thousands of files, or one query whose intermediate rows must be joined or
   aggregated across files, does not fit one node. Users currently fan out by
   hand (Slurm arrays, Ray, Dask) and lose the single-query model.
2. **Governed storage.** The readers take cloud credentials only from the
   process environment (`S3::default()` plus `AWS_*`, an Azure account key, a
   GCS key file). `ObjectStorageOptions` has no credential fields. So
   polars-bio cannot use short-lived, per-dataset credentials vended by a
   catalog, cannot read with an Azure SAS token or a GCS bearer token at all,
   and cannot read two datasets with different credentials in one query.

Two upstream developments make both solvable now:

- **datafusion-distributed 5.0.0** targets DataFusion 55 / Arrow 59, which is
  what polars-bio pins. It is a library: a distributed plan is an ordinary
  physical plan with network nodes, so the existing execute-and-stream path to
  Polars is unchanged.
- **Lakekeeper 0.15** catalogs non-Iceberg data through its Generic Table API
  and vends location-scoped credentials for it (S3, GCS, Azure), next to
  Iceberg tables in the same warehouse and permission model. A directory of
  BAM or VCF files can be a governed catalog entry.

The main technical risk is correctness, not feasibility: the distributed
planner places no restriction on operators it does not know, so
`IntervalJoinExec`, `PileupExec` and `FastqcExec` would each run per task on a
slice of their input and return wrong results without an error. This change is
designed around that risk (see design D6 and D7).

## What Changes

Five phases. Each is releasable on its own, and phases 1 and 2 are useful
without distributed execution.

1. **Per-scan storage credentials** (`storage-credentials`)
   - `storage_options` and `credential_provider` parameters on every public
     function that already accepts object-storage options.
   - S3 access key, secret and session token; Azure SAS token; GCS OAuth
     bearer token. Explicit credentials take precedence over the environment.
   - A refreshable provider so a long scan survives credential expiry.
   - Secrets are redacted from logs, reprs, plans and frame metadata.
2. **Lakekeeper catalog access** (`catalog-access`)
   - Optional extra `polars-bio[lakekeeper]` (depends on `pylakekeeper`).
   - Resolve a generic table to its location, format and vended credentials;
     list its files; scan one file or a set of files as one LazyFrame.
   - A catalog denial is an error. polars-bio never falls back to ambient
     credentials for a catalog-governed location.
3. **Distributed scans** (`distributed-execution`)
   - Opt-in `pb.distributed.connect([...])`; off by default; single-node
     behaviour unchanged.
   - Worker entry point `python -m polars_bio.worker`, shipped in the same
     wheel, with TLS, a shared token and a path allowlist.
   - An eligibility gate that keeps a plan local unless every node can be
     serialized, and a single-task guard for every operator not certified as
     distribution-safe.
   - Indexed BAM and VCF and BGZF FASTQ are split across tasks; any set of
     files is distributed one file per task.
4. **Distributed interval operations** (`distributed-execution`)
   - `merge`, `cluster`, `subtract`, `complement`: contig shuffles.
   - `count_overlaps`, `coverage`, `nearest`: the prebuilt index is shipped
     with the plan, up to a size cap.
   - `overlap`: the build side is broadcast to every probe task.
5. **Iceberg tables** (`iceberg-tables`), gated on a version-alignment spike
   - Register an Iceberg table from a REST catalog with vended credentials.
   - Distributed scans; append with data files written by tasks and one
     atomic commit by the coordinator.

No existing signature is removed and no default changes. There is no
**BREAKING** item.

## Out of Scope

- Fault tolerance. A failed worker fails the query; there is no task retry.
- Cluster management. polars-bio provides the worker entry point and takes a
  list of worker URLs. It does not launch workers on Slurm or Kubernetes.
- Operations that run in Polars after `scan_*` returns a LazyFrame. Only work
  executed by DataFusion is distributed.
- Remote indexed reads. Multi-partition reads are local-file only today, so
  one large object in S3 or GCS stays a single task. Distributed reads of such
  a file need either a path that every worker can open or the follow-up below.
- Distributed `depth` and `fastqc` (they need a partial/final split).
- Worker-side sinks for bio formats. Writes run on the coordinator.
- S3 remote signing, Delta and Lance readers, and creating or dropping catalog
  entries.
- Row-order guarantees (see issue #421).

## Follow-ups (separate changes)

- Remote indexed reads in datafusion-bio-formats (BAI, TBI, CSI, GZI over
  object storage).
- Partial/final `depth` and `fastqc`.
- Worker-side credential refresh with a forwarded catalog token.
- Worker discovery helpers for Slurm and Kubernetes.

## Impact

- Affected specs: `storage-credentials` (new), `catalog-access` (new),
  `distributed-execution` (new), `iceberg-tables` (new), `packaging`.
- Affected code in polars-bio: `Cargo.toml`, `Cargo.lock`, `pyproject.toml`,
  `src/context.rs`, `src/option.rs`, `src/scan.rs`, `src/lib.rs`, new
  `src/distributed/`, `polars_bio/io.py`, `polars_bio/sql.py`,
  `polars_bio/context.py`, new `polars_bio/catalog.py`,
  `polars_bio/distributed.py` and `polars_bio/worker.py`,
  `docs/features/cloud.md`, new `docs/features/distributed.md` and
  `docs/features/catalog.md`, `CHANGELOG.md`, `bioconda-recipe/`.
- Upstream, datafusion-bio-formats: credential fields and a refreshable
  credential source in `bio-format-core`; a `proto` feature with a codec per
  scan node; a split contract for indexed and BGZF scans.
- Upstream, datafusion-bio-functions: access to the `SessionStateBuilder`
  that `new_with_bio` builds; codecs for the range operators; index shipping
  for `CountOverlapsExec` and `NearestExec`; a broadcast-safe build side for
  `IntervalJoinExec`.
- New Rust dependencies: `datafusion-distributed` 5.0.0, `tonic`,
  `arrow-flight`. `datafusion-proto` 55.2.0 and `prost` are already in the
  lockfile. Phase 5 adds `iceberg`, `iceberg-catalog-rest` and the DataFusion
  integration.
- Release coupling: `datafusion-distributed` joins the bump chain
  (formats → functions → polars-bio). 5.0.0 pairs with DataFusion 55 and 6.x
  with DataFusion 56; the 5.x line was cut one day before 6.0.0, so expect no
  backports while `datafusion-python` holds polars-bio on 55.
- Related changes: `update-object-store-concurrency-default` (same option
  surface), `add-partition-parallel-lazyframe-range-ops` (inputs fed from
  Python streams stay local under this change).
