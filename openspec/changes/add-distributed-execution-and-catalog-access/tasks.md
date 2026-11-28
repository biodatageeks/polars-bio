# Tasks: add-distributed-execution-and-catalog-access

Dependency order:
- Phase 0 is spikes on throwaway branches. 0.1–0.5 gate phase 3; 0.5 gates
  4.3; 0.6 gates phase 5. Phases 1 and 2 do not wait for phase 0.
- Phase 1 starts upstream in `datafusion-bio-formats` (1.1–1.2) and needs a
  tag before 1.3 can pin it.
- Phase 2 depends on phase 1.
- Phase 3 depends on phase 1 (credentials travel in the plan) and on upstream
  work in both `datafusion-bio-formats` (3.6–3.7) and
  `datafusion-bio-functions` (3.1). 3.1, 3.6 and 3.7 can run in parallel.
- Phase 4 depends on phase 3.
- Phase 5 depends on phases 1 and 3 and on the outcome of spike 0.6.

Each phase is releasable on its own. A phase is announced only after its
parity tests and its benchmark task are done.

## 0. Spikes (polars-bio, throwaway branches)

- [ ] 0.1 Add `datafusion-distributed = "5.0.0"` behind a `distributed`
  feature. Record the build time, the `release` and `dist` wheel sizes on
  macOS arm64 and Linux x86_64, and the peak link memory of the fat-LTO build.
  Compare with the 100 MB wheel budget.
- [ ] 0.2 Start two in-process workers. Show that a Parquet `GROUP BY` through
  `pb.sql` produces a distributed plan and that a BAM scan stays local.
- [ ] 0.3 Build a plan whose root has one output partition and is fed by a
  multi-task stage (a BAM write with `sort_on_write` over a shuffled
  aggregate). Record whether the root runs once or once per task.
- [ ] 0.4 Plan a `UNION ALL` of four single-file scans with a task-count
  handler that reports one task per scan. Record whether each child runs in
  its own task and whether any child runs twice.
- [ ] 0.5 Wrap the build side of an `IntervalJoinExec` in `BroadcastExec` by
  hand. Record whether every probe task receives the full build side and
  whether the result equals the single-node result.
- [ ] 0.6 Find a revision set of `iceberg`, `iceberg-catalog-rest` and the
  DataFusion integration that builds against DataFusion 55.2 / Arrow 59. Load
  a table from a local Lakekeeper with vended credentials and record whether
  the credentials reach a worker through the default file-IO codec.
- [ ] 0.7 Write the spike results into `design.md` (replace each *unverified*
  marker) and update the affected decisions before starting phase 3.

## 1. Per-scan storage credentials

- [ ] 1.1 (formats) Fix the `storage_options` key set against the keys Polars
  documents for `scan_parquet` and the Iceberg REST storage property names.
  Record the table in `docs/features/cloud.md`.
- [ ] 1.2 (formats) Add credential fields to `ObjectStorageOptions` and build
  the S3, Azure and GCS operators from them when present. Keep today's
  builders when absent. Redact secrets in `Debug` and `Display`.
- [ ] 1.3 (formats) Add a refreshable credential source with a cached value,
  an expiry and a refresh margin. Unit-test refresh, a failing source and
  concurrent readers.
- [ ] 1.4 (formats) Tag a release.
- [ ] 1.5 Pin the release. Add `storage_options` and `credential_provider` to
  every public function that accepts object-storage options, and to the PyO3
  option structs. Reject unknown keys.
- [ ] 1.6 Redact secrets in stored lazy-scan options, reprs, `EXPLAIN` output
  and error messages. Add a test that greps captured logs and reprs for a
  known secret.
- [ ] 1.7 Tests against a local S3 emulator: a session-token read; two scans
  with different credentials in one query; expiry followed by a refresh;
  a provider that raises; no arguments (ambient behaviour unchanged).
- [ ] 1.8 Tests for an Azure SAS token and a GCS bearer token at the option
  level (the operator is built with the token); live-backend tests are manual
  and recorded in the PR.
- [ ] 1.9 Update `docs/features/cloud.md` and `CHANGELOG.md`.

## 2. Lakekeeper catalog access

- [ ] 2.1 Add the `lakekeeper` extra to `pyproject.toml`. Add
  `polars_bio/catalog.py` with a lazy import and an error that names the extra.
- [ ] 2.2 Implement `pb.catalog.lakekeeper(...)` and a load that returns the
  location, the format, `storage_options` and a `credential_provider` that
  reloads the table.
- [ ] 2.3 Translate vended keys for S3, Azure and GCS. Fail when the catalog
  returns no credentials and `client-managed` was not requested.
- [ ] 2.4 Add a listing function in Rust over the OpenDAL layer that takes the
  same options, and expose it as `Catalog.files`.
- [ ] 2.5 Implement `Catalog.scan` for one file, with reader selection from
  `format=`, the table format, then the extension.
- [ ] 2.6 Implement multi-file scans as a `UNION ALL` of single-file tables,
  for `Catalog.scan(glob=...)` and for list arguments to `scan_*`. Report the
  first schema mismatch by file and column.
- [ ] 2.7 Integration tests with Lakekeeper and an S3 emulator in
  docker-compose: read with `read_data`; denied without it; expiry and
  reload; multi-file scan. Run them in a separate CI job.
- [ ] 2.8 Add `docs/features/catalog.md`, including the single-node Iceberg
  path through `pl.scan_iceberg`. Update `CHANGELOG.md`.

## 3. Distributed scans

- [ ] 3.1 (functions) Expose the `SessionStateBuilder` that `new_with_bio`
  builds, without changing `new_with_bio`. Tag a release.
- [ ] 3.2 Add `build_session_state(config, role)` and use it for the existing
  context. Assert in a test that the default session's registered functions,
  optimizer rules and config are unchanged.
- [ ] 3.3 Add the `distributed` feature, the planner wrapper and the
  eligibility gate (dry-run encode, plan-size cap). Implement `auto`,
  `required` and `off`.
- [ ] 3.4 Add the single-task guard for every uncertified operator. Unit-test
  the task count of plans that contain each range operator, `PileupExec`,
  `FastqcExec` and each write node.
- [ ] 3.5 Add `pb.distributed.connect`, `disconnect` and `explain`, with the
  version check and the static worker list.
- [ ] 3.6 (formats) Add the `proto` feature and codecs for `BamExec`,
  `VcfExec` and `FastqExec`, including object-storage options, residual
  filters and schema metadata. Round-trip tests per node.
- [ ] 3.7 (formats) Implement `SplittableScan` for the three nodes. Property
  test: for random task counts the variants are disjoint, complete, and share
  a schema and a partition count. Include unmapped reads for BAM.
- [ ] 3.8 Implement the task-count and scale-up handlers over
  `SplittableScan`. Register `BioConfig` as a propagated config extension.
- [ ] 3.9 Add the worker entry point with TLS, the bearer token, the loopback
  default and allowed roots.
- [ ] 3.10 Ship credentials in the plan. Enforce the TLS rule and the minimum
  TTL check.
- [ ] 3.11 File-level distribution for multi-file scans (uses 2.6 and the
  result of 0.4).
- [ ] 3.12 Parity suite on an in-process cluster: each of BAM, VCF and FASTQ
  with a filter, a projection, a limit and an aggregate, compared with the
  single-node result as sorted multisets, at 1, 2 and 5 workers.
- [ ] 3.13 Failure tests: a worker that cannot open the path; a worker that
  dies mid-query; a version mismatch; a missing token; `required` mode on an
  ineligible plan.
- [ ] 3.14 Add `docs/features/distributed.md`: what distributes and what does
  not, the shared-path requirement, security, and a Slurm example.
- [ ] 3.15 Benchmark BAM, VCF and FASTQ scans with a filter at 1, 2, 4 and 8
  nodes on a shared filesystem. Publish wall time and total CPU time.
- [ ] 3.16 (formats) Add codecs for the remaining scan nodes, one PR per
  format group. Each one extends the parity suite.

## 4. Distributed interval operations

- [ ] 4.1 (functions) Codecs for `MergeExec`, `ClusterExec`, `SubtractExec`
  and `ComplementExec`. Review each for logic that depends on the local
  partition index. Certify each one that passes parity.
- [ ] 4.2 (functions) Codecs for `CountOverlapsExec` and `NearestExec` that
  carry the left-side batches. Enforce the embedded-index size cap.
- [ ] 4.3 (functions) Codec for `IntervalJoinExec` and a rule that broadcasts
  the build side for join types that never emit unmatched build rows.
- [ ] 4.4 Parity suite: every operation, every supported join type, with
  randomly generated intervals and with a BAM-against-BED case, at 1, 2 and 5
  workers.
- [ ] 4.5 Benchmark `overlap` and `count_overlaps` of a WGS BAM against a
  target BED at 1, 2, 4 and 8 nodes. Publish the results.
- [ ] 4.6 Update `docs/features/distributed.md` and `CHANGELOG.md`.

## 5. Iceberg tables (starts only if spike 0.6 succeeds)

- [ ] 5.1 Add the Iceberg dependencies behind an `iceberg` feature at the
  revision set from 0.6. Measure the wheel size again.
- [ ] 5.2 Implement `pb.register_iceberg` with a REST catalog and vended
  credentials. Test with `pb.sql` and as an input to `overlap`.
- [ ] 5.3 Distributed Iceberg scans, with a file-IO codec that carries the
  storage credentials if 0.6 showed that the default one drops them.
- [ ] 5.4 Implement `pb.write_iceberg(..., mode="append")` with task-side data
  files and one coordinator commit. Test a failure before the commit.
- [ ] 5.5 Integration tests against Lakekeeper. Update the docs and
  `CHANGELOG.md`.

## 6. Release hygiene (per phase)

- [ ] 6.1 Run `openspec validate add-distributed-execution-and-catalog-access --strict`.
- [ ] 6.2 Confirm that the default test suite passes with distributed mode off
  and that no test in it starts a worker.
- [ ] 6.3 Update `bioconda-recipe/` for new build dependencies and confirm the
  recipe builds.
- [ ] 6.4 Add `datafusion-distributed` to the dependency-bump checklist in the
  `prepare-release` skill.
