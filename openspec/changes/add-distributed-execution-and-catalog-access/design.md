# Design: distributed execution and catalog-governed storage access

## Context

This change spans three repositories (polars-bio, datafusion-bio-formats,
datafusion-bio-functions) and adds two external systems to the picture: a set
of worker processes and a catalog. The findings below come from reading source
on 2026-11-28. Nothing was compiled or run, so every item marked *unverified*
is closed by a spike in phase 0 before the phase that depends on it starts.

### What datafusion-distributed needs from an embedder

- The distributed planner on the session, a list of worker URLs, and a gRPC
  worker per URL.
- For every custom `ExecutionPlan` that crosses the wire: a
  `PhysicalExtensionCodec` registered on coordinator and workers.
- For every custom leaf that should be split: a desired-task-count handler and
  a scale-up handler that returns one plan variant per task. Variants must
  share a schema and a partition count.

### How its planner treats plans it does not understand

| Situation | Planner behaviour (`inject_network_boundaries.rs`) | Consequence for polars-bio |
|---|---|---|
| Custom leaf, no task-count handler | Exactly one task | Safe: the scan is not split |
| No network boundary survives | Original plan is returned | Safe: local execution |
| Unknown non-leaf node | No restriction; inherits the children's task count | **Unsafe**: `IntervalJoinExec`, `PileupExec`, `FastqcExec` would each see a slice of their input |
| Build-side joins | Broadcast handling only for `HashJoinExec`, `NestedLoopJoinExec`, `CrossJoinExec` | `IntervalJoinExec` (CollectLeft, because polars-bio sets `repartition_joins=false`) is not covered |
| Stage boundaries | Only above hash `RepartitionExec`, above `BroadcastExec`, below `CoalescePartitionsExec` / `SortPreservingMergeExec` | Operators that already require contig-partitioned input get shuffles for free |

### What polars-bio and its upstream crates look like today

- `src/context.rs:209` builds the session with `SessionContext::new_with_bio`,
  which hides the `SessionStateBuilder` and installs `BioQueryPlanner`.
- datafusion-bio-formats v1.17.0 has 20 scan nodes and 5 write nodes. None has
  a codec, none uses `DataSourceExec`, and their fields are `pub(crate)`.
- `BamExec` and `VcfExec` already hold `partition_assignments:
  Vec<PartitionAssignment>` (regions per partition, balanced from index size
  estimates). `FastqExec` holds BGZF block ranges or byte ranges.
- Multi-partition reads are gated on `StorageType::LOCAL`. A remote file is one
  sequential partition.
- `bio-format-core` builds its S3 operator with `S3::default()` and reads
  endpoint and region from the environment; Azure takes an account key; GCS
  takes a key file or ambient credentials.
- datafusion-bio-functions v0.27.0: `CountOverlapsExec` and `NearestExec`
  collect the left side while planning and embed the index in the node.
  `MergeExec`, `ClusterExec`, `SubtractExec` and `ComplementExec` require
  input partitioned on contig. `PileupExec` and `FastqcExec` have one output
  partition and consume all input partitions themselves.
- DataFrame and LazyFrame inputs are registered as a `StreamingTable` over a
  one-shot Arrow C stream owned by Python (`src/scan.rs`).

### What the catalog side provides

- Lakekeeper Generic Tables: namespace, name, opaque `format`, `base-location`,
  free-form schema and properties; `read_data` / `write_data` permissions;
  vended credentials (S3 STS, Azure SAS, GCS OAuth token) scoped to the table
  location; S3 remote signing. Existing files can be registered. The catalog
  does not coordinate writes or validate schema.
- `pylakekeeper`: a small Python client (httpx, pydantic) with OAuth2 flows.
- iceberg-rust's REST client sends `header.X-Iceberg-Access-Delegation` when
  configured and passes vended storage credentials into the table's file IO.
  Its OpenDAL storage crate has no remote signing.
- datafusion-distributed ships `datafusion-distributed-iceberg`: read-only,
  version 0.1.0, not on crates.io, pinned to git revisions, storage for local,
  S3 and GCS. Its default file-IO codec sends the file IO's properties to
  workers; no code path was found that sends the separate storage-credentials
  list (*unverified*, spike 0.6).

## Goals / Non-Goals

**Goals**
- G1. A scan can be given its own credentials, including short-lived ones that
  are refreshed during the scan.
- G2. A user can read governed raw files (BAM, CRAM, VCF, FASTQ and the other
  supported formats) through a Lakekeeper generic table.
- G3. Scans and DataFusion-side operations can run across worker processes.
- G4. A distributed query returns the same rows as the single-node query, or
  runs locally, or fails with an error. It never returns different rows.
- G5. Nothing changes for a user who does not opt in.
- G6. Iceberg tables in the same catalog can be read and appended to.

**Non-Goals**
- Fault tolerance, task retry, or speculative execution.
- Launching or discovering workers.
- Distributing work that Polars executes after the LazyFrame boundary.
- Remote indexed reads, distributed `depth` / `fastqc`, worker-side bio-format
  sinks, S3 remote signing. These are listed as follow-ups in the proposal.

## Architecture

```
  Python driver process                                   Worker process (xN)
 ┌──────────────────────────────────────────┐            ┌──────────────────────────┐
 │ Polars LazyFrame  ◀── Arrow C stream ──┐ │            │ python -m polars_bio.worker
 │                                        │ │  plan      │                          │
 │ polars_bio (Python)                    │ │  fragment  │  same wheel, same        │
 │   io.py  catalog.py  distributed.py    │ │  (+creds)  │  build_session_state()   │
 │                                        │ │ ─────────▶ │  codecs, UDFs, BioConfig │
 │ polars_bio (Rust, PyO3)                │ │  gRPC/TLS  │                          │
 │   build_session_state()                │ │ ◀───────── │  runs its plan variant   │
 │   eligibility gate ─▶ distributed plan │ │  Arrow     │                          │
 │   head stage (coordinator) ────────────┘ │  batches   └────────────┬─────────────┘
 └───────────────┬──────────────────────────┘                         │ ranged reads
                 │ load table (bearer token)                          ▼
                 ▼                                         ┌──────────────────────────┐
        ┌────────────────┐   location + short-lived creds  │ shared filesystem or     │
        │   Lakekeeper   │ ──────────────────────────────▶ │ object store             │
        └────────────────┘                                 └──────────────────────────┘
```

The coordinator is the existing driver process. Workers are the same wheel
started with a different entry point. The catalog is consulted only by the
driver, during planning.

## Decisions

### D1. Five phases with one shared option surface

Credentials come first because both the catalog and the workers need them: a
catalog returns credentials that a scan must accept, and a worker must receive
whatever the scan was given. Phases 1 and 2 ship value on a single node.
Phase 5 is last because its dependency set is the least settled.

- **Alternative considered:** one change per phase. Rejected for now because
  the option surface (`storage_options`, credentials in the plan, catalog
  identifiers) is shared, and designing it once avoids rework. The phases are
  written so that the change can still be split at any phase boundary.

### D2. Credentials are per scan, explicit, and refreshable

Every public function that already takes object-storage options gains two
keyword arguments:

```python
storage_options: dict[str, str] | None = None
credential_provider: Callable[[], tuple[dict[str, str], int | None]] | None = None
```

- `storage_options` is a closed set of keys. An unknown key is an error.
  Key spellings follow the names Polars accepts for `scan_parquet`, so one
  dict works for both libraries, and the Iceberg REST property names
  (`s3.access-key-id`, `s3.session-token`, `adls.sas-token.*`,
  `gcs.oauth2.token`, ...) are accepted as aliases so that a catalog response
  can be passed through. The exact spellings are fixed in task 1.1.
- `credential_provider` returns options and an expiry (Unix seconds, or
  `None`). This is the shape Polars uses. polars-bio calls it before the first
  request and again when the expiry is within a margin (default 300 s).
- Precedence: provider, then `storage_options`, then the environment. When
  neither argument is given, behaviour is exactly today's.
- The credentials travel in `ObjectStorageOptions`, so each scan node carries
  its own. Two scans in one query can use different credentials.
- Redaction: `Debug`, `Display`, `EXPLAIN`, log lines, error messages and
  stored lazy-scan options show `***` for secret values.

In Rust the provider is an `Arc<dyn CredentialSource>` with a cached value and
an expiry. The Python callable is one implementation (it takes the GIL in a
blocking task; polars-bio already releases the GIL during execution). The
OpenDAL operators are built with explicit credentials when a source is present
and with today's builders otherwise.

- **Alternative considered:** set `AWS_*` in the process environment before a
  scan. Rejected: it is process-global, not safe with concurrent queries,
  cannot express two credentials, and does not cover Azure SAS or GCS tokens.
- **Alternative considered:** OpenDAL-native option names. Rejected: users
  already know the Polars names, and catalogs return the Iceberg names.

### D3. Catalog access lives in Python and uses `pylakekeeper`

```python
cat = pb.catalog.lakekeeper(base_url=..., warehouse=..., auth=...)
cat.files("genomics.cohort_2026")                      # list[str]
cat.scan("genomics.cohort_2026", path="NA12878.bam")   # LazyFrame
cat.scan("genomics.cohort_2026", glob="*.vcf.gz")      # one LazyFrame, many files
```

- `pylakekeeper` is an optional dependency behind `polars-bio[lakekeeper]`.
  Importing `polars_bio` never imports it. Calling `pb.catalog.lakekeeper`
  without it raises an error that names the extra.
- A load returns the base location, the format, and vended credentials. The
  resolver turns them into `storage_options` plus a `credential_provider`
  that reloads the table, so phase 1 does the rest.
- Reader selection: an explicit `format=` argument, then the generic table's
  `format` when it names a supported reader, then the file extension.
- Access delegation defaults to vended credentials. `client-managed` is an
  explicit opt-in. If the catalog returns no credentials and
  `client-managed` was not requested, the call fails. It does not fall back to
  the environment, because that would let a user who was denied by the catalog
  read with whatever the process happens to hold.
- File listing goes through the Rust OpenDAL layer with the same credentials,
  so it works on all three backends.

- **Alternative considered:** a Rust catalog client. Deferred: the single-node
  case does not need it, and `pylakekeeper` already implements the OAuth2
  flows. A Rust client becomes necessary only for worker-side refresh, which
  is a follow-up.

### D4. A multi-file dataset is a `UNION ALL` of single-file scans

Upstream table providers take one path. Instead of building multi-file
providers for 20 formats, `cat.scan(..., glob=...)` and list arguments to
`scan_*` register one table per file and plan a `UNION ALL`.

- Schemas must match. A mismatch is an error that names the first differing
  file and column (VCF files with different INFO fields are the common case;
  the user selects fields explicitly).
- Under distributed execution the planner replaces a union with an operator
  that runs each child in its own task, which gives file-level distribution
  without a split contract (*unverified*, spike 0.4).

- **Alternative considered:** `pl.concat` of LazyFrames. Rejected for the
  distributed case because the union would then happen in Polars, after the
  LazyFrame boundary, on the driver.

### D5. One session builder, shared by coordinator and worker

A single Rust function `build_session_state(config, role)` produces the
session state for both roles: `BioQueryPlanner`, the interval-join rule, the
range UDTFs, `depth`, `fastqc`, the `seq_*` functions, `BioConfig`, and (when
the `distributed` feature is on) the codecs. The coordinator role adds the
planner wrapper from D6 and the worker resolver.

- This removes the main source of coordinator/worker drift: a function
  registered on one side only.
- It needs upstream to expose the builder that `new_with_bio` uses today.
- Distributed mode is chosen when the session is built. `connect` and
  `disconnect` replace the session; tables registered by name must be
  registered again. This keeps the default session byte-for-byte what it is
  today, including the optimizer flag that the distributed planner turns off.
- `BioConfig` is registered as a propagated config extension so that
  `datafusion.bio.*` values (coordinate system, interval-join algorithm) reach
  workers.

### D6. An eligibility gate decides before the distributed planner runs

polars-bio does not hand planning to the library's query planner directly. It
wraps `BioQueryPlanner` in its own planner, which:

1. builds the single-node physical plan as today;
2. stops if distributed mode is off or the worker list is empty;
3. dry-run encodes the whole plan with the codec stack the coordinator would
   use. A node without a codec fails here (this covers Python-stream inputs
   and any format whose codec has not been written yet);
4. rejects an encoded plan larger than `distributed.max_plan_bytes`
   (default 64 MiB), which bounds in-memory tables shipped inside a plan;
5. calls the library's public `create_distributed_plan`.

`distributed.mode` controls what a rejection means:

| Mode | Ineligible plan | Use |
|---|---|---|
| `auto` (default once connected) | Runs locally; the reason is logged and shown by `explain` | Interactive work |
| `required` | Raises an error with the reason | Pipelines that must not silently run on one node |
| `off` | Gate is skipped | Temporarily disabling without disconnecting |

- **Alternative considered:** the library's blacklist of node types. Rejected
  as the primary mechanism: it lists what to exclude, so a new operator is
  distributed by default. The gate lists what is known to work.

### D7. Operators are single-task unless certified

A desired-task-count handler returns "exactly one task" for every polars-bio
and bio-functions operator that is not on a certified list. An operator is
certified when it has (a) a codec with a round-trip test, (b) a review for
logic that depends on its own partition index or on seeing all input, and
(c) a parity test against the single-node result on an in-process cluster.

Initial state at the end of phase 3: every range operator, `PileupExec`,
`FastqcExec`, `SchemaOverrideExec` and every write node are single-task. Write
nodes that request a single input partition already land in the coordinator's
head stage; BAM and CRAM writers with `sort_on_write` do not request it, so
they rely on this guard (*unverified* head-stage behaviour, spike 0.3).

Parity means equal as multisets. Row order is not part of the contract.

### D8. The split contract lives upstream; the glue lives in polars-bio

datafusion-bio-formats gains, behind a `proto` cargo feature:

- an encode/decode pair per scan node, aggregated into one codec;
- a trait implemented by splittable scans:

```rust
pub trait SplittableScan {
    /// Estimated compressed bytes and the largest useful task count.
    fn split_hint(&self) -> SplitHint;
    /// `tasks` disjoint variants that together cover the scan,
    /// all with the same schema and the same partition count.
    fn split(&self, tasks: usize, partitions_per_task: usize)
        -> Result<Vec<Arc<dyn ExecutionPlan>>>;
}
```

polars-bio implements the two datafusion-distributed handlers once, generically
over that trait. Upstream does not depend on datafusion-distributed, so its
release train is not coupled to it.

- BAM and VCF split by re-running the existing index-size balancing for
  `tasks × partitions_per_task` partitions and dealing them out in contiguous
  groups, padding with empty assignments so that all variants have equal
  partition counts. FASTQ splits its BGZF block ranges or byte ranges the same
  way.
- Single-partition scans (gzip, BED, FASTA, any remote file) return a hint of
  one task. They take part in file-level distribution only.
- The codec carries `ObjectStorageOptions` including credentials, residual
  filters (logical expressions) and schema metadata.

- **Alternative considered:** reading `DistributedTaskContext` inside
  `execute()` to pick a slice. Rejected: it makes upstream depend on the
  library and hides the split from `EXPLAIN`.
- **Alternative considered:** porting the readers to `DataSourceExec` /
  `FileSource` to reuse the built-in file-scan handlers. Rejected for this
  change: it is a rewrite of 20 readers and still needs codecs.

### D9. Strategy per interval operation

| Operation | Node | Strategy | Notes |
|---|---|---|---|
| `merge`, `cluster`, `subtract`, `complement` | `MergeExec`, `ClusterExec`, `SubtractExec`, `ComplementExec` | Certify; shuffles on contig appear because the nodes already require contig-partitioned input | Parallelism is bounded by contig count and skew. `ComplementExec` emits contigs that have no input rows and needs the partition-index review from D7 |
| `count_overlaps`, `coverage`, `nearest` | `CountOverlapsExec`, `NearestExec` | Codec ships the collected left-side batches and rebuilds the index on the worker | Above `distributed.max_embedded_index_bytes` (default 256 MiB) the operator stays single-task |
| `overlap` | `IntervalJoinExec` (CollectLeft) | A rule wraps the build side in `BroadcastExec` for join types that never emit unmatched build rows; every probe task receives the whole build side | Other join types stay single-task. *Unverified* that the public `BroadcastExec` is sufficient outside the library's own rule (spike 0.5) |
| `depth`, `fastqc` | `PileupExec`, `FastqcExec` | Single task | Follow-up: partial/final split |
| writes | `*WriteExec` | Coordinator head stage | `sort_on_write` relies on the D7 guard |

- **Alternative considered for `overlap`:** partitioned mode with a hash on
  contig. Kept as a later option. It needs no broadcast, but parallelism is
  capped by the number of contigs and chr1 is about five times chr21, so the
  broadcast of a small annotation side is the better default for the common
  reads-against-targets query.

### D10. Credentials on the wire, and what a worker may read

- Version 1 ships the credentials that were resolved at planning time inside
  the plan. The coordinator refreshes them before planning when the expiry is
  within `distributed.credential_min_ttl` (default 900 s). A query that
  outlives its credentials fails with an expiry error that names the scan.
- The coordinator refuses to send a plan that contains credentials over a
  channel without TLS, unless `distributed.allow_plaintext_credentials` is
  set. Local in-process clusters used in tests are exempt.
- Coordinator and workers share a bearer token; a worker rejects requests
  without it.
- A worker binds to loopback unless told otherwise, and takes an optional list
  of allowed roots (local path prefixes and object-store URI prefixes). A plan
  that names a location outside the list is rejected before it runs.

- **Alternative considered:** forward the user's catalog token and let every
  worker load its own credentials. Better (refreshable, the catalog stays the
  authority), but it needs a Rust catalog client on workers. Follow-up.

### D11. Workers are the same wheel at the same version

`python -m polars_bio.worker --bind HOST:PORT [--tls-cert --tls-key]
[--token-file] [--allow-root ...]` starts a tonic server from the extension
module with the GIL released. On `connect`, the coordinator asks each worker
for its version and keeps only workers whose polars-bio version equals its
own. If none remains, `connect` fails and lists what it found. The worker list
is static; calling `connect` again replaces it.

### D12. Iceberg through the catalog, with the commit on the coordinator

- `pb.register_iceberg(name, catalog=..., identifier=...)` registers a
  catalog-backed table provider in the session. It requests vended
  credentials and works with `pb.sql` and as an input to range operations.
- In distributed mode the scan uses the library's Iceberg integration, with a
  file-IO codec that carries the storage credentials if spike 0.6 confirms the
  default codec drops them.
- `pb.write_iceberg(source, catalog=..., identifier=..., mode="append")`
  writes Parquet data files in the tasks that produce the rows and commits one
  snapshot from the coordinator. A failure before the commit leaves the table
  unchanged; orphan data files may remain and are reported.
- The whole phase is behind an `iceberg` cargo feature and starts only after
  spike 0.6 shows a revision set that builds against DataFusion 55.2 / Arrow
  59. If it does not, the phase waits for the DataFusion 56 bump.

For a single node, `pl.scan_iceberg` and PyIceberg already work with polars-bio
through LazyFrames and need no code here; the docs gain a short section.

### D13. Dependencies and packaging

- `datafusion-distributed = "5.0.0"` behind a `distributed` cargo feature that
  release wheels enable. A build without the feature has no gRPC stack and
  `pb.distributed.connect` raises a clear error.
- The pin moves with the DataFusion major (6.x with DataFusion 56) and is added
  to the dependency comment block in `Cargo.toml`.
- Wheel size and fat-LTO link memory are measured in spike 0.1 against the
  100 MB budget before any code is merged.
- `pylakekeeper` is an optional extra and is not added to the bioconda run
  requirements.

## Risks / Trade-offs

- **Silent wrong results from an uncertified operator** → D6 gate plus D7
  guard, and a parity suite that runs every certified operator against the
  single-node result in CI.
- **API churn in a young library** (4→5 replaced the routing API; 5.x will not
  get fixes) → the integration is confined to `src/distributed/` and uses the
  smallest public surface: `create_distributed_plan`, the handler traits,
  `Worker`, `BroadcastExec`.
- **Credentials inside plans** → TLS rule, redaction, minimum TTL check.
- **A worker executes any plan it is sent** → token, loopback default, allowed
  roots. The deployment guide states that workers belong on a trusted network.
- **No fault tolerance** → documented; `required` mode makes failures loud.
- **All results return through the coordinator** → documented; the workloads
  that benefit are filters, aggregations, counts and interval operations whose
  output is smaller than their input.
- **Wheel size and build memory** → measured in spike 0.1; the feature flag is
  the escape hatch.
- **Parallelism of contig shuffles is uneven** → accepted for phase 4;
  bin-based partitioning is a later optimisation.

## Migration Plan

Nothing to migrate. All new behaviour is opt-in. Rollback for a user is
`pb.distributed.disconnect()` or `distributed.mode = "off"`; rollback for a
release is building without the `distributed` and `iceberg` features.

## Open Questions

1. Head-stage behaviour when a single-partition root is fed by a multi-task
   stage (spike 0.3).
2. Whether a union of single-file scans yields one task per file with only a
   trivial task-count handler (spike 0.4).
3. Whether the public `BroadcastExec` suffices for a non-hash join (spike 0.5).
4. Which iceberg-rust and DataFusion-integration revisions build against
   DataFusion 55.2, and whether vended storage credentials survive the
   library's file-IO codec (spike 0.6).
5. Exact `storage_options` key spellings, to be matched against Polars'
   documented keys (task 1.1).
6. Whether `ComplementExec` and `SubtractExec` use the local partition index
   in a way that breaks when partitions are spread over tasks (task 4.1).
