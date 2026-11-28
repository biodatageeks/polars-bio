## ADDED Requirements

### Requirement: Opt-in distributed mode

Distributed execution SHALL be disabled by default. It SHALL be enabled only
by an explicit call that supplies worker addresses, and SHALL be reversible
within the same process. With distributed mode disabled, query planning,
results and performance characteristics SHALL be the same as before this
change.

#### Scenario: Default session
- **WHEN** a user imports polars-bio and runs a query without connecting to
  workers
- **THEN** the query SHALL run in the local process
- **AND** no network listener or connection SHALL be opened

#### Scenario: Connect and disconnect
- **WHEN** a user calls `pb.distributed.connect([...])` and later
  `pb.distributed.disconnect()`
- **THEN** queries issued between the two calls SHALL be considered for
  distribution
- **AND** queries issued after `disconnect` SHALL run locally

### Requirement: Worker process

The polars-bio wheel SHALL provide a worker entry point that serves plan
fragments sent by a coordinator. A worker SHALL register the same functions,
table functions, operators and configuration extensions as the coordinator
session.

#### Scenario: Start a worker
- **WHEN** a user runs `python -m polars_bio.worker --bind HOST:PORT`
- **THEN** the process SHALL serve distributed tasks on that address until it
  is stopped

#### Scenario: Function parity
- **WHEN** a distributed query calls a polars-bio SQL function such as
  `seq_revcomp`
- **THEN** the function SHALL resolve on the worker and return the same values
  as on a single node

#### Scenario: Session options reach workers
- **WHEN** the coordinator session sets a `datafusion.bio.*` option, such as
  the coordinate system, before a distributed query
- **THEN** every task of that query SHALL run with the same value

### Requirement: Worker version match

The coordinator SHALL send tasks only to workers that report the same
polars-bio version as the coordinator.

#### Scenario: Mismatched worker
- **WHEN** `connect` is given a worker that reports a different version
- **THEN** that worker SHALL NOT receive tasks
- **AND** the mismatch SHALL be reported to the user

#### Scenario: No compatible worker
- **WHEN** no listed worker reports the coordinator's version
- **THEN** `connect` SHALL raise an error that lists each worker and the
  version it reported

### Requirement: Eligibility gate and local fallback

Before distributing a query, polars-bio SHALL check that every node of its
physical plan can be serialized and that the serialized plan is within the
configured size limit. A plan that fails the check SHALL NOT be distributed.
The `distributed.mode` option SHALL select what happens next: `auto` runs the
plan locally, `required` raises an error, and `off` skips distribution.

#### Scenario: Input fed from a Python DataFrame
- **WHEN** a query in `auto` mode reads a Polars or Pandas DataFrame that was
  passed from Python
- **THEN** the query SHALL run locally and return the single-node result

#### Scenario: Format without a codec
- **WHEN** a query in `auto` mode scans a format whose scan operator cannot be
  serialized
- **THEN** the query SHALL run locally and SHALL NOT fail

#### Scenario: Required mode
- **WHEN** a query in `required` mode fails the eligibility check
- **THEN** an error SHALL state which node made the plan ineligible

#### Scenario: Explain
- **WHEN** a user calls `pb.distributed.explain` on a query
- **THEN** the output SHALL show either the stages and task counts of the
  distributed plan or the reason the query runs locally

### Requirement: Result equivalence

A distributed query SHALL return the same multiset of rows as the same query
run on a single node. An operator that has not been certified as safe to run
in more than one task SHALL run in exactly one task. Row order is not
guaranteed in either mode.

#### Scenario: Uncertified operator
- **WHEN** a distributed plan contains an operator that is not certified, such
  as the pileup operator behind `pb.depth`
- **THEN** the stage that contains it SHALL run in exactly one task
- **AND** the result SHALL equal the single-node result

#### Scenario: Certified operator
- **WHEN** a certified scan or operator runs in several tasks
- **THEN** the rows it produces across all tasks SHALL equal, as a multiset,
  the rows it produces on a single node

#### Scenario: Writes
- **WHEN** a distributed query ends in a bio-format write
- **THEN** exactly one output SHALL be written, by one task

### Requirement: Distributed scans

Indexed BAM and VCF scans and BGZF FASTQ scans with a block index SHALL be
split across tasks into disjoint parts that together cover the scan. Filter,
projection and limit pushdown SHALL apply within each task. A scan that
cannot be split SHALL run in one task.

#### Scenario: Indexed BAM across workers
- **WHEN** a filtered scan of an indexed BAM runs with three workers that can
  all open the file
- **THEN** each worker SHALL read a disjoint set of regions
- **AND** every record, including unmapped records, SHALL be returned exactly
  once

#### Scenario: One task per file
- **WHEN** a query scans several files as one dataset
- **THEN** the files SHALL be assigned to different tasks, including files
  whose scan cannot be split

#### Scenario: Unsplittable input
- **WHEN** a query scans a single gzip-compressed file or a single remote
  object without a usable index
- **THEN** that scan SHALL run in one task

#### Scenario: Path not visible to a worker
- **WHEN** a worker cannot open a path named in its task
- **THEN** the query SHALL fail with an error that names the worker and the
  path

### Requirement: Distributed interval operations

`merge`, `cluster`, `subtract` and `complement` SHALL distribute by
partitioning their input on contig. `count_overlaps`, `coverage` and `nearest`
SHALL distribute the streamed side and send the prebuilt index of the other
side to every task, up to a configured size. `overlap` SHALL distribute the
probe side and broadcast the build side for join types that never emit
unmatched build rows.

#### Scenario: Overlap of a large BAM with a small BED
- **WHEN** `pb.overlap` runs in distributed mode with reads from a BAM as the
  probe side and a BED file as the build side
- **THEN** the BAM scan SHALL be split across tasks
- **AND** every task SHALL join against the complete BED content
- **AND** the result SHALL equal the single-node result

#### Scenario: Join type that emits unmatched build rows
- **WHEN** an interval join must emit build-side rows that have no match
- **THEN** the join SHALL run in one task

#### Scenario: Index above the size limit
- **WHEN** the prebuilt index of a `count_overlaps`, `coverage` or `nearest`
  operation is larger than the configured limit
- **THEN** the operation SHALL run in one task

### Requirement: Credentials and transport security

Credentials given to a scan SHALL be available to the tasks that run it. The
coordinator SHALL NOT send a plan that contains credentials over a connection
without TLS unless the user has explicitly allowed it. Coordinator and workers
SHALL authenticate requests with a shared token when one is configured.

#### Scenario: Credentials reach workers
- **WHEN** a distributed scan was given per-scan credentials
- **THEN** every task of that scan SHALL read with those credentials

#### Scenario: Plaintext channel
- **WHEN** a plan contains credentials and a worker address does not use TLS
- **THEN** the query SHALL fail before any plan is sent

#### Scenario: Credentials close to expiry
- **WHEN** the credentials of a scan expire within the configured minimum
  lifetime at planning time
- **THEN** the coordinator SHALL refresh them before sending the plan

#### Scenario: Missing token
- **WHEN** a worker configured with a token receives a request without it
- **THEN** the worker SHALL reject the request

### Requirement: Worker access limits

A worker SHALL listen on the loopback interface unless another address is
given. A worker SHALL accept an optional list of allowed roots and SHALL
reject any task that reads or writes a location outside them.

#### Scenario: Default bind address
- **WHEN** a worker is started without a bind address
- **THEN** it SHALL accept connections from the local host only

#### Scenario: Location outside the allowed roots
- **WHEN** a worker with allowed roots receives a task that names a location
  outside them
- **THEN** the worker SHALL reject the task before reading any data

### Requirement: Failure semantics

A distributed query SHALL either return a complete result or fail. A failure
of any task SHALL fail the query with an error that identifies the worker.

#### Scenario: Worker stops during a query
- **WHEN** a worker process exits while one of its tasks is running
- **THEN** the query SHALL raise an error
- **AND** no partial result SHALL be returned as if it were complete
