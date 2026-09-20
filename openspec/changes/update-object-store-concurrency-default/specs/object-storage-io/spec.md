## ADDED Requirements

### Requirement: Concurrent object-store fetches by default

Every public I/O function that accepts `concurrent_fetches` SHALL default it to `8`.
This covers `read_*`, `scan_*`, `describe_*` and `register_*`. The value SHALL be
passed unchanged to the object-storage layer so that a whole-object read from S3,
GCS or HTTP, where supported by the reader, is split into up to `concurrent_fetches` parallel ranged requests of
`chunk_size` MiB each.

#### Scenario: Default read from object storage uses parallel ranged requests
- **WHEN** a user calls `pb.scan_fasta("s3://bucket/file.fasta")` (or any other
  `read_*`/`scan_*`/`register_*` function) without `concurrent_fetches`
- **THEN** the signature default SHALL be `8`
- **AND** the object SHALL be read with up to 8 concurrent ranged requests

#### Scenario: Explicit single sequential S3 request
- **WHEN** a user passes `concurrent_fetches=1` for a whole-object S3 stream
- **THEN** the object SHALL be streamed over one sequential request
- **AND** no size preflight (HEAD) SHALL be issued for that stream

#### Scenario: Explicit sequential HTTP or GCS fetching
- **WHEN** a user passes `concurrent_fetches=1` for HTTP or GCS
- **THEN** parallel fetching SHALL be disabled
- **AND** the reader MAY still issue chunked requests and a HEAD preflight

#### Scenario: Lazy-scan fallback preserves the default
- **WHEN** a lazy scan has no stored object-storage options
- **THEN** its fallback SHALL use `concurrent_fetches=8`
- **AND** explicitly stored options SHALL be returned unchanged

#### Scenario: Consistent default across access patterns
- **WHEN** the same file is read via `read_*`, `scan_*` and `register_*`
- **THEN** all three SHALL use the same `concurrent_fetches` default

### Requirement: Documented option semantics

Every function that documents its own parameters SHALL describe `concurrent_fetches` uniformly.
The description names the backends (S3, GCS, HTTP, where supported by the reader), the default,
and that `1` disables parallel fetching (a single sequential whole-object
request on S3; HTTP/GCS may still use chunks and HEAD). A function whose docstring
defers to another function's parameter list inherits that description.

#### Scenario: Docstring names the backends and the default
- **WHEN** a user reads the rendered API documentation of any `read_*`,
  `scan_*`, `describe_*` or `register_*` function with `concurrent_fetches`
- **THEN** the parameter description SHALL mention S3, GCS and HTTP, the
  default of 8, and `1` as the sequential option
