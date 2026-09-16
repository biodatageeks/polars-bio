## ADDED Requirements

### Requirement: Concurrent object-store fetches by default

Every public I/O function that accepts `concurrent_fetches` SHALL default it to `8`.
This covers `read_*`, `scan_*`, `describe_*` and `register_*`. The value SHALL be
passed unchanged to the object-storage layer so that a whole-object read from S3,
GCS or HTTP is split into up to `concurrent_fetches` parallel ranged requests of
`chunk_size` MiB each.

#### Scenario: Default read from object storage uses parallel ranged requests
- **WHEN** a user calls `pb.scan_a3m("s3://bucket/file.a3m")` (or any other
  `read_*`/`scan_*`/`register_*` function) without `concurrent_fetches`
- **THEN** the signature default SHALL be `8`
- **AND** the object SHALL be read with up to 8 concurrent ranged requests

#### Scenario: Explicit single sequential request
- **WHEN** a user passes `concurrent_fetches=1`
- **THEN** the object SHALL be streamed over one sequential request
- **AND** no size preflight (HEAD) SHALL be issued for that read

#### Scenario: Consistent default across access patterns
- **WHEN** the same file is read via `read_*`, `scan_*` and `register_*`
- **THEN** all three SHALL use the same `concurrent_fetches` default

### Requirement: Documented option semantics

Every function that documents its own parameters SHALL describe `concurrent_fetches` uniformly.
The description states which backends honour it (S3, GCS, HTTP), the default,
and that `1` selects a single sequential request. A function whose docstring
defers to another function's parameter list inherits that description.

#### Scenario: Docstring names the backends and the default
- **WHEN** a user reads the rendered API documentation of any `read_*`,
  `scan_*`, `describe_*` or `register_*` function with `concurrent_fetches`
- **THEN** the parameter description SHALL mention S3, GCS and HTTP, the
  default of 8, and `1` as the sequential option
