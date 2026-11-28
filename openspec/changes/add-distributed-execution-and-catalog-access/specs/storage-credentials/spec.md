## ADDED Requirements

### Requirement: Per-scan storage credentials

polars-bio SHALL accept a `storage_options` mapping on every public `read_*`,
`scan_*`, `describe_*` and `register_*` function that accepts object-storage
options, carrying credentials for that call only. The supported credentials SHALL
include an S3 access key, secret key and session token, an Azure SAS token,
and a GCS OAuth bearer token, together with the region, endpoint and account
settings they need. The mapping SHALL be a closed set of documented keys.

#### Scenario: Read with a short-lived S3 session token
- **WHEN** a user calls `pb.scan_bam("s3://bucket/a.bam", storage_options=...)`
  with an access key, a secret key and a session token
- **THEN** every request for that scan SHALL be signed with those credentials
- **AND** the process environment SHALL NOT be read or modified for them

#### Scenario: Two scans with different credentials in one query
- **WHEN** a query joins two scans that were given different `storage_options`
- **THEN** each scan SHALL use its own credentials
- **AND** the query SHALL succeed when each credential grants access only to
  its own location

#### Scenario: Unknown key
- **WHEN** `storage_options` contains a key that is not in the documented set
- **THEN** the call SHALL raise an error that names the key
- **AND** no request SHALL be sent to the object store

#### Scenario: Azure SAS token and GCS bearer token
- **WHEN** a user passes an Azure SAS token or a GCS OAuth bearer token
- **THEN** the scan SHALL authenticate with that token

### Requirement: Credential precedence

Explicit credentials SHALL take precedence over ambient credentials. The
order SHALL be: `credential_provider`, then `storage_options`, then the
process environment. A call that passes neither argument SHALL behave exactly
as it did before this change.

#### Scenario: Ambient behaviour is unchanged
- **WHEN** a user calls a `scan_*` function without `storage_options` and
  without `credential_provider`
- **THEN** credentials SHALL be resolved as they were before this change

#### Scenario: Explicit credentials override the environment
- **WHEN** `AWS_ACCESS_KEY_ID` is set in the environment and a different
  access key is passed in `storage_options`
- **THEN** the scan SHALL use the key from `storage_options`

### Requirement: Refreshable credential provider

The same functions SHALL accept a `credential_provider` callable that returns
a mapping of storage options and an expiry time, or no expiry. polars-bio
SHALL call it before the first request, and again before the credentials
expire, so that a scan that runs longer than the credential lifetime
completes.

#### Scenario: Refresh before expiry
- **WHEN** a scan is still running and the current credentials are within the
  refresh margin of their expiry
- **THEN** the provider SHALL be called again
- **AND** later requests SHALL use the new credentials

#### Scenario: Provider failure
- **WHEN** the provider raises an exception
- **THEN** the scan SHALL fail with an error that includes the provider's
  message
- **AND** the scan SHALL NOT continue with ambient credentials

#### Scenario: Credentials without expiry
- **WHEN** the provider returns no expiry
- **THEN** it SHALL be called once for the scan

### Requirement: Secret redaction

Secret values SHALL NOT appear in log output, object reprs, `EXPLAIN` output,
error messages, or metadata stored on a LazyFrame or DataFrame.

#### Scenario: Plan output
- **WHEN** a user prints the plan of a scan that was given credentials
- **THEN** secret values SHALL be shown as a fixed mask

#### Scenario: Error message
- **WHEN** a request fails with an authorization error
- **THEN** the raised error SHALL NOT contain the secret key, the session
  token, the SAS token or the bearer token
