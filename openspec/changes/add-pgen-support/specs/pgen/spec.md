## ADDED Requirements

### Requirement: PGEN Genotype Input

The system SHALL read PLINK 2 PGEN/PVAR/PSAM filesets through dedicated eager,
lazy, registration, and description methods, emitting one row per PVAR variant.

#### Scenario: Lazy scan with the default genotype field
- **WHEN** a user calls `scan_pgen` without a genotype field selection
- **THEN** the `genotypes` struct has exactly one child, `GT`
- **AND** each `GT` entry holds two allele indices per selected sample
- **AND** the scan remains lazy until collection.

#### Scenario: Selecting several genotype fields
- **WHEN** a user calls `read_pgen` with `genotype_fields=["GT", "PHASED"]`
- **THEN** the `genotypes` struct has those children in that order
- **AND** `PHASED` distinguishes a missing call from an unphased one.

#### Scenario: Unsupported genotype field
- **WHEN** a genotype field outside `GT`, `ALT_COUNT`, `PHASED`, `DS`,
  `DS_STORED`, and `HDS` is given
- **THEN** the call raises `ValueError` before any file is opened.

#### Scenario: Empty genotype field selection
- **WHEN** `genotype_fields` is an empty sequence
- **THEN** the call raises `ValueError` before any file is opened.

### Requirement: PGEN Companion Discovery

The system SHALL locate the PVAR and PSAM companions from the `.pgen` basename
and SHALL accept explicit locations.

#### Scenario: Automatic discovery
- **WHEN** a `.pgen` is read with no companion paths given
- **THEN** a neighbouring `.pvar`, or `.pvar.zst` when the former is absent, and
  the shared-basename `.psam` are used.

#### Scenario: Explicit companions
- **WHEN** `pvar_path` or `psam_path` is given
- **THEN** that location is used in place of discovery.

#### Scenario: Explicit index at every entry point
- **WHEN** a fileset whose index lives outside the `.pgen` is read, scanned,
  registered, or described
- **THEN** every one of those entry points accepts `pgi_path` and passes it to
  the provider, so none of them is unable to open the fileset.

#### Scenario: Absent companion
- **WHEN** a required companion cannot be opened
- **THEN** the error names the location that was tried.

### Requirement: PGEN Sample Selection

The system SHALL construct selectable sample names from PSAM identifiers and
SHALL emit a requested subset in the requested order.

#### Scenario: Subset and reorder
- **WHEN** `samples=["s8", "s2", "s1"]` is given
- **THEN** each genotype child holds three per-sample entries in that order.

#### Scenario: Identifier mode
- **WHEN** `psam_id_mode="fid_iid"` is given for a PSAM declaring only IID
- **THEN** selectable names take the form `0:IID`, the FID defaulting to `0`.

#### Scenario: Absent requested sample under the default policy
- **WHEN** a requested sample name is not in the PSAM
- **THEN** the call raises rather than silently emitting fewer samples.

#### Scenario: Absent requested sample under the ignore policy
- **WHEN** `missing_sample_policy="ignore"` is given
- **THEN** absent requested names are omitted from the selection
- **AND** the remaining requested names are emitted in order.

#### Scenario: Unsupported identifier mode
- **WHEN** an identifier mode outside `iid`, `fid_iid`, and `fid_iid_sid` is
  given
- **THEN** the call raises `ValueError` before any file is opened.

### Requirement: PGEN Read Coalescing Control

The system SHALL let a caller bound how PGEN byte ranges are coalesced, without
changing the emitted content.

#### Scenario: Range gap does not change content
- **WHEN** the same fileset is read with different `max_range_gap` values
- **THEN** the emitted rows and genotypes are identical.

#### Scenario: Provider defaults are preserved
- **WHEN** a tuning option is left unset
- **THEN** the provider default is used rather than a zero value.

#### Scenario: Registration exposes the same controls
- **WHEN** a PGEN on object storage is registered rather than scanned
- **THEN** `register_pgen` accepts `max_range_gap`, `max_range_bytes`, and
  `batch_soft_byte_limit` and forwards them, so a registered table can be tuned
  the same way a scan can.

### Requirement: PGEN Metadata Reporting

The system SHALL report file-level PGEN properties and sample identity through
schema metadata.

#### Scenario: Metadata after a scan
- **WHEN** `get_metadata` is called on a PGEN scan
- **THEN** the header reports storage mode, index provenance, specification
  baseline, emitted sample names, full PSAM identities, and the selected
  genotype fields.

#### Scenario: Description without disturbing a registered table
- **WHEN** `describe_pgen` is called for a file already registered under
  another name
- **THEN** the emitted columns and file properties are returned
- **AND** the previously registered table remains queryable.

### Requirement: PGEN Registration Errors

The system SHALL surface fileset and selection errors as exceptions.

#### Scenario: Failed registration preserves the existing table
- **WHEN** a registration under an existing table name fails on an absent
  sample name
- **THEN** an exception is raised
- **AND** the table previously registered under that name remains queryable.
