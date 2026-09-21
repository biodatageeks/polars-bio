## ADDED Requirements

### Requirement: Opt-in VCF Record Layout Carry

The system SHALL let a caller of `read_vcf` or `scan_vcf` request, with
`preserve_record_layout=True`, that each record's own INFO key order and FORMAT
key list be carried in the frame, and SHALL use them in `write_vcf` and
`sink_vcf` to write every record's keys in the source's order.

#### Scenario: Keys are written in the record's own order
- **WHEN** a VCF record reads `AF=0.5;DP=10` and `GT:PS:DP  0/1:.:25`, and its
  header declares `DP` before `AF` and `GT`, `PS`, `DP`
- **AND** the file is read with `preserve_record_layout=True` and written back
- **THEN** the written record reads `AF=0.5;DP=10` and `GT:PS:DP  0/1:.:25`
- **AND** the FORMAT key whose value is missing in every sample is kept.

#### Scenario: Layout survives a row filter
- **WHEN** rows are filtered out of a frame that carries the layout
- **THEN** each remaining record is written with its own key layout.

#### Scenario: Eager and lazy paths agree
- **WHEN** the option is used with `read_vcf` and `write_vcf`
- **THEN** the result is the same as with `scan_vcf` and `sink_vcf`.

#### Scenario: Layout columns are plumbing
- **WHEN** a frame carrying the layout is written
- **THEN** `_vcf_info_keys` and `_vcf_format_keys` appear neither as INFO keys
  nor as header declarations in the output.

#### Scenario: Default is unchanged
- **WHEN** the option is omitted or false
- **THEN** the frame has no `_vcf_info_keys` or `_vcf_format_keys` column
- **AND** records are written in the header's key order, omitting a FORMAT key
  that is missing in every sample, exactly as before.

#### Scenario: Values are not part of the layout
- **WHEN** a source record spells a value non-canonically, such as `QUAL` `50.0`
  or `AF=0.50`
- **THEN** the value is written in canonical form (`50`, `0.5`) with or without
  the option, because values round-trip through typed columns.

### Requirement: Reserved Layout Names Stay Data When the File Declares Them

The system SHALL treat a column named `_vcf_info_keys` or `_vcf_format_keys` as
the file's own field whenever the VCF header declares that name as an INFO or
FORMAT field, and SHALL NOT treat it as record layout.

#### Scenario: A declared field is written as data
- **WHEN** a VCF declares `##INFO=<ID=_vcf_info_keys,...>` and a record carries
  `_vcf_info_keys=mine`
- **AND** the file is read without the option and written back
- **THEN** the written record still carries `_vcf_info_keys=mine`
- **AND** the declaration is kept in the header.

#### Scenario: The carry is refused for such a file
- **WHEN** that file is read with `preserve_record_layout=True`
- **THEN** the read fails with an error that names the conflicting field,
  instead of producing two columns with one name.

#### Scenario: The carry is refused for a nested FORMAT field too
- **WHEN** a multi-sample VCF declares `##FORMAT=<ID=_vcf_format_keys,...>`, so
  the field is a child of `genotypes` rather than a top-level column
- **AND** the file is read with `preserve_record_layout=True`
- **THEN** the read fails with an error that names the conflicting field
- **AND** read without the option, the field is written back as ordinary data.
