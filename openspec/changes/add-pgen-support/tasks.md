## 1. API and execution

- [x] 1.1 Add `PgenReadOptions` and an `InputFormat::Pgen` registration arm.
- [x] 1.2 Add `read_pgen`, `scan_pgen`, `describe_pgen`, and `register_pgen`.
- [x] 1.3 Validate the input path suffix, the genotype fields, the PSAM
  identifier mode, and the missing-sample policy.
- [x] 1.4 Default `genotype_fields` to `("GT",)`, narrowing the provider
  default of every available child.
- [x] 1.5 Accept explicit `pvar_path`, `psam_path`, and `pgi_path`, falling
  back to basename discovery.
- [x] 1.6 Pass `max_range_gap`, `max_range_bytes`, and
  `batch_soft_byte_limit` through, using the provider default when unset.
- [x] 1.7 Return registration errors rather than panicking in the extension.

## 2. Metadata

- [x] 2.1 Extract the `bio.pgen.*` schema metadata: storage mode, index
  provenance, and specification baseline.
- [x] 2.2 Report the emitted sample order and the full PSAM identities from
  the `genotypes` field metadata.
- [x] 2.3 Report the selected genotype fields from the emitted struct.
- [x] 2.4 Route PGEN through `_format_to_string` and the header passthrough
  list so the metadata reaches `get_metadata`.

## 3. Validation

- [x] 3.1 Test variant metadata, the GT-only default, and each of the five
  genotype fields.
- [x] 3.2 Test sample selection and reordering, both missing-sample policies,
  and all three PSAM identifier modes.
- [x] 3.3 Test that explicit companions are used and that an absent companion
  names the location tried.
- [x] 3.4 Test that the range knobs do not change emitted content, and that
  the invariance check can fail.
- [x] 3.5 Test that content is independent of `target_partitions`.
- [x] 3.6 Test register and describe, including that a failed registration
  preserves the existing table.
