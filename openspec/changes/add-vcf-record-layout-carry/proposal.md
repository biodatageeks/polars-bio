# Change: Carry a VCF record's key layout through a read and a write

## Why

A VCF read into a frame and written back carries the same data but not the same
lines. INFO and FORMAT keys come out in the header's order rather than the
record's own, and a FORMAT key that is present but missing in every sample
(`GT:PS:DP  0/1:.:25`) is dropped. Both are valid VCF. But a VCF-in / VCF-out
pipeline — filter a file, write it back — cannot be diffed or checksummed
against its input, and every record looks changed.

Neither fact is recoverable from the typed columns: every record carries every
key the header declares, in schema order, and a missing value parses to the
same null as an absent key.

`datafusion-bio-format-vcf` already solves this with two per-record layout
columns, `_vcf_info_keys` and `_vcf_format_keys`, which its serializer honours.
polars-bio could not use them: the reader option was not exposed, and the
serializer recognises the columns by Arrow field metadata, which a Polars frame
does not keep.

## What Changes

- Add `preserve_record_layout: bool = False` to `read_vcf` and `scan_vcf`. When
  set, the frame gains two `String` columns, `_vcf_info_keys` and
  `_vcf_format_keys`.
- `write_vcf` / `sink_vcf` restore the layout marker on those two columns by
  name, so the serializer writes each record's keys in the source's own order
  and keeps a carried FORMAT key whose value is missing.
- A column whose name the VCF header declares as an INFO or FORMAT field is that
  file's own data and is never treated as layout plumbing. The reader refuses
  the carry for such a file with an error naming the field.
- Off by default. Without the option neither the frame nor the written output
  changes.
- Not offered on `read_bcf` / `scan_bcf`: a BCF record has no source text.
- Document what is and is not restored: key layout, not the spelling of values,
  which are still parsed and re-serialized in canonical form.

Not part of this change: writing the source header back verbatim (#467) is a
bug fix and ships in the same pull request without a proposal, per
`openspec/AGENTS.md`.

## Impact

- Affected specs: `vcf`
- Affected code: `polars_bio/io.py` (`read_vcf`, `scan_vcf`, `_scan_variant`),
  `src/option.rs` (`VcfReadOptions`), `src/scan.rs`, `src/write.rs`
  (`apply_vcf_metadata_to_schema`, `extract_vcf_fields_from_schema`),
  `docs/features/writing.md`, `tests/test_vcf_write_record_layout.py`
- Issue: #468. Downstream: biodatageeks/vepyr#126.
