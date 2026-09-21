## 1. Reading

- [x] 1.1 Add `preserve_record_layout` to `VcfReadOptions` and open the provider
  `with_record_layout()` when it is set.
- [x] 1.2 Expose it on `read_vcf` and `scan_vcf`; keep it off `read_bcf` and
  `scan_bcf`, which share `_scan_variant`.

## 2. Writing

- [x] 2.1 Restore the `bio.vcf.record_layout` marker on `_vcf_info_keys` and
  `_vcf_format_keys` by column name in `apply_vcf_metadata_to_schema`.
- [x] 2.2 Leave a column alone when the header declares its name as an INFO or
  FORMAT field.
- [x] 2.3 Never offer the two names as INFO in the metadata-less heuristic
  classifier.

## 3. Validation

- [x] 3.1 Records with a non-header INFO order, a present-but-missing FORMAT key
  and a GT that is not first come back byte for byte, through both the lazy and
  the eager pair, and after a row filter.
- [x] 3.2 The layout columns never appear in the written file.
- [x] 3.3 A file declaring a reserved name keeps that field as data on a default
  read and write, and the carry is refused for it.
- [x] 3.4 Values are re-serialized canonically with the carry on.
- [x] 3.5 Without the option the frame and the output are unchanged.
- [x] 3.6 GIAB HG002 chr22 (50,861 records): every record byte-identical with
  the carry; no measurable cost (2.5 s vs 2.4 s).

## 4. Documentation

- [x] 4.1 Docstrings, `docs/features/writing.md`, CHANGELOG.
