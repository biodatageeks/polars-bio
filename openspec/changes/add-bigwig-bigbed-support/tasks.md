## 1. Provider Feasibility Spike

- [x] 1.1 Treat `openspec/changes/add-bigwig-bigbed-support` in polars-bio as the authoritative tracker for both polars-bio and `biodatageeks/datafusion-bio-formats`; do not create a mirrored provider OpenSpec change.
- [x] 1.2 Add minimal provider dependency on `bigtools` with read-only features in `biodatageeks/datafusion-bio-formats`, pinned to the `biodatageeks/bigtools` fork while the upstream compatibility PR is open.
- [x] 1.3 Prototype opening a local BigWig and BigBed file and extracting chromosome metadata without scanning records.
- [ ] 1.4 Verify BigBed autoSQL parsing falls back to `rest` for files without usable autoSQL or with unsupported autoSQL field types.
- [x] 1.5 Confirm that provider crates build against the same DataFusion and Arrow versions used by polars-bio.

## 2. DataFusion BigWig Provider

- [x] 2.1 Implement `BigWigTableProvider` schema and metadata.
- [x] 2.2 Implement `BigWigExec` full-chromosome scans using bigtools interval iteration.
- [x] 2.3 Implement projection flags for `chrom`, `start`, `end`, and `value`.
- [x] 2.4 Implement genomic filter analysis and interval-region pruning for `chrom`, `start`, and `end`.
- [ ] 2.5 Implement provider-side residual filtering for `value` comparisons when feasible.
- [ ] 2.6 Implement partition planning from DataFusion `target_partitions`.
- [ ] 2.7 Add provider tests for full scan, projected scan, genomic predicate scan, value residual filter, empty projection/count, and partition count.

## 3. DataFusion BigBed Provider

- [x] 3.1 Implement `BigBedTableProvider` schema discovery from autoSQL and fallback `rest` schema.
- [x] 3.2 Implement `BigBedExec` full-chromosome scans using bigtools interval iteration.
- [x] 3.3 Implement scalar autoSQL field parsing for projected fields.
- [x] 3.4 Implement projection flags so non-projected extra fields are not parsed.
- [x] 3.5 Implement genomic filter analysis and interval-region pruning for `chrom`, `start`, and `end`.
- [ ] 3.6 Implement provider-side residual filtering for supported parsed scalar fields when feasible.
- [ ] 3.7 Implement partition planning and boundary ownership filtering.
- [ ] 3.8 Add provider tests for autoSQL schema, fallback `rest`, projection, genomic predicates, scalar residual predicates, empty projection/count, and partition count.

## 4. Rust/PyO3 Integration

- [x] 4.1 Add `InputFormat.BigWig` and `InputFormat.BigBed`.
- [x] 4.2 Add `BigWigReadOptions` and `BigBedReadOptions` with object storage options, coordinate-system flag, and BigBed schema mode.
- [x] 4.3 Extend `ReadOptions` to carry BigWig and BigBed options.
- [x] 4.4 Register BigWig and BigBed providers in `src/scan.rs`.
- [x] 4.5 Extend format detection only for internal path/table routing where explicit formats are already expected.
- [x] 4.6 Expose new read option classes and table registration through `src/lib.rs`.

## 5. Python API Integration

- [x] 5.1 Add `IOOperations.scan_bigwig` and `IOOperations.read_bigwig`.
- [x] 5.2 Add `IOOperations.scan_bigbed` and `IOOperations.read_bigbed`.
- [x] 5.3 Add `SQL.register_bigwig` and `SQL.register_bigbed`.
- [x] 5.4 Export the new APIs from `polars_bio/__init__.py`.
- [x] 5.5 Add BigWig and BigBed column type metadata to `predicate_translator.py` and `_FORMAT_COLUMN_TYPES`.
- [x] 5.6 Extend metadata extraction so source format, path, header, and coordinate-system metadata are visible for BigWig and BigBed scans.

## 6. Python Tests

- [x] 6.1 Add small BigWig and BigBed fixtures with known intervals and at least two chromosomes.
- [x] 6.2 Test eager reads and lazy scans for both formats.
- [ ] 6.3 Test `use_zero_based=True`, `use_zero_based=False`, and global default behavior.
- [ ] 6.4 Test projection pushdown returns only requested columns and plan output includes `BigWigExec` or `BigBedExec` projection details.
- [x] 6.5 Test genomic predicate pushdown against equivalent non-pushdown results.
- [ ] 6.6 Test parallel partition behavior by setting `datafusion.execution.target_partitions`.
- [x] 6.7 Test BigBed autoSQL field parsing and fallback `rest` behavior.
- [ ] 6.8 Test unsupported remote paths fail with clear errors.

## 7. Documentation and Release Notes

- [ ] 7.1 Update API docs for BigWig and BigBed read/scan/register methods.
- [ ] 7.2 Document initial storage limitations and coordinate semantics.
- [ ] 7.3 Add examples for selecting BigWig signal columns and filtering BigBed annotations by genomic interval.
- [ ] 7.4 Add changelog entry.
