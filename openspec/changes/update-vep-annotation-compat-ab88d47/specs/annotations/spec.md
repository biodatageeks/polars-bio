## ADDED Requirements
### Requirement: Stable Python Annotation Entry Points
The library SHALL keep `pb.annotations.annotate_variants` and `pb.annotations.create_vep_cache` as the supported public API entry points when updating the pinned `datafusion-bio-functions` revision.

#### Scenario: Annotate variants via native cache
- **WHEN** a user calls `pb.annotations.annotate_variants(vcf_path, cache_path, cache_format="native", ...)`
- **THEN** the call SHALL execute without requiring API changes in user code
- **AND** annotation columns SHALL be returned in the output schema without the internal `cache_` prefix

#### Scenario: Convert native cache to parquet/fjall
- **WHEN** a user calls `pb.annotations.create_vep_cache(source_path, output_path, output_format=...)`
- **THEN** the call SHALL support both `"parquet"` and `"fjall"` output formats
- **AND** the produced cache SHALL be usable by `pb.annotations.annotate_variants`

### Requirement: Lookup Argument Compatibility
The annotation lookup path SHALL normalize and validate lookup arguments before calling `lookup_variants()` so behavior stays predictable across backend revisions.

#### Scenario: Match mode normalization
- **WHEN** a user passes a supported `match_mode` value with surrounding whitespace or mixed case
- **THEN** the implementation SHALL normalize it to a valid canonical mode before lookup execution

#### Scenario: Empty column list handling
- **WHEN** a user passes `columns=[]` and a non-default lookup mode
- **THEN** the implementation SHALL fall back to backend-compatible default projection columns instead of emitting an empty projection argument

### Requirement: chr-Prefixed Chromosome Preservation
Annotation output SHALL preserve the original VCF chromosome value in result rows, including `chr`-prefixed names.

#### Scenario: Unmatched chr-prefixed row
- **WHEN** input VCF rows use `chr`-prefixed chromosome names and a row has no cache match
- **THEN** the row SHALL still be present in annotation output
- **AND** the output `chrom` value SHALL remain `chr`-prefixed
