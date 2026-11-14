## ADDED Requirements

### Requirement: Region Sequence Extraction

polars-bio SHALL provide `pb.subseq(fasta, regions, *, strand_col=None)`.
`fasta` SHALL be a local FASTA path with a `.fai` index; a BGZF-compressed
FASTA SHALL also have a `.gzi` index. `regions` SHALL be a frame or a path
with contig, start and end columns. The function SHALL return a LazyFrame
containing the region columns plus a `sequence` column. The same operation
SHALL be available in SQL as `SELECT * FROM subseq('<fasta>', '<regions_table>')`.

#### Scenario: Parity with samtools faidx
- **WHEN** `pb.subseq` extracts the regions of a BED file from an indexed reference
- **THEN** every `sequence` SHALL be byte-identical to `samtools faidx` output for the same region

#### Scenario: Minus-strand regions
- **WHEN** `strand_col="strand"` is given and a region's strand value is `-`
- **THEN** that region's `sequence` SHALL be the reverse complement of the reference bases

#### Scenario: Coordinate system from metadata
- **WHEN** the regions frame carries 1-based closed coordinate metadata, and an equivalent frame carries 0-based half-open metadata
- **THEN** both SHALL extract identical sequences

#### Scenario: Invalid region
- **WHEN** a region names a contig that is absent from the `.fai`, or ends past the contig's length
- **THEN** `pb.subseq` SHALL raise an error that identifies the offending region

#### Scenario: Unindexable compression
- **WHEN** `fasta` is plain gzip (not BGZF) or has no `.fai`
- **THEN** `pb.subseq` SHALL raise an error that explains the index requirement and suggests `bgzip` and `samtools faidx`

### Requirement: Indexed Random Access

`pb.subseq` SHALL read only the parts of the FASTA needed for the requested
regions, through the `.fai` and `.gzi` indexes, and SHALL NOT decompress or
scan the whole file.

#### Scenario: Few regions from a large reference
- **WHEN** 1,000 exon regions are extracted from a BGZF-compressed whole-genome reference
- **THEN** the bytes read from the FASTA SHALL be on the order of the BGZF blocks covering those regions, far below the file size, as measured by the reader's byte counter in tests
