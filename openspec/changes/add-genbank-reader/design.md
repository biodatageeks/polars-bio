# GenBank reader design

## Public contract

The three entry points accept the same view, `qualifier_fields`, compression, storage and coordinate options. The lazy reader also exposes the usual projection and predicate switches, and the eager reader collects that scan. SQL registration fixes a view and its promoted fields under a table name. A user can register both views of one file under different names. Reject unknown views, duplicate promoted names, collisions with fixed columns and feature-only qualifier options on the records view during schema construction.

| View | Proposed columns and interpretation |
| --- | --- |
| `records` (default) | `name` is the LOCUS identifier, `length` is the declared base count (Int64), and `molecule_type`, `topology`, `division`, `date`, `definition`, `accession`, `version` and `organism` are strings. Keep the date in LOCUS spelling, the primary accession and the accession.version token. `taxonomy` is an ordered list of strings. `sequence` is an uppercase string built from ORIGIN residues. Missing optional fields, including sequence in a CONTIG-only record, are null. |
| `features` | `record_id` equals the parent LOCUS `name` so the views join directly. `type` and `location` are strings, `start` and `end` are nullable Int64, and `strand` is `+`, `-` or null. `parts` is a list of structs with nullable `start`, `end` and `strand`. The default `qualifiers` column is an Arrow map from string keys to ordered lists of nullable strings. |

Group repeated qualifiers by key in order of first appearance and keep values in encounter order. A flag such as `/pseudo` has a single null value, an explicitly empty quoted value has an empty string, and a missing key has no entry. Polars 1.x represents the Arrow map as a list of key/value structs. Polars 2.x exposes a native map. Promoted columns are nullable strings taking the first value, matching the Stockholm/GFF style of scalar extraction. The full map distinguishes repetitions and flags. `qualifier_fields=["gene", "qualifiers"]` requests both forms, while `[]` requests only fixed feature columns. SQL users declare promoted fields at registration. For a direct Polars selection such as `scan_genbank(path, view="features").select("gene", "translation")`, use the GFF projection pattern to declare the required qualifier columns before resolving the projected schema. Preserve root columns used by filters and aliased expressions.

## Streaming parser

In `datafusion/bio-format-genbank`, separate GenBank record framing from feature locations and qualifier folding. Use a buffered byte-line reader with a reusable buffer and explicit states for LOCUS, header continuations, FEATURES, pending location, pending qualifier, ORIGIN, CONTIG and a standalone column-one `//`. Recognize line roles by indentation and quote state. Handle older LOCUS spacing, wrapped DEFINITION and ORGANISM lineage, multi-line quoted values, doubled quotes, unquoted values, flags, blank lines and CRLF. Fold ordinary wrapped qualifier text with spaces and strip formatting whitespace from translations. Keep unknown qualifier keys in the map. Use the NCBI example and the INSDC grammar as references. [NCBI sample record](https://www.ncbi.nlm.nih.gov/genbank/samplerecord/) · [INSDC feature table definition](https://www.insdc.org/submitting-standards/feature-table/)

Build the decode mask from projected columns and filter dependencies. At a feature boundary, finish its requested fields, evaluate pending filters and append a surviving row. At a record boundary, finish the records-view row or reset the parent context in the features view. Flush Arrow batches at configured row and byte budgets, allowing one oversized requested field to occupy its own batch. Thousands of features in one LOCUS can therefore yield multiple batches. Scanning a sequential stream still consumes the framing bytes of unselected data. A coordinates-only query must allocate no sequence, translation or qualifier-map value buffers. A count must preserve row cardinality with an empty Arrow projection. Report malformed framing, unclosed quotes, invalid supported location syntax and truncated records with the path, LOCUS when known and line or offset.

## Location semantics

Keep the location expression in native GenBank coordinates, joining physical continuation lines and removing their layout indentation. Parse the selected derived fields with a small checked grammar for points, inclusive ranges, fuzzy endpoints, between-base sites, remote accession prefixes and nested `join`, `order` and `complement`. Keep decoding independent of Arrow so a future EMBL reader can share it. Apply coordinate conversion once, after interpreting the expression. In zero-based mode a local range `a..b` becomes `[a-1,b)` and a point becomes a one-base interval.

| Input or condition | Proposed result |
| --- | --- |
| `complement(join(2..5,9..12))` | Parts in biological traversal order (9..12, then 2..5), both on the minus strand. For ordinary same-record compounds, scalar bounds are the local envelope. Mixed strand gives null scalar strand. |
| `order(<1..5,9..>12)` | Parts and envelope use the stated numeric anchors. The raw expression preserves uncertainty and the distinction between order and join. |
| `100^101` | Between-base site at offset 100 gives `[100,100)` in zero-based mode. One-based closed mode has no empty-interval convention here, so its numeric bounds are null. |
| `AB000123.1:1..50` or mixed local/remote parts | Raw text retains the accession and coordinates. The remote part has null local bounds. Local parts remain usable. Scalar bounds are null whenever the feature includes a remote part. |
| Origin-spanning join on a circular record | Preserve the ordered parts on either side of the origin. Scalar bounds are null. Detect wrapping in traversal order with strand and topology so an ordinary reverse-strand join retains its valid envelope. |

Well-formed INSDC forms outside the v1 derived-coordinate grammar retain their raw text and yield null derived fields. Exercise this separately from malformed input. Use checked coordinate arithmetic and bounded nesting.

## Provider, storage and partitioning

Build the provider schema from options alone. Reuse the upstream core object-storage readers, compression detection and DataFusion session batch/partition settings. Use Stockholm's `table_provider.rs` and `physical_exec.rs` as references for an aligned scan. Its existing range opener handles local plain input. The FASTQ provider supplies a precedent for seeking BGZF. GenBank needs its own record boundary alignment in both cases.

1. Ship a single-stream execution path first for plain, BGZF and multi-member gzip, with local and object-store parity. Use it as the reference for parallel scans.
2. For seekable plain files, probe around nominal split offsets and advance to the start following a complete terminator line. For BGZF, use known block starts from a GZI index or discover them from BGZF block headers when no index exists. Seek, decompress across blocks as needed, then align to the same logical record boundaries. Inspect preceding context at a split to distinguish column one from a fragment of a line. Never treat a block edge or `//` inside a quoted value as a record boundary.
3. Coalesce repeated split points. Assign half-open ranges of record starts and let the owning worker complete each record. Include the first record and validate the final terminator. Use one stream for ordinary gzip and for backends lacking usable range reads. Exercise S3, GCS and Azure reads through the shared options. Distinguish concurrent network fetches from the number of DataFusion partitions.
4. Start exact pushdown with string equality on records-view `name` and features-view `record_id` and `type`. Those fields are known before reading expensive payloads. Report unsupported predicates to DataFusion and preserve Polars residuals and their columns. Apply a pushed limit only after all predicates below it are exact. Verify the scan with physical plans and test-only decoded-byte counters.

## Downstream integration

The checkout uses DataFusion 55.2 and the `datafusion-bio-formats` v1.17.0 tag. Land the upstream crate first, then move the format dependencies together to a compatible upstream revision and regenerate `Cargo.lock`. In polars-bio extend `src/option.rs`, `src/scan.rs` and `src/lib.rs` with one format and view-specific read options. Route `io.py` through `_read_file` and the existing Polars IO source. Reuse the private registration leases used for Stockholm so concurrent scans of one path can use different views, qualifiers and coordinate systems. Resolve `use_zero_based=None` via the session helper and preserve feature coordinate metadata across eager and lazy collection. The implementation groups all three public functions in `polars_bio/genbank.py`, reuses `py_register_table` for SQL, and exports them in `__init__.py`.
