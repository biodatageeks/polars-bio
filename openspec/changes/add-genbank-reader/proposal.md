# Add a native GenBank reader

## Why

A GenBank file contains both sequence records and feature annotations. A user extracting just feature coordinates currently has to parse the file elsewhere. A projection-aware reader can return those coordinates while skipping assembly of the two largest fields, the record sequence and the feature translation. [Issue #523](https://github.com/biodatageeks/polars-bio/issues/523)

## What changes

- Build a custom, streaming Rust parser and DataFusion provider in a new upstream crate, `datafusion-bio-format-genbank`. The parser writes projected fields into Arrow batches. Biopython and gb-io.py provide independent comparisons during development.
- Expose `scan_genbank`, `read_genbank` and `register_genbank` for `.gb` and `.gbk`. Choose one row per record by default, or one row per feature with `view="features"`.
- Support plain, gzip and BGZF input, existing object storage options, feature coordinate conversion, qualifier projection, and SQL and Polars filters.
- Verify equivalent output before comparing protein FASTA extraction and feature table construction across readers. EMBL input is a follow-up that can reuse the location and qualifier code.

## Impact

The upstream change owns the file format, Arrow schema, scan execution and partitioning. The dependent polars-bio change owns the Rust bindings, Polars source integration, SQL registration and user documentation. The proposed capability is `genbank-io`. The two repositories can review their changes independently once the schema fixtures are agreed.
