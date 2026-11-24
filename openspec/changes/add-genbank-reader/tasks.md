## 1. Agree on the contract and fixtures

- [x] 1.1 Freeze the two schemas, LOCUS join key, qualifier promotion and location examples in the design. Create small hand-checkable golden records covering every location case, multiline and repeated qualifiers, missing ORIGIN, multi-record files and empty input.
- [x] 1.2 Record Biopython expectations for comparable fields. Use hand-written expectations for raw syntax, flags, coordinate conventions and ambiguous bounds. Add malformed/truncated and chunk-boundary fixtures.

## 2. Implement the custom parser upstream

- [x] 2.1 Add the workspace crate and its schema, byte-line parser, location grammar and Arrow builders. Implement the serial records path, followed by features, then drive both from projection and filter masks.
- [x] 2.2 Test at small buffer and batch sizes, across CRLF and missing final newline after a valid terminator. Verify error context, multi-batch single records, stable empty schemas and physical counters for skipped payloads.
- [x] 2.3 Implement the TableProvider and streaming execution plan. Cover zero-column count, reordered projection, exact and residual filters, filtered limit and early consumer cancellation.

## 3. Add storage and parallel execution upstream

- [x] 3.1 Wire plain, gzip and BGZF input to core object storage options. Test local paths and emulated S3, GCS and Azure, including compression overrides and read failures.
- [x] 3.2 Implement record-aligned plain ranges and BGZF virtual offsets with and without GZI. Verify a split on each side of a terminator, a terminator crossing blocks, an oversized record, CRLF, and several candidate splits mapping to one record boundary.
- [x] 3.3 Assert actual partition counts and compare row multiplicities and values against a serial read for both views. Cover range-capable object inputs, sequential-only inputs and ordinary gzip. Run upstream format, lint and crate tests.

## 4. Integrate in polars-bio

- [x] 4.1 Pin all companion format crates to one compatible revision containing GenBank. Regenerate the lockfile, add PyO3 format/options and provider registration, then build the extension.
- [x] 4.2 Add lazy/eager readers, SQL registration and public exports. Wire both views to private scan leases, source metadata, session coordinate resolution and view-appropriate predicate translation.
- [x] 4.3 Add GFF-style qualifier selection, retaining columns needed only by residual predicates. Verify direct promotion, explicit promotion, map plus promotion, repeated keys and name collisions.
- [x] 4.4 Add end-to-end pytest coverage for SQL vs eager vs lazy rows, schema discovery, concurrent scans on one file, coordinate overrides, feature joins to records, map conversion on supported Polars lines, unsupported predicates and filter-before-limit. Run existing IO, metadata and pushdown regression suites.

## 5. Benchmark and document

- [x] 5.1 Pin input checksums for several bacterial assemblies and a large multi-record file. Compare polars-bio, Biopython, gb-io.py and microBioRust on extracting annotated CDS translations to FAA and building equivalent feature DataFrames. Verify that each adapter supports the workload and record any unsupported combination.
- [x] 5.2 Fix the FAA header convention, missing-translation rule and feature columns across tools. Check normalized output digests and row counts before timing. Report versions, threads, compression, elapsed time, input throughput and peak RSS. Include narrow and full projections and charge partition discovery to end-to-end time.
- [x] 5.3 Update `docs/features/reading.md`, `docs/api/reading.md`, `docs/api/sql.md` and the changelog. Include both view schemas, qualifier examples, circular and remote locations, coordinate defaults and partition fallbacks.

## Verification

The provider is committed on the companion branch `feat/genbank-reader` at `8f482475948cf569ff645c0925f21bc2c1c05792`. All direct companion dependencies use that revision. The locked release extension was built and exercised locally. The published revision is under review in [datafusion-bio-formats PR 278](https://github.com/biodatageeks/datafusion-bio-formats/pull/278).

The upstream GenBank, core and MSA test suites passed (222 tests). GenBank Clippy passed with warnings denied. Python tests cover the new API, existing IO, frame metadata and pushdown, including S3, GCS and Azure against a local HTTP fixture implementing their read protocols. The public feature schema was also checked on Polars 1.40.0, 1.44.2, 2.0.x and 2.1.0. Input digests, output equivalence and timings are recorded in [the benchmark report](benchmarks.md).
