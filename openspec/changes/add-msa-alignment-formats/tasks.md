## 1. Fixtures and Oracles (before any parser code)

- [x] 1.1 Treat this change as the authoritative tracker for both polars-bio and `biodatageeks/datafusion-bio-formats`; do not create a mirrored provider OpenSpec change.
- [x] 1.2 Vendor fixtures under `tests/data/io/msa/` with a `README.md` recording source URL and retrieval date: Pfam PF00001 seed (`https://www.ebi.ac.uk/interpro/wwwapi/entry/pfam/PF00001/?annotation=alignment:seed`, gunzipped), Rfam RF00001 seed (`https://rfam.org/family/RF00001/alignment/stockholm`, interleaved), hh-suite `data/query.a3m`, and a head of hh-suite `scripts/hhpred/example/test.a3m` that keeps the three `ss_*` pseudo-sequences plus ~200 homologs.
- [x] 1.3 Check in `tests/data/io/msa/generate_fixtures.py` that (a) fetches the sources and hh-suite `scripts/reformat.pl` (GPL-3 — fetched at generation time, **never vendored**), (b) requires `esl-reformat`/`esl-alistat` on `PATH`, (c) asserts the cross-implementation agreements from `design.md` (reformat.pl ↔ Easel byte-identical dotted A2M; `a3m → sto → a3m` sequence round-trip; Easel de-interleave preserves `#=GS` and non-`GA`/`TC`/`NC` `#=GF` lines verbatim), and (d) writes the expected-output files: `PF00001_pfam.sto` and `RF00001_pfam.sto` (`esl-reformat pfam` canonical single-block forms), `PF00001_hmmalign.sto` (`#=GR PP`, `#=GC PP_cons`/`RF`, 200-column blocks, via pyhmmer), `multi.sto`, `query_dotted.a2m`, `query_hh.sto`, `hdr.a3m` (`#A3M#` + `#` lines prepended), `expected.json` (per-alignment `n_sequences`/`alignment_length` from `esl-alistat`), plus hand-written minimal edge files: missing trailing `//`, wrong header, empty file, single-record file, `.gz`/`.bgz` variants.
- [x] 1.4 Add `pyhmmer` and `biopython` to `[project.optional-dependencies]` next to `pysam`/`pyBigWig`, and to the CI test extras; parity tests `pytest.importorskip` both. No Perl or HMMER binary is required to run the suite.
- [x] 1.5 Mark the live `esl-alistat`/`esl-reformat` tests `skipif(shutil.which("esl-alistat") is None)`. (Scoped down: this repository has no GitHub workflow that runs `pytest` — tests run via `make test` — so there is no CI job to add `bioconda::hmmer` to. The live checks run wherever HMMER is on `PATH`; the checked-in `expected.json` carries the `esl-alistat` values everywhere else.)
- [x] 1.6 Write the `describe_sto`/`gs`-bag comparison against the `*_pfam.sto` canonical forms: line-identical for `#=GS` and every `#=GF` feature except `GA`/`TC`/`NC` (compared numerically because Easel normalises them), `#=GC` compared per feature order-insensitively.

## 2. Provider Crate: `datafusion-bio-format-msa` (companion repo)

- [x] 2.1 Scaffold `datafusion/bio-format-msa` mirroring `bio-format-fasta` (`Cargo.toml` with workspace deps, `README.md`, `lib.rs`); add to the workspace members.
- [x] 2.2 `storage.rs`: reuse `datafusion_bio_format_core::object_storage` for local/GCS/S3/HTTP and `gz`/`bgz`; no new storage code. (Implemented as one `open_lines` returning a boxed `AsyncBufRead`, so both parsers are storage-agnostic.)
- [x] 2.3 `fastalike.rs`: `MsaFlavor { A2m, A3m }`, `FastaLikeTableProvider`, `FastaLikeExec`; pre-filter that skips `#`-prefixed lines before the first `>`; schema identical to FASTA (`name`, `description`, `sequence`); whitespace-only identifier split. (Own line reader rather than `noodles-fasta`: its `read_definition` has no `>` check, and one reader serves every storage/compression combination.)
- [x] 2.4 `stockholm/reader.rs`: line-oriented parser implementing the rules in `design.md` (header validation, `#=GF/GS/GC/GR` dispatch, block concatenation by name, `//` termination, missing-terminator tolerance with a warning, other `#` lines ignored). Annotation-only mode never stores sequence text.
- [x] 2.5 `stockholm/table_provider.rs` + `physical_exec.rs`: schema (`alignment_id`, `name`, `sequence`, `gs`, `gr`) with `gs_fields` promotion (first occurrence wins; `"gs"` sentinel keeps the bag); `alignment_id` fallback `ID → AC → ordinal`; projected columns only are built; `count(*)` yields zero-column batches with a row count.
- [x] 2.6 Stockholm partition planning: scan for `//` byte offsets, split alignments across `target_partitions` by cumulative bytes, one partition for single-alignment files; ordinals are assigned globally so `alignment_id` fallback is stable across partitions.
- [x] 2.7 Provider tests (Rust): 29 integration tests over the fixtures for full scan, projection-only, `count(*)`, `gs_fields`, multi-alignment, interleaved, `#=GR PP`, missing `//`, wrong header, empty file, compressed variants, partition count, ordinal fallback, annotations.
- [x] 2.8 `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, `cargo test -p datafusion-bio-format-msa` green; committed as `7c5aea0` on `feat/msa-formats` in the companion worktree.
- [ ] 2.9 Push `feat/msa-formats` and open the upstream PR referencing bio-formats#245 (outward-facing; awaiting go-ahead).

## 3. Coordinated Dependency Bump (polars-bio)

- [ ] 3.1 After the upstream PR merges and a tag is cut, move **all** `datafusion-bio-format-*` git pins in `Cargo.toml` to the new tag in one commit (the `datafusion-bio-format-msa` dependency line is already present, currently pointing at `v1.12.1` which does not carry the crate); regenerate `Cargo.lock`. Local development uses an uncommitted `[patch]` in the worktree's `.cargo/config.toml`.
- [x] 3.2 `cargo check` and the existing IO suites run green against the patched crates **before** the MSA bindings were exercised (see §6.9 for the post-binding regression run).

## 4. Rust/PyO3 Integration

- [x] 4.1 Add `InputFormat::{A2m, A3m, Sto}` and their `Display` strings in `src/option.rs`.
- [x] 4.2 Add `MsaReadOptions { object_storage_options, gs_fields: Option<Vec<String>> }` with `#[pyclass]`, `default()`, and a `msa_read_options` field on `ReadOptions` (constructor signature updated).
- [x] 4.3 Add the `register_table` arms in `src/scan.rs` (one shared arm for A2M/A3M selecting `MsaFlavor`, one for Stockholm).
- [x] 4.4 Add `py_describe_sto(path, object_storage_options)` in `src/lib.rs` running the annotation-only reader and returning the long-format batch; register `MsaReadOptions` and the function in the module.
- [x] 4.5 `cargo fmt`, `cargo clippy --all-features` clean, `maturin develop --release` built.

## 5. Python API Integration

- [x] 5.1 `polars_bio/io.py`: `scan_a2m`/`read_a2m`, `scan_a3m`/`read_a3m`, `scan_sto`/`read_sto` (object-store, compression, `projection_pushdown`, `predicate_pushdown` parameters matching `scan_fasta`; `gs_fields` on the Stockholm pair), `describe_sto`, with docstrings following the FASTA examples and the documented one-liner to drop `ss_*` pseudo-sequence rows.
- [x] 5.2 `_FORMAT_COLUMN_TYPES["A2m"]`, `["A3m"]` → string cols `{name, description, sequence}`; `["Sto"]` → `{alignment_id, name, sequence}`; pushdown routes through `apply_predicate_pushdown`/`apply_projection_pushdown` unchanged (gs/gr bags and promoted columns take the permissive path).
- [x] 5.3 Format routing: `_format_to_string` maps the three formats; compression detection (`.gz`/`.bgz`) is handled by the shared core sniffing. (No extension→format auto-detection exists in polars-bio — every format is an explicit function — so `.stk`/`.stockholm` need no extra wiring.)
- [x] 5.4 `polars_bio/sql.py`: `register_a2m`, `register_a3m`, `register_sto(gs_fields=...)`.
- [x] 5.5 `polars_bio/__init__.py`: export the nine new functions plus `describe_sto` and add them to `__all__`.
- [x] 5.6 `black` and `ruff` clean on the changed files (remaining ruff findings in `io.py`/`__init__.py` are pre-existing E402/F401).

## 6. Tests (polars-bio)

- [x] 6.1 `tests/test_msa_io.py`: schema/dtype assertions for all three formats; eager vs lazy equality; SQL registration and `count(*)`.
- [x] 6.2 Byte-level parity for every A2M/A3M fixture against Biopython `SeqIO` (`name`, `sequence`, record count, ragged lengths, pseudo-sequence rows in order). (`hdr.a3m` is compared to the records of `query.a3m`: Biopython's `fasta-blast` mode mis-assigns every record the first id.)
- [x] 6.3 A3M semantic parity: uppercase-or-`-` count constant per row and equal to `pyhmmer.easel.MSAFile(format="a2m")` match columns after stripping `ss_*`/`sa_*`/`aa_*` rows; dotted A2M equals Easel's reconstruction row for row.
- [x] 6.4 Stockholm parity against pyhmmer for PF00001, RF00001 (interleaved), `PF00001_hmmalign.sto`, `query_hh.sto` (names, aligned rows, counts); `#=GR PP` against the block-concatenating line splitter and `#=GC PP_cons` against `msa.posterior_probabilities`; `multi.sto` per-alignment counts.
- [x] 6.5 `describe_sto` differential tests: repeated `DR`/`CC` preserved in order, `GC` value length equals `alignment_length`, `n_sequences` equals `esl-alistat`, interleaved `#=GF`/`#=GC` equal to the `esl-reformat pfam` canonical form.
- [x] 6.6 `gs_fields=["AC"]` promotion and `["AC", "gs"]` sentinel; missing feature → null; SQL over a promoted column.
- [x] 6.7 Edge cases: missing trailing `//`, wrong header error message names the path, empty file, single record, `.gz`/`.bgz` equal uncompressed.
- [x] 6.8 Pushdown: projection-only scan, equality predicate on `name` identical with `predicate_pushdown=True/False`, `target_partitions>1` on `multi.sto` equals single-partition result (prior `target_partitions` restored in the context manager).
- [x] 6.9 `pytest tests/test_msa_io.py` (41 passed, 3 live-HMMER tests passing with `esl-alistat` on `PATH`) and the existing IO suites (`test_io*.py`, `test_fasta_write.py`, `test_pgen_io.py`, `test_bcf_io.py`, `test_context_options.py`: 406 passed) green against the patched crates.

## 7. Documentation

- [x] 7.1 `docs/features/reading.md`: rows for A2M, A3M, Stockholm in the support matrix and a format-specific-notes section (schema, verbatim semantics, pseudo-sequence filter, comma-identifier deviation, partitioning note, non-goals).
- [x] 7.2 API reference entries for the nine new functions and `describe_sto` (`docs/api/reading.md`, `docs/api/sql.md`).
- [x] 7.3 `CHANGELOG.md` `[Unreleased]` entry referencing #459 and bio-formats#245.

## 8. Release and Follow-up Communication

- [ ] 8.1 Post the final schema and non-goals on polars-bio#459 and bio-formats#245; ask the reporter to validate on their A3M/A2M/STO workloads before release.
- [ ] 8.2 Open the polars-bio PR referencing #459 once §3.1 lands.

## 9. Deferred Follow-ups (not part of this change)

- [ ] 9.1 `expand_inserts=True` for A3M→A2M rectangular expansion (requires whole-alignment buffering).
- [ ] 9.2 `skip_pseudo_sequences=True` for A3M.
- [ ] 9.3 `write_*`/`sink_*` for A2M/A3M (near-free via the FASTA writer) and Stockholm (block width and annotation round-trip decisions).
