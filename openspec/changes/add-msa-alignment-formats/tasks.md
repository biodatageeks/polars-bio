## 1. Fixtures and Oracles (before any parser code)

- [ ] 1.1 Treat this change as the authoritative tracker for both polars-bio and `biodatageeks/datafusion-bio-formats`; do not create a mirrored provider OpenSpec change.
- [ ] 1.2 Vendor fixtures under `tests/data/msa/` with a `README.md` recording source URL and retrieval date: Pfam PF00001 seed (`https://www.ebi.ac.uk/interpro/wwwapi/entry/pfam/PF00001/?annotation=alignment:seed`, gunzipped), Rfam RF00001 seed (`https://rfam.org/family/RF00001/alignment/stockholm`, interleaved), hh-suite `data/query.a3m`, and a head of hh-suite `scripts/hhpred/example/test.a3m` that keeps the three `ss_*` pseudo-sequences plus ~200 homologs.
- [ ] 1.3 Check in `tests/data/msa/generate_fixtures.py` that (a) fetches the sources and hh-suite `scripts/reformat.pl` (GPL-3 — fetched at generation time, **never vendored**), (b) requires `esl-reformat`/`esl-alistat` on `PATH`, (c) asserts the cross-implementation agreements from `design.md` (reformat.pl ↔ Easel byte-identical dotted A2M; `a3m → sto → a3m` sequence round-trip; Easel de-interleave preserves `#=GS` and non-`GA`/`TC`/`NC` `#=GF` lines verbatim), and (d) writes the expected-output files: `PF00001_pfam.sto` and `RF00001_pfam.sto` (`esl-reformat pfam` canonical single-block forms), `PF00001_hmmalign.sto` (`#=GR PP`, `#=GC PP_cons`/`RF`, 200-column blocks, via pyhmmer), `multi.sto`, `query_dotted.a2m`, `query_hh.sto`, `hdr.a3m` (`#A3M#` + `#` lines prepended), `expected.json` (per-alignment `n_sequences`/`alignment_length` from `esl-alistat`), plus hand-written minimal edge files: missing trailing `//`, wrong header, empty file, single-record file, `.gz`/`.bgz` variants.
- [ ] 1.4 Add `pyhmmer` and `biopython` to `[project.optional-dependencies]` next to `pysam`/`pyBigWig`, and to the CI test extras; parity tests `pytest.importorskip` both. No Perl or HMMER binary is required to run the suite.
- [ ] 1.5 Mark the live `esl-alistat`/`esl-reformat` tests `skipif(shutil.which("esl-alistat") is None)`. (Scoped down: this repository has no GitHub workflow that runs `pytest` — tests run via `make test` — so there is no CI job to add `bioconda::hmmer` to. The live checks run wherever HMMER is on `PATH`; the checked-in `expected.json` carries the `esl-alistat` values everywhere else.)
- [ ] 1.6 Write the `describe_sto`/`gs`-bag comparison against the `*_pfam.sto` canonical forms: line-identical for `#=GS` and every `#=GF` feature except `GA`/`TC`/`NC` (compared numerically because Easel normalises them), `#=GC` compared per feature order-insensitively.

## 2. Provider Crate: `datafusion-bio-format-msa` (companion repo)

- [ ] 2.1 Scaffold `datafusion/bio-format-msa` mirroring `bio-format-fasta` (`Cargo.toml` with workspace deps, `README.md`, `lib.rs`); add to the workspace members.
- [ ] 2.2 `storage.rs`: reuse `datafusion_bio_format_core::object_storage` for local/GCS/S3/HTTP and `gz`/`bgz`; no new storage code.
- [ ] 2.3 `fastalike/`: `MsaFlavor { A2m, A3m }`, `FastaLikeTableProvider`, `FastaLikeExec` delegating record parsing to `noodles-fasta`; pre-filter that skips `#`-prefixed lines before the first `>`; schema identical to FASTA (`name`, `description`, `sequence`); whitespace-only identifier split.
- [ ] 2.4 `stockholm/reader.rs`: line-oriented parser implementing the rules in `design.md` (header validation, `#=GF/GS/GC/GR` dispatch, block concatenation by name, `//` termination, missing-terminator tolerance with a warning, other `#` lines ignored). Expose an annotation-only mode that never allocates sequence buffers.
- [ ] 2.5 `stockholm/table_provider.rs` + `physical_exec.rs`: schema (`alignment_id`, `name`, `sequence`, `gs`, `gr`) with `gs_fields` promotion (first occurrence wins; `"gs"` sentinel keeps the bag); `alignment_id` fallback `ID → AC → ordinal`; projection flags so unprojected `sequence`/`gs`/`gr` are never built; empty projection supports `count(*)`.
- [ ] 2.6 Stockholm partition planning: scan for `//` byte offsets, split alignments across `target_partitions`, one partition for single-alignment files; ordinals are assigned globally so `alignment_id` fallback is stable across partitions.
- [ ] 2.7 Provider tests (Rust): every fixture from §1 for full scan, projection-only, `count(*)`, `gs_fields`, multi-alignment, interleaved, missing `//`, wrong header, empty file, compressed variants, partition count.
- [ ] 2.8 `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, `cargo test -p datafusion-bio-format-msa`; open the upstream PR referencing bio-formats#245.

## 3. Coordinated Dependency Bump (polars-bio)

- [ ] 3.1 After the upstream PR merges, move **all** `datafusion-bio-format-*` git pins in `Cargo.toml` to the new rev in one commit and add `datafusion-bio-format-msa`; regenerate `Cargo.lock`.
- [ ] 3.2 `cargo check` and run the existing `tests/test_io_*.py` suite green **before** any MSA binding code, so a dependency regression is not confused with new code.

## 4. Rust/PyO3 Integration

- [ ] 4.1 Add `InputFormat::{A2m, A3m, Sto}` and their `Display` strings in `src/option.rs`.
- [ ] 4.2 Add `MsaReadOptions { object_storage_options, gs_fields: Option<Vec<String>> }` with `#[pyclass]`, `Default`, and a `msa_read_options` field on `ReadOptions` (constructor signature updated).
- [ ] 4.3 Add three `register_table` arms in `src/scan.rs` constructing the provider with the right `MsaFlavor` / Stockholm provider.
- [ ] 4.4 Add `py_describe_sto(path, object_storage_options)` in `src/lib.rs` running the annotation-only reader and returning the long-format Arrow batch; register `MsaReadOptions` and the function in the module.
- [ ] 4.5 `cargo fmt`, `cargo clippy -- -D warnings`, `maturin develop --release`.

## 5. Python API Integration

- [ ] 5.1 `polars_bio/io.py`: `scan_a2m`/`read_a2m`, `scan_a3m`/`read_a3m`, `scan_sto`/`read_sto` (object-store, compression, `projection_pushdown`, `predicate_pushdown` parameters matching `scan_fasta`; `gs_fields` on the Stockholm pair), `describe_sto`, with docstrings following the FASTA examples and a documented one-liner to drop `ss_*` pseudo-sequence rows.
- [ ] 5.2 `_FORMAT_COLUMN_TYPES["A2m"]`, `["A3m"]` → string cols `{name, description, sequence}`; `["Sto"]` → `{alignment_id, name, sequence}` plus any `gs_fields` names; verify pushdown routes through `apply_predicate_pushdown`/`apply_projection_pushdown` unchanged.
- [ ] 5.3 Extension detection for `.a2m`, `.a3m`, `.sto`, `.stk`, `.stockholm` and their `.gz`/`.bgz` forms wherever internal path→format routing already exists.
- [ ] 5.4 `polars_bio/sql.py`: `register_a2m`, `register_a3m`, `register_sto(gs_fields=...)`.
- [ ] 5.5 `polars_bio/__init__.py`: export the nine new functions and add them to `__all__`.
- [ ] 5.6 `black`, `ruff --fix`, `isort`, `mypy` clean.

## 6. Tests (polars-bio)

- [ ] 6.1 `tests/test_msa_io.py`: schema/dtype assertions for all three formats; eager vs lazy equality; SQL registration and `count(*)`.
- [ ] 6.2 Byte-level parity for every A2M/A3M fixture against Biopython `SeqIO.parse(..., "fasta-blast")` (`name`, `sequence`, record count, ragged lengths, pseudo-sequence rows in order).
- [ ] 6.3 A3M semantic parity: uppercase-or-`-` count constant per row and equal to `pyhmmer.easel.MSAFile(format="a2m")` match columns after stripping `ss_*`/`sa_*`/`aa_*` rows.
- [ ] 6.4 Stockholm parity against pyhmmer for PF00001, RF00001 (interleaved), `PF00001_hmmalign.sto` (`gr` PP equals `msa.posterior_probabilities`), and `multi.sto` (two `alignment_id` values, per-alignment row counts).
- [ ] 6.5 `describe_sto` differential test against the naive line-splitter: repeated `DR`/`CC` preserved in order, `GC` value length equals `alignment_length`, `n_sequences` equals pyhmmer.
- [ ] 6.6 `gs_fields=["AC"]` promotion and `["AC", "gs"]` sentinel; missing feature → null.
- [ ] 6.7 Edge cases: missing trailing `//`, wrong header error message names the path, empty file, single record, `.gz`/`.bgz` equal uncompressed.
- [ ] 6.8 Pushdown: projection-only scan, equality predicate on `name` identical with `predicate_pushdown=True/False`, `target_partitions>1` on `multi.sto` equals single-partition result (restore the prior `target_partitions` in the fixture teardown — see the fastqc leak in memory).
- [ ] 6.9 Run `python -m pytest tests/test_msa_io.py tests/test_io_*.py -v` green.

## 7. Documentation

- [ ] 7.1 `docs/features/reading.md`: rows for A2M, A3M, Stockholm (schema tables, compression, object-store support, the pseudo-sequence filter, the comma-identifier deviation, single-alignment partitioning note).
- [ ] 7.2 API reference entries for the nine new functions and `describe_sto`.
- [ ] 7.3 `CHANGELOG.md` `[Unreleased]` entry referencing #459 and bio-formats#245.

## 8. Release and Follow-up Communication

- [ ] 8.1 Post the final schema and non-goals on polars-bio#459 and bio-formats#245; ask the reporter to validate on their A3M/A2M/STO workloads before release.
- [ ] 8.2 Open the polars-bio PR referencing #459; run the full CI matrix (confirm `pyhmmer` wheels resolve on every job or the parity tests skip cleanly).

## 9. Deferred Follow-ups (not part of this change)

- [ ] 9.1 `expand_inserts=True` for A3M→A2M rectangular expansion (requires whole-alignment buffering).
- [ ] 9.2 `skip_pseudo_sequences=True` for A3M.
- [ ] 9.3 `write_*`/`sink_*` for A2M/A3M (near-free via the FASTA writer) and Stockholm (block width and annotation round-trip decisions).
