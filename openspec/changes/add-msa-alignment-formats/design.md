## Context

polars-bio reads every format through a `datafusion-bio-format-*` provider crate pinned by git
rev in `Cargo.toml`, plumbed through `InputFormat` (`src/option.rs:131`), a per-format
`*ReadOptions` struct, `register_table` in `src/scan.rs`, and finally `polars_bio/io.py`,
`sql.py` and `__init__.py`. FASTA (`bio-format-fasta`) is the closest existing sibling.

Format references, all read for this design:

- Stockholm: <https://sonnhammer.sbc.su.se/Stockholm.html>
- A2M: <https://nemates.org/uky/520/Lab/lab8/SAM_T08/a2m-desc.html>
- A3M: <https://github.com/soedinglab/hh-suite/wiki#file-formats>

Real files probed while designing: hh-suite `data/query.a3m` (59 seqs) and
`scripts/hhpred/example/test.a3m` (2,547 seqs incl. `ss_dssp`/`ss_pred`/`ss_conf`
pseudo-sequences); Pfam PF00001 seed (63 seqs, 49 `#=GF` lines, single block); Rfam RF00001
seed (712 seqs, **interleaved**, two blocks). Findings that shaped the decisions below:

- A2M's spec says insert-column dots "may be omitted", and Easel's own A2M *writer* emits
  dotless output (0 dots, 901 lowercase in the probe). Dotless A2M is byte-for-byte A3M, so
  neither format is guaranteed rectangular.
- A3M may start with `#A3M#`/`#` lines before the first `>`, and may carry reserved
  `>ss_pred`, `>ss_conf`, `>ss_dssp`, `>sa_dssp`, `>aa_dssp` pseudo-sequence records (the
  hhpred example has three, *ahead of* the query).
- `#=GF` features **repeat** and are order-significant (PF00001: `DR`×8, `DC`, `WK`×2, ten
  `CC` continuation lines). Any dict-shaped representation silently collapses them.
- Interleaving is current practice, not legacy: Rfam ships wrapped seeds and Easel writes
  200-column blocks by default. Biopython's newer `Bio.Align` Stockholm parser cannot read
  them; ours must.
- No Rust crate exists for Stockholm (`crates.io` "stockholm" is an algo-trading crate;
  `hmmer-pure-rs` embeds a parser but is far too heavy a dependency).

Stakeholders: issue reporter (@marinegor-apheris, protein MSA workloads; volunteered to
implement upstream — the maintainer chose to implement both sides and ask them to validate
against real data), polars-bio maintainers, `datafusion-bio-formats` maintainers.

## Goals / Non-Goals

- Goals:
  - Lazy, streaming, object-store-aware readers for `.a2m`, `.a3m`, `.sto`/`.stk`/`.stockholm`
    (plus `.gz`/`.bgz`), with the same API ergonomics as `scan_fasta`.
  - Lossless: every byte of sequence and every annotation line is recoverable from the
    DataFrame plus `describe_sto`.
  - Projection and predicate pushdown for the string columns.
  - Parity-tested against the reference implementation (Easel via `pyhmmer`) and Biopython.
- Non-Goals:
  - Writing / `sink_*` for any of the three formats.
  - Interpreting alignment semantics: no A3M→A2M expansion, no match/insert column
    reconstruction, no consensus. `expand_inserts` is a documented follow-up.
  - Splitting `name/start-end` Stockholm sequence names into columns.
  - Honouring the A2M-only "identifier terminated by whitespace **or comma**" rule (see
    Decisions).

## Decisions

- **One crate, `datafusion-bio-format-msa`, not three.** A2M and A3M parse identically (both
  FASTA-shaped; we do not interpret gap characters) and Stockholm shares the object-store and
  compression plumbing. Three crates would triple the Cargo/CI/version surface for roughly one
  parser's worth of code.
  - Alternatives: `bio-format-a2m` + `bio-format-a3m` + `bio-format-stockholm` (rejected:
    boilerplate); folding A2M/A3M into `bio-format-fasta` behind a flag (rejected: FASTA's
    writer/serializer would have to learn about `#` header lines and pseudo-sequences, and the
    formats deserve their own `InputFormat` variants for detection and docs).

- **Crate layout** mirrors `bio-format-fasta`:
  - `storage.rs` — `datafusion_bio_format_core::object_storage` for local/GCS/S3/HTTP and
    `gz`/`bgz`.
  - `fastalike/{table_provider,physical_exec}.rs` — A2M **and** A3M via one code path
    parameterized by `MsaFlavor { A2m, A3m }`. Record reading delegates to
    `noodles-fasta`; a pre-filter skips lines starting with `#` before the first `>`
    (`noodles-fasta`'s `read_definition` is a bare `read_line` with no `>` check, so it would
    otherwise treat `#A3M#` as a definition line).
  - `stockholm/{reader,table_provider,physical_exec}.rs` — hand-written line parser.

- **A2M/A3M schema is identical to FASTA**: `name` Utf8 non-null (header up to first
  whitespace), `description` Utf8 nullable (rest of header), `sequence` LargeUtf8 non-null,
  verbatim. Rationale: lossless, fully streaming, composes with everything that already
  consumes `scan_fasta` output, and exactly what the reporter asked for.
  - Alternatives: always expand A3M to rectangular (rejected: needs the max insert count per
    match column, i.e. buffering the whole alignment before emitting any row — breaks the
    streaming model every other format follows, and is lossy); raw + `aligned_sequence`
    column (rejected: same buffering cost plus doubled sequence memory).
  - Pseudo-sequences (`ss_*`, `sa_*`, `aa_*`) are emitted as ordinary rows and documented with
    the one-line Polars filter to drop them. A `skip_pseudo_sequences` flag is a cheap
    follow-up if asked for.
  - The A2M "identifier terminated by whitespace **or comma**" rule is deliberately **not**
    honoured: real-world A2M/A3M headers are UniRef-style (`>tr|…| … n=1 Tax=…, …`) where a
    comma split would mangle names, and FASTA/A3M split on whitespace only. Documented as a
    deviation from the SAM A2M page.

- **Stockholm schema — one row per sequence per alignment**:
  - `alignment_id` Utf8 non-null — `#=GF ID` if present, else `#=GF AC`, else the alignment's
    0-based ordinal within the file as a string.
  - `name` Utf8 non-null — verbatim; `name/start-end` is **not** split.
  - `sequence` LargeUtf8 non-null — concatenation of that name's sequence lines across all
    interleaved blocks, verbatim (`.` and `-` both preserved).
  - `gs` List<Struct<tag: Utf8 non-null, value: Utf8 nullable>> nullable — that sequence's
    `#=GS` lines, file order, duplicates preserved.
  - `gr` same type — that sequence's `#=GR` lines, each value already concatenated across
    blocks.
  - Optional `gs_fields: list[str]` promotes named `#=GS` features to top-level nullable Utf8
    columns (first occurrence wins; the `gs` bag is still available via the `"gs"` sentinel,
    exactly like GFF's `"attributes"` sentinel in `attr_fields`).
  - Rationale for `List<Struct>` rather than Arrow `Map`: it is the type GFF `attributes`
    already uses (`bio-format-gff/src/table_provider.rs:51`), Polars has no Map dtype, and
    it preserves repeats and order.
  - Alternatives: broadcast `#=GF`/`#=GC` onto every row (rejected: `#=GC` strings are
    alignment-width and `#=GF` blocks run to dozens of lines — Pfam-scale duplication);
    flat well-known columns only (rejected: silently drops `SS_cons`, `RF`, `PP`, every GF
    field); single JSON column (rejected: unqueryable from SQL, inconsistent with VCF INFO /
    GFF attributes).

- **`describe_sto(path)` → `pl.DataFrame`**, one row per (alignment, annotation line) with
  columns `alignment_id` Utf8, `kind` Utf8 (`"GF"` | `"GC"`), `feature` Utf8, `value`
  LargeUtf8, `n_sequences` UInt32, `alignment_length` UInt32. Long format because GF features
  repeat and continuation order matters. Implemented as a dedicated `py_describe_sto` that
  runs the Stockholm reader in an annotation-only mode (no sequence materialisation) rather
  than smuggling GF/GC through Arrow schema metadata — schema metadata is per-table, and a
  multi-alignment file has many GF blocks.

- **Streaming and partitioning**: A2M/A3M stream record-by-record like FASTA in a single
  partition (row order is preserved, so "query is the first row" holds). Stockholm partitions
  on `//` byte offsets, so a multi-alignment file spreads across `target_partitions`. A
  single-alignment file is one partition; the parser streams block-by-block but must hold
  that alignment's `name → sequence` map until `//`, because interleaving makes that
  unavoidable. Memory is bounded by one alignment, never by the file. Empty projection
  (`count(*)`) skips sequence buffering entirely.
  - `FastaReadOptions.parallel` (`src/option.rs:867`) is an unused flag upstream; it is not
    replicated.

- **polars-bio wiring**: `InputFormat::{A2m, A3m, Sto}`; a single `MsaReadOptions`
  (`object_storage_options`, `gs_fields: Option<Vec<String>>`) shared by all three variants
  — three near-empty option structs on an already-15-field `ReadOptions` is noise; three
  `register_table` arms; `_FORMAT_COLUMN_TYPES["A2m"|"A3m"]` string cols
  `{name, description, sequence}` and `["Sto"]` `{alignment_id, name, sequence}` so pushdown
  works. Extension detection: `.a2m`, `.a3m`, `.sto`, `.stk`, `.stockholm`, each also with
  `.gz`/`.bgz`.

- **Stockholm parsing rules** (where Sonnhammer's page is silent, Easel behaviour wins):
  - File MUST start with `# STOCKHOLM 1.0`; anything else is an error naming the path.
  - Lines: blank → ignored; `//` → end of alignment; `#=GF F text` / `#=GS name F text` /
    `#=GC F string` / `#=GR name F string` → annotations; other `#` → comment, ignored;
    otherwise `name sequence`.
  - `#=GR` and `#=GC` values and sequence lines concatenate across blocks by name; `#=GF` and
    `#=GS` are one record per line.
  - A missing trailing `//` at EOF emits the pending alignment (Easel accepts this) with a
    warning-level log line.
  - No line-length or name-length limits are enforced (the spec's 10,000-char "limit" is a
    recommendation Pfam-full already exceeds).
  - Sequence-length consistency across rows is **not** validated by the reader; it is a
    property of the file the user can check in Polars.

## Test Oracles

Three independent reference implementations were probed against the real files listed in
Context. Two of them are the tools that *define* these formats in practice:

- **Easel / HMMER 3.4 CLI** (`esl-alistat`, `esl-reformat`) — Eddy lab, the Stockholm
  reference implementation. Built from source in ~1 min; `bioconda::hmmer` on CI. Not
  pip-installable.
- **hh-suite `scripts/reformat.pl`** — the canonical A3M/A2M converter. Pure Perl
  (`use strict; use warnings` only), runs standalone with no hh-suite build. GPL-3, so it is
  **fetched by the fixture generator, never vendored** into this Apache-2.0 repo.
- **pyhmmer** (Easel bindings, pip wheels) and **Biopython** — the in-test oracles that run on
  every CI job.

Key cross-check: `reformat.pl a3m a2m` and Easel's A2M reader produce **byte-identical**
dotted A2M for `query.a3m` (59/59 rows, width 849). Two unrelated implementations agreeing
exactly is the strongest evidence available that our verbatim A3M passthrough is being
judged against the right semantics.

| Format | Property | Oracle | Evidence |
|---|---|---|---|
| sto | nseq and alignment length per alignment, multi-alignment files | `esl-alistat` (CLI) and `pyhmmer.easel.MSAFile(format="stockholm")` | PF00001 → 63/722; interleaved RF00001 → 712/230; `multi.sto` → two alignments; `hmmalign` output → 63/724 |
| sto | **de-interleaving** (block concatenation of sequences, `#=GR`, `#=GC`) | `esl-reformat pfam` rewrites any Stockholm into canonical single-block form; the result is checked in as an expected-output fixture | RF00001: 712 sequence lines out, `#=GC` 4 lines (2 features × 2 blocks) → 2, all 50 `#=GF` lines verbatim and in order |
| sto | verbatim `#=GF`/`#=GS` lines, order and repeats (`describe_sto`, `gs` bag) | `esl-reformat pfam` output is line-identical for `#=GS` and for 21 of 24 `#=GF` features; Easel **parses and normalises** `GA`/`TC`/`NC` (`30.50 30.50;` → `30.5 30.5`, reordered) and emits `#=GC RF` first | so: line-identical assertion for every feature except `GA`/`TC`/`NC`, which are compared numerically; `#=GC` compared per feature, order-insensitive |
| sto | `#=GR`/`#=GC` per-residue lines, interleaved | fixture **generated** by `pyhmmer.hmmer.hmmalign` (GR `PP`, GC `PP_cons`/`RF`, 200-col blocks); `gr` PP equals `msa.posterior_probabilities` | oracle-generated, no hand edits |
| sto | sequences and names of a Stockholm derived from an A3M | `reformat.pl a3m sto` (keeps the first header token as the name, drops the description — Stockholm names cannot contain spaces) | 59/59 names and sequences round-trip `a3m → sto → a3m` byte-identical |
| sto | named `#=GF`/`#=GC` keys, single-block files | Biopython `Bio.Align` stockholm | works on PF00001; fails on interleaved input, so single-block only |
| a3m | alignment semantics: match-column count constant per row and equal to the reference | `esl-alistat --informat a2m` / `pyhmmer.easel.MSAFile(format="a2m")` (Easel reads dotless A2M, i.e. A3M) **and** `reformat.pl a3m a2m` | query.a3m → 59/849 from both; test.a3m (pseudo-seqs stripped) → 2544/1821, 290 match cols on every row |
| a3m | `ss_*`/`sa_*`/`aa_*` pseudo-sequences | `reformat.pl` keeps them as rows (2547/2547, first four names `ss_dssp`, `ss_pred`, `ss_conf`, `1a7j_A`); Easel rejects the file (`ss_conf` digits → "invalid sequence characters") | confirms "emit as rows, document the filter"; Easel-based checks strip them first |
| a2m/a3m | name, description, sequence bytes, ragged rows, record count | Biopython `SeqIO.parse(f, "fasta")` | ids identical, ragged on both a3m files |
| a3m | leading `#A3M#`/`#` lines | Biopython `"fasta-blast"` (plain `"fasta"` refuses such files) | 59 records recovered |
| a2m (dotted) | rectangular parse | expected-output fixture generated by `reformat.pl` **and** Easel (byte-identical); read back by Biopython `Bio.Align` a2m and pyhmmer | 59 × 849 |
| all | `.gz`/`.bgz` | same oracle on the decompressed bytes | — |

How the two tiers fit together:

1. **Fixture generation** (`tests/data/msa/generate_fixtures.py`, run by a maintainer, output
   checked in): fetches the source files and `reformat.pl`, requires `esl-reformat`/
   `esl-alistat` on `PATH`, and asserts the cross-implementation agreements above before
   writing expected-output files (`*_pfam.sto` canonical single-block forms,
   `query_dotted.a2m`, `query_hh.sto`, `PF00001_hmmalign.sto`, `multi.sto`, `hdr.a3m`) plus an
   `expected.json` with per-alignment `n_sequences`/`alignment_length` from `esl-alistat`.
2. **In-test parity** (`tests/test_msa_io.py`, every CI job): compares our output against the
   checked-in expected files and against pyhmmer/Biopython live. No Perl, no HMMER binary
   needed to run the suite.
3. **Optional live-CLI job**: one CI job installs `bioconda::hmmer` and runs the
   `esl-alistat`/`esl-reformat` assertions live; tests are `skipif(shutil.which("esl-alistat")
   is None)` elsewhere.

Fixtures are vendored small under `tests/data/msa/` with their source URL and retrieval
date in a `README.md`: PF00001 seed (InterPro API), RF00001 seed (rfam.org), hh-suite
`query.a3m` and a `≤200`-record head of `test.a3m` that keeps the three pseudo-sequences,
plus the generated expected-output files above.

## Risks / Trade-offs

- **Stockholm has no formal spec; Pfam/Rfam/HMMER each stretch it.** → Easel behaviour is the
  tie-breaker, the parsing rules above are written down, and every rule has a fixture.
- **Coupled dependency bump.** All 14 `datafusion-bio-format-*` pins move together to the rev
  that adds the 15th crate; past bumps (DataFusion 50→53, bio-formats v1.8.x) have carried
  unrelated regressions. → Land the upstream crate first, bump once, and run the full
  `tests/test_io_*.py` suite before touching polars-bio code.
- **Single-alignment Stockholm files cannot be partitioned.** → Documented; memory is bounded
  by one alignment; `count(*)` avoids sequence buffering.
- **`pyhmmer` availability** on the CI matrix (py3.10–3.13, linux/macOS). → It installed
  cleanly on macOS arm64 / py3.12 during design; it goes in `[project.optional-dependencies]`
  next to `pysam`/`pyBigWig` and the parity tests `pytest.importorskip` it, so a missing wheel
  degrades to skipped parity tests, never a red build.
- **Pseudo-sequence rows surprise users** who expect only homologs. → Documented with the
  filter; `skip_pseudo_sequences` is a one-flag follow-up.

## Migration Plan

Additive only; nothing to migrate. Sequence: (1) upstream crate + tests on a
`datafusion-bio-formats` branch, (2) merge upstream, (3) single coordinated pin bump in
polars-bio with the existing IO suite green, (4) polars-bio bindings, docs, tests, (5) ask the
reporter to validate on their workloads, (6) release. Rollback is reverting the pin bump.

## Open Questions

- None blocking. `expand_inserts` and `skip_pseudo_sequences` are tracked as follow-ups in
  `tasks.md` §9 and will get their own change if requested.
