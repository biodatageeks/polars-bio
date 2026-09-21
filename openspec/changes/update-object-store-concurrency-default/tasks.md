# Implementation Tasks

## 1. Tests first
- [x] 1.1 `tests/test_object_store_defaults.py`: assert `inspect.signature(fn).parameters["concurrent_fetches"].default == 8` for every public function exposing them in `polars_bio.io` and `polars_bio.sql`; run and see it fail on the 33 functions still at 1

## 2. Implementation
- [x] 2.1 Flip `concurrent_fetches: int = 1` → `= 8` in `polars_bio/io.py` (32) and `polars_bio/sql.py` (`register_fasta`); 33 in total
- [x] 2.2 Replace the three docstring variants with one sentence naming S3/GCS/HTTP, the default 8 and `1` as the choice to disable parallel fetching; drop the stale "[GCS]" prefix
- [x] 2.3 Run 1.1 and see it pass; run `pytest tests/test_io_vcf.py tests/test_io_fasta.py -q` as a smoke check that defaults still bind

## 3. Docs
- [x] 3.1 `docs/features/cloud.md`: mark S3 concurrent requests supported (footnote: needs datafusion-bio-formats#253 in the pin) and add a "Tuning concurrent requests" section
- [x] 3.2 CHANGELOG `[Unreleased]` → `### Changed`

## 4. Validation
- [x] 4.1 `openspec validate update-object-store-concurrency-default --strict`
- [x] 4.2 black / isort / ruff on changed files; pre-commit passes

## 5. Review fixes
- [x] 5.1 Pin merged datafusion-bio-formats#253 in Cargo.toml and Cargo.lock so S3 honors the new default without depending on #460
- [x] 5.2 Set lazy-scan fallback concurrency to 8 and cover missing and explicit stored options
- [x] 5.3 Qualify HTTP/GCS HEAD behavior, option availability and the 8/64 MiB chunk defaults in documentation and spec
- [x] 5.4 Verify S3 request behavior against a local HTTP server with the pinned core, including explicit concurrency 1
- [x] 5.5 Run targeted Python tests, native checks and OpenSpec validation (123 Python tests, 19 Rust tests, Clippy and pre-commit passed)
