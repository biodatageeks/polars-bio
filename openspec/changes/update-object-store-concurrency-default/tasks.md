# Implementation Tasks

## 1. Tests first
- [x] 1.1 `tests/test_object_store_defaults.py`: assert `inspect.signature(fn).parameters["concurrent_fetches"].default == 8` and `chunk_size` default `8` for every public function exposing them in `polars_bio.io` and `polars_bio.sql`; run and see it fail on the 33 functions still at 1

## 2. Implementation
- [x] 2.1 Flip `concurrent_fetches: int = 1` → `= 8` in `polars_bio/io.py` (32) and `polars_bio/sql.py` (`register_fasta`); 33 in total
- [x] 2.2 Replace the three docstring variants with one sentence naming S3/GCS/HTTP, the default 8 and `1` as the sequential choice; drop the stale "[GCS]" prefix
- [x] 2.3 Run 1.1 and see it pass; run `pytest tests/test_io_vcf.py tests/test_io_fasta.py -q` as a smoke check that defaults still bind

## 3. Docs
- [x] 3.1 `docs/features/cloud.md`: mark S3 concurrent requests supported (footnote: needs datafusion-bio-formats#253 in the pin) and add a "Tuning concurrent requests" section
- [x] 3.2 CHANGELOG `[Unreleased]` → `### Changed`

## 4. Validation
- [x] 4.1 `openspec validate update-object-store-concurrency-default --strict`
- [x] 4.2 black / isort / ruff on changed files; pre-commit passes
