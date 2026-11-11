# Implementation Tasks

## 1. Tests first
- [x] 1.1 `tests/test_progress_bar.py`: default value; bar shown by default and absent when the option is off for a reader, `pb.sql`, a range operation and `pb.depth`; option read at collect time; accepted and rejected values; other tqdm bars unaffected; `TQDM_DISABLE=1` still suppresses the bar. Run and see it fail

## 2. Implementation
- [x] 2.1 `POLARS_BIO_PROGRESS_BAR` in `polars_bio/constants.py`, exported from `polars_bio/__init__.py`, default `"true"` set in `Context.__init__`
- [x] 2.2 Validate the value in `Context.set_option`
- [x] 2.3 `polars_bio/_progress.py` with `row_progress_bar()`; use it in `io.py`, `utils.py`, `range_op_io.py` and `pileup_op.py`
- [x] 2.4 Run 1.1 and see it pass; confirm by mutation that the tests fail when the option is ignored and when `disable=False` is passed explicitly

## 3. Docs
- [x] 3.1 `docs/features/reading.md` Progress output section and `docs/faq.md`
- [x] 3.2 `set_option` docstring
- [x] 3.3 CHANGELOG `[Unreleased]` → `### Added`

## 4. Validation
- [x] 4.1 `openspec validate add-progress-bar-option --strict`
- [x] 4.2 black / isort on changed files; related test modules pass
