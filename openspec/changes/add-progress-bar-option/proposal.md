# Change: Add a session option for the processed-rows progress bar

GitHub issue: https://github.com/biodatageeks/polars-bio/issues/504

## Why

Every `collect()` of a LazyFrame returned by a reader, `pb.sql`, a range
operation or `pb.depth` prints a tqdm bar to stderr
(`798810rows [00:00, 9152164.89rows/s]`). The bar was created unconditionally in
four places. Pipelines that run several operations per sample fill their logs,
CI output and notebooks with bars that carry no information.

The only way to suppress it was `TQDM_DISABLE=1` (documented in #432), which:

1. is process-wide: it also hides the progress bars of the application and of
   every other library that uses tqdm;
2. has to be set before tqdm is imported, so a running notebook kernel must be
   restarted.

#496 put polars-bio's logging under the application's control; the progress bar
was the remaining output an application could not switch off for polars-bio
alone.

## What Changes

- New session option `datafusion.bio.progress_bar`, exported as
  `pb.POLARS_BIO_PROGRESS_BAR`, default `"true"`. It is set and read with the
  existing `pb.set_option` / `pb.get_option`, next to the coordinate-system
  options.
- New function `pb.set_progress_bar(enabled)`, next to `pb.set_loglevel`, as a
  shorthand for `pb.set_option(pb.POLARS_BIO_PROGRESS_BAR, enabled)`. The option
  stays the single source of truth, so both spellings always agree.
- The four `tqdm(unit="rows")` calls (`io.py`, `utils.py`, `range_op_io.py`,
  `pileup_op.py`) are replaced by one helper, `polars_bio/_progress.py`, which
  reads the option when a LazyFrame starts streaming. The option can therefore
  be changed at any time and applies to frames created before the change.
- When the option is on, the bar is created without a `disable` argument, so
  `TQDM_DISABLE=1` keeps working. Passing `disable=False` explicitly, as
  sketched in the issue, would override the environment variable and break the
  behavior documented in #432.
- `pb.set_option` accepts `True`/`False` and the strings `"true"`/`"false"`
  (any case) for this option and raises `ValueError` for anything else.
  Extension options are otherwise stored without validation, so a typo such as
  `"flase"` would be accepted and silently leave the bar on.
- Docs: `docs/features/reading.md` (Progress output), `docs/faq.md`, the API
  reference (`docs/api/auxiliary.md`), the `set_option` docstring and the
  changelog.

Not included: making the bar disappear automatically when stderr is not a TTY
(tqdm's `disable=None`). The issue lists it as an optional, separate change of
the default. The stored value is a string, so an `"auto"` value can be added
later without breaking this option.

## Impact

- Affected specs: `progress-output` (new capability)
- Affected code: `polars_bio/_progress.py` (new), `polars_bio/constants.py`,
  `polars_bio/context.py`, `polars_bio/__init__.py`, `polars_bio/io.py`,
  `polars_bio/utils.py`, `polars_bio/range_op_io.py`, `polars_bio/pileup_op.py`
- Python only; no Rust change. `datafusion.bio.*` keys are already stored by the
  Rust session context for the Python layer to read back.
- Not breaking: the default keeps the bar on. Code that patched the module-level
  `tqdm` name in those four modules (unsupported, mentioned in the issue as a
  workaround) no longer has an effect.
