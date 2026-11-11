"""Row progress bar shown while polars-bio streams batches into Polars."""

from tqdm.auto import tqdm

from .constants import POLARS_BIO_PROGRESS_BAR
from .context import Context


def progress_bar_enabled() -> bool:
    """Return whether the `datafusion.bio.progress_bar` option is on."""
    value = Context().get_option(POLARS_BIO_PROGRESS_BAR)
    return value is None or value.strip().lower() != "false"


def row_progress_bar() -> tqdm:
    """Create the processed-rows bar for one stream of batches.

    The option is read here, when a LazyFrame starts streaming, so it can be
    changed at any time and also applies to frames created earlier.
    """
    if progress_bar_enabled():
        # Leave `disable` unset: tqdm takes its default from `TQDM_DISABLE`,
        # which an explicit `disable=False` would override.
        return tqdm(unit="rows")
    return tqdm(unit="rows", disable=True)
