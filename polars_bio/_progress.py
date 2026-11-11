"""Row progress bar shown while polars-bio streams batches into Polars."""

from tqdm.auto import tqdm

from .constants import POLARS_BIO_PROGRESS_BAR
from .context import Context


def set_progress_bar(enabled: bool):
    """Turn the processed-rows progress bar on or off.

    polars-bio prints the bar to stderr while a LazyFrame returned by a file
    reader, `pb.sql`, a range operation or `pb.depth` is collected. This is a
    shorthand for `pb.set_option(pb.POLARS_BIO_PROGRESS_BAR, enabled)`.

    Args:
        enabled: `True` (the default state) shows the bar, `False` hides it.

    Raises:
        ValueError: If `enabled` is not a boolean.

    !!! note
        Only polars-bio's own progress bars are affected; bars of the
        application and of other libraries that use `tqdm` stay visible.
        The setting can be changed at any time, also after `polars_bio` is
        imported, and applies to LazyFrames created before the change.

        The `TQDM_DISABLE=1` environment variable hides the bar regardless
        of this setting.

        Example:
        ```python
        import polars_bio as pb

        pb.set_progress_bar(False)
        ```
    """
    Context().set_option(POLARS_BIO_PROGRESS_BAR, enabled)


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
