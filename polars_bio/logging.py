import logging

from polars_bio.polars_bio import py_reset_log_level_cache

# No level is set at import: until `set_loglevel` is called, the library follows
# the application's logging configuration.
logger = logging.getLogger("polars_bio")

_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}


def set_loglevel(level: str):
    """Set the log level for the polars_bio logger.

    Args:
        level: The log level to set, case-insensitive. Can be "debug", "info",
            "warn", "warning", "error", or "critical". "error" hides warnings;
            "critical" also hides errors.

    !!! note
        polars-bio sets no level of its own at import, so until this function
        is called its records follow the application's logging configuration:
        an application whose root logger is at `INFO` receives polars-bio `INFO`
        records, and one without logging configuration sees warnings only.

        This function leaves the application's root logger, handlers, and
        formatters unchanged. Log records propagate to application-configured
        handlers, so the application is responsible for configuring their output.

        The level also applies to the Rust side of polars-bio, including its
        dependency crates such as DataFusion: their records are emitted under
        the `polars_bio` logger hierarchy, for example
        `polars_bio.datafusion_bio_function_ranges.physical_planner`.

        The level can be changed at any time, also after operations have run.
        Change it with this function rather than with
        `logging.getLogger("polars_bio").setLevel()`: the Rust side caches logger
        levels, and only this function refreshes that cache.

        Example:
        ```python
        import logging
        import polars_bio as pb

        logging.basicConfig(level=logging.INFO)  # Application logging setup
        pb.set_loglevel("warning")  # Filter only polars-bio records
        ```
    """
    level = level.lower()
    if level not in _LEVELS:
        raise ValueError(f"{level} is not a valid log level")
    logger.setLevel(_LEVELS[level])
    py_reset_log_level_cache()
