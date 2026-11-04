import logging

logger = logging.getLogger("polars_bio")
logger.setLevel(logging.WARN)


def set_loglevel(level: str):
    """Set the log level for the polars_bio logger.

    Args:
        level: The log level to set. Can be "debug", "info", "warn", or "warning".

    !!! note
        This function leaves the application's root logger, handlers, and
        formatters unchanged. Log records propagate to application-configured
        handlers, so the application is responsible for configuring their output.

        Set the level before running operations: Rust logging caches its filters,
        so changing the level afterwards may require restarting the Python session.

        Example:
        ```python
        import logging
        import polars_bio as pb

        logging.basicConfig(level=logging.INFO)  # Application logging setup
        pb.set_loglevel("warning")  # Filter only polars-bio records
        ```
    """
    level = level.lower()
    if level == "debug":
        logger.setLevel(logging.DEBUG)
    elif level == "info":
        logger.setLevel(logging.INFO)
    elif level == "warn" or level == "warning":
        logger.setLevel(logging.WARN)
    else:
        raise ValueError(f"{level} is not a valid log level")
