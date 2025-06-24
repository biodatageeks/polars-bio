import logging

logging.basicConfig()
root_logger = logging.getLogger()
root_logger.setLevel(logging.WARN)
logger = logging.getLogger("polars_bio")
logger.setLevel(logging.WARN)


def set_loglevel(level: str):
    """
    Set the log level for the logger and root logger.

    Parameters:
        level: The log level to set. Can be "debug", "info", "warn", or "warning".

    !!! note
        Please note that the log level should be set as a **first** step after importing the library.
        Once set it can be only **decreased**, not increased. In order to increase the log level, you need to restart the Python session.
        ```python
        import polars_bio as pb
        pb.set_loglevel("info")
        ```
    """
    level = level.lower()
    if level == "debug":
        logger.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
        logging.basicConfig(level=logging.DEBUG)
    elif level == "info":
        logger.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
        logging.basicConfig(level=logging.INFO)
    elif level == "warn" or level == "warning":
        logger.setLevel(logging.WARN)
        root_logger.setLevel(logging.WARN)
        logging.basicConfig(level=logging.WARN)
    else:
        raise ValueError(f"{level} is not a valid log level")
