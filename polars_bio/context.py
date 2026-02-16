import datetime
from typing import Optional

import datafusion

from polars_bio.polars_bio import BioSessionContext

from .constants import (
    POLARS_BIO_COORDINATE_SYSTEM_CHECK,
    POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED,
)
from .logging import logger


def singleton(cls):
    """Decorator to make a class a singleton."""
    instances = {}

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance


@singleton
class Context:
    def __init__(self):
        logger.info("Creating BioSessionContext")
        seed = str(datetime.datetime.now().timestamp())
        self.ctx = BioSessionContext(seed=seed)
        # Standard DataFusion options
        datafusion_conf = {
            "datafusion.execution.target_partitions": "1",
            "datafusion.execution.parquet.schema_force_view_types": "true",
            "datafusion.execution.skip_physical_aggregate_schema_check": "true",
        }
        for k, v in datafusion_conf.items():
            self.ctx.set_option(k, v)

        # polars-bio specific options (stored in Rust context, not Python SessionConfig)
        # Default to 1-based coordinates (zero_based=false) to match VCF/GFF native formats
        self.ctx.set_option(POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, "false")

        # Default to lenient coordinate system check (warn if metadata is missing, use global config)
        self.ctx.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, "false")

        self.ctx.set_option("bio.interval_join_algorithm", "coitrees")
        self.config = datafusion.context.SessionConfig(datafusion_conf)

    def set_option(self, key, value):
        # Convert bool to string for DataFusion context
        if isinstance(value, bool):
            value = "true" if value else "false"
        self.ctx.set_option(key, value)
        # Only mirror standard DataFusion options to the Python SessionConfig.
        # Extension namespaces (e.g., `bio.*`, `datafusion.bio.*`) are handled
        # by the Rust context and are not recognized by Python bindings, which would panic.
        if (
            isinstance(key, str)
            and key.startswith("datafusion.")
            and not key.startswith("datafusion.bio.")
        ):
            self.config.set(key, value)

    def get_option(self, key):
        """Get the value of a configuration option.

        Args:
            key: The configuration key.

        Returns:
            The current value of the option as a string, or None if not set.
        """
        return self.ctx.get_option(key)


def set_option(key, value):
    """Set a configuration option.

    Args:
        key: The configuration key.
        value: The value to set (bool values are converted to "true"/"false").

    Example:
        ```python
        import polars_bio as pb
        pb.set_option("datafusion.bio.coordinate_system_zero_based", False)
        ```
    """
    Context().set_option(key, value)


def get_option(key):
    """Get the value of a configuration option.

    Args:
        key: The configuration key.

    Returns:
        The current value of the option as a string, or None if not set.

    Example:
        ```python
        import polars_bio as pb
        pb.get_option("datafusion.bio.coordinate_system_zero_based")
        'true'
        ```
    """
    return Context().get_option(key)


def _resolve_zero_based(use_zero_based: Optional[bool]) -> bool:
    """Resolve the effective zero_based value based on explicit parameter and global config.

    Priority: explicit parameter > global config > default (False, i.e., 1-based)

    Args:
        use_zero_based: If True, use 0-based half-open coordinates. If False, use 1-based closed.
                        If None, use the global configuration.

    Returns:
        True if coordinates should be 0-based, False if 1-based.
    """
    if use_zero_based is not None:
        # Explicit parameter takes precedence
        return use_zero_based
    else:
        # Use global configuration from DataFusion context
        value = Context().get_option(POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED)
        # Default to False (1-based) if not set, otherwise parse string to bool
        return value is not None and value.lower() == "true"


ctx = Context().ctx
