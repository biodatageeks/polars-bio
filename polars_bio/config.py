"""Global configuration system for polars-bio.

This module provides session-level configuration options that affect the behavior
of polars-bio operations. Configuration can be set globally and overridden per-call.

Example:
    >>> import polars_bio as pb
    >>> # Get current configuration
    >>> pb.get_option("bio.coordinate_system_zero_based")
    True
    >>> # Set configuration to use 1-based coordinates
    >>> pb.set_bio_option("bio.coordinate_system_zero_based", False)
    >>> pb.get_option("bio.coordinate_system_zero_based")
    False
    >>> # Reset to default
    >>> pb.reset_bio_option("bio.coordinate_system_zero_based")
"""

from typing import Any, Dict, Optional

# Default configuration values
_DEFAULTS: Dict[str, Any] = {
    "bio.coordinate_system_zero_based": True,
}

# Current configuration (starts with defaults)
_config: Dict[str, Any] = _DEFAULTS.copy()

# Option descriptions for documentation
_DESCRIPTIONS: Dict[str, str] = {
    "bio.coordinate_system_zero_based": (
        "If True (default), genomic coordinates are output in 0-based half-open format. "
        "If False, coordinates are output in 1-based closed format. "
        "This affects all I/O operations (scan_vcf, scan_gff, scan_bam, etc.) "
        "unless explicitly overridden with the one_based parameter."
    ),
}


def get_bio_option(key: str) -> Any:
    """Get the value of a configuration option.

    Args:
        key: The configuration key (e.g., "bio.coordinate_system_zero_based")

    Returns:
        The current value of the configuration option.

    Raises:
        KeyError: If the configuration key is not recognized.

    Example:
        >>> import polars_bio as pb
        >>> pb.get_bio_option("bio.coordinate_system_zero_based")
        True
    """
    if key not in _DEFAULTS:
        raise KeyError(
            f"Unknown configuration option: {key!r}. "
            f"Available options: {list(_DEFAULTS.keys())}"
        )
    return _config.get(key, _DEFAULTS[key])


def set_bio_option(key: str, value: Any) -> None:
    """Set a configuration option.

    Args:
        key: The configuration key (e.g., "bio.coordinate_system_zero_based")
        value: The value to set.

    Raises:
        KeyError: If the configuration key is not recognized.
        TypeError: If the value type is incorrect.

    Example:
        >>> import polars_bio as pb
        >>> pb.set_bio_option("bio.coordinate_system_zero_based", False)
        >>> pb.get_bio_option("bio.coordinate_system_zero_based")
        False
    """
    if key not in _DEFAULTS:
        raise KeyError(
            f"Unknown configuration option: {key!r}. "
            f"Available options: {list(_DEFAULTS.keys())}"
        )

    # Type checking
    expected_type = type(_DEFAULTS[key])
    if not isinstance(value, expected_type):
        raise TypeError(
            f"Expected {expected_type.__name__} for {key!r}, got {type(value).__name__}"
        )

    _config[key] = value


def reset_bio_option(key: str) -> None:
    """Reset a configuration option to its default value.

    Args:
        key: The configuration key to reset.

    Raises:
        KeyError: If the configuration key is not recognized.

    Example:
        >>> import polars_bio as pb
        >>> pb.set_bio_option("bio.coordinate_system_zero_based", False)
        >>> pb.reset_bio_option("bio.coordinate_system_zero_based")
        >>> pb.get_bio_option("bio.coordinate_system_zero_based")
        True
    """
    if key not in _DEFAULTS:
        raise KeyError(
            f"Unknown configuration option: {key!r}. "
            f"Available options: {list(_DEFAULTS.keys())}"
        )
    _config[key] = _DEFAULTS[key]


def reset_all_bio_options() -> None:
    """Reset all configuration options to their default values.

    Example:
        >>> import polars_bio as pb
        >>> pb.set_bio_option("bio.coordinate_system_zero_based", False)
        >>> pb.reset_all_bio_options()
        >>> pb.get_bio_option("bio.coordinate_system_zero_based")
        True
    """
    _config.clear()
    _config.update(_DEFAULTS)


def describe_bio_option(key: str) -> str:
    """Get a description of a configuration option.

    Args:
        key: The configuration key.

    Returns:
        A description of the option including its current and default values.

    Raises:
        KeyError: If the configuration key is not recognized.

    Example:
        >>> import polars_bio as pb
        >>> print(pb.describe_bio_option("bio.coordinate_system_zero_based"))
    """
    if key not in _DEFAULTS:
        raise KeyError(
            f"Unknown configuration option: {key!r}. "
            f"Available options: {list(_DEFAULTS.keys())}"
        )

    current = _config.get(key, _DEFAULTS[key])
    default = _DEFAULTS[key]
    desc = _DESCRIPTIONS.get(key, "No description available.")

    return f"""{key}
    Current value: {current!r}
    Default value: {default!r}
    Description: {desc}"""


def list_bio_options() -> Dict[str, Any]:
    """List all available configuration options and their current values.

    Returns:
        A dictionary of all configuration options and their current values.

    Example:
        >>> import polars_bio as pb
        >>> pb.list_bio_options()
        {'bio.coordinate_system_zero_based': True}
    """
    return {key: _config.get(key, default) for key, default in _DEFAULTS.items()}


def _resolve_zero_based(one_based: Optional[bool]) -> bool:
    """Resolve the effective zero_based value based on explicit parameter and global config.

    Priority: explicit parameter > global config > default (True)

    Args:
        one_based: If True, use 1-based coordinates. If False, use 0-based.
                   If None, use the global configuration.

    Returns:
        True if coordinates should be 0-based, False if 1-based.
    """
    if one_based is not None:
        # Explicit parameter takes precedence (invert: one_based=True means zero_based=False)
        return not one_based
    else:
        # Use global configuration
        return get_bio_option("bio.coordinate_system_zero_based")
