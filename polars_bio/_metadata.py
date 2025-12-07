"""Unified metadata abstraction for coordinate system tracking.

This module provides functions to get and set coordinate system metadata
on different DataFrame types (Polars, Pandas) and DataFusion tables.
"""

import warnings
from typing import Optional, Tuple, Union

import pandas as pd
import polars as pl

from .exceptions import CoordinateSystemMismatchError, MissingCoordinateSystemError

# Metadata key used for coordinate system
COORDINATE_SYSTEM_KEY = "coordinate_system_zero_based"


def _has_config_meta(df) -> bool:
    """Check if object has config_meta attribute (Polars or wrapper types)."""
    return hasattr(df, "config_meta")


def set_coordinate_system(
    df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame], zero_based: bool
) -> None:
    """Set coordinate system metadata on a DataFrame.

    Args:
        df: The DataFrame to set metadata on. Can be Polars DataFrame/LazyFrame,
            wrapper types (e.g., GffLazyFrameWrapper), or Pandas DataFrame.
        zero_based: True for 0-based half-open coordinates, False for 1-based closed.

    Raises:
        TypeError: If df is not a supported DataFrame type.

    Example:
        >>> import polars as pl
        >>> import polars_bio as pb
        >>> from polars_bio._metadata import set_coordinate_system
        >>>
        >>> df = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        >>> set_coordinate_system(df, zero_based=True)
    """
    if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        df.config_meta.set(**{COORDINATE_SYSTEM_KEY: zero_based})
    elif _has_config_meta(df):
        # Wrapper types like GffLazyFrameWrapper that delegate to underlying LazyFrame
        df.config_meta.set(**{COORDINATE_SYSTEM_KEY: zero_based})
    elif isinstance(df, pd.DataFrame):
        df.attrs[COORDINATE_SYSTEM_KEY] = zero_based
    else:
        raise TypeError(
            f"Cannot set coordinate system on {type(df).__name__}. "
            f"Supported types: pl.DataFrame, pl.LazyFrame, pd.DataFrame"
        )


def get_coordinate_system(
    df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str],
    ctx=None,
) -> Optional[bool]:
    """Get coordinate system metadata from a DataFrame or table.

    Args:
        df: The DataFrame or table name to read metadata from.
        ctx: DataFusion context (required when df is a table name string).

    Returns:
        True if 0-based, False if 1-based, None if metadata not set.

    Raises:
        TypeError: If df is not a supported type.

    Example:
        >>> import polars_bio as pb
        >>> lf = pb.scan_vcf("file.vcf")
        >>> from polars_bio._metadata import get_coordinate_system
        >>> get_coordinate_system(lf)
        True
    """
    if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        metadata = df.config_meta.get_metadata()
        return metadata.get(COORDINATE_SYSTEM_KEY)
    elif _has_config_meta(df):
        # Wrapper types like GffLazyFrameWrapper that delegate to underlying LazyFrame
        metadata = df.config_meta.get_metadata()
        return metadata.get(COORDINATE_SYSTEM_KEY)
    elif isinstance(df, pd.DataFrame):
        return df.attrs.get(COORDINATE_SYSTEM_KEY)
    elif isinstance(df, str):
        # Table name - read from Arrow schema metadata
        if ctx is None:
            from .context import ctx as default_ctx

            ctx = default_ctx
        try:
            table = ctx.table(df)
            schema = table.schema()
            metadata = schema.metadata or {}
            key = "bio.coordinate_system_zero_based"
            if key in metadata:
                return metadata[key].lower() == "true"
        except Exception:
            pass
        return None
    else:
        raise TypeError(
            f"Cannot get coordinate system from {type(df).__name__}. "
            f"Supported types: pl.DataFrame, pl.LazyFrame, pd.DataFrame, str (table name)"
        )


def _get_input_type_name(
    df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str]
) -> str:
    """Get a human-readable name for the input type."""
    if isinstance(df, pl.LazyFrame):
        return "Polars LazyFrame"
    elif isinstance(df, pl.DataFrame):
        return "Polars DataFrame"
    elif _has_config_meta(df):
        # Wrapper types like GffLazyFrameWrapper
        return f"Polars LazyFrame ({type(df).__name__})"
    elif isinstance(df, pd.DataFrame):
        return "Pandas DataFrame"
    elif isinstance(df, str):
        return f"table '{df}'"
    else:
        return type(df).__name__


def _get_metadata_hint(df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str]) -> str:
    """Get a hint on how to set metadata for the given input type."""
    if isinstance(df, (pl.DataFrame, pl.LazyFrame)) or _has_config_meta(df):
        return (
            "For Polars DataFrames, use polars-bio I/O functions (scan_*, read_*) "
            "which automatically set the metadata, or set it manually:\n"
            "  df.config_meta.set(coordinate_system_zero_based=True)"
        )
    elif isinstance(df, pd.DataFrame):
        return (
            "For Pandas DataFrames, set the attribute before passing to range operations:\n"
            '  df.attrs["coordinate_system_zero_based"] = True  # for 0-based coords\n'
            '  df.attrs["coordinate_system_zero_based"] = False  # for 1-based coords'
        )
    elif isinstance(df, str):
        return (
            "For file paths, use polars-bio I/O functions (scan_*, read_*) "
            "instead of passing the path directly, as they set Arrow schema metadata."
        )
    else:
        return "Use polars-bio I/O functions to ensure metadata is set correctly."


def _get_global_zero_based() -> bool:
    """Get the global coordinate system setting from context.

    Returns:
        True if global config is set to 0-based, False for 1-based (default).
    """
    from .constants import POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED
    from .context import get_option

    value = get_option(POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED)
    return value is not None and value.lower() == "true"


def _get_coordinate_system_check() -> bool:
    """Get the coordinate system check setting from context.

    Returns:
        True if strict check is enabled (default), False to use fallback mode.
    """
    from .constants import POLARS_BIO_COORDINATE_SYSTEM_CHECK
    from .context import get_option

    value = get_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK)
    # Default to True (strict check) if not set or if set to "true"
    return value is None or value.lower() == "true"


def validate_coordinate_systems(
    df1: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str],
    df2: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str],
    ctx=None,
) -> bool:
    """Validate that both inputs have the same coordinate system.

    The behavior when metadata is missing is controlled by the session parameter
    `datafusion.bio.coordinate_system_check`:
    - When "true" (default): Raises MissingCoordinateSystemError
    - When "false": Falls back to `datafusion.bio.coordinate_system_zero_based` and emits a warning

    Args:
        df1: First DataFrame or table name.
        df2: Second DataFrame or table name.
        ctx: DataFusion context (required when inputs are table names).

    Returns:
        True if 0-based coordinates, False if 1-based coordinates.

    Raises:
        MissingCoordinateSystemError: If either input lacks coordinate system metadata
            and datafusion.bio.coordinate_system_check is "true".
        CoordinateSystemMismatchError: If inputs have different coordinate systems.

    Example:
        >>> import polars_bio as pb
        >>> from polars_bio._metadata import validate_coordinate_systems
        >>>
        >>> df1 = pb.scan_vcf("file1.vcf")
        >>> df2 = pb.scan_vcf("file2.vcf")
        >>> zero_based = validate_coordinate_systems(df1, df2)
    """
    cs1 = get_coordinate_system(df1, ctx)
    cs2 = get_coordinate_system(df2, ctx)

    # Get the check setting from session config
    coordinate_system_check = _get_coordinate_system_check()

    # Handle missing metadata
    if cs1 is None or cs2 is None:
        if coordinate_system_check:
            # Strict mode: raise error for missing metadata
            if cs1 is None:
                input_type = _get_input_type_name(df1)
                hint = _get_metadata_hint(df1)
                raise MissingCoordinateSystemError(
                    f"{input_type} is missing coordinate system metadata.\n\n{hint}"
                )
            if cs2 is None:
                input_type = _get_input_type_name(df2)
                hint = _get_metadata_hint(df2)
                raise MissingCoordinateSystemError(
                    f"{input_type} is missing coordinate system metadata.\n\n{hint}"
                )
        else:
            # Fallback mode: use global config and emit warning
            global_zero_based = _get_global_zero_based()
            cs_str = "0-based" if global_zero_based else "1-based"

            missing_inputs = []
            if cs1 is None:
                missing_inputs.append(_get_input_type_name(df1))
            if cs2 is None:
                missing_inputs.append(_get_input_type_name(df2))

            warnings.warn(
                f"Coordinate system metadata is missing for: {', '.join(missing_inputs)}. "
                f"Using global POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED setting ({cs_str}). "
                f"Set metadata explicitly on DataFrames or use polars-bio I/O functions "
                f"(scan_*, read_*) to avoid this warning.",
                UserWarning,
                stacklevel=4,
            )

            # Use global config for missing values
            if cs1 is None:
                cs1 = global_zero_based
            if cs2 is None:
                cs2 = global_zero_based

    # Check for mismatch
    if cs1 != cs2:
        cs1_str = "0-based" if cs1 else "1-based"
        cs2_str = "0-based" if cs2 else "1-based"
        raise CoordinateSystemMismatchError(
            f"Coordinate system mismatch: "
            f"first input uses {cs1_str} coordinates, "
            f"second input uses {cs2_str} coordinates. "
            f"Re-read one of the inputs with matching coordinate system "
            f"(e.g., use one_based=True or one_based=False parameter)."
        )

    return cs1


def validate_coordinate_system_single(
    df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str],
    ctx=None,
) -> bool:
    """Validate and get coordinate system from a single input.

    The behavior when metadata is missing is controlled by the session parameter
    `datafusion.bio.coordinate_system_check`:
    - When "true" (default): Raises MissingCoordinateSystemError
    - When "false": Falls back to `datafusion.bio.coordinate_system_zero_based` and emits a warning

    Args:
        df: DataFrame or table name.
        ctx: DataFusion context (required when df is a table name).

    Returns:
        True if 0-based coordinates, False if 1-based coordinates.

    Raises:
        MissingCoordinateSystemError: If input lacks coordinate system metadata
            and datafusion.bio.coordinate_system_check is "true".
    """
    cs = get_coordinate_system(df, ctx)

    # Get the check setting from session config
    coordinate_system_check = _get_coordinate_system_check()

    if cs is None:
        if coordinate_system_check:
            input_type = _get_input_type_name(df)
            hint = _get_metadata_hint(df)
            raise MissingCoordinateSystemError(
                f"{input_type} is missing coordinate system metadata.\n\n{hint}"
            )
        else:
            # Fallback mode: use global config and emit warning
            global_zero_based = _get_global_zero_based()
            cs_str = "0-based" if global_zero_based else "1-based"
            input_type = _get_input_type_name(df)

            warnings.warn(
                f"Coordinate system metadata is missing for: {input_type}. "
                f"Using global POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED setting ({cs_str}). "
                f"Set metadata explicitly on DataFrames or use polars-bio I/O functions "
                f"(scan_*, read_*) to avoid this warning.",
                UserWarning,
                stacklevel=4,
            )
            cs = global_zero_based

    return cs
