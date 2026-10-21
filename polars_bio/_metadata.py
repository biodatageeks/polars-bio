"""Unified metadata abstraction for coordinate system tracking.

This module provides functions to get and set coordinate system metadata
on different DataFrame types (Polars, Pandas) and DataFusion tables.
"""

from __future__ import annotations

import json
import warnings
from typing import TYPE_CHECKING, Any, Optional, Union

import polars as pl

from ._path_utils import path_format_suffix
from .exceptions import CoordinateSystemMismatchError, MissingCoordinateSystemError

if TYPE_CHECKING:
    import pandas as pd


def _is_pandas_dataframe(obj: Any) -> bool:
    """Check if object is a pandas DataFrame without requiring pandas."""
    try:
        import pandas as pd

        return isinstance(obj, pd.DataFrame)
    except ImportError:
        return False


# Metadata key used for coordinate system
COORDINATE_SYSTEM_KEY = "coordinate_system_zero_based"

# Source file metadata keys (standardized across all formats)
SOURCE_FORMAT_KEY = "source_format"
SOURCE_PATH_KEY = "source_path"
SOURCE_HEADER_KEY = "source_header"

# DEPRECATED: VCF metadata keys (kept for backward compatibility in wrappers)
# Use get_metadata() instead - VCF metadata now stored in source_header
VCF_INFO_FIELDS_KEY = "vcf_info_fields"
VCF_FORMAT_FIELDS_KEY = "vcf_format_fields"
VCF_SAMPLE_NAMES_KEY = "vcf_sample_names"


def _has_config_meta(df) -> bool:
    """Check if object has config_meta attribute (Polars or wrapper types)."""
    return hasattr(df, "config_meta")


def _is_file_path(s: str) -> bool:
    """Check if a string looks like a file path.

    Detects file paths by checking for:
    - Path separators (/, \\)
    - Relative path prefixes (./, ../)
    - Common bioinformatics file extensions, including compressed files
    """
    import os

    common_extensions = {
        ".bed",
        ".vcf",
        ".bcf",
        ".gff",
        ".gff3",
        ".bam",
        ".cram",
        ".parquet",
        ".csv",
    }
    extension = path_format_suffix(s)
    return (
        os.path.sep in s
        or s.startswith("./")
        or s.startswith("../")
        or extension in common_extensions
    )


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
        ```python
        import polars as pl
        import polars_bio as pb
        from polars_bio._metadata import set_coordinate_system

        df = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
        set_coordinate_system(df, zero_based=True)
        ```
    """
    if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        df.config_meta.set(**{COORDINATE_SYSTEM_KEY: zero_based})
    elif _has_config_meta(df):
        # Wrapper types like GffLazyFrameWrapper that delegate to underlying LazyFrame
        df.config_meta.set(**{COORDINATE_SYSTEM_KEY: zero_based})
    elif _is_pandas_dataframe(df):
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
        ```python
        import polars_bio as pb
        from polars_bio._metadata import get_coordinate_system

        lf = pb.scan_vcf("file.vcf")
        get_coordinate_system(lf)  # Returns: True
        ```
    """
    if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        metadata = df.config_meta.get_metadata()
        return metadata.get(COORDINATE_SYSTEM_KEY)
    elif _has_config_meta(df):
        # Wrapper types like GffLazyFrameWrapper that delegate to underlying LazyFrame
        metadata = df.config_meta.get_metadata()
        return metadata.get(COORDINATE_SYSTEM_KEY)
    elif _is_pandas_dataframe(df):
        return df.attrs.get(COORDINATE_SYSTEM_KEY)
    elif isinstance(df, str):
        # File paths cannot have metadata until they're read by I/O functions
        if _is_file_path(df):
            return None

        # Table name - read from Arrow schema metadata
        if ctx is None:
            from .context import ctx as default_ctx

            ctx = default_ctx
        try:
            table = ctx.table(df)
            schema = table.schema()
            metadata = schema.metadata or {}
            # Handle both str and bytes keys/values (Arrow metadata can be bytes)
            key_str = "bio.coordinate_system_zero_based"
            key_bytes = b"bio.coordinate_system_zero_based"
            if key_str in metadata:
                value = metadata[key_str]
                if isinstance(value, bytes):
                    value = value.decode("utf-8")
                return value.lower() == "true"
            elif key_bytes in metadata:
                value = metadata[key_bytes]
                if isinstance(value, bytes):
                    value = value.decode("utf-8")
                return value.lower() == "true"
        except Exception:
            pass
        return None
    else:
        raise TypeError(
            f"Cannot get coordinate system from {type(df).__name__}. "
            f"Supported types: pl.DataFrame, pl.LazyFrame, pd.DataFrame, str (table name)"
        )


def _get_input_type_name(
    df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str],
) -> str:
    """Get a human-readable name for the input type."""
    if isinstance(df, pl.LazyFrame):
        return "Polars LazyFrame"
    elif isinstance(df, pl.DataFrame):
        return "Polars DataFrame"
    elif _has_config_meta(df):
        # Wrapper types like GffLazyFrameWrapper
        return f"Polars LazyFrame ({type(df).__name__})"
    elif _is_pandas_dataframe(df):
        return "Pandas DataFrame"
    elif isinstance(df, str):
        if _is_file_path(df):
            return f"file path '{df}'"
        return f"table '{df}'"
    else:
        return type(df).__name__


_READER_HINT = (
    "  pb.scan_parquet(path, zero_based=True)   # or pb.scan_csv / pb.read_parquet / pb.read_csv\n"
    "  (zero_based=True: 0-based half-open, e.g. BED; False: 1-based closed, e.g. VCF/GFF)"
)


_ZERO_BASED_NOTE = "  (zero_based=True: 0-based half-open, e.g. BED; False: 1-based closed, e.g. VCF/GFF)"


def _path_reader_hint(path: str) -> str:
    """Reader suggestion for a file path, chosen by its suffix."""
    suffix = path_format_suffix(path)
    if suffix == ".bed":
        return "  pb.scan_bed(path)   # BED intervals are 0-based half-open"
    if suffix == ".csv":
        return f"  pb.scan_csv(path, zero_based=True)   # or pb.read_csv\n{_ZERO_BASED_NOTE}"
    if suffix == ".parquet":
        return (
            f"  pb.scan_parquet(path, zero_based=True)   # or pb.read_parquet\n"
            f"{_ZERO_BASED_NOTE}"
        )
    return _READER_HINT


def _get_metadata_hint(df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str]) -> str:
    """Get a hint on how to record the coordinate system for the given input type."""
    if isinstance(df, (pl.DataFrame, pl.LazyFrame)) or _has_config_meta(df):
        return (
            "Load the data with a polars-bio reader that records the coordinate system:\n"
            f"{_READER_HINT}\n"
            "or tag an existing frame:\n"
            "  df.config_meta.set(coordinate_system_zero_based=True)"
        )
    if _is_pandas_dataframe(df):
        return (
            "Load the data with pb.read_csv(..., zero_based=...) / "
            "pb.read_parquet(..., zero_based=...), or tag the existing DataFrame:\n"
            '  df.attrs["coordinate_system_zero_based"] = True   # 0-based half-open (e.g. BED)\n'
            '  df.attrs["coordinate_system_zero_based"] = False  # 1-based closed (e.g. VCF/GFF)'
        )
    if isinstance(df, str) and _is_file_path(df):
        return (
            "This file is read without coordinate handling. Load it with a polars-bio reader "
            "that records the coordinate system and pass the frame instead of the path:\n"
            f"{_path_reader_hint(df)}"
        )
    if isinstance(df, str):
        return (
            "For registered tables, ensure the table was registered with coordinate system "
            "metadata. Use polars-bio I/O functions (scan_*, read_*) to load data first."
        )
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
        True if strict check is enabled, False for fallback/lenient mode (default).
    """
    from .constants import POLARS_BIO_COORDINATE_SYSTEM_CHECK
    from .context import get_option

    value = get_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK)
    # Default is "false" (lenient mode) set in context.py
    # Return True only if explicitly set to "true"
    return value is not None and value.lower() == "true"


# Paths read by polars-bio's native VCF/BCF reader in range operations.
_NATIVE_PATH_SUFFIXES = frozenset({".vcf", ".bcf"})


def _native_reader_zero_based(read_options=None) -> bool:
    """Coordinate system the native VCF/BCF reader applies to a range-op path.

    The reader uses the ``zero_based`` of the VCF read options passed for that
    input; without them it falls back to ``VcfReadOptions::default()``, which is
    0-based (src/option.rs). The global setting is not consulted.
    """
    vcf_options = getattr(read_options, "vcf_read_options", None)
    if vcf_options is None:
        return True
    return bool(vcf_options.zero_based)


def _resolve_coordinate_system(
    df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str],
    ctx=None,
    read_options=None,
) -> tuple[Optional[bool], Optional[str]]:
    """Resolve an input's coordinate system for a range operation.

    Args:
        df: The range-operation input.
        ctx: DataFusion context used to look up registered tables.
        read_options: The ``ReadOptions`` the range operation passes for this input.

    Returns:
        ``(zero_based, evidence)``. ``evidence`` is ``"tag"`` for inputs carrying
        coordinate-system metadata, ``"native-path"`` for VCF/BCF paths (resolved
        to the ``zero_based`` of their VCF read options, or ``True``, the reader
        default, without them), or ``None`` when the system is unknown (untagged
        frames, CSV/Parquet paths, tables without metadata); ``zero_based`` is
        ``None`` exactly when ``evidence`` is ``None``.
    """
    if isinstance(df, str):
        if path_format_suffix(df) in _NATIVE_PATH_SUFFIXES:
            return _native_reader_zero_based(read_options), "native-path"
        if _is_file_path(df):
            return None, None
    zero_based = get_coordinate_system(df, ctx)
    return zero_based, ("tag" if zero_based is not None else None)


def _cs_name(zero_based: bool) -> str:
    return "0-based" if zero_based else "1-based"


def _missing_error(
    df, other_zero_based=None, position=None
) -> MissingCoordinateSystemError:
    subject = (
        f"{position} input ({_get_input_type_name(df)})"
        if position
        else _get_input_type_name(df)
    )
    message = f"{subject} is missing coordinate system metadata"
    if other_zero_based is not None:
        message += (
            f", while the other input is {_cs_name(other_zero_based)}. polars-bio cannot "
            f"tell whether this input is 0-based or 1-based, and guessing would shift "
            f"overlaps by one position"
        )
    message = message[0].upper() + message[1:]
    return MissingCoordinateSystemError(f"{message}.\n\n{_get_metadata_hint(df)}")


def _warn_assumed(inputs) -> bool:
    global_zero_based = _get_global_zero_based()
    names = ", ".join(_get_input_type_name(d) for d in inputs)
    warnings.warn(
        f"Coordinate system metadata is missing for: {names}. Assuming "
        f"{_cs_name(global_zero_based)} (global POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED). "
        f"If that is wrong, overlaps are shifted by one position. Load the data with "
        f"pb.scan_csv/pb.scan_parquet(..., zero_based=True|False) (or read_csv/read_parquet) "
        f"to record the coordinate system and silence this warning.",
        UserWarning,
        stacklevel=5,
    )
    return global_zero_based


def validate_coordinate_systems(
    df1: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str],
    df2: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str],
    ctx=None,
    read_options1=None,
    read_options2=None,
) -> bool:
    """Resolve the shared coordinate system of two range-operation inputs.

    Each input is *tagged* (coordinate-system metadata), a *native path*
    (VCF/BCF file, resolved to the ``zero_based`` of the VCF read options the
    range operation passes for it, or 0-based, the reader default, without them)
    or *unknown* (untagged frame, CSV/Parquet path, table without metadata).
    ``read_options1``/``read_options2`` are the read options applied to
    ``df1``/``df2``.

    - Both known: they must agree, otherwise ``CoordinateSystemMismatchError``.
    - One tagged, the other unknown: ``MissingCoordinateSystemError`` in every mode,
      because guessing could shift results by one position.
    - Otherwise, with an unknown input: when ``datafusion.bio.coordinate_system_check``
      is ``"false"`` (the default, lenient) the global setting is used and a
      ``UserWarning`` is emitted; when ``"true"`` (strict)
      ``MissingCoordinateSystemError`` is raised.

    Returns:
        True if 0-based coordinates, False if 1-based coordinates.

    Raises:
        MissingCoordinateSystemError: See the rules above.
        CoordinateSystemMismatchError: If the inputs use different coordinate systems.
    """
    cs1, evidence1 = _resolve_coordinate_system(df1, ctx, read_options1)
    cs2, evidence2 = _resolve_coordinate_system(df2, ctx, read_options2)
    strict = _get_coordinate_system_check()

    if cs1 is None and cs2 is None:
        if strict:
            raise _missing_error(df1, position="first")
        cs1 = cs2 = _warn_assumed([df1, df2])
    elif cs1 is None or cs2 is None:
        known_cs, known_evidence = (cs2, evidence2) if cs1 is None else (cs1, evidence1)
        missing_df, position = (df1, "first") if cs1 is None else (df2, "second")
        if strict or known_evidence == "tag":
            raise _missing_error(missing_df, known_cs, position)
        # The known side is a native path: the unknown side is assumed to use the
        # global setting (with a warning) and must still agree with the reader.
        assumed = _warn_assumed([missing_df])
        cs1, cs2 = (assumed, cs2) if cs1 is None else (cs1, assumed)

    if cs1 != cs2:
        raise CoordinateSystemMismatchError(
            f"Coordinate system mismatch: first input uses {_cs_name(cs1)} coordinates, "
            f"second input uses {_cs_name(cs2)} coordinates. Re-read one of the inputs with "
            f"use_zero_based=True|False (scan_*/read_*, including "
            f"pb.scan_csv/pb.scan_parquet(..., zero_based=..., use_zero_based=...))."
        )
    return cs1


def validate_coordinate_system_single(
    df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str],
    ctx=None,
    read_options=None,
) -> bool:
    """Resolve the coordinate system of a single range-operation input.

    Tagged inputs and native (VCF/BCF) paths resolve to their system (for a
    path, the ``zero_based`` of ``read_options``' VCF options, else 0-based). An unknown
    input uses the global setting with a ``UserWarning`` in lenient mode (the
    default) and raises ``MissingCoordinateSystemError`` in strict mode.

    Returns:
        True if 0-based coordinates, False if 1-based coordinates.
    """
    zero_based, _ = _resolve_coordinate_system(df, ctx, read_options)
    if zero_based is None:
        if _get_coordinate_system_check():
            raise _missing_error(df)
        zero_based = _warn_assumed([df])
    return zero_based


# =============================================================================
# VCF Metadata Functions
# =============================================================================


def set_vcf_metadata(
    df: Union[pl.DataFrame, pl.LazyFrame],
    info_fields: Optional[dict] = None,
    format_fields: Optional[dict] = None,
    sample_names: Optional[list] = None,
) -> None:
    """Set VCF-specific metadata on a DataFrame (convenience wrapper).

    This is a convenience wrapper that stores VCF metadata in the standardized
    source_header field. Use get_metadata() to access the underlying data.

    This metadata is used when writing VCF files to preserve field definitions
    (Number, Type, Description) from the original VCF header.

    Args:
        df: The DataFrame to set metadata on.
        info_fields: Dict mapping INFO field names to their VCF definitions.
            Each value is a dict with keys: 'number', 'type', 'description'.
            Example: {"AF": {"number": "A", "type": "Float", "description": "Allele Frequency"}}
        format_fields: Dict mapping FORMAT field names to their VCF definitions.
            Example: {"GT": {"number": "1", "type": "String", "description": "Genotype"}}
        sample_names: List of sample names from the VCF header.

    Example:
        ```python
        import polars as pl
        import polars_bio as pb
        from polars_bio._metadata import set_vcf_metadata

        df = pb.read_vcf("file.vcf")
        set_vcf_metadata(df, info_fields={"AF": {"number": "A", "type": "Float", "description": "Allele Frequency"}})
        ```
    """
    if not isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        raise TypeError(
            f"Cannot set VCF metadata on {type(df).__name__}. "
            f"Supported types: pl.DataFrame, pl.LazyFrame"
        )

    # Build header dict for VCF-specific fields
    header = {}
    if info_fields is not None:
        header["info_fields"] = info_fields
    if format_fields is not None:
        header["format_fields"] = format_fields
    if sample_names is not None:
        header["sample_names"] = sample_names

    # Get existing source metadata to preserve format/path
    existing = get_metadata(df)

    # Merge with existing header
    existing_header = existing.get("header") or {}
    merged_header = {**existing_header, **header}

    # Set source metadata with merged header
    set_source_metadata(
        df,
        format=existing.get("format") or "vcf",
        path=existing.get("path") or "",
        header=merged_header if merged_header else None,
    )


def get_vcf_metadata(
    df: Union[pl.DataFrame, pl.LazyFrame],
) -> dict:
    """Get VCF-specific metadata from a DataFrame (convenience wrapper).

    This is a convenience wrapper that extracts VCF fields from the standardized
    source_header field.

    Args:
        df: The DataFrame to read metadata from.

    Returns:
        A dict with keys 'info_fields', 'format_fields', 'sample_names'.
        Each value is None if not set, otherwise contains the metadata dict/list.

    Example:
        ```python
        import polars_bio as pb
        from polars_bio._metadata import get_vcf_metadata

        df = pb.read_vcf("file.vcf")
        meta = get_vcf_metadata(df)
        print(meta["info_fields"])
        ```
    """
    if not isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        raise TypeError(
            f"Cannot get VCF metadata from {type(df).__name__}. "
            f"Supported types: pl.DataFrame, pl.LazyFrame"
        )

    # Get source metadata
    source = get_metadata(df)
    header = source.get("header") or {}

    # Extract VCF-specific fields from header
    result = {
        "info_fields": header.get("info_fields"),
        "format_fields": header.get("format_fields"),
        "sample_names": header.get("sample_names"),
    }

    return result


def set_source_metadata(df, format: str, path: str = "", header: dict = None):
    """Set standardized source file metadata.

    Stores metadata about the source file format, path, and format-specific
    header information. This standardized approach works across all file
    formats (VCF, FASTQ, BAM, GFF, BED, FASTA, CRAM).

    Args:
        df (object): Polars DataFrame or LazyFrame (or Pandas DataFrame)
        format: File format identifier (e.g., "vcf", "fastq", "bam")
        path: Original file path (default: "")
        header: Format-specific header data as dict (default: None)
                For VCF: {"info_fields": {...}, "format_fields": {...}, "sample_names": [...], ...}
                For other formats: format-specific metadata

    Example:
        ```python
        import polars_bio as pb
        lf = pb.scan_vcf("sample.vcf")
        header = {"info_fields": {...}, "sample_names": ["sample1"]}
        pb.set_source_metadata(lf, format="vcf", path="sample.vcf", header=header)
        ```
    """
    if _has_config_meta(df):
        # Polars DataFrame/LazyFrame
        metadata_updates = {
            SOURCE_FORMAT_KEY: format,
            SOURCE_PATH_KEY: path,
            SOURCE_HEADER_KEY: json.dumps(header) if header else "",
        }
        df.config_meta.set(**metadata_updates)
    elif _is_pandas_dataframe(df):
        # Pandas DataFrame
        if not hasattr(df, "attrs"):
            df.attrs = {}
        df.attrs[SOURCE_FORMAT_KEY] = format
        df.attrs[SOURCE_PATH_KEY] = path
        df.attrs[SOURCE_HEADER_KEY] = json.dumps(header) if header else ""


def get_metadata(df) -> dict:
    """Get all metadata attached to a DataFrame or LazyFrame.

    Returns all metadata including:
    - Source file information (format, path)
    - Format-specific metadata (VCF INFO/FORMAT fields, FASTQ quality encoding, etc.)
    - Comprehensive Arrow schema metadata (if available)

    Args:
        df (object): Polars DataFrame or LazyFrame (or Pandas DataFrame)

    Returns:
        Dict with keys:
        - "format": File format identifier (e.g., "vcf", "fastq", "bam")
        - "path": Original file path
        - "coordinate_system_zero_based": Boolean indicating coordinate system (True=0-based, False=1-based, None=not set)
        - "header": Format-specific header data as dict, may include:
            - For VCF: "info_fields", "format_fields", "sample_names", "version", "contigs", "filters", etc.
            - For FASTQ: quality encoding information
            - For other formats: format-specific metadata
            - "_datafusion_table_name": Internal DataFusion table name (for debugging)

    Examples:
        Get all metadata from a VCF file:
        ```python
        import polars_bio as pb
        lf = pb.scan_vcf("file.vcf")
        meta = pb.get_metadata(lf)
        ```

        Access basic metadata:
        ```python
        meta["format"]                        # Returns: 'vcf'
        meta["path"]                          # Returns: 'file.vcf'
        meta["coordinate_system_zero_based"]  # Returns: False (1-based for VCF)
        ```

        Access VCF-specific metadata:
        ```python
        info_fields = meta["header"]["info_fields"]
        format_fields = meta["header"]["format_fields"]
        sample_names = meta["header"]["sample_names"]
        version = meta["header"]["version"]
        contigs = meta["header"]["contigs"]
        ```
    """
    result = {
        "format": None,
        "path": None,
        "coordinate_system_zero_based": None,
        "header": None,
    }

    if _has_config_meta(df):
        # Polars DataFrame/LazyFrame
        try:
            metadata = df.config_meta.get_metadata()
        except (KeyError, AttributeError, TypeError):
            return result

        result["format"] = metadata.get(SOURCE_FORMAT_KEY)
        result["path"] = metadata.get(SOURCE_PATH_KEY)
        result["coordinate_system_zero_based"] = metadata.get(COORDINATE_SYSTEM_KEY)

        header_json = metadata.get(SOURCE_HEADER_KEY)
        if header_json:
            try:
                result["header"] = json.loads(header_json)
            except (json.JSONDecodeError, TypeError):
                pass

    elif _is_pandas_dataframe(df):
        # Pandas DataFrame
        if hasattr(df, "attrs"):
            result["format"] = df.attrs.get(SOURCE_FORMAT_KEY)
            result["path"] = df.attrs.get(SOURCE_PATH_KEY)
            result["coordinate_system_zero_based"] = df.attrs.get(COORDINATE_SYSTEM_KEY)

            header_json = df.attrs.get(SOURCE_HEADER_KEY)
            if header_json:
                try:
                    result["header"] = json.loads(header_json)
                except (json.JSONDecodeError, TypeError):
                    pass

    return result


def print_metadata_json(df: Union[pl.DataFrame, pl.LazyFrame], indent: int = 2) -> None:
    """Print metadata as pretty-formatted JSON.

    Args:
        df: Polars DataFrame or LazyFrame
        indent: Number of spaces for indentation (default: 2)

    Example:
        ```python
        import polars_bio as pb
        lf = pb.scan_vcf("file.vcf")
        pb.print_metadata_json(lf)
        ```
    """
    meta = get_metadata(df)
    print(json.dumps(meta, indent=indent, default=str))


def print_metadata_summary(df: Union[pl.DataFrame, pl.LazyFrame]) -> None:
    """Print a human-readable summary of all metadata.

    Displays a formatted summary of all metadata attached to a DataFrame or LazyFrame,
    including format, path, coordinate system, and format-specific information.

    Args:
        df: Polars DataFrame or LazyFrame

    Example:
        ```python
        import polars_bio as pb
        lf = pb.scan_vcf("file.vcf")
        pb.print_metadata_summary(lf)
        ```
    """
    meta = get_metadata(df)
    if not meta or not any([meta.get("format"), meta.get("path"), meta.get("header")]):
        print("No metadata available")
        return

    print("=" * 70)
    print("Metadata Summary")
    print("=" * 70)
    print()

    # Basic metadata
    if meta.get("format"):
        print(f"Format: {meta['format']}")
    if meta.get("path"):
        print(f"Path: {meta['path']}")
    if meta.get("coordinate_system_zero_based") is not None:
        coord_sys = "0-based" if meta["coordinate_system_zero_based"] else "1-based"
        print(f"Coordinate System: {coord_sys}")

    # Format-specific metadata
    if meta.get("header"):
        header = meta["header"]
        print()
        print("Format-specific metadata:")
        print("-" * 70)

        # VCF-specific
        if meta.get("format") == "vcf":
            if "version" in header:
                print(f"  VCF Version: {header['version']}")
            if "sample_names" in header:
                samples = header["sample_names"]
                print(f"  Samples ({len(samples)}): {', '.join(samples[:5])}")
                if len(samples) > 5:
                    print(f"    ... and {len(samples) - 5} more")
            if "info_fields" in header:
                print(f"  INFO fields: {len(header['info_fields'])}")
                for field_id in list(header["info_fields"].keys())[:3]:
                    field = header["info_fields"][field_id]
                    print(
                        f"    - {field_id}: {field.get('type')} ({field.get('description', 'No description')})"
                    )
                if len(header["info_fields"]) > 3:
                    print(f"    ... and {len(header['info_fields']) - 3} more")
            if "format_fields" in header:
                print(f"  FORMAT fields: {len(header['format_fields'])}")
                for field_id in list(header["format_fields"].keys())[:3]:
                    field = header["format_fields"][field_id]
                    print(
                        f"    - {field_id}: {field.get('type')} ({field.get('description', 'No description')})"
                    )
                if len(header["format_fields"]) > 3:
                    print(f"    ... and {len(header['format_fields']) - 3} more")
            if "contigs" in header and header["contigs"]:
                print(f"  Contigs: {len(header['contigs'])}")
            if "filters" in header and header["filters"]:
                print(f"  Filters: {len(header['filters'])}")

        # Other formats can be added here as needed

    print()
    print("=" * 70)
