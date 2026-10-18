"""Coordinate-aware CSV and Parquet readers.

Thin wrappers over ``pl.scan_csv`` / ``pl.scan_parquet`` that record the coordinate
system of the data, so range operations never have to guess it (issue #486).
"""

from __future__ import annotations

from typing import Optional

import polars as pl

from ._metadata import set_coordinate_system, set_source_metadata

_BOOL_MESSAGE = "{name} must be True (0-based half-open, e.g. BED) or False (1-based closed, e.g. VCF/GFF)"


def _check_bool(name: str, value, allow_none: bool = False) -> None:
    if value is None and allow_none:
        return
    if not isinstance(value, bool):
        suffix = ", or None to keep the source system" if allow_none else ""
        raise TypeError(_BOOL_MESSAGE.format(name=name) + suffix + f"; got {value!r}")


def _rename_interval_columns(
    lf: pl.LazyFrame, *, zero_based: bool, chrom: str, start: str, end, source
):
    schema = lf.collect_schema()
    names = schema.names()
    wanted = [("chrom", chrom), ("start", start)] + (
        [("end", end)] if end is not None else []
    )
    missing = [src for _, src in wanted if src not in names]
    if missing:
        message = f"Interval column(s) {missing} not found in {source!r}. Available columns: {names}."
        if end == "end" and missing == ["end"]:
            message += " For point data without an end column, pass end=None to derive it from start."
        raise ValueError(message)
    renames = {src: dst for dst, src in wanted if src != dst}
    clash = sorted(
        dst for dst in renames.values() if dst in names and dst not in renames
    )
    if clash:
        raise ValueError(
            f"Cannot rename interval columns to {clash}: {source!r} already has column(s) "
            f"with that name. Rename or drop them first."
        )
    if end is None and "end" in names and "end" not in renames:
        raise ValueError(
            f"end=None derives an 'end' column, but {source!r} already has one; pass end='end' to use it."
        )
    if renames:
        lf = lf.rename(renames)
    if end is None:
        start_dtype = schema[start]
        derived = pl.col("start") + 1 if zero_based else pl.col("start")
        lf = lf.with_columns(derived.cast(start_dtype).alias("end"))
    return lf


def _apply_coordinate_system(
    lf: pl.LazyFrame,
    *,
    zero_based: bool,
    use_zero_based: Optional[bool],
    fmt: str,
    source,
) -> pl.LazyFrame:
    """Convert ``start`` to the requested output system and tag the frame.

    ``zero_based`` declares the source system; ``use_zero_based`` selects the output
    system (``None`` keeps the source system). The tag records the output system.
    """
    schema = lf.collect_schema()
    if "start" not in schema:
        raise ValueError(
            f"Recording a coordinate system needs a 'start' column; {source!r} has {schema.names()}."
        )
    if not schema["start"].is_integer():
        raise TypeError(
            f"Column 'start' must be an integer column, got {schema['start']} in {source!r}."
        )
    out_zero_based = zero_based if use_zero_based is None else use_zero_based
    if out_zero_based != zero_based:
        shift = (
            1 if zero_based else -1
        )  # 0-based -> 1-based: +1; 1-based -> 0-based: -1
        lf = lf.with_columns(
            (pl.col("start") + shift).cast(schema["start"], strict=True)
        )
    set_source_metadata(lf, format=fmt, path=str(source))
    set_coordinate_system(lf, out_zero_based)
    return lf


def _collect_with_metadata(lf: pl.LazyFrame) -> pl.DataFrame:
    df = lf.collect()
    df.config_meta.set(**lf.config_meta.get_metadata())
    return df


def _scan(
    reader, fmt, source, zero_based, use_zero_based, chrom, start, end, polars_kwargs
) -> pl.LazyFrame:
    _check_bool("zero_based", zero_based)
    _check_bool("use_zero_based", use_zero_based, allow_none=True)
    lf = reader(source, **polars_kwargs)
    lf = _rename_interval_columns(
        lf, zero_based=zero_based, chrom=chrom, start=start, end=end, source=source
    )
    return _apply_coordinate_system(
        lf, zero_based=zero_based, use_zero_based=use_zero_based, fmt=fmt, source=source
    )


def scan_csv(
    source,
    *,
    zero_based: bool,
    use_zero_based: Optional[bool] = None,
    chrom: str = "chrom",
    start: str = "start",
    end: Optional[str] = "end",
    **polars_kwargs,
) -> pl.LazyFrame:
    """Lazily read a CSV file with its coordinate system recorded.

    Parameters:
        source: Path(s) accepted by ``pl.scan_csv``.
        zero_based: Coordinate system of the data in the file: True for 0-based
            half-open (e.g. BED), False for 1-based closed (e.g. VCF/GFF). Required.
        use_zero_based: Coordinate system of the result. None (default) keeps the
            source system; otherwise ``start`` is converted.
        chrom: Name of the chromosome column; renamed to ``chrom``.
        start: Name of the start column; renamed to ``start``.
        end: Name of the end column; renamed to ``end``. None for point data:
            ``end`` is derived from ``start`` in the source system.
        **polars_kwargs: Passed to ``pl.scan_csv`` unchanged.
    """
    return _scan(
        pl.scan_csv,
        "csv",
        source,
        zero_based,
        use_zero_based,
        chrom,
        start,
        end,
        polars_kwargs,
    )


def scan_parquet(
    source,
    *,
    zero_based: bool,
    use_zero_based: Optional[bool] = None,
    chrom: str = "chrom",
    start: str = "start",
    end: Optional[str] = "end",
    **polars_kwargs,
) -> pl.LazyFrame:
    """Lazily read Parquet data with its coordinate system recorded.

    Parameters are as for [scan_csv][polars_bio._tabular_io.scan_csv];
    ``source`` and ``**polars_kwargs`` are passed to ``pl.scan_parquet``.
    """
    return _scan(
        pl.scan_parquet,
        "parquet",
        source,
        zero_based,
        use_zero_based,
        chrom,
        start,
        end,
        polars_kwargs,
    )


def read_csv(
    source,
    *,
    zero_based: bool,
    use_zero_based: Optional[bool] = None,
    chrom: str = "chrom",
    start: str = "start",
    end: Optional[str] = "end",
    **polars_kwargs,
) -> pl.DataFrame:
    """Read a CSV file into a DataFrame with its coordinate system recorded.

    Parameters are as for [scan_csv][polars_bio._tabular_io.scan_csv].
    """
    return _collect_with_metadata(
        scan_csv(
            source,
            zero_based=zero_based,
            use_zero_based=use_zero_based,
            chrom=chrom,
            start=start,
            end=end,
            **polars_kwargs,
        )
    )


def read_parquet(
    source,
    *,
    zero_based: bool,
    use_zero_based: Optional[bool] = None,
    chrom: str = "chrom",
    start: str = "start",
    end: Optional[str] = "end",
    **polars_kwargs,
) -> pl.DataFrame:
    """Read Parquet data into a DataFrame with its coordinate system recorded.

    Parameters are as for [scan_parquet][polars_bio._tabular_io.scan_parquet].
    """
    return _collect_with_metadata(
        scan_parquet(
            source,
            zero_based=zero_based,
            use_zero_based=use_zero_based,
            chrom=chrom,
            start=start,
            end=end,
            **polars_kwargs,
        )
    )
