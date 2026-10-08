"""Polars IO-plugin registration with consistent error types across polars versions."""

from typing import Any, Callable, Iterator

import polars as pl
from polars.io.plugins import register_io_source as _register_io_source


def register_io_source(
    io_source: Callable[..., Iterator[pl.DataFrame]], **kwargs: Any
) -> pl.LazyFrame:
    """Register ``io_source`` as a polars IO plugin.

    Polars 1.x wraps any exception raised by an IO source, its own errors
    included, in ``pl.exceptions.ComputeError``; polars 2.x propagates it
    unchanged. Re-raise everything as ``ComputeError`` so callers see the same
    error type on both versions.
    """

    def _source(*args: Any) -> Iterator[pl.DataFrame]:
        try:
            yield from io_source(*args)
        except pl.exceptions.ComputeError:
            raise
        except Exception as e:
            raise pl.exceptions.ComputeError(f"{type(e).__name__}: {e}") from e

    return _register_io_source(_source, **kwargs)
