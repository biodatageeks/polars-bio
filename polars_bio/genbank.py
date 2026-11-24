"""GenBank record and feature tables."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

from ._frame_metadata import get_metadata as _get_frame_metadata
from ._frame_metadata import set_metadata as _set_frame_metadata
from .context import _resolve_zero_based, ctx
from .io import _read_file
from .polars_bio import (
    GenBankReadOptions,
    InputFormat,
    PyObjectStorageOptions,
    ReadOptions,
    py_register_table,
)

_FEATURE_COLUMNS = ("record_id", "type", "start", "end", "strand", "location", "parts")
_FEATURES = set(_FEATURE_COLUMNS)


def _options(view, fields, zero, storage):
    if view not in ("records", "features"):
        raise ValueError("view must be 'records' or 'features'")
    if fields is not None:
        if view != "features":
            raise ValueError("qualifier_fields requires view='features'")
        if isinstance(fields, str):
            raise TypeError("qualifier_fields must be a sequence of names")
        fields = list(fields)
        if any(not isinstance(f, str) or not f or f in _FEATURES for f in fields):
            raise ValueError(
                "invalid qualifier name or collision with a feature column"
            )
        if len(set(fields)) != len(fields):
            raise ValueError("duplicate qualifier column")
    return ReadOptions(
        genbank_read_options=GenBankReadOptions(view, fields, zero, storage)
    )


def _roots(expressions):
    """Only explicit input names can introduce promoted qualifiers."""
    for expr in expressions:
        if isinstance(expr, str):
            yield expr
        elif isinstance(expr, pl.Expr):
            yield from expr.meta.root_names()
        elif isinstance(expr, (list, tuple)):
            yield from _roots(expr)


class _FeatureScan(pl.LazyFrame):
    """Resolve direct qualifier selections while keeping the default map schema."""

    def __init__(self, frame, scan, fields, filters=()):
        self._ldf = frame._ldf
        self._scan = scan
        self._fields = fields
        self._filters = filters
        metadata = _get_frame_metadata(frame)
        if metadata:
            _set_frame_metadata(self, **metadata)

    @classmethod
    def _from_pyldf(cls, ldf):
        return pl.LazyFrame._from_pyldf(ldf)

    def _resolve(self, roots, filters):
        required = list(self._fields)
        for name in roots:
            if name not in _FEATURES and name not in required:
                required.append(name)
        for predicates, constraints in filters:
            for name in (*_roots(predicates), *constraints):
                if name not in _FEATURES and name not in required:
                    required.append(name)
        frame = self._scan(required)
        for predicates, constraints in filters:
            frame = frame.filter(*predicates, **constraints)
        return frame

    def select(self, *exprs, **named_exprs):
        roots = tuple(_roots((*exprs, *named_exprs.values())))
        if all(n in _FEATURES or n in self._fields for n in roots):
            return super().select(*exprs, **named_exprs)
        frame = self._resolve(roots, self._filters)
        return frame.select(*exprs, **named_exprs)

    def filter(self, *predicates, **constraints):
        filters = (*self._filters, (predicates, constraints))
        frame = self._resolve((), filters).select([*_FEATURE_COLUMNS, *self._fields])
        return _FeatureScan(frame, self._scan, self._fields, filters)


def scan_genbank(
    path: str,
    view: str = "records",
    *,
    qualifier_fields: Sequence[str] | None = None,
    use_zero_based: bool | None = None,
    projection_pushdown: bool = True,
    predicate_pushdown: bool = True,
    chunk_size: int = 8,
    concurrent_fetches: int = 8,
    allow_anonymous: bool = True,
    enable_request_payer: bool = False,
    max_retries: int = 5,
    timeout: int = 300,
    compression_type: str = "auto",
) -> pl.LazyFrame:
    """Lazily scan nucleotide or protein GenBank records or their features.

    Choose `view="features"` for locations, ordered parts and qualifiers. Pass
    qualifier names in `qualifier_fields` to declare scalar columns for use
    throughout a lazy query. Include `"qualifiers"` to retain the full map.
    Direct feature selections also support qualifier promotion. Coordinates
    follow the session setting unless `use_zero_based` is supplied.
    """
    storage = PyObjectStorageOptions(
        chunk_size=chunk_size,
        concurrent_fetches=concurrent_fetches,
        allow_anonymous=allow_anonymous,
        enable_request_payer=enable_request_payer,
        max_retries=max_retries,
        timeout=timeout,
        compression_type=compression_type,
    )
    zero = _resolve_zero_based(use_zero_based)

    def scan(fields):
        return _read_file(
            str(path),
            InputFormat.GenBank,
            _options(view, fields, zero, storage),
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero,
        )

    frame = scan(qualifier_fields)
    if view == "features":
        fields = ["qualifiers"] if qualifier_fields is None else list(qualifier_fields)
        return _FeatureScan(frame, scan, fields)
    return frame


def read_genbank(path: str, view: str = "records", **options) -> pl.DataFrame:
    """Read a GenBank table eagerly. Accepts the same options as scan_genbank."""
    return scan_genbank(path, view, **options).collect()


def register_genbank(
    path: str,
    name: str | None = None,
    *,
    view: str = "records",
    qualifier_fields: Sequence[str] | None = None,
    use_zero_based: bool | None = None,
    chunk_size: int = 8,
    concurrent_fetches: int = 8,
    allow_anonymous: bool = True,
    enable_request_payer: bool = False,
    max_retries: int = 5,
    timeout: int = 300,
    compression_type: str = "auto",
) -> None:
    """Register a GenBank view for SQL. Declare promoted qualifiers at registration."""
    storage = PyObjectStorageOptions(
        chunk_size=chunk_size,
        concurrent_fetches=concurrent_fetches,
        allow_anonymous=allow_anonymous,
        enable_request_payer=enable_request_payer,
        max_retries=max_retries,
        timeout=timeout,
        compression_type=compression_type,
    )
    opts = _options(
        view, qualifier_fields, _resolve_zero_based(use_zero_based), storage
    )
    py_register_table(ctx, str(path), name, InputFormat.GenBank, opts)
