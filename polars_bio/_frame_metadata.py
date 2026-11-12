"""Instance-owned metadata for a bounded set of ordinary Polars operations.

The ``config_meta`` spelling and Parquet representation are compatible with
polars-config-meta (MIT; see licenses/polars-config-meta/LICENSE). There is no
frame registry, method discovery, or dependency on that package.
"""

from __future__ import annotations

import json
import threading
from functools import wraps

import polars as pl
from polars.dataframe.group_by import GroupBy
from polars.lazyframe.group_by import LazyGroupBy

_FRAME_ATTR = "_polars_bio_metadata"
_GROUP_ATTR = "_polars_bio_group_metadata"
_NAMESPACE = "config_meta"
_FRAMES = (pl.DataFrame, pl.LazyFrame)
_GROUPS = (GroupBy, LazyGroupBy)
_PRESERVING = (
    "filter",
    "select",
    "with_columns",
    "drop",
    "rename",
    "sort",
    "slice",
    "head",
    "tail",
    "limit",
    "clone",
    "cast",
    "unique",
    "reverse",
    "explode",
)
_COMBINING = ("join", "join_asof", "join_where", "update", "merge_sorted")
# Explicit class/method pairs; no discovery from signatures or annotations.
PATCH_MANIFEST = (
    *((cls, name, "preserve") for cls in _FRAMES for name in _PRESERVING),
    (pl.DataFrame, "lazy", "preserve"),
    (pl.DataFrame, "__getitem__", "preserve"),
    (pl.LazyFrame, "collect", "preserve"),
    *((cls, "group_by", "group") for cls in _FRAMES),
    # GroupBy.len delegates to agg in tested Polars versions. Contract tests
    # cover len explicitly so a changed upstream implementation fails CI.
    *((cls, name, "aggregate") for cls in _GROUPS for name in ("agg", "head", "tail")),
    *((cls, "having", "having") for cls in _GROUPS),
    *((cls, name, "invalidate") for cls in _FRAMES for name in _COMBINING),
    *((pl.DataFrame, name, "invalidate") for name in ("vstack", "hstack", "extend")),
    # Removed in Polars 2; keep every other manifest entry mandatory.
    *(
        ((pl.LazyFrame, "with_context", "invalidate"),)
        if pl.__version__.startswith("1.")
        else ()
    ),
)

_lock = threading.RLock()
_initialized = False
_initializing = False
_original_methods = {}
_installed_methods = {}
_namespace_descriptors = {}


def _conflict():
    return RuntimeError(
        "polars-bio frame metadata conflicts with another config_meta provider or "
        "a replaced metadata wrapper. Remove direct imports of polars_config_meta "
        "and other conflicting providers, then restart the Python interpreter. "
        "The polars-config-meta distribution may remain installed but unused."
    )


def _check_namespace_ownership():
    # Read descriptors directly: do not instantiate/cache namespace accessors.
    if not _initialized or any(
        cls.__dict__.get(_NAMESPACE) is not _namespace_descriptors.get(cls)
        for cls in _FRAMES
    ):
        raise _conflict()


def get_metadata(frame) -> dict:
    """Return a shallow snapshot, including for existing source adapters."""
    _check_namespace_ownership()
    if isinstance(frame, _FRAMES):
        return dict(frame.__dict__.get(_FRAME_ATTR, {}))
    return frame.config_meta.get_metadata()


def set_metadata(frame, **values) -> None:
    """Update metadata without allocating an accessor on ordinary frames."""
    _check_namespace_ownership()
    if isinstance(frame, _FRAMES):
        if values:
            frame.__dict__.setdefault(_FRAME_ATTR, {}).update(values)
    else:
        frame.config_meta.set(**values)


def copy_metadata(source, result):
    """Copy complete metadata, replacing obsolete destination state."""
    metadata = get_metadata(source)
    if result is not source:
        clear_metadata(result)
        set_metadata(result, **metadata)
    return result


def clear_metadata(frame) -> None:
    """Invalidate a native frame's metadata; live accessors read it per call."""
    _check_namespace_ownership()
    frame.__dict__.pop(_FRAME_ATTR, None)


def _wrapper(original, mode):
    source_attr = _GROUP_ATTR if mode in ("aggregate", "having") else _FRAME_ATTR
    target_attr = _GROUP_ATTR if mode in ("group", "having") else _FRAME_ATTR
    result_types = _GROUPS if mode in ("group", "having") else _FRAMES

    @wraps(original)
    def call(self, *args, **kwargs):
        metadata = self.__dict__.get(source_attr)
        has_group_snapshot = source_attr == _GROUP_ATTR and source_attr in self.__dict__
        if metadata:
            _check_namespace_ownership()
        result = original(self, *args, **kwargs)
        if mode == "invalidate":
            # Clear even on an untagged receiver: internal operations may have
            # propagated metadata from another input. Preserve exceptions.
            if isinstance(result, _FRAMES):
                result.__dict__.pop(_FRAME_ATTR, None)
        elif result is not self and isinstance(result, result_types):
            if mode == "group" or (mode == "having" and has_group_snapshot):
                # An empty snapshot must also be remembered: an eager group's
                # source may be tagged later, before its lazy plan is built.
                result.__dict__[target_attr] = metadata.copy() if metadata else None
            elif metadata:
                result.__dict__[target_attr] = metadata.copy()
            elif mode == "aggregate" and has_group_snapshot:
                result.__dict__.pop(target_attr, None)
        return result

    return call


class FrameMetadata:
    """Compatibility namespace: set, snapshot reads, and Parquet persistence."""

    def __init__(self, frame):
        _check_namespace_ownership()
        # A weak owner breaks temporary_frame.config_meta.get_metadata(): Python
        # may drop the temporary before the accessor's method is called. The
        # strong owner is safe because _MetadataNamespace never caches the
        # accessor on the frame, so no frame -> accessor -> frame cycle exists.
        self._owner = frame

    def _frame(self):
        _check_namespace_ownership()
        return self._owner

    def set(self, **values) -> None:
        set_metadata(self._frame(), **values)

    def get_metadata(self) -> dict:
        return get_metadata(self._frame())

    def write_parquet(self, file_path, **kwargs) -> None:
        """Write legacy JSON metadata; kwargs retain PyArrow writer semantics.

        Common options and scalar schemas use native Polars writing. Other
        options or schemas use a fallback that materializes lazy frames. Plain
        Polars writers do not automatically serialize this namespace.
        """
        _write_parquet(self._frame(), file_path, kwargs)


class _MetadataNamespace:
    """Non-caching replacement for Polars' NameSpace descriptor (issue #503).

    Polars caches the accessor via setattr(instance, name, accessor); together
    with FrameMetadata's strong owner that is a reference cycle, so large frames
    wait for the cyclic GC instead of being freed on their last reference.
    ``_ns`` mirrors Polars' descriptor attribute used by rollback detection.
    """

    def __init__(self, namespace):
        self._ns = namespace

    def __get__(self, instance, owner):
        return self._ns if instance is None else self._ns(instance)


def _native_parquet_options(options):
    supported = {"compression", "row_group_size", "data_page_size", "write_statistics"}
    if options.keys() - supported:
        return None
    compression = options.get("compression", "snappy")
    if compression is None or compression in ("none", "NONE"):
        compression = "uncompressed"
    if not isinstance(compression, str) or compression not in (
        "snappy",
        "gzip",
        "brotli",
        "zstd",
        "lz4",
        "uncompressed",
    ):
        return None
    row_size = options.get("row_group_size")
    if row_size is None:
        row_size = 1024 * 1024  # PyArrow's default, not Polars' smaller default.
    if type(row_size) is not int or not 0 < row_size <= 64 * 1024 * 1024:
        return None
    page_size = options.get("data_page_size")
    if page_size is not None and (type(page_size) is not int or page_size <= 0):
        return None
    statistics = options.get("write_statistics", True)
    if type(statistics) is not bool:
        return None
    return dict(
        compression=compression,
        row_group_size=row_size,
        data_page_size=page_size,
        statistics=statistics,
    )


def _native_parquet_schema(schema):
    from pyarrow import types

    # Polars can rebuild nested/dictionary Arrow schemas and drop their schema
    # metadata even when arrow_schema= is supplied. Only use the verified scalar
    # path; the legacy PyArrow writer preserves both metadata locations otherwise.
    checks = (
        types.is_primitive,
        types.is_string,
        types.is_large_string,
        types.is_string_view,
        types.is_binary,
        types.is_large_binary,
        types.is_binary_view,
        types.is_decimal,
        types.is_null,
    )
    return all(any(check(field.type) for check in checks) for field in schema)


def _write_parquet(frame, file_path, options):
    metadata_json = json.dumps(get_metadata(frame))
    native_options = _native_parquet_options(options)
    table = None
    if native_options is not None:
        if isinstance(frame, pl.LazyFrame):
            schema = frame.collect_schema().to_arrow()
        else:
            table = frame.to_arrow()
            schema = table.schema
        if _native_parquet_schema(schema):
            schema = schema.with_metadata(
                {
                    **(schema.metadata or {}),
                    b"polars_plugin_meta": metadata_json.encode(),
                }
            )
            writer = (
                frame.sink_parquet
                if isinstance(frame, pl.LazyFrame)
                else frame.write_parquet
            )
            writer(
                file_path,
                metadata={"polars_plugin_meta": metadata_json},
                arrow_schema=schema,
                **native_options,
            )
            return

    import pyarrow.parquet as pq

    if table is None:
        table = (
            frame.collect() if isinstance(frame, pl.LazyFrame) else frame
        ).to_arrow()
    table = table.replace_schema_metadata(
        {**(table.schema.metadata or {}), b"polars_plugin_meta": metadata_json.encode()}
    )
    pq.write_table(table, file_path, **options)


def initialize() -> None:
    """Install the fixed manifest once, with preflight and owned rollback."""
    global _initialized, _initializing
    with _lock:
        if _initializing:
            raise RuntimeError(
                "polars-bio metadata initialization is already in progress"
            )
        if _initialized:
            _check_namespace_ownership()
            if any(
                getattr(cls, name) is not wrapper
                for (cls, name), wrapper in _installed_methods.items()
            ):
                raise _conflict()
            return
        for cls in _FRAMES:
            if hasattr(cls, _NAMESPACE) or _NAMESPACE in cls._accessors:
                raise _conflict()
        originals = {}
        wrappers = {}
        for cls, name, mode in PATCH_MANIFEST:
            original = getattr(cls, name, None)
            if not callable(original):
                raise RuntimeError(
                    f"polars-bio metadata requires {cls.__name__}.{name}; "
                    f"Polars {pl.__version__} is incompatible with this provider"
                )
            originals[cls, name] = original
            wrappers[cls, name] = _wrapper(original, mode)
        _initializing = True
        try:
            pl.api.register_dataframe_namespace(_NAMESPACE)(FrameMetadata)
            if hasattr(pl.LazyFrame, _NAMESPACE):
                raise _conflict()
            pl.api.register_lazyframe_namespace(_NAMESPACE)(FrameMetadata)
            for cls in _FRAMES:
                # Load-bearing, not defensive: Polars' NameSpace keeps the
                # registered class in private _ns, the same coupling as the
                # rollback below. It proves the descriptor is ours before it is
                # swapped for the non-caching one (#503). If Polars renames _ns,
                # initialize() fails on import and the provider CI matrix fails.
                if (
                    getattr(cls.__dict__.get(_NAMESPACE), "_ns", None)
                    is not FrameMetadata
                ):
                    raise _conflict()
                setattr(cls, _NAMESPACE, _MetadataNamespace(FrameMetadata))
            for (cls, name), wrapper in wrappers.items():
                if getattr(cls, name) is not originals[cls, name]:
                    raise _conflict()
                setattr(cls, name, wrapper)
            _namespace_descriptors.update(
                (cls, cls.__dict__[_NAMESPACE]) for cls in _FRAMES
            )
            _original_methods.update(originals)
            _installed_methods.update(wrappers)
            _initialized = True
        except BaseException:
            for (cls, name), wrapper in wrappers.items():
                if cls.__dict__.get(name) is wrapper:
                    setattr(cls, name, originals[cls, name])
            for cls in _FRAMES:
                descriptor = cls.__dict__.get(_NAMESPACE)
                # Polars' namespace descriptor stores the registered class in
                # private _ns; rollback tests guard this upstream coupling.
                if getattr(descriptor, "_ns", None) is FrameMetadata:
                    delattr(cls, _NAMESPACE)
                    cls._accessors.discard(_NAMESPACE)
            _namespace_descriptors.clear()
            _original_methods.clear()
            _installed_methods.clear()
            raise
        finally:
            _initializing = False
