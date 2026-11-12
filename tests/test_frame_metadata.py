"""Provider contracts; also runnable without the polars-bio Rust extension.

Loading the canonical submodule directly lets the supported-version CI matrix
exercise the provider independently. A later application import reuses it.
Installation/conflict scenarios run in fresh subprocesses.
"""

import gc
import importlib.util
import inspect
import io
import json
import subprocess
import sys
import textwrap
import weakref
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from polars.dataframe.group_by import GroupBy
from polars.lazyframe.group_by import LazyGroupBy
from polars.testing import assert_frame_equal

PROVIDER = Path(__file__).resolve().parents[1] / "polars_bio" / "_frame_metadata.py"
BOOTSTRAP = f"""
import importlib.util
import polars as pl
spec = importlib.util.spec_from_file_location("isolated_frame_metadata", {str(PROVIDER)!r})
fm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fm)
"""
if "polars_bio._frame_metadata" not in sys.modules:
    spec = importlib.util.spec_from_file_location(
        "polars_bio._frame_metadata", PROVIDER
    )
    fm = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = fm
    spec.loader.exec_module(fm)
    fm.initialize()
else:
    fm = sys.modules["polars_bio._frame_metadata"]

META = {
    "coordinate_system_zero_based": True,
    "source_format": "vcf",
    "source_path": "example.vcf",
    "source_header": '{"samples":["S1","S2"],"tag_types":{"XI":"i"},"layout":{"format":"GT"}}',
}
ROWS = {"chrom": ["1", "1", "2"], "start": [1, 3, 5], "end": [2, 4, 6]}
# Independent expectations: do not build these from the implementation manifest.
FRAME_METHODS = "filter select with_columns drop rename sort slice head tail limit clone cast unique reverse explode".split()
COMBINING = "join join_asof join_where update merge_sorted".split()
POLARS_1 = pl.__version__.startswith("1.")
EXPECTED_PAIRS = (
    {
        (cls, name)
        for cls in (pl.DataFrame, pl.LazyFrame)
        for name in FRAME_METHODS + COMBINING + ["group_by"]
    }
    | {
        (pl.DataFrame, name)
        for name in ("lazy", "__getitem__", "vstack", "hstack", "extend")
    }
    | {(pl.LazyFrame, "collect")}
    | ({(pl.LazyFrame, "with_context")} if POLARS_1 else set())
    | {
        (cls, name)
        for cls in (GroupBy, LazyGroupBy)
        for name in ("agg", "head", "tail", "having")
    }
)


def run_isolated(source):
    result = subprocess.run(
        [sys.executable, "-c", BOOTSTRAP + textwrap.dedent(source)],
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def tagged(cls=pl.DataFrame, metadata=META):
    frame = cls(ROWS)
    frame.config_meta.set(**metadata)
    return frame


def test_manifest_signatures_and_reinitialization():
    assert len(EXPECTED_PAIRS) == (57 if POLARS_1 else 56)
    assert set(fm._installed_methods) == EXPECTED_PAIRS
    before = {pair: getattr(*pair) for pair in EXPECTED_PAIRS}
    fm.initialize()
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda _: fm.initialize(), range(12)))
    for pair, wrapper in before.items():
        assert getattr(*pair) is wrapper
        assert wrapper.__wrapped__ is fm._original_methods[pair]
        assert inspect.signature(wrapper) == inspect.signature(wrapper.__wrapped__)


def test_exact_patch_scope_in_fresh_process():
    run_isolated(
        """
        from polars.dataframe.group_by import GroupBy
        from polars.lazyframe.group_by import LazyGroupBy
        classes = (pl.DataFrame, pl.LazyFrame, pl.Series, GroupBy, LazyGroupBy)
        before = {cls: dict(vars(cls)) for cls in classes}
        functions = (pl.concat, pl.collect_all)
        fm.initialize()
        changed = {(cls, name) for cls in classes for name, value in before[cls].items()
                   if vars(cls).get(name) is not value}
        assert changed == set(fm._installed_methods)
        assert len(changed) == (57 if pl.__version__.startswith("1.") else 56)
        assert hasattr(pl.LazyFrame, "with_context") == pl.__version__.startswith("1.")
        assert (pl.concat, pl.collect_all) == functions
        assert not hasattr(pl.Series, "config_meta")
    """
    )


def test_snapshot_setter_temporary_accessors_and_no_forwarding():
    frame = tagged()
    assert frame.config_meta.set(extra=[1]) is None
    snapshot = frame.config_meta.get_metadata()
    snapshot["source_path"] = "changed"
    assert frame.config_meta.get_metadata()["source_path"] == "example.vcf"
    snapshot["extra"].append(2)
    assert frame.config_meta.get_metadata()["extra"] == [1, 2]
    assert frame.head(1).config_meta.get_metadata() == frame.config_meta.get_metadata()
    assert pl.DataFrame().config_meta.get_metadata() == {}
    for name in ("merge", "update", "clear_metadata", "select"):
        with pytest.raises(AttributeError):
            getattr(frame.config_meta, name)
    cached = frame.config_meta
    fm.clear_metadata(frame)
    assert cached.get_metadata() == {}


def test_accessors_and_frames_are_collectible():
    frame = tagged()
    ref = weakref.ref(frame)
    accessor = frame.config_meta
    del frame
    gc.collect()
    assert accessor.get_metadata() == META
    del accessor
    gc.collect()
    assert ref() is None
    assert fm.get_metadata(pl.DataFrame(ROWS)) == {}


def test_independent_frames_concurrently():
    def work(index):
        frame = pl.LazyFrame(ROWS)
        fm.set_metadata(frame, index=index, **META)
        result = frame.filter(pl.col("start") > 0).collect()
        assert result.config_meta.get_metadata() == {**META, "index": index}
        return weakref.ref(result), weakref.ref(frame)

    with ThreadPoolExecutor(max_workers=8) as pool:
        refs = list(pool.map(work, range(150)))
    gc.collect()
    assert all(ref() is None for pair in refs for ref in pair)


PRESERVE_CASES = [
    (cls, name) for cls in (pl.DataFrame, pl.LazyFrame) for name in FRAME_METHODS
] + [(pl.DataFrame, "lazy"), (pl.DataFrame, "__getitem__"), (pl.LazyFrame, "collect")]


@pytest.mark.parametrize("cls,method", PRESERVE_CASES)
@pytest.mark.parametrize("has_metadata", [False, True])
def test_preserving_manifest(cls, method, has_metadata):
    frame = cls({**ROWS, "values": [[1], [2], [3]]})
    if has_metadata:
        fm.set_metadata(frame, **META)
    arguments = {
        "filter": (pl.col("start") > 1,),
        "select": ("chrom", "start", "end"),
        "with_columns": (pl.col("end").alias("stop"),),
        "drop": ("values",),
        "rename": ({"end": "stop"},),
        "sort": ("start",),
        "slice": (1, 1),
        "head": (1,),
        "tail": (1,),
        "limit": (1,),
        "cast": ({"start": pl.Int32},),
        "explode": ("values",),
        "__getitem__": (["chrom", "start"],),
    }
    result = getattr(frame, method)(*arguments.get(method, ()))
    expected_type = (
        pl.LazyFrame
        if method == "lazy"
        else pl.DataFrame if method == "collect" else cls
    )
    assert type(result) is expected_type
    assert fm.get_metadata(result) == (META if has_metadata else {})
    if has_metadata:
        fm.set_metadata(result, source_path="result")
        assert fm.get_metadata(frame) == META
    else:
        assert fm._FRAME_ATTR not in result.__dict__
        assert "config_meta" not in result.__dict__


def test_indexing_non_frame_results_and_original_errors():
    frame = tagged()
    assert isinstance(frame["start"], pl.Series)
    assert frame[0, "start"] == 1
    assert not hasattr(frame["start"], fm._FRAME_ATTR)
    for cls in (pl.DataFrame, pl.LazyFrame):
        frame = tagged(cls)
        messages = []
        for method in (fm._original_methods[cls, "select"], cls.select):
            with pytest.raises(pl.exceptions.ColumnNotFoundError) as error:
                result = method(frame, "missing")
                if isinstance(result, pl.LazyFrame):
                    result.collect()
            messages.append(str(error.value))
        assert messages[0] == messages[1]
        assert fm.get_metadata(frame) == META


@pytest.mark.parametrize("cls", [pl.DataFrame, pl.LazyFrame])
@pytest.mark.parametrize("has_metadata", [False, True])
@pytest.mark.parametrize("operation", ["agg", "len", "head", "tail", "having"])
def test_grouping_snapshots(cls, has_metadata, operation):
    source = cls(ROWS)
    if has_metadata:
        fm.set_metadata(source, **META)
    group = source.group_by("chrom", maintain_order=True)
    # Includes the empty snapshot: later tags must not leak through eager internals.
    fm.set_metadata(source, coordinate_system_zero_based=False, source_path="later")
    if operation == "having":
        group = group.having(pl.len() > 1)
    result = (
        group.agg(start=pl.col("start").min(), end=pl.col("end").max())
        if operation in ("agg", "having")
        else group.len(name="n") if operation == "len" else getattr(group, operation)(1)
    )
    assert type(result) is cls
    assert fm.get_metadata(result) == (META if has_metadata else {})
    if cls is pl.LazyFrame:
        assert fm.get_metadata(result.collect(engine="streaming")) == (
            META if has_metadata else {}
        )
    fm.set_metadata(result, source_path="result")
    again = group.len()
    assert fm.get_metadata(again) == (META if has_metadata else {})


def test_lazy_propagation_does_not_collect_or_resolve_schema(monkeypatch):
    source = tagged(pl.LazyFrame)

    def unexpected(*args, **kwargs):
        pytest.fail("metadata propagation evaluated the lazy input")

    monkeypatch.setattr(pl.LazyFrame, "collect", unexpected)
    monkeypatch.setattr(pl.LazyFrame, "collect_schema", unexpected)
    result = (
        source.filter(pl.col("start") > 0).group_by("chrom").agg(pl.col("start").min())
    )
    assert fm.get_metadata(result) == META


@pytest.mark.parametrize("cls", [pl.DataFrame, pl.LazyFrame])
def test_grouping_named_keys_empty_and_having_identity(cls):
    frame = tagged(cls)
    group = frame.group_by(chromosome=pl.col("chrom"), maintain_order=True)
    result = group.having(pl.len() > 1)
    assert (result is group) is (cls is pl.LazyFrame)
    result = result.agg(begin=pl.col("start").min())
    materialized = result.collect() if cls is pl.LazyFrame else result
    assert materialized.to_dicts() == [{"chromosome": "1", "begin": 1}]
    assert fm.get_metadata(result) == META
    empty = frame.head(0).group_by("chrom").len()
    assert fm.get_metadata(empty) == META
    assert (empty.collect() if cls is pl.LazyFrame else empty).height == 0


COMBINE_CASES = [
    (cls, name) for cls in (pl.DataFrame, pl.LazyFrame) for name in COMBINING
] + [
    (pl.DataFrame, "vstack"),
    (pl.DataFrame, "hstack"),
    (pl.DataFrame, "extend"),
]
if POLARS_1:
    COMBINE_CASES.append((pl.LazyFrame, "with_context"))


@pytest.mark.parametrize("cls,method", COMBINE_CASES)
@pytest.mark.parametrize(
    "state",
    ["matching", "conflicting", "left_unknown", "right_unknown", "both_unknown"],
)
def test_multi_input_invalidation(cls, method, state):
    left = cls(ROWS)
    right = cls({"extra": [1, 2, 3]}) if method == "hstack" else cls(ROWS)
    if state not in ("left_unknown", "both_unknown"):
        fm.set_metadata(left, **META)
    if state not in ("right_unknown", "both_unknown"):
        fm.set_metadata(
            right, **{**META, "coordinate_system_zero_based": state != "conflicting"}
        )
    before = fm.get_metadata(left), fm.get_metadata(right)
    kwargs = {"on": "start"} if method in ("join", "join_asof", "update") else {}
    extra = (
        (pl.col("start") < pl.col("start_right"),)
        if method == "join_where"
        else ("start",) if method == "merge_sorted" else ()
    )
    result = getattr(left, method)(right, *extra, **kwargs)
    assert fm.get_metadata(result) == {}
    assert fm.get_metadata(right) == before[1]
    assert fm.get_metadata(left) == ({} if method == "extend" else before[0])


@pytest.mark.parametrize("method", ["vstack", "hstack", "extend"])
def test_in_place_invalidation_reaches_cached_accessor(method):
    frame = tagged()
    accessor = frame.config_meta
    other = (
        pl.DataFrame({"extra": [1, 2, 3]}) if method == "hstack" else pl.DataFrame(ROWS)
    )
    result = getattr(frame, method)(
        other, **({} if method == "extend" else {"in_place": True})
    )
    assert result is frame
    assert accessor.get_metadata() == {}


def test_failed_combinations_leave_metadata_and_concat_remains_untagged():
    left, right = tagged(), tagged()
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        left.join(right, on="missing")
    assert fm.get_metadata(left) == fm.get_metadata(right) == META
    assert fm.get_metadata(pl.concat([left.lazy(), right.lazy()])) == {}
    stream = left.lazy().collect_batches(lazy=True)
    assert hasattr(stream, "__arrow_c_stream__")
    if POLARS_1:
        assert hasattr(stream._inner, "__arrow_c_stream__")


@pytest.mark.parametrize("cls", [pl.DataFrame, pl.LazyFrame])
@pytest.mark.parametrize("buffer", [False, True])
@pytest.mark.parametrize(
    "options",
    [
        {},
        {"compression": "zstd", "row_group_size": 1},
        {"use_dictionary": False},
        {"write_statistics": ["start"]},
    ],
)
def test_parquet_legacy_metadata_and_options(tmp_path, cls, buffer, options):
    frame = tagged(cls)
    destination = io.BytesIO() if buffer else tmp_path / "metadata.parquet"
    assert frame.config_meta.write_parquet(destination, **options) is None
    if buffer:
        destination.seek(0)
    schema = pq.read_schema(destination)
    assert json.loads(schema.metadata[b"polars_plugin_meta"]) == META
    if buffer:
        destination.seek(0)
    assert (
        json.loads(pl.read_parquet_metadata(destination)["polars_plugin_meta"]) == META
    )
    if buffer:
        destination.seek(0)
    file = pq.ParquetFile(destination)
    assert (
        file.metadata.row_group(0).column(0).compression
        == options.get("compression", "snappy").upper()
    )
    if "row_group_size" in options:
        assert file.metadata.num_row_groups == 3
    if options.get("use_dictionary") is False:
        assert "RLE_DICTIONARY" not in file.metadata.row_group(0).column(0).encodings


def test_native_lazy_parquet_does_not_collect_source_and_fallback_does(
    tmp_path, monkeypatch
):
    frame = tagged(pl.LazyFrame)
    original = frame.collect
    calls = []

    def collect(*args, **kwargs):
        calls.append(True)
        return original(*args, **kwargs)

    monkeypatch.setattr(frame, "collect", collect)
    frame.config_meta.write_parquet(tmp_path / "native.parquet")
    assert calls == []
    frame.config_meta.write_parquet(tmp_path / "fallback.parquet", use_dictionary=False)
    assert calls == [True]


@pytest.mark.parametrize("options", [{}, {"use_dictionary": False}])
def test_parquet_preserves_unrelated_schema_metadata(tmp_path, monkeypatch, options):
    frame = tagged()
    table = frame.to_arrow().replace_schema_metadata({b"external": b"value"})
    monkeypatch.setattr(frame, "to_arrow", lambda: table)
    path = tmp_path / "external.parquet"
    frame.config_meta.write_parquet(path, **options)
    assert pq.read_schema(path).metadata[b"external"] == b"value"


def test_parquet_invalid_options_and_non_json_values(tmp_path):
    path = tmp_path / "bad.parquet"
    frame = tagged()
    with pytest.raises(TypeError):
        frame.config_meta.write_parquet(path, unknown_option=True)
    fm.set_metadata(frame, unserializable=object())
    with pytest.raises(TypeError):
        frame.config_meta.write_parquet(path)


@pytest.mark.parametrize("lazy", [False, True])
@pytest.mark.parametrize(
    "options", [{}, {"use_dictionary": True}, {"use_dictionary": False}]
)
def test_parquet_nested_temporal_categorical_values(lazy, options):
    source = pl.DataFrame(
        {
            "chrom": ["1", "2"],
            "start": [1, 2],
            "calls": [[1, None], [2, 3]],
            "info": [{"ref": "A", "score": 0.1}, {"ref": "C", "score": None}],
            "when": [datetime(2026, 1, 1), None],
            "category": ["a", "b"],
        }
    ).with_columns(pl.col("category").cast(pl.Categorical))
    frame = source.lazy() if lazy else source
    fm.set_metadata(frame, **META)
    output = io.BytesIO()
    frame.config_meta.write_parquet(output, **options)
    output.seek(0)
    # Independently check fallback files through Arrow as well. Valid
    # dictionary-free categorical files exceed some Polars 1.x decoder paths.
    arrow = pq.read_table(output)
    assert_frame_equal(source, pl.from_arrow(arrow))
    assert json.loads(arrow.schema.metadata[b"polars_plugin_meta"]) == META
    if options.get("use_dictionary") is not False:
        output.seek(0)
        assert_frame_equal(source, pl.read_parquet(output))


@pytest.mark.parametrize("first", [False, True])
def test_foreign_namespace_conflicts(first):
    run_isolated(
        f"""
        if not {first!r}:
            fm.initialize()
            frame = pl.LazyFrame({{"a": [1]}})
            frame.config_meta.set(a=1)
            cached = frame.config_meta
            group = frame.group_by("a")
        class Foreign:
            def __init__(self, frame): pass
        pl.api.register_lazyframe_namespace("config_meta")(Foreign)
        actions = [fm.initialize] if {first!r} else [
            fm.initialize, cached.get_metadata, lambda: cached.set(b=2),
            lambda: fm.get_metadata(frame), lambda: frame.head(1), lambda: group.len(),
        ]
        for action in actions:
            try: action()
            except RuntimeError as error:
                assert "restart" in str(error) and "polars_config_meta" in str(error)
            else: raise AssertionError("conflict was not rejected")
        if {first!r}:
            assert not hasattr(pl.DataFrame, "config_meta")
    """
    )


@pytest.mark.skipif(
    importlib.util.find_spec("polars_config_meta") is None,
    reason="optional legacy plugin is not installed",
)
@pytest.mark.parametrize("first", [False, True])
def test_actual_legacy_plugin_import_orders(first):
    run_isolated(
        f"""
        if {first!r}:
            import polars_config_meta
        else:
            fm.initialize()
            frame = pl.LazyFrame({{"a": [1]}})
            frame.config_meta.set(a=1)
            cached = frame.config_meta
            import polars_config_meta
        try:
            fm.initialize() if {first!r} else cached.get_metadata()
        except RuntimeError as error:
            assert "restart" in str(error)
        else:
            raise AssertionError("legacy conflict was not detected")
    """
    )


def test_original_distribution_is_not_imported():
    run_isolated(
        """
        import sys
        fm.initialize()
        frame = pl.LazyFrame({"a": [1]})
        frame.config_meta.set(a=1)
        assert frame.collect().config_meta.get_metadata() == {"a": 1}
        assert "polars_config_meta" not in sys.modules
    """
    )


@pytest.mark.parametrize(
    "failure", ["preflight", "namespace", "method", "reentrant", "foreign"]
)
def test_installation_failure_and_owned_rollback(failure):
    run_isolated(
        f"""
        original = {{(cls, name): getattr(cls, name) for cls, name, _ in fm.PATCH_MANIFEST}}
        accessors = {{cls: set(cls._accessors) for cls in (pl.DataFrame, pl.LazyFrame)}}
        failure = {failure!r}
        real_register = pl.api.register_lazyframe_namespace
        real_setattr = setattr
        if failure == "preflight":
            pl.LazyFrame.join = None
        elif failure in ("namespace", "reentrant", "foreign"):
            def register(name):
                def apply(namespace):
                    real_register(name)(namespace)
                    if failure == "reentrant":
                        fm.initialize()
                    if failure == "foreign":
                        class Foreign: pass
                        real_register(name)(Foreign)
                    raise RuntimeError("injected registration failure")
                return apply
            pl.api.register_lazyframe_namespace = register
        else:
            def setter(cls, name, value):
                if cls is pl.LazyFrame and name == "filter" and hasattr(value, "__wrapped__"):
                    raise RuntimeError("injected method installation failure")
                real_setattr(cls, name, value)
            fm.setattr = setter
        try: fm.initialize()
        except RuntimeError: pass
        else: raise AssertionError("injected failure did not fire")
        assert not fm._initialized and not fm._installed_methods
        for (cls, name), value in original.items():
            if failure == "preflight" and cls is pl.LazyFrame and name == "join":
                real_setattr(cls, name, value)
            else:
                assert getattr(cls, name) is value, (cls, name)
        for cls in (pl.DataFrame, pl.LazyFrame):
            if failure == "foreign" and cls is pl.LazyFrame:
                assert cls.config_meta.__name__ == "Foreign"
            else:
                assert not hasattr(cls, "config_meta")
                assert cls._accessors == accessors[cls]
        if failure != "foreign":
            pl.api.register_lazyframe_namespace = real_register
            fm.setattr = real_setattr
            fm.initialize()
            assert fm._initialized
    """
    )


@pytest.mark.parametrize("cls", [pl.DataFrame, pl.LazyFrame])
def test_accessor_is_not_cached_and_frame_is_freed_by_refcount(cls):
    # Issue #503: Polars' NameSpace caches the accessor on the instance; with the
    # accessor's strong owner that formed a cycle only the cyclic GC could free.
    frame = tagged(cls)
    assert frame.config_meta.get_metadata() == META
    assert "config_meta" not in frame.__dict__
    assert frame.config_meta is not frame.config_meta
    assert cls.config_meta is fm.FrameMetadata
    ref = weakref.ref(frame)
    gc.disable()
    try:
        del frame
        assert ref() is None
    finally:
        gc.enable()
