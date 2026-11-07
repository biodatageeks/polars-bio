"""Isolated feasibility probe for metadata on ordinary GroupBy chains.

Run in a fresh interpreter without importing polars-bio/polars-config-meta.
This is not a production provider: it installs the ten proposed grouping
wrappers and one collect bridge, without namespace/installation/conflict logic.
"""

import inspect
import json
import platform
from functools import wraps

import polars as pl
from polars.dataframe.group_by import GroupBy
from polars.lazyframe.group_by import LazyGroupBy

FRAME_META = "_pb_probe_frame_metadata"
GROUP_META = "_pb_probe_group_metadata"
METADATA = {
    "coordinate_system_zero_based": True,
    "source_format": "vcf",
    "source_path": "example.vcf",
    "source_header": '{"samples":["S1"],"layout":{"format":"GT"}}',
}
GROUP_PATCHES = [(cls, "group_by") for cls in (pl.DataFrame, pl.LazyFrame)] + [
    (cls, method)
    for cls in (GroupBy, LazyGroupBy)
    for method in ("agg", "head", "tail", "having")
]


def carrying(original, source_key, destination_key):
    @wraps(original)
    def call(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        metadata = getattr(self, source_key, None)
        if metadata and result is not self:
            setattr(result, destination_key, metadata.copy())
        return result

    return call


def main():
    originals = {
        (cls, name): getattr(cls, name)
        for cls, name in GROUP_PATCHES + [(pl.LazyFrame, "collect")]
    }
    operations = {
        "agg": lambda group: group.agg(pl.col("start").min(), pl.col("end").max()),
        "len": lambda group: group.len(name="n"),
        "head": lambda group: group.head(1),
        "tail": lambda group: group.tail(1),
        "having_agg": lambda group: group.having(pl.len() > 1).agg(
            pl.col("start").min(), pl.col("end").max()
        ),
    }
    rows = {
        "chrom": ["chr1", "chr1", "chr2"],
        "start": [10, 30, 50],
        "end": [20, 40, 60],
    }
    expected = {}
    for cls in (pl.DataFrame, pl.LazyFrame):
        for name, operation in operations.items():
            result = operation(cls(rows).group_by("chrom", maintain_order=True))
            if isinstance(result, pl.LazyFrame):
                result = result.collect()
            expected[cls.__name__, name] = result.to_dicts()

    def invalid_aggregation(frame):
        try:
            result = frame.group_by("chrom").agg(pl.col("missing").min())
            if isinstance(result, pl.LazyFrame):
                result.collect()
        except Exception as error:
            return type(error).__name__, str(error)
        raise AssertionError("Expected an invalid aggregation to fail")

    errors = {
        cls.__name__: invalid_aggregation(cls(rows))
        for cls in (pl.DataFrame, pl.LazyFrame)
    }
    lazy_methods = ("collect", "collect_schema")
    lazy_calls = dict.fromkeys(lazy_methods, 0)

    def counting(original, name):
        @wraps(original)
        def call(self, *args, **kwargs):
            lazy_calls[name] += 1
            return original(self, *args, **kwargs)

        return call

    for cls, name in GROUP_PATCHES:
        original = originals[cls, name]
        source_key = FRAME_META if name == "group_by" else GROUP_META
        destination_key = GROUP_META if name in ("group_by", "having") else FRAME_META
        setattr(cls, name, carrying(original, source_key, destination_key))
    pl.LazyFrame.collect = carrying(pl.LazyFrame.collect, FRAME_META, FRAME_META)
    for name in lazy_methods:
        setattr(pl.LazyFrame, name, counting(getattr(pl.LazyFrame, name), name))

    report = []
    for cls in (pl.DataFrame, pl.LazyFrame):
        for tagged in (False, True):
            for name, operation in operations.items():
                frame = cls(rows)
                if tagged:
                    setattr(frame, FRAME_META, METADATA.copy())
                before = lazy_calls.copy()
                group = frame.group_by("chrom", maintain_order=True)
                assert type(group) is (GroupBy if cls is pl.DataFrame else LazyGroupBy)
                if tagged:
                    snapshot = getattr(group, GROUP_META)
                    assert snapshot == METADATA
                    assert snapshot is not getattr(frame, FRAME_META)
                    getattr(frame, FRAME_META)["coordinate_system_zero_based"] = False
                result = operation(group)
                assert type(result) is cls
                if cls is pl.LazyFrame:
                    assert lazy_calls == before, "Grouping collected/resolved lazy data"
                if tagged:
                    assert getattr(result, FRAME_META) == METADATA
                    assert getattr(result, FRAME_META) is not snapshot
                else:
                    assert not hasattr(group, GROUP_META)
                    assert not hasattr(result, FRAME_META)
                materialized = (
                    result.collect(engine="streaming")
                    if cls is pl.LazyFrame
                    else result
                )
                assert materialized.to_dicts() == expected[cls.__name__, name]
                if tagged:
                    assert getattr(materialized, FRAME_META) == METADATA
                report.append(
                    {
                        "frame": cls.__name__,
                        "tagged": tagged,
                        "operation": name,
                        "result": "passed",
                    }
                )

        frame = cls(rows)
        setattr(frame, FRAME_META, METADATA.copy())
        assert invalid_aggregation(frame) == errors[cls.__name__]
        group = frame.group_by("chrom")
        filtered = group.having(pl.len() > 1)
        assert (filtered is group) is (cls is pl.LazyFrame)
        assert getattr(filtered, GROUP_META) == METADATA
        if filtered is not group:
            assert getattr(filtered, GROUP_META) is not getattr(group, GROUP_META)

    assert all(
        inspect.signature(getattr(cls, name)) == inspect.signature(original)
        for (cls, name), original in originals.items()
    )
    print(
        json.dumps(
            {
                "python": platform.python_version(),
                "polars": pl.__version__,
                "group_wrappers": len(GROUP_PATCHES),
                "collect_bridge": True,
                "results": report,
                "snapshot_isolation": "passed",
                "having_identity": "passed",
                "original_errors": "passed",
                "signatures": "passed",
                "lazy_execution": "passed",
                "limitations": "Feasibility only; no application integration, namespace ownership, or complete patch manifest tested.",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
