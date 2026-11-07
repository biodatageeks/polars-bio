"""Feasibility probe only: normal Polars syntax without replacing base methods.

This deliberately implements a few methods to expose the boundaries of subclass
propagation. It is not the proposed production provider or a complete API.
Run in a fresh process without importing polars-bio/polars-config-meta.
"""

import inspect
import json
from functools import wraps

import polars as pl


def adopt(frame, metadata):
    if isinstance(frame, pl.LazyFrame):
        result = BioLazyFrame._from_pyldf(frame._ldf)
    elif isinstance(frame, pl.DataFrame):
        result = BioDataFrame._from_pydf(frame._df)
    else:
        return frame
    result._bio_metadata = dict(metadata)
    return result


def preserving(method):
    @wraps(method)
    def call(self, *args, **kwargs):
        return adopt(method(self, *args, **kwargs), getattr(self, "_bio_metadata", {}))

    return call


class BioLazyFrame(pl.LazyFrame):
    filter = preserving(pl.LazyFrame.filter)
    select = preserving(pl.LazyFrame.select)
    with_columns = preserving(pl.LazyFrame.with_columns)
    collect = preserving(pl.LazyFrame.collect)


class BioDataFrame(pl.DataFrame):
    lazy = preserving(pl.DataFrame.lazy)
    filter = preserving(pl.DataFrame.filter)
    select = preserving(pl.DataFrame.select)


def main():
    originals = {
        (cls, name): getattr(cls, name)
        for cls, name in [
            (pl.LazyFrame, "filter"),
            (pl.LazyFrame, "select"),
            (pl.LazyFrame, "collect"),
            (pl.DataFrame, "lazy"),
        ]
    }
    metadata = {
        "coordinate_system_zero_based": True,
        "source_header": '{"samples":["S1"]}',
    }
    native = pl.LazyFrame({"chrom": ["chr1", "chr2"], "start": [0, 10], "end": [5, 15]})
    source = adopt(native, metadata)
    query = (
        source.filter(pl.col("start") > 0)
        .with_columns(length=pl.col("end") - pl.col("start"))
        .select("chrom", "start", "end", "length")
    )
    result = query.collect(engine="streaming")
    eager = result.select("chrom", "start", "end").filter(pl.col("start") > 0)
    batches = query.collect_batches(lazy=True, engine="streaming")
    boundaries = {
        "concat": pl.concat([query, query]),
        "group_by_agg": query.group_by("chrom").agg(pl.col("start").min()),
        "collect_all": pl.collect_all([query])[0],
        "unoverridden_sort": query.sort("start"),
    }
    assert (
        query._bio_metadata == result._bio_metadata == eager._bio_metadata == metadata
    )
    assert result.to_dicts() == [{"chrom": "chr2", "start": 10, "end": 15, "length": 5}]
    assert isinstance(query, pl.LazyFrame) and isinstance(result, pl.DataFrame)
    assert all(
        getattr(cls, name) is method for (cls, name), method in originals.items()
    )
    assert source._ldf is native._ldf
    assert hasattr(batches._inner, "__arrow_c_stream__")
    print(
        json.dumps(
            {
                "polars": pl.__version__,
                "normal_chain": "passed",
                "eager_chain": "passed",
                "is_lazyframe": isinstance(query, pl.LazyFrame),
                "exact_lazyframe_type": type(query) is pl.LazyFrame,
                "same_native_plan_on_adoption": source._ldf is native._ldf,
                "base_methods_unchanged": True,
                "signature_preserved": inspect.signature(BioLazyFrame.filter)
                == inspect.signature(pl.LazyFrame.filter),
                "native_stream_retained": True,
                "boundaries": {
                    name: {
                        "type": type(obj).__name__,
                        "metadata": getattr(obj, "_bio_metadata", None),
                    }
                    for name, obj in boundaries.items()
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
