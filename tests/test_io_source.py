import polars as pl
import pytest

from polars_bio._io_source import register_io_source

SCHEMA = {"a": pl.Int64}


def _lazy(source):
    return register_io_source(source, schema=SCHEMA)


def test_source_batches_pass_through():
    def source(with_columns, predicate, n_rows, batch_size):
        yield pl.DataFrame({"a": [1, 2]})
        yield pl.DataFrame({"a": [3]})

    assert _lazy(source).collect()["a"].to_list() == [1, 2, 3]


def test_non_polars_error_becomes_compute_error():
    # DataFusion errors reach Python as plain Exception. Polars 1.x wrapped them in
    # ComputeError, polars 2.x does not; the wrapper keeps ComputeError on both.
    def source(with_columns, predicate, n_rows, batch_size):
        raise Exception("DataFusion error: Execution error: boom")
        yield  # pragma: no cover

    with pytest.raises(pl.exceptions.ComputeError, match="boom"):
        _lazy(source).collect()


def test_polars_error_becomes_compute_error():
    # Polars 1.x wraps its own errors raised by a source too; match that on 2.x.
    def source(with_columns, predicate, n_rows, batch_size):
        raise pl.exceptions.SchemaError("bad schema")
        yield  # pragma: no cover

    with pytest.raises(pl.exceptions.ComputeError, match="SchemaError: bad schema"):
        _lazy(source).collect()


def test_compute_error_is_not_rewrapped():
    def source(with_columns, predicate, n_rows, batch_size):
        raise pl.exceptions.ComputeError("already a compute error")
        yield  # pragma: no cover

    with pytest.raises(pl.exceptions.ComputeError, match="already a compute error"):
        _lazy(source).collect()


def test_error_after_first_batch_is_wrapped():
    def source(with_columns, predicate, n_rows, batch_size):
        yield pl.DataFrame({"a": [1]})
        raise ValueError("late failure")

    with pytest.raises(pl.exceptions.ComputeError, match="late failure"):
        _lazy(source).collect()
