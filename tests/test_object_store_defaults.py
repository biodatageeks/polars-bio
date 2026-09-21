"""Object-store option defaults must be uniform across every I/O entry point.

The value matters for throughput: ``concurrent_fetches=8`` enables parallel
ranged requests from S3/GCS/HTTP (issue #459 follow-up). Setting it to ``1``
disables parallel fetching, with one sequential request on S3; HTTP/GCS may
still use chunks and HEAD. The SQL ``register_*``
functions already defaulted to 8; ``read_*``/``scan_*``/``describe_*`` did not.
"""

import inspect

import pytest

import polars_bio as pb
from polars_bio.io import (
    IOOperations,
    _extract_py_object_storage_options,
    _object_storage_options_store,
)
from polars_bio.polars_bio import PyObjectStorageOptions, ReadOptions
from polars_bio.sql import SQL


def _public_functions_with(param: str):
    seen = set()
    for owner in (IOOperations, SQL):
        for name, member in inspect.getmembers(owner):
            if name.startswith("_") or not callable(member):
                continue
            try:
                sig = inspect.signature(member)
            except (TypeError, ValueError):
                continue
            if param in sig.parameters and name not in seen:
                seen.add(name)
                yield name, sig


CONCURRENT = sorted(dict(_public_functions_with("concurrent_fetches")).items())


def test_every_object_store_entry_point_is_covered():
    # Guard against the parametrised tests silently shrinking to nothing.
    names = {name for name, _ in CONCURRENT}
    assert {
        "read_vcf",
        "scan_vcf",
        "scan_fasta",
        "register_fasta",
        "register_vcf",
    } <= names
    assert len(names) >= 30


@pytest.mark.parametrize("name,sig", CONCURRENT, ids=[n for n, _ in CONCURRENT])
def test_concurrent_fetches_defaults_to_parallel_reads(name, sig):
    assert sig.parameters["concurrent_fetches"].default == 8, name


def test_module_level_aliases_share_the_defaults():
    assert inspect.signature(pb.scan_vcf).parameters["concurrent_fetches"].default == 8
    assert (
        inspect.signature(pb.register_vcf).parameters["concurrent_fetches"].default == 8
    )


@pytest.mark.parametrize("read_options", [None, ReadOptions()])
def test_missing_stored_options_use_parallel_default(monkeypatch, read_options):
    monkeypatch.delitem(_object_storage_options_store, id(read_options), raising=False)

    options = _extract_py_object_storage_options(read_options)

    assert options.concurrent_fetches == 8
    assert options.chunk_size == 8


def test_stored_sequential_options_are_preserved(monkeypatch):
    read_options = ReadOptions()
    options = PyObjectStorageOptions(
        allow_anonymous=False,
        enable_request_payer=True,
        compression_type="auto",
        concurrent_fetches=1,
        chunk_size=16,
    )
    monkeypatch.setitem(_object_storage_options_store, id(read_options), options)

    assert _extract_py_object_storage_options(read_options) is options
