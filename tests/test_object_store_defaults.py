"""Object-store option defaults must be uniform across every I/O entry point.

The value matters for throughput: with ``concurrent_fetches=1`` a whole-object
read from S3/GCS/HTTP is one sequential request, while ``8`` splits it into
parallel ranged requests (issue #459 follow-up). The SQL ``register_*``
functions already defaulted to 8; ``read_*``/``scan_*``/``describe_*`` did not.
"""

import inspect

import pytest

import polars_bio as pb
from polars_bio.io import IOOperations
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
