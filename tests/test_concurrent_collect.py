"""
Concurrent use of one file-backed scan must not race on its catalog table.

Every collect() (and every GFF/GTF attribute select()) registers its provider
under a private lease name, so two callbacks can never deregister or replace
each other's table between registration and lookup.
"""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from threading import Lock

import polars as pl
import pytest

import polars_bio as pb

DATA_DIR = Path(__file__).parent / "data" / "io"

SCANS = {
    "vcf": lambda: pb.scan_vcf(str(DATA_DIR / "vcf" / "ensembl.vcf")),
    "bam": lambda: pb.scan_bam(str(DATA_DIR / "bam" / "NA12878_10k.bam")),
    "bed": lambda: pb.scan_bed(str(DATA_DIR / "bed" / "test.bed")),
    "fastq": lambda: pb.scan_fastq(str(DATA_DIR / "fastq" / "example.fastq")),
    "gff": lambda: pb.scan_gff(
        str(DATA_DIR / "gff" / "gencode.v38.annotation.gff3")
    ).lazy(),
    "gtf": lambda: pb.scan_gtf(str(DATA_DIR / "gtf" / "test.gtf")).lazy(),
}

ATTRIBUTE_SCANS = {
    "gff": (
        lambda: pb.scan_gff(str(DATA_DIR / "gff" / "gencode.v38.annotation.gff3")),
        "gene_name",
    ),
    "gtf": (lambda: pb.scan_gtf(str(DATA_DIR / "gtf" / "test.gtf")), "gene_id"),
}


def _catalog_tables(prefix: str) -> set[str]:
    tables = pb.sql("SELECT table_name FROM information_schema.tables").collect()
    return {name for name in tables["table_name"].to_list() if name.startswith(prefix)}


@pytest.fixture
def lease_names(monkeypatch):
    import polars_bio.io as io_module

    original_lease = io_module._registered_table_lease
    names = []
    names_lock = Lock()

    @contextmanager
    def recording_lease(context, path, name, input_format, read_options):
        with names_lock:
            names.append(name)
        with original_lease(context, path, name, input_format, read_options) as table:
            yield table

    monkeypatch.setattr(io_module, "_registered_table_lease", recording_lease)
    return names


@pytest.mark.parametrize("fmt", SCANS)
def test_same_scan_concurrent_collects_use_unique_leases(fmt, lease_names):
    query = SCANS[fmt]().select(pl.len().alias("n"))
    expected = query.collect().item()
    lease_names.clear()
    tables_before = _catalog_tables("_pb_")

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: query.collect().item(), range(8)))

    assert results == [expected] * 8
    assert len(lease_names) == 8
    assert len(set(lease_names)) == len(lease_names)
    assert _catalog_tables("_pb_") == tables_before


@pytest.mark.parametrize("fmt", SCANS)
def test_self_join_on_one_scan(fmt):
    scan = SCANS[fmt]()
    n = scan.select(pl.len()).collect().item()
    joined = scan.select(pl.len().alias("a")).join(
        scan.select(pl.len().alias("b")), how="cross"
    )
    for _ in range(10):
        assert joined.collect().row(0) == (n, n)


@pytest.mark.parametrize("fmt", ATTRIBUTE_SCANS)
def test_concurrent_attribute_selects_use_unique_leases(fmt, lease_names):
    make_scan, attribute = ATTRIBUTE_SCANS[fmt]
    scan = make_scan()
    expected = scan.select([attribute]).collect()
    lease_names.clear()
    tables_before = _catalog_tables("_pb_")

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(
            executor.map(lambda _: scan.select([attribute]).collect(), range(8))
        )

    assert all(result.equals(expected) for result in results)
    select_leases = [
        name for name in lease_names if name.startswith(f"_pb_{fmt}_proj_")
    ]
    assert len(select_leases) == 8
    assert len(set(select_leases)) == len(select_leases)
    assert _catalog_tables("_pb_") == tables_before
