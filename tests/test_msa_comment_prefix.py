"""Custom full-line comments must not become alignment sequence data."""

import gzip
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import polars as pl
import pytest

import polars_bio as pb
import polars_bio.io as io_module

ALIGNMENT = (
    "#A3M#\r\n"
    "; Downloaded from OpenProteinSet\r\n"
    ">query description;kept\r\n"
    "ACd\r\n"
    "; comment inside a wrapped sequence\r\n"
    "e.-\r\n"
    "; comment between records\r\n"
    ">hit\r\n"
    "A--\r\n"
    "; trailing comment without newline"
)
EXPECTED = [("query", "description;kept", "ACde.-"), ("hit", None, "A--")]


def write_alignment(tmp_path, fmt, compression, text=ALIGNMENT):
    path = tmp_path / f"alignment.{fmt}{compression}"
    data = text.encode()
    if compression == ".gz":
        path.write_bytes(gzip.compress(data))
    elif compression == ".bgz":
        pysam = pytest.importorskip("pysam")
        with pysam.BGZFile(str(path), "wb") as output:
            output.write(data)
    else:
        path.write_bytes(data)
    return str(path)


@pytest.mark.parametrize("fmt", ["a2m", "a3m"])
@pytest.mark.parametrize("compression", ["", ".gz", ".bgz"])
@pytest.mark.parametrize("api", ["read", "scan", "register"])
def test_comment_prefix_across_apis_and_compression(tmp_path, fmt, compression, api):
    path = write_alignment(tmp_path, fmt, compression)
    method = getattr(pb, f"{api}_{fmt}")
    if api == "register":
        name = f"comments_{fmt}_{compression.lstrip('.') or 'plain'}"
        method(path, name, comment_prefix=";")
        result = pb.sql(f"SELECT * FROM {name}").collect()
        assert pb.sql(f"SELECT count(*) AS n FROM {name}").collect().item() == 2
    else:
        result = method(path, comment_prefix=";")
        if api == "scan":
            assert result.select("name").limit(1).collect().rows() == [("query",)]
            assert result.select(pl.len()).collect().item() == 2
            assert (
                result.filter(pl.col("name") == "hit").collect().rows() == EXPECTED[1:]
            )
            result = result.collect()
    assert result.rows() == EXPECTED


@pytest.mark.parametrize("fmt", ["a2m", "a3m"])
@pytest.mark.parametrize("prefix", [";;", "注:"])
def test_comment_prefix_is_literal_and_not_an_inline_delimiter(tmp_path, fmt, prefix):
    path = write_alignment(
        tmp_path,
        fmt,
        "",
        f"{prefix} provenance\n>query\nAC{prefix}GT\n {prefix}kept\n{prefix}removed\n",
    )
    result = getattr(pb, f"scan_{fmt}")(path, comment_prefix=prefix).collect()
    assert result["sequence"].to_list() == [f"AC{prefix}GT {prefix}kept"]


@pytest.mark.parametrize("fmt", ["a2m", "a3m"])
def test_default_keeps_sequence_lines_and_leading_hash_headers(tmp_path, fmt):
    path = write_alignment(tmp_path, fmt, "", "#A3M#\n>query\nAC\n;kept\n#kept\n")
    scan = getattr(pb, f"scan_{fmt}")
    assert scan(path).collect()["sequence"].to_list() == ["AC;kept#kept"]
    assert scan(path, comment_prefix=None).collect().equals(scan(path).collect())
    assert scan(path, comment_prefix="#").collect()["sequence"].to_list() == ["AC;kept"]


@pytest.mark.parametrize("fmt", ["a2m", "a3m"])
def test_comment_only_file_is_empty(tmp_path, fmt):
    path = write_alignment(tmp_path, fmt, "", "; first\n; last")
    scan = getattr(pb, f"scan_{fmt}")(path, comment_prefix=";")
    assert scan.collect().height == 0
    assert scan.select(pl.len()).collect().item() == 0


@pytest.mark.parametrize("api", ["read", "scan", "register"])
@pytest.mark.parametrize("fmt", ["a2m", "a3m"])
@pytest.mark.parametrize("prefix", ["", "\n", ";\r"])
def test_invalid_comment_prefix_is_rejected(tmp_path, api, fmt, prefix):
    path = write_alignment(tmp_path, fmt, "", ">query\nAC\n")
    with pytest.raises(
        ValueError, match="comment_prefix must be non-empty and contain no line breaks"
    ):
        getattr(pb, f"{api}_{fmt}")(path, comment_prefix=prefix)


@pytest.mark.parametrize("fmt", ["a2m", "a3m"])
def test_comment_prefix_is_scoped_to_each_scan(tmp_path, monkeypatch, fmt):
    path = write_alignment(tmp_path, fmt, "", ">query\nAC\n;semicolons\n#hashes\n")
    scan = getattr(pb, f"scan_{fmt}")
    scans = [scan(path, comment_prefix=";"), scan(path, comment_prefix="#")]
    original = io_module.py_register_table
    barrier = Barrier(2)

    def synchronized_registration(*args, **kwargs):
        table = original(*args, **kwargs)
        barrier.wait(timeout=10)
        return table

    with monkeypatch.context() as patch:
        patch.setattr(io_module, "py_register_table", synchronized_registration)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(lambda frame: frame.collect()["sequence"].to_list(), scans)
            )
    assert results == [["AC#hashes"], ["AC;semicolons"]]
    assert scans[0].collect()["sequence"].to_list() == ["AC#hashes"]
