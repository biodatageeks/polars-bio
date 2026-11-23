import gzip
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl
import pytest
from Bio import SeqIO

import polars_bio as pb

SAMPLE = Path(__file__).parent / "data/genbank/example.gbk"


def test_records_and_features():
    records = pb.read_genbank(SAMPLE)
    assert records.equals(pb.scan_genbank(SAMPLE).collect())
    with SAMPLE.open() as stream:
        expected = list(SeqIO.parse(stream, "genbank"))
    assert records["name"].to_list() == [r.name for r in expected]
    assert records["length"].to_list() == [len(r) for r in expected]
    record = records.row(0, named=True)
    assert record["sequence"] == str(expected[0].seq)
    assert record["definition"] == expected[0].description + "."
    assert record["taxonomy"] == expected[0].annotations["taxonomy"]
    assert records["sequence"][1] is None

    features = pb.read_genbank(SAMPLE, "features", use_zero_based=True)
    assert features["type"].to_list() == [f.type for f in expected[0].features]
    assert features["record_id"].to_list() == [record["name"]] * 5
    assert features["start"].to_list() == [1, None, 0, 10, None]
    assert features["end"].to_list() == [12, None, 12, 10, None]
    reverse = features.row(0, named=True)
    assert reverse["location"] == "complement(join(2..5,9..12))"
    assert reverse["parts"] == [
        {"start": 8, "end": 12, "strand": "-"},
        {"start": 1, "end": 5, "strand": "-"},
    ]
    bag = reverse["qualifiers"]
    if isinstance(bag, list):  # Polars 1.x exposes an Arrow map as key/value structs.
        bag = {item["key"]: item["value"] for item in bag}
    assert bag == {
        "gene": ["abc"],
        "note": ['has "quotes" and a /slash', "again"],
        "pseudo": [None],
        "empty": [""],
    }
    closed = pb.read_genbank(SAMPLE, "features", use_zero_based=False)
    assert closed["start"].to_list() == [2, None, 1, None, None]
    assert closed.row(1, named=True)["parts"] == [
        {"start": 18, "end": 20, "strand": "+"},
        {"start": 1, "end": 3, "strand": "+"},
    ]


def test_promotion_residuals_and_sql():
    lf = pb.scan_genbank(SAMPLE, "features")
    out = (
        lf.filter(pl.col("type") == "CDS").select("translation", "anticodon").collect()
    )
    assert out.rows() == [("MAAA", "(pos:1..3, aa:Met)")]
    assert lf.filter(pl.col("translation").str.starts_with("MA")).limit(1).collect()[
        "type"
    ].to_list() == ["CDS"]
    assert lf.select(pl.col("gene").alias("g")).collect()["g"].to_list() == [
        "abc",
        None,
        None,
        None,
        None,
    ]
    promoted = pb.scan_genbank(
        SAMPLE, "features", qualifier_fields=["note", "qualifiers"]
    )
    assert (
        promoted.select("note", "qualifiers").collect()["note"][0]
        == 'has "quotes" and a /slash'
    )
    pb.register_genbank(
        SAMPLE, "gb_feat_test", view="features", qualifier_fields=["translation"]
    )
    assert pb.sql(
        "select translation from gb_feat_test where type='CDS'"
    ).collect().rows() == [("MAAA",)]
    assert pb.sql("select count(*) from gb_feat_test").collect().item() == 5
    assert lf.select(pl.len()).collect().item() == 5


@pytest.mark.parametrize("compression", ["plain", "gzip", "bgzf", "indexed_bgzf"])
def test_multirecord_compression(tmp_path, compression):
    text = SAMPLE.read_bytes().replace(b"\n", b"\r\n").rstrip(b"\r\n")
    file = tmp_path / ("data.gb" if compression == "plain" else "data.gb.gz")
    if compression == "gzip":
        half = len(text) // 2
        file.write_bytes(gzip.compress(text[:half]) + gzip.compress(text[half:]))
    elif compression in {"bgzf", "indexed_bgzf"}:
        from Bio import bgzf

        with bgzf.BgzfWriter(file, "wb") as stream:
            for line in text.splitlines(keepends=True):
                if line.startswith(b"//"):
                    stream.write(b"/")
                    stream.flush()
                    line = line[1:]
                stream.write(line)
                stream.flush()
        if compression == "indexed_bgzf":
            import struct

            data = file.read_bytes()
            pos = logical = 0
            offsets = []
            while pos < len(data):
                size = int.from_bytes(data[pos + 16 : pos + 18], "little") + 1
                length = int.from_bytes(data[pos + size - 4 : pos + size], "little")
                if pos and length:
                    offsets.append((pos, logical))
                pos += size
                logical += length
            Path(f"{file}.gzi").write_bytes(
                struct.pack("<Q", len(offsets))
                + b"".join(struct.pack("<QQ", *pair) for pair in offsets)
            )
    else:
        file.write_bytes(text)
    key = "datafusion.execution.target_partitions"
    old = pb.get_option(key)
    try:
        pb.set_option(key, "3")
        for view in ["features", "records"]:
            order = ["type", "location"] if view == "features" else ["name"]
            assert (
                pb.read_genbank(file, view)
                .sort(order)
                .equals(pb.read_genbank(SAMPLE, view).sort(order))
            )
    finally:
        pb.set_option(key, old)


def test_concurrent_views_and_predicates():
    views = [
        pb.scan_genbank(SAMPLE).select("name"),
        pb.scan_genbank(SAMPLE, "features").select("protein_id"),
        pb.scan_genbank(SAMPLE, "features", qualifier_fields=["gene"]).select("gene"),
    ]
    with ThreadPoolExecutor(6) as pool:
        output = list(pool.map(lambda frame: frame.collect(), views * 2))
    assert [r.columns[0] for r in output] == ["name", "protein_id", "gene"] * 2
    assert output[0].height == 2 and output[1].height == 5

    def filtered(enabled):
        return (
            pb.scan_genbank(
                SAMPLE,
                "features",
                qualifier_fields=["translation"],
                predicate_pushdown=enabled,
                projection_pushdown=enabled,
            )
            .filter(
                (pl.col("type") == "CDS") & pl.col("translation").str.contains("AA")
            )
            .select("location")
            .limit(1)
            .collect()
        )

    assert filtered(True).equals(filtered(False))


def test_validation_and_empty_input(tmp_path):
    for options in [
        dict(view="other"),
        dict(qualifier_fields=["gene"]),
        dict(view="features", qualifier_fields=["type"]),
        dict(view="features", qualifier_fields=["gene", "gene"]),
    ]:
        with pytest.raises(ValueError):
            pb.scan_genbank(SAMPLE, **options)
    p = tmp_path / "invalid.gb"
    p.write_text("")
    assert pb.read_genbank(p, "features").shape == (0, 8)
    p.write_bytes(SAMPLE.read_bytes().rstrip()[:-2])
    with pytest.raises(Exception, match=r"TEST_B.*missing //"):
        pb.read_genbank(p)
    p.write_text(
        "LOCUS       BAD 10 bp DNA\nFEATURES             Location/Qualifiers\n"
        '     gene            1..2\n                     /note="open\n//\n'
    )
    with pytest.raises(Exception, match="unterminated qualifier"):
        pb.read_genbank(p, "features")


def test_feature_metadata_and_session_defaults():
    from polars_bio._frame_metadata import get_metadata

    key = pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED
    original = pb.get_option(key)
    try:
        for zero in [False, True]:
            pb.set_option(key, zero)
            frame = pb.scan_genbank(SAMPLE, "features")
            assert get_metadata(frame)["coordinate_system_zero_based"] is zero
            result = frame.select("start").collect()
            assert result["start"][0] == 2 - int(zero)
            assert (
                get_metadata(pb.read_genbank(SAMPLE, "features"))[
                    "coordinate_system_zero_based"
                ]
                is zero
            )
    finally:
        pb.set_option(key, original)


def test_limit_stops_before_incomplete_record(tmp_path):
    path = tmp_path / "truncated.gb"
    path.write_bytes(SAMPLE.read_bytes().rstrip()[:-2])
    frame = pb.scan_genbank(path)
    assert frame.select("name").head(1).collect().item() == "TEST_A"
    with pytest.raises(pl.exceptions.ComputeError, match=r"TEST_B.*missing //"):
        frame.collect()


def test_explicit_compression_and_large_record(tmp_path):
    p = tmp_path / "assembly.data"
    # Each split probe inside the large first record reaches the same terminator.
    text = SAMPLE.read_bytes().replace(
        b'                     /note="again"',
        b'                     /note="' + b"x" * 200_000 + b'"',
    )
    p.write_bytes(gzip.compress(text))
    expected = (
        pb.scan_genbank(p, "features", compression_type="gz")
        .select("type", "start")
        .collect()
    )
    p.write_bytes(text)
    key = "datafusion.execution.target_partitions"
    previous = pb.get_option(key)
    try:
        pb.set_option(key, "8")
        assert (
            pb.scan_genbank(p, "features")
            .select("type", "start")
            .collect()
            .equals(expected)
        )
    finally:
        pb.set_option(key, previous)
