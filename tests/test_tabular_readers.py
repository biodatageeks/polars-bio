"""pb.scan_csv / scan_parquet / read_csv / read_parquet (issue #486)."""

from __future__ import annotations

import polars as pl
import pytest

import polars_bio as pb
from polars_bio._metadata import get_coordinate_system, get_metadata


@pytest.fixture
def files(tmp_path):
    bed = pl.DataFrame(
        {
            "chrom": ["1", "1"],
            "start": [101, 300],
            "end": [200, 400],
            "name": ["a", "b"],
        }
    )
    bed.write_csv(tmp_path / "bed.csv")
    bed.write_parquet(tmp_path / "bed.parquet")
    pl.DataFrame({"contig": ["1"], "pos": [101], "ref": ["A"]}).write_parquet(
        tmp_path / "calls.parquet"
    )
    pl.DataFrame(
        {"chrom": ["1"], "chromStart": [101], "chromEnd": [200], "start": [0]}
    ).write_csv(tmp_path / "clash.csv")
    pl.DataFrame({"chrom": ["1"], "start": ["1,000"], "end": [2000]}).write_csv(
        tmp_path / "text_start.csv"
    )
    for i in range(2):
        bed.write_parquet(tmp_path / f"part{i}.parquet")
    return tmp_path


READERS = [
    ("scan_csv", "bed.csv"),
    ("read_csv", "bed.csv"),
    ("scan_parquet", "bed.parquet"),
    ("read_parquet", "bed.parquet"),
]


def _rows(frame):
    return (frame.collect() if isinstance(frame, pl.LazyFrame) else frame).sort("start")


@pytest.mark.parametrize("reader, name", READERS)
def test_zero_based_is_required(files, reader, name):
    with pytest.raises(TypeError, match="zero_based"):
        getattr(pb, reader)(str(files / name))


@pytest.mark.parametrize("reader, name", READERS)
@pytest.mark.parametrize("bad", ["yes", 1, None])
def test_zero_based_must_be_bool(files, reader, name, bad):
    with pytest.raises(TypeError, match="zero_based must be True"):
        getattr(pb, reader)(str(files / name), zero_based=bad)


@pytest.mark.parametrize("reader, name", READERS)
@pytest.mark.parametrize("zero_based", [True, False])
def test_declares_and_tags_without_conversion(files, reader, name, zero_based):
    frame = getattr(pb, reader)(str(files / name), zero_based=zero_based)
    assert isinstance(
        frame, pl.LazyFrame if reader.startswith("scan") else pl.DataFrame
    )
    assert get_coordinate_system(frame) is zero_based
    assert _rows(frame)["start"].to_list() == [101, 300]
    meta = get_metadata(frame)
    assert meta["format"] == name.split(".")[1]
    assert meta["path"] == str(files / name)


@pytest.mark.parametrize(
    "zero_based, use_zero_based, starts",
    [
        (True, False, [102, 301]),
        (False, True, [100, 299]),
        (True, True, [101, 300]),
        (False, False, [101, 300]),
    ],
)
def test_conversion(files, zero_based, use_zero_based, starts):
    lf = pb.scan_csv(
        str(files / "bed.csv"), zero_based=zero_based, use_zero_based=use_zero_based
    )
    rows = _rows(lf)
    assert rows["start"].to_list() == starts
    assert rows["end"].to_list() == [200, 400]
    assert get_coordinate_system(lf) is use_zero_based


def test_use_zero_based_must_be_bool_or_none(files):
    with pytest.raises(TypeError, match="use_zero_based must be True"):
        pb.scan_csv(str(files / "bed.csv"), zero_based=True, use_zero_based="no")


def test_rename_and_point_data(files):
    lf = pb.scan_parquet(
        str(files / "calls.parquet"),
        zero_based=False,
        chrom="contig",
        start="pos",
        end=None,
    )
    rows = lf.collect()
    assert rows.columns == ["chrom", "start", "ref", "end"]
    assert rows.row(0) == ("1", 101, "A", 101)
    assert get_coordinate_system(lf) is False


@pytest.mark.parametrize(
    "zero_based, use_zero_based, expected",
    [(True, None, (101, 102)), (False, True, (100, 101))],
)
def test_point_end_derived_in_source_system(
    files, zero_based, use_zero_based, expected
):
    rows = pb.read_parquet(
        str(files / "calls.parquet"),
        zero_based=zero_based,
        use_zero_based=use_zero_based,
        chrom="contig",
        start="pos",
        end=None,
    )
    assert (rows["start"][0], rows["end"][0]) == expected


def test_missing_default_end_suggests_end_none(files):
    with pytest.raises(ValueError, match="pass end=None"):
        pb.scan_parquet(
            str(files / "calls.parquet"), zero_based=False, chrom="contig", start="pos"
        )


def test_missing_named_column_lists_available(files):
    with pytest.raises(ValueError, match=r"\['nope'\] not found.*Available columns"):
        pb.scan_csv(str(files / "bed.csv"), zero_based=True, chrom="nope")


def test_rename_clash(files):
    with pytest.raises(
        ValueError, match=r"Cannot rename interval columns to \['start'\]"
    ):
        pb.scan_csv(
            str(files / "clash.csv"),
            zero_based=True,
            start="chromStart",
            end="chromEnd",
        )


def test_end_none_with_existing_end_column(files):
    with pytest.raises(ValueError, match="already has one"):
        pb.scan_csv(str(files / "bed.csv"), zero_based=True, end=None)


def test_polars_kwargs_pass_through(files):
    assert len(pb.read_csv(str(files / "bed.csv"), zero_based=True, n_rows=1)) == 1
    lf = pb.scan_parquet(str(files / "bed.parquet"), zero_based=True, n_rows=1)
    assert len(lf.collect()) == 1


def test_conversion_preserves_dtype(files):
    path = files / "int32.parquet"
    pl.DataFrame(
        {"chrom": ["1"], "start": [101], "end": [200]},
        schema={"chrom": pl.Utf8, "start": pl.Int32, "end": pl.Int32},
    ).write_parquet(path)
    rows = pb.read_parquet(str(path), zero_based=True, use_zero_based=False)
    assert rows.schema["start"] == pl.Int32
    assert rows["start"][0] == 102


def test_unsigned_underflow_raises(files):
    path = files / "uint.parquet"
    pl.DataFrame(
        {"chrom": ["1"], "start": [0], "end": [5]},
        schema={"chrom": pl.Utf8, "start": pl.UInt32, "end": pl.UInt32},
    ).write_parquet(path)
    with pytest.raises(pl.exceptions.InvalidOperationError):
        pb.read_parquet(str(path), zero_based=False, use_zero_based=True)


def test_non_integer_start_rejected(files):
    with pytest.raises(TypeError, match="'start' must be an integer column"):
        pb.scan_csv(str(files / "text_start.csv"), zero_based=True)


def test_parquet_glob_and_list_sources(files):
    glob_lf = pb.scan_parquet(str(files / "part*.parquet"), zero_based=True)
    list_lf = pb.scan_parquet(
        [str(files / "part0.parquet"), str(files / "part1.parquet")], zero_based=True
    )
    for lf in (glob_lf, list_lf):
        assert len(lf.collect()) == 4
        assert get_coordinate_system(lf) is True


def test_read_keeps_tag_after_collect(files):
    df = pb.read_csv(str(files / "bed.csv"), zero_based=True)
    assert get_coordinate_system(df) is True
    assert get_coordinate_system(df.lazy()) is True
