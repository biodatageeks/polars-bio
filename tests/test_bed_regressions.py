"""Public reader regressions for BED3 row loss and swallowed errors (#456)."""

import gzip

import polars as pl
import pysam
import pytest

import polars_bio as pb

ROWS = [("chr1", 0, 5), ("chr1", 4, 8), ("chr1", 21, 29)]


@pytest.fixture(params=["plain", "gzip", "bgzf"])
def bed_file(tmp_path, request):
    def write(content):
        path = tmp_path / "records.bed"
        path.write_bytes(content)
        if request.param == "gzip":
            path = tmp_path / "records.bed.gz"
            path.write_bytes(gzip.compress(content))
        elif request.param == "bgzf":
            compressed = tmp_path / "records.bed.bgz"
            pysam.tabix_compress(str(path), str(compressed), force=True)
            path = compressed
        return str(path)

    return write


@pytest.fixture(params=["read", "scan", "sql"])
def read_bed(request):
    def collect(path):
        if request.param == "read":
            return pb.read_bed(path, use_zero_based=True)
        if request.param == "scan":
            return pb.scan_bed(path, use_zero_based=True).collect()
        pb.register_bed(path, "bed_regression")
        return pb.sql("SELECT * FROM bed_regression").collect()

    return collect


@pytest.mark.parametrize("width", range(3, 13), ids=lambda width: f"BED{width}")
def test_bed3_to_bed12_preserve_rows(bed_file, read_bed, width):
    lines = []
    for index, (chrom, start, end) in enumerate(ROWS, 1):
        fields = [
            chrom,
            start,
            end,
            f"r{index}",
            0,
            "+",
            start,
            end,
            0,
            1,
            end - start,
            0,
        ]
        lines.append("\t".join(map(str, fields[:width])))
    frame = read_bed(bed_file(("\n".join(lines) + "\n").encode()))
    assert frame.columns == ["chrom", "start", "end", "name"]
    assert frame.select("chrom", "start", "end").rows() == ROWS
    assert frame["name"].to_list() == ([None] * 3 if width == 3 else ["r1", "r2", "r3"])


@pytest.mark.parametrize("ending", ["\n", "\r\n", ""])
def test_bed3_line_endings_preserve_all_rows(bed_file, read_bed, ending):
    separator = ending or "\n"
    content = separator.join("\t".join(map(str, row)) for row in ROWS) + ending
    frame = read_bed(bed_file(content.encode()))
    assert frame.select("chrom", "start", "end").rows() == ROWS
    assert frame["name"].null_count() == 3


@pytest.mark.parametrize(
    "content",
    [
        b"chr1\t4\n",
        b"chr1\t0\t5\tr1\nchr1\t4\nchr1\t21\t29\tr3\n",
        b"chr1\t0\tbad\n",
        b"chr1\t0\t5\t\xff\n",
    ],
)
def test_malformed_records_raise_instead_of_disappearing(bed_file, read_bed, content):
    with pytest.raises(Exception, match="BED"):
        read_bed(bed_file(content))


@pytest.mark.parametrize("suffix, name", [("", None), ("\t.", None), ("\t", "")])
def test_missing_dot_and_empty_names(bed_file, read_bed, suffix, name):
    frame = read_bed(bed_file(f"chr1\t0\t5{suffix}\n".encode()))
    assert frame.rows() == [("chr1", 0, 5, name)]


@pytest.mark.parametrize("content", [b"", b"# comment\n\n"])
def test_empty_inputs_return_zero_rows(bed_file, read_bed, content):
    assert read_bed(bed_file(content)).height == 0


def test_bed3_count_and_projection(bed_file):
    path = bed_file(b"chr1\t0\t5\nchr1\t4\t8\nchr1\t21\t29\n")
    lazy = pb.scan_bed(path, use_zero_based=True)
    assert lazy.select(pl.len()).collect().item() == 3
    assert lazy.select("end", "name").collect().rows() == [
        (5, None),
        (8, None),
        (29, None),
    ]
    pb.register_bed(path, "bed_count_regression")
    assert pb.sql("SELECT COUNT(*) FROM bed_count_regression").collect().item() == 3


@pytest.mark.parametrize("zero_based", [True, False])
@pytest.mark.parametrize("lazy", [True, False])
def test_bed3_empty_intervals_in_both_coordinate_systems(bed_file, zero_based, lazy):
    path = bed_file(b"chr1\t0\t0\nchr1\t5\t5\n")
    if lazy:
        frame = pb.scan_bed(path, use_zero_based=zero_based).collect()
    else:
        frame = pb.read_bed(path, use_zero_based=zero_based)
    offset = int(not zero_based)
    assert frame.select("start", "end").rows() == [(offset, 0), (5 + offset, 5)]
