"""sink_vcf on a frame whose input INFO column was renamed to INFO_<id>."""

import polars as pl

import polars_bio as pb
from polars_bio._metadata import set_coordinate_system

HEADER = {
    "info_fields": {
        "AF": {
            "number": "A",
            "type": "Float",
            "description": "Cohort allele frequency",
        },
        "DP": {"number": "1", "type": "Integer", "description": "Depth"},
    },
    "format_fields": {},
    "sample_names": [],
}

CORE = ["chrom", "start", "end", "id", "ref", "alt", "qual", "filter"]


def _frame() -> pl.LazyFrame:
    lf = pl.LazyFrame(
        {
            "chrom": ["chr1", "chr1"],
            "start": pl.Series([100, 200], dtype=pl.UInt32),
            "end": pl.Series([100, 200], dtype=pl.UInt32),
            "id": [".", "."],
            "ref": ["A", "C"],
            "alt": ["G", "T"],
            "qual": [50.0, 60.0],
            "filter": ["PASS", "PASS"],
            "INFO_AF": pl.Series([[0.25], [0.5]], dtype=pl.List(pl.Float32)),
            "DP": pl.Series([10, 20], dtype=pl.Int32),
            # An annotation column that shares the input field's id.
            "AF": pl.Series([0.001, 0.002], dtype=pl.Float32),
        }
    )
    pb.set_source_metadata(lf, format="vcf", path="", header=HEADER)
    set_coordinate_system(lf, zero_based=False)
    return lf


def _read(path):
    text = path.read_text().splitlines()
    header = [line for line in text if line.startswith("##")]
    records = [line for line in text if not line.startswith("#")]
    return header, records


def test_prefixed_input_column_is_written_under_its_vcf_id(tmp_path):
    out = tmp_path / "out.vcf"
    pb.sink_vcf(_frame(), str(out))
    header, records = _read(out)

    assert sum(line.startswith("##INFO=<ID=AF,") for line in header) == 1
    assert not any("INFO_AF" in line for line in header + records)
    assert [r.split("\t")[7] for r in records] == ["AF=0.25;DP=10", "AF=0.5;DP=20"]


def test_a_real_field_named_with_the_prefix_is_left_alone(tmp_path):
    header = {
        "info_fields": {
            "INFO_X": {
                "number": "1",
                "type": "Integer",
                "description": "really named so",
            }
        },
        "format_fields": {},
        "sample_names": [],
    }
    lf = _frame().select(CORE).with_columns(pl.Series("INFO_X", [1, 2], dtype=pl.Int32))
    pb.set_source_metadata(lf, format="vcf", path="", header=header)
    set_coordinate_system(lf, zero_based=False)
    out = tmp_path / "out.vcf"
    pb.sink_vcf(lf, str(out))
    _, records = _read(out)
    assert [r.split("\t")[7] for r in records] == ["INFO_X=1", "INFO_X=2"]
