"""Coordinate-system resolution for range operations (issue #486).

Every case overlaps a variant at 1-based position 101 with the BED interval
[101, 200) (1-based 102..200), so the correct result is always 0 rows; a
1-row result means the coordinates were shifted by one position.
"""

from __future__ import annotations

import warnings

import pandas as pd
import polars as pl
import pytest

import polars_bio as pb
from polars_bio import CoordinateSystemMismatchError, MissingCoordinateSystemError
from polars_bio._metadata import _resolve_coordinate_system, set_coordinate_system

SCHEMA = {"chrom": pl.Utf8, "start": pl.Int64, "end": pl.Int64}
VAR1 = {"chrom": ["1"], "start": [101], "end": [101]}  # 1-based closed
VAR0 = {"chrom": ["1"], "start": [100], "end": [101]}  # same variant, 0-based half-open
BEDRAW = {"chrom": ["1"], "start": [101], "end": [200]}  # BED values, 0-based half-open
BED1 = {"chrom": ["1"], "start": [102], "end": [200]}  # same interval, 1-based closed

VCF_TEXT = (
    "##fileformat=VCFv4.2\n##contig=<ID=1,length=1000>\n"
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n1\t101\t.\tA\tG\t.\tPASS\t.\n"
)

MISSING, MISMATCH = MissingCoordinateSystemError, CoordinateSystemMismatchError
WARN1 = ("warn", 1)  # lenient fallback: warning + 1 row


@pytest.fixture(scope="module")
def data_files(tmp_path_factory):
    d = tmp_path_factory.mktemp("coords")
    pl.DataFrame(BEDRAW).write_parquet(d / "bed.parquet")
    pl.DataFrame(BEDRAW).write_csv(d / "bed.csv")
    (d / "r.bed").write_text("1\t101\t200\n")
    (d / "variant.vcf").write_text(VCF_TEXT)
    return {
        "Pparquet": str(d / "bed.parquet"),
        "Pcsv": str(d / "bed.csv"),
        "Pbed": str(d / "r.bed"),
        "Pvcf": str(d / "variant.vcf"),
    }


@pytest.fixture(params=["lenient", "strict"])
def check_mode(request):
    pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, False)
    pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_CHECK, request.param == "strict")
    yield request.param
    pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_CHECK, False)


def _lf(rows, zero_based=None):
    lf = pl.LazyFrame(rows, schema=SCHEMA)
    if zero_based is not None:
        set_coordinate_system(lf, zero_based)
    return lf


def make_input(name, data_files):
    frames = {
        "T1var": lambda: _lf(VAR1, False),
        "T0var": lambda: _lf(VAR0, True),
        "T1bed": lambda: _lf(BED1, False),
        "T0bed": lambda: _lf(BEDRAW, True),
        "Ubed": lambda: _lf(BEDRAW),
        "Uvar": lambda: _lf(VAR1),
        "Ubed_pd": lambda: pd.DataFrame(BEDRAW),
    }
    return frames[name]() if name in frames else data_files[name]


def run_overlap(left, right):
    """Return (outcome, coordinate warnings, error message)."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            result = pb.overlap(left, right)
            result = result.collect() if isinstance(result, pl.LazyFrame) else result
            outcome, message = len(result), ""
        except Exception as e:  # noqa: BLE001 - the outcome is the exception type
            outcome, message = type(e), str(e)
    coord_warnings = [
        str(w.message) for w in caught if "oordinate system" in str(w.message)
    ]
    return outcome, coord_warnings, message


# id, left, right, lenient, strict
MATRIX = [
    ("1", "T1var", "T1bed", 0, 0),
    ("2", "T0var", "T0bed", 0, 0),
    ("3", "T1var", "T0bed", MISMATCH, MISMATCH),
    ("4", "T1var", "Ubed", MISSING, MISSING),
    ("4b", "T1var", "Ubed_pd", MISSING, MISSING),
    ("5", "Ubed", "T0var", MISSING, MISSING),
    ("6", "Uvar", "Ubed", WARN1, MISSING),
    ("9", "Pvcf", "Pvcf", 1, 1),
    ("10", "T1var", "Pparquet", MISSING, MISSING),
    ("10b", "T1var", "Pcsv", MISSING, MISSING),
    ("11", "Pparquet", "Pcsv", WARN1, MISSING),
    ("11b", "Ubed", "Pparquet", WARN1, MISSING),
]


@pytest.mark.parametrize(
    "case_id, left, right, lenient, strict", MATRIX, ids=[c[0] for c in MATRIX]
)
def test_case_matrix(check_mode, data_files, case_id, left, right, lenient, strict):
    expected = lenient if check_mode == "lenient" else strict
    outcome, coord_warnings, message = run_overlap(
        make_input(left, data_files), make_input(right, data_files)
    )
    if expected == WARN1:
        assert outcome == 1, message
        assert coord_warnings, "expected a coordinate-system warning"
    elif isinstance(expected, int):
        assert outcome == expected, message
        assert not coord_warnings, coord_warnings
    else:
        assert outcome is expected, f"got {outcome}: {message}"


class TestMessages:
    def test_mixed_error_names_input_and_other_system(self, check_mode, data_files):
        _, _, message = run_overlap(
            make_input("T1var", data_files), make_input("Ubed", data_files)
        )
        assert message.startswith(
            "Second input (Polars LazyFrame) is missing coordinate system metadata"
        )
        assert "the other input is 1-based" in message
        assert "shift overlaps by one position" in message
        assert "pb.scan_parquet(path, zero_based=True)" in message
        assert "config_meta.set" in message

    def test_pandas_hint(self, check_mode, data_files):
        _, _, message = run_overlap(
            make_input("T1var", data_files), make_input("Ubed_pd", data_files)
        )
        assert "Pandas DataFrame" in message
        assert "pb.read_csv" in message
        assert 'df.attrs["coordinate_system_zero_based"]' in message

    def test_path_hint(self, check_mode, data_files):
        _, _, message = run_overlap(
            make_input("T1var", data_files), make_input("Pparquet", data_files)
        )
        assert "file path" in message
        assert "instead of the path" in message

    def test_lenient_warning_points_to_readers(self, data_files):
        pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, False)
        pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_CHECK, False)
        _, coord_warnings, _ = run_overlap(
            make_input("Uvar", data_files), make_input("Ubed", data_files)
        )
        (warning,) = coord_warnings
        assert warning.startswith(
            "Coordinate system metadata is missing for: Polars LazyFrame, Polars LazyFrame."
        )
        assert "Assuming 1-based" in warning
        assert "POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED" in warning
        assert "shifted by one position" in warning
        assert "pb.scan_csv/pb.scan_parquet(..., zero_based=True|False)" in warning

    def test_mismatch_hint_names_readers(self, check_mode, data_files):
        _, _, message = run_overlap(
            make_input("T1var", data_files), make_input("T0bed", data_files)
        )
        assert "first input uses 1-based" in message
        assert "use_zero_based=True|False" in message
        assert "pb.scan_csv/pb.scan_parquet" in message


@pytest.mark.parametrize(
    "path, evidence",
    [
        ("/d/a.vcf", "native-path"),
        ("/d/a.vcf.gz", "native-path"),
        ("/d/A.BCF", "native-path"),
        ("s3://bucket/a.vcf.gz?versionId=1", "native-path"),
        ("/d/a.csv", None),
        ("/d/a.csv.gz", None),
        ("/d/a.parquet", None),
        ("gs://bucket/x.parquet", None),
    ],
)
def test_path_classification(path, evidence):
    pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, False)
    value, got = _resolve_coordinate_system(path)
    assert got == evidence
    assert value == (False if evidence else None)


def test_concat_drops_tag_and_raises_with_hint(check_mode, data_files):
    merged = pl.concat([_lf(BED1, False), _lf(BED1, False)])
    _, _, message = run_overlap(make_input("T1var", data_files), merged)
    assert (
        "Second input (Polars LazyFrame) is missing coordinate system metadata"
        in message
    )
    assert "pb.scan_parquet" in message


def test_single_input_rules(check_mode, data_files):
    """merge() uses validate_coordinate_system_single: tagged runs, untagged follows the mode."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert len(pb.merge(_lf(BED1, False)).collect()) == 1
    assert not [w for w in caught if "oordinate system" in str(w.message)]
    if check_mode == "strict":
        with pytest.raises(MissingCoordinateSystemError, match="config_meta.set"):
            pb.merge(_lf(BEDRAW)).collect()
    else:
        with pytest.warns(UserWarning, match="Assuming 1-based"):
            pb.merge(_lf(BEDRAW)).collect()


FORMAT_CASES = [
    ("7", "T1bed", "Pvcf", "only supported for CSV and Parquet"),
    ("8", "T0bed", "Pvcf", "only supported for CSV and Parquet"),
    ("12", "Ubed", "Pvcf", "only supported for CSV and Parquet"),
    ("13", "T1var", "Pbed", "Only CSV, Parquet, VCF, and BCF files are supported"),
]


@pytest.mark.parametrize(
    "case_id, left, right, match", FORMAT_CASES, ids=[c[0] for c in FORMAT_CASES]
)
def test_unsupported_inputs_report_format_error_first(
    check_mode, data_files, case_id, left, right, match
):
    outcome, coord_warnings, message = run_overlap(
        make_input(left, data_files), make_input(right, data_files)
    )
    assert outcome is ValueError, f"got {outcome}: {message}"
    assert match in message
    assert not coord_warnings, coord_warnings


def test_single_input_bed_path_reports_format_error(check_mode, data_files):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(
            ValueError, match="Only CSV, Parquet, VCF, and BCF files are supported"
        ):
            pb.merge(data_files["Pbed"]).collect()
    assert not [w for w in caught if "oordinate system" in str(w.message)]


def test_vepyr_style_flow(check_mode, tmp_path):
    """A 1-based tagged frame (as vepyr produces) against a BED-like CSV."""
    bed_csv = tmp_path / "regions.csv"
    pl.DataFrame(BEDRAW).write_csv(bed_csv)
    variants = _lf(VAR1, False)

    with pytest.raises(MissingCoordinateSystemError, match="pb.scan_parquet"):
        pb.overlap(variants, pl.read_csv(bed_csv)).collect()
    with pytest.raises(CoordinateSystemMismatchError, match="use_zero_based"):
        pb.overlap(variants, pb.scan_csv(str(bed_csv), zero_based=True)).collect()
    regions = pb.scan_csv(str(bed_csv), zero_based=True, use_zero_based=False)
    assert len(pb.overlap(variants, regions).collect()) == 0
