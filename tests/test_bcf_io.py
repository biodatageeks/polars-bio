"""BCF integration and VCF/VCF-Zarr capability-parity tests."""

import inspect
import json
import shutil
from contextlib import contextmanager

import polars as pl
import pytest
from _expected import DATA_DIR
from polars.testing import assert_frame_equal

import polars_bio as pb
from polars_bio.context import ctx
from polars_bio.polars_bio import (
    InputFormat,
    ReadOptions,
    VcfReadOptions,
    py_read_table,
    py_register_table,
)

VCF_DIR = DATA_DIR / "io" / "vcf"
BCF_DIR = DATA_DIR / "io" / "bcf"
VCF_ZARR_PATH = DATA_DIR / "io" / "vcf_zarr" / "multi_chrom.vcz"
INDEXED_VCF_PATH = VCF_DIR / "multi_chrom.vcf.gz"
INDEXED_BCF_PATH = BCF_DIR / "multi_chrom.bcf"
TARGET_PARTITIONS = "datafusion.execution.target_partitions"
SORT_COLUMNS = ["chrom", "start", "ref", "alt", "id"]

PARITY_CASES = [
    pytest.param("ensembl.vcf", "ensembl.bcf", id="ensembl"),
    pytest.param("ensembl-2.vcf", "ensembl-2.bcf", id="ensembl-2"),
    pytest.param("antku_small.vcf.gz", "antku_small.bcf", id="antku-small"),
    pytest.param("multisample.vcf", "multisample.bcf", id="multisample"),
    pytest.param("genotype_missing.vcf", "genotype_missing.bcf", id="genotype-missing"),
    pytest.param("multisample.vcf.gz", "multisample_large.bcf", id="multisample-large"),
    pytest.param(
        "single_sample_collision.vcf",
        "single_sample_collision.bcf",
        id="single-sample-collision",
    ),
    pytest.param("vep.vcf", "vep.bcf", id="vep"),
    pytest.param("vep_annotate_test.vcf", "vep_annotate_test.bcf", id="vep-annotate"),
    pytest.param("multi_chrom.vcf.gz", "multi_chrom.bcf", id="multi-chrom"),
    pytest.param(
        "info_missing_array.vcf", "info_missing_array.bcf", id="missing-info-array"
    ),
    pytest.param("info_bare_key.vcf", "info_bare_key.bcf", id="bare-info-key"),
    pytest.param(
        "info_bare_key_realdata.vcf",
        "info_bare_key_realdata.bcf",
        id="bare-info-key-realdata",
    ),
]


def _sorted(frame: pl.DataFrame) -> pl.DataFrame:
    """Canonicalize partition-dependent row order without changing the schema."""
    return frame.sort(SORT_COLUMNS)


@contextmanager
def _target_partitions(partitions: int):
    original = pb.get_option(TARGET_PARTITIONS)
    pb.set_option(TARGET_PARTITIONS, str(partitions))
    try:
        yield
    finally:
        if original is not None:
            pb.set_option(TARGET_PARTITIONS, original)


def _find_bcf_exec(plan):
    # datafusion.ExecutionPlan erases the native node class; `display()` is the
    # stable Python API that exposes the underlying execution-plan node name.
    if plan.display().lstrip().startswith("BcfExec:"):
        return plan
    for child in plan.children():
        result = _find_bcf_exec(child)
        if result is not None:
            return result
    return None


@pytest.mark.parametrize(("vcf_name", "bcf_name"), PARITY_CASES)
def test_converted_bcf_matches_vcf_eager_and_lazy(vcf_name: str, bcf_name: str):
    vcf_path = str(VCF_DIR / vcf_name)
    bcf_path = str(BCF_DIR / bcf_name)

    expected = _sorted(pb.read_vcf(vcf_path, predicate_pushdown=False))
    eager = _sorted(pb.read_bcf(bcf_path, predicate_pushdown=False))
    lazy = _sorted(pb.scan_bcf(bcf_path, predicate_pushdown=False).collect())

    assert_frame_equal(eager, expected)
    assert_frame_equal(lazy, expected)


@pytest.mark.parametrize(("vcf_name", "bcf_name"), PARITY_CASES)
def test_converted_bcf_describe_matches_vcf(vcf_name: str, bcf_name: str):
    expected = pb.describe_vcf(str(VCF_DIR / vcf_name)).sort(["field_type", "name"])
    actual = pb.describe_vcf(str(BCF_DIR / bcf_name)).sort(["field_type", "name"])

    assert_frame_equal(actual, expected)


def test_invalid_flag_value_is_rejected_in_both_encodings():
    with pytest.raises(Exception, match="invalid flag|Error reading INFO field"):
        pb.read_vcf(str(VCF_DIR / "info_invalid_flag_value.vcf"), info_fields=["DB"])

    with pytest.raises(Exception, match="Flag.*incompatible String encoding"):
        pb.read_bcf(str(BCF_DIR / "info_invalid_flag_value.bcf"), info_fields=["DB"])


@pytest.mark.parametrize("projection_pushdown", [False, True])
def test_bcf_projection_pushdown_matches_vcf(projection_pushdown: bool):
    columns = ["chrom", "start", "ref", "alt", "CSQ"]
    expected = (
        pb.scan_vcf(str(VCF_DIR / "vep.vcf"), projection_pushdown=projection_pushdown)
        .select(columns)
        .collect()
    )
    actual = (
        pb.scan_bcf(str(BCF_DIR / "vep.bcf"), projection_pushdown=projection_pushdown)
        .select(columns)
        .collect()
    )

    assert_frame_equal(actual, expected)


def test_bcf_csi_range_pushdown_matches_vcf_and_unpushed_decode():
    predicate = (
        (pl.col("chrom") == "chr22")
        & (pl.col("start") >= 200_000)
        & (pl.col("start") <= 400_000)
    )
    columns = ["chrom", "start", "end", "ref", "alt", "DP", "AF"]
    expected = (
        pb.scan_vcf(str(INDEXED_VCF_PATH), predicate_pushdown=True)
        .filter(predicate)
        .select(columns)
        .collect()
    )
    indexed = (
        pb.scan_bcf(str(INDEXED_BCF_PATH), predicate_pushdown=True)
        .filter(predicate)
        .select(columns)
        .collect()
    )
    sequential = (
        pb.scan_bcf(str(INDEXED_BCF_PATH), predicate_pushdown=False)
        .filter(predicate)
        .select(columns)
        .collect()
    )

    assert indexed.height == 20
    assert_frame_equal(indexed, expected)
    assert_frame_equal(indexed, sequential)


@pytest.mark.parametrize("partitions", [1, 2, 4, 8])
def test_bcf_csi_full_scan_uses_target_partitions_without_changing_rows(
    partitions: int,
):
    expected = _sorted(pb.read_vcf(str(INDEXED_VCF_PATH)))
    table_name = f"bcf_csi_partitions_{partitions}"

    with _target_partitions(partitions):
        table = py_register_table(
            ctx,
            str(INDEXED_BCF_PATH),
            table_name,
            InputFormat.Vcf,
            ReadOptions(vcf_read_options=VcfReadOptions(info_fields=[])),
        )
        plan = py_read_table(ctx, table.name).execution_plan()
        bcf_exec = _find_bcf_exec(plan)
        actual = _sorted(pb.read_bcf(str(INDEXED_BCF_PATH)))

    assert bcf_exec is not None, "BcfExec node not found in execution plan"
    assert bcf_exec.partition_count == partitions
    assert_frame_equal(actual, expected)


def test_unindexed_bcf_scan_stays_single_partition():
    bcf_path = BCF_DIR / "ensembl.bcf"
    assert not (BCF_DIR / "ensembl.bcf.csi").exists()

    with _target_partitions(4):
        table = py_register_table(
            ctx,
            str(bcf_path),
            "bcf_without_csi",
            InputFormat.Vcf,
            ReadOptions(vcf_read_options=VcfReadOptions(info_fields=[])),
        )
        bcf_exec = _find_bcf_exec(py_read_table(ctx, table.name).execution_plan())

    assert bcf_exec is not None, "BcfExec node not found in execution plan"
    assert bcf_exec.partition_count == 1


def test_bcf_multisample_format_and_sample_order_match_vcf():
    options = {
        "info_fields": ["AF"],
        "format_fields": ["GT", "DP"],
        "samples": ["NA12880", "NA12878"],
    }
    expected = pb.read_vcf(str(VCF_DIR / "multisample.vcf"), **options)
    actual = pb.read_bcf(str(BCF_DIR / "multisample.bcf"), **options)

    assert_frame_equal(actual, expected)
    assert actual["genotypes"].to_list()[0] == {
        "GT": ["0/0", "0/1"],
        "DP": [20, 25],
    }


def test_bcf_typed_genotype_dosage_values_and_schema():
    actual = pb.read_bcf(
        str(BCF_DIR / "multisample.bcf"),
        format_fields=["GT"],
        genotype_output="dosage",
    )
    dosage = actual["genotypes"].struct.field("GT")

    assert dosage.dtype == pl.List(pl.Int8)
    assert dosage.to_list() == [[1, 2, 0], [0, 1, 2], [2, 0, 1]]


def test_bcf_typed_genotype_dosage_preserves_missingness_eager_and_lazy():
    options = {"format_fields": ["GT"], "genotype_output": "dosage"}
    path = str(BCF_DIR / "genotype_missing.bcf")
    eager = pb.read_bcf(path, **options)
    lazy = pb.scan_bcf(path, **options).collect()

    assert_frame_equal(eager, lazy)
    dosage = eager["genotypes"].struct.field("GT")
    assert dosage.dtype == pl.List(pl.Int8)
    assert dosage.to_list() == [[1, 2], [None, None], [None, None]]


def test_bcf_single_sample_typed_dosage_preserves_top_level_format_layout():
    options = {"format_fields": ["GT"], "genotype_output": "dosage"}
    eager = pb.read_bcf(str(BCF_DIR / "single_sample_collision.bcf"), **options)
    lazy = pb.scan_bcf(
        str(BCF_DIR / "single_sample_collision.bcf"), **options
    ).collect()

    assert_frame_equal(eager, lazy)
    assert "genotypes" not in eager.columns
    assert eager["GT"].dtype == pl.Int8
    assert eager["GT"].to_list() == [1, 2]


@pytest.mark.parametrize("extension", [".bcf", ".BCF"])
def test_bcf_path_is_supported_by_range_operations(tmp_path, extension: str):
    vcf_path = str(VCF_DIR / "ensembl.vcf")
    bcf_path = tmp_path / f"cohort{extension}"
    shutil.copyfile(BCF_DIR / "ensembl.bcf", bcf_path)
    with pytest.warns(UserWarning, match="Coordinate system metadata is missing"):
        expected = pb.overlap(vcf_path, vcf_path).collect()
    with pytest.warns(UserWarning, match="Coordinate system metadata is missing"):
        actual = pb.overlap(str(bcf_path), str(bcf_path)).collect()

    assert_frame_equal(actual.sort(actual.columns), expected.sort(expected.columns))


def test_variant_api_signatures_only_expose_meaningful_genotype_options():
    for reader in (pb.read_vcf, pb.scan_vcf):
        parameters = inspect.signature(reader).parameters
        assert "genotype_output" not in parameters
        assert "genotype_encoding_raw" not in parameters

    for reader in (pb.read_bcf, pb.scan_bcf):
        parameters = inspect.signature(reader).parameters
        assert parameters["genotype_output"].default == "string"
        assert "genotype_encoding_raw" not in parameters

    for reader in (pb.read_vcf_zarr, pb.scan_vcf_zarr):
        assert "genotype_encoding_raw" in inspect.signature(reader).parameters


@pytest.mark.parametrize("reader", [pb.read_vcf, pb.scan_vcf])
def test_vcf_readers_reject_bcf_paths(reader):
    with pytest.raises(ValueError, match="read_bcf.*scan_bcf"):
        reader(str(BCF_DIR / "ensembl.bcf"))


@pytest.mark.parametrize("reader", [pb.read_bcf, pb.scan_bcf])
def test_bcf_readers_reject_non_bcf_paths(reader):
    with pytest.raises(ValueError, match="path ending in '.bcf'"):
        reader(str(VCF_DIR / "ensembl.vcf"))


def test_removed_variant_arguments_raise_type_error():
    with pytest.raises(TypeError, match="genotype_output"):
        pb.scan_vcf("unused.vcf", genotype_output="dosage")
    with pytest.raises(TypeError, match="genotype_encoding_raw"):
        pb.read_vcf("unused.vcf", genotype_encoding_raw=False)
    with pytest.raises(TypeError, match="genotype_encoding_raw"):
        pb.scan_bcf("unused.bcf", genotype_encoding_raw=True)


def test_scan_bcf_accepts_case_insensitive_extension(tmp_path):
    uppercase_path = tmp_path / "cohort.BCF"
    shutil.copyfile(BCF_DIR / "ensembl.bcf", uppercase_path)

    expected = pb.read_bcf(str(BCF_DIR / "ensembl.bcf"))
    actual = pb.scan_bcf(str(uppercase_path)).collect()

    assert_frame_equal(actual, expected)


@pytest.mark.parametrize("use_zero_based", [False, True])
def test_bcf_coordinate_modes_match_vcf(use_zero_based: bool):
    expected = pb.read_vcf(str(VCF_DIR / "ensembl.vcf"), use_zero_based=use_zero_based)
    actual = pb.read_bcf(str(BCF_DIR / "ensembl.bcf"), use_zero_based=use_zero_based)

    assert_frame_equal(actual, expected)


def test_bcf_source_metadata_preserves_vcf_header_contract():
    vcf_metadata = pb.scan_vcf(str(INDEXED_VCF_PATH)).config_meta.get_metadata()
    bcf_metadata = pb.scan_bcf(str(INDEXED_BCF_PATH)).config_meta.get_metadata()
    vcf_header = json.loads(vcf_metadata["source_header"])
    bcf_header = json.loads(bcf_metadata["source_header"])

    assert bcf_metadata["source_format"] == "bcf"
    assert bcf_metadata["source_path"] == str(INDEXED_BCF_PATH)
    for key in ("info_fields", "format_fields", "sample_names", "contigs"):
        assert bcf_header[key] == vcf_header[key]


def test_register_bcf_sql_matches_registered_vcf():
    pb.register_vcf(str(INDEXED_VCF_PATH), "bcf_parity_vcf")
    pb.register_vcf(str(INDEXED_BCF_PATH), "bcf_parity_bcf")
    query = (
        'SELECT chrom, start, "DP" FROM {table} '
        "WHERE chrom = 'chr21' AND start >= 300000 ORDER BY start"
    )

    expected = pb.sql(query.format(table="bcf_parity_vcf")).collect()
    actual = pb.sql(query.format(table="bcf_parity_bcf")).collect()

    assert_frame_equal(actual, expected)


@pytest.mark.parametrize("format_name", ["vcf", "bcf", "vcf-zarr"])
def test_bcf_has_the_common_lazy_read_contract_of_vcf_and_vcf_zarr(
    format_name: str,
):
    if format_name == "vcf-zarr":
        lazy = pb.scan_vcf_zarr(str(VCF_ZARR_PATH), info_fields=["DP"])
    elif format_name == "bcf":
        lazy = pb.scan_bcf(str(INDEXED_BCF_PATH), info_fields=["DP"])
    else:
        lazy = pb.scan_vcf(str(INDEXED_VCF_PATH), info_fields=["DP"])

    result = (
        lazy.filter(pl.col("start") > 0)
        .select(["chrom", "start", "ref", "alt", "DP"])
        .head(2)
        .collect()
    )

    assert isinstance(lazy, pl.LazyFrame)
    assert result.columns == ["chrom", "start", "ref", "alt", "DP"]
    assert result.height == 2
    assert result["DP"].dtype.is_integer()
