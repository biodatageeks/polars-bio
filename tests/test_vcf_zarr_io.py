import shutil
from pathlib import Path

import polars as pl
import pytest
from _expected import DATA_DIR

import polars_bio as pb

VCF_ZARR = DATA_DIR / "io" / "vcf_zarr" / "multi_chrom.vcz"


def _write_i32_2d_array(root: Path, name: str, rows: int, samples: int, value):
    array_path = root / name
    array_path.mkdir(parents=True, exist_ok=True)
    (array_path / ".zarray").write_text(
        f"""{{
  "shape": [{rows}, {samples}],
  "chunks": [{rows}, {samples}],
  "dtype": "<i4",
  "fill_value": -1,
  "order": "C",
  "filters": null,
  "dimension_separator": ".",
  "compressor": null,
  "zarr_format": 2
}}"""
    )
    (array_path / ".zattrs").write_text('{"_ARRAY_DIMENSIONS":["variants","samples"]}')
    chunk = bytearray()
    for row in range(rows):
        for sample in range(samples):
            chunk.extend(int(value(row, sample)).to_bytes(4, "little", signed=True))
    (array_path / "0.0").write_bytes(chunk)


def _write_i32_3d_array(
    root: Path, name: str, rows: int, samples: int, width: int, value
):
    array_path = root / name
    array_path.mkdir(parents=True, exist_ok=True)
    (array_path / ".zarray").write_text(
        f"""{{
  "shape": [{rows}, {samples}, {width}],
  "chunks": [{rows}, {samples}, {width}],
  "dtype": "<i4",
  "fill_value": -1,
  "order": "C",
  "filters": null,
  "dimension_separator": ".",
  "compressor": null,
  "zarr_format": 2
}}"""
    )
    (array_path / ".zattrs").write_text(
        '{"_ARRAY_DIMENSIONS":["variants","samples","ploidy"]}'
    )
    chunk = bytearray()
    for row in range(rows):
        for sample in range(samples):
            for item in range(width):
                chunk.extend(
                    int(value(row, sample, item)).to_bytes(4, "little", signed=True)
                )
    (array_path / "0.0.0").write_bytes(chunk)


def _write_bool_2d_array(root: Path, name: str, rows: int, samples: int, value):
    array_path = root / name
    array_path.mkdir(parents=True, exist_ok=True)
    (array_path / ".zarray").write_text(
        f"""{{
  "shape": [{rows}, {samples}],
  "chunks": [{rows}, {samples}],
  "dtype": "|b1",
  "fill_value": false,
  "order": "C",
  "filters": null,
  "dimension_separator": ".",
  "compressor": null,
  "zarr_format": 2
}}"""
    )
    (array_path / ".zattrs").write_text('{"_ARRAY_DIMENSIONS":["variants","samples"]}')
    chunk = bytearray()
    for row in range(rows):
        for sample in range(samples):
            chunk.append(1 if value(row, sample) else 0)
    (array_path / "0.0").write_bytes(chunk)


def _sampled_format_store(tmp_path: Path) -> Path:
    root = tmp_path / "sampled.vcz"
    shutil.copytree(VCF_ZARR, root)
    shutil.rmtree(root / "sample_id")
    shutil.copytree(root / "contig_id", root / "sample_id")
    (root / "sample_id" / ".zattrs").write_text('{"_ARRAY_DIMENSIONS":["samples"]}')
    _write_i32_2d_array(
        root,
        "call_GT",
        1000,
        2,
        lambda row, sample: (sample + 1) * 10_000 + row,
    )
    _write_i32_2d_array(
        root,
        "call_DP",
        1000,
        2,
        lambda row, sample: (sample + 1) * 1_000 + row,
    )
    return root


def _spec_genotype_store(tmp_path: Path) -> Path:
    root = tmp_path / "genotype.vcz"
    shutil.copytree(VCF_ZARR, root)
    shutil.rmtree(root / "sample_id")
    shutil.copytree(root / "contig_id", root / "sample_id")
    (root / "sample_id" / ".zattrs").write_text('{"_ARRAY_DIMENSIONS":["samples"]}')
    _write_i32_3d_array(
        root,
        "call_genotype",
        1000,
        2,
        2,
        lambda _row, sample, ploidy: {
            (0, 0): 0,
            (0, 1): 1,
            (1, 0): 1,
            (1, 1): 0,
        }.get((sample, ploidy), -2),
    )
    _write_bool_2d_array(
        root, "call_genotype_phased", 1000, 2, lambda _row, sample: sample == 1
    )
    return root


def _write_malformed_array_metadata(root: Path, name: str):
    array_path = root / name
    if array_path.exists():
        shutil.rmtree(array_path)
    array_path.mkdir(parents=True)
    (array_path / ".zarray").write_text("{not valid json")


def _write_corrupt_chunk(root: Path, array_name: str, chunk_name: str):
    (root / array_name / chunk_name).write_bytes(b"not a valid zarr chunk")


def test_scan_vcf_zarr_returns_lazyframe():
    lf = pb.scan_vcf_zarr(str(VCF_ZARR))

    assert isinstance(lf, pl.LazyFrame)


def test_scan_vcf_zarr_projects_core_columns():
    df = pb.scan_vcf_zarr(str(VCF_ZARR)).select(["chrom", "start"]).head(5).collect()

    assert df.columns == ["chrom", "start"]
    assert df.height == 5


def test_scan_vcf_zarr_reads_requested_info_field():
    df = (
        pb.scan_vcf_zarr(str(VCF_ZARR), info_fields=["DP"])
        .select(["chrom", "DP"])
        .head(2)
        .collect()
    )

    assert df.columns == ["chrom", "DP"]
    assert df.height == 2
    assert df.schema["DP"] == pl.Int8


def test_scan_vcf_zarr_auto_discovers_info_field():
    df = pb.scan_vcf_zarr(str(VCF_ZARR)).select(["chrom", "DP"]).head(2).collect()

    assert df.columns == ["chrom", "DP"]
    assert df.height == 2
    assert df.schema["DP"] == pl.Int8


def test_scan_vcf_zarr_reads_list_valued_float_info_field():
    df = (
        pb.scan_vcf_zarr(str(VCF_ZARR), info_fields=["AF"])
        .filter(pl.col("start") == 5_000_100)
        .select("AF")
        .collect()
    )

    assert df.height == 1
    assert df.schema["AF"] == pl.List(pl.Float32)
    assert len(df["AF"].to_list()[0]) == 1


def test_scan_vcf_zarr_info_projection_prunes_format_chunks(tmp_path):
    store = _sampled_format_store(tmp_path)
    lf = (
        pb.scan_vcf_zarr(
            str(store), info_fields=["DP"], format_fields=["DP"], samples=["22"]
        )
        .filter(pl.col("start") == 5_000_100)
        .select("DP")
    )
    _write_corrupt_chunk(store, "call_DP", "0.0")

    df = lf.collect()

    assert df.columns == ["DP"]
    assert df.height == 1


def test_scan_vcf_zarr_format_projection_prunes_info_chunks(tmp_path):
    store = _sampled_format_store(tmp_path)
    lf = (
        pb.scan_vcf_zarr(
            str(store), info_fields=["DP"], format_fields=["DP"], samples=["22"]
        )
        .filter(pl.col("start") == 5_000_100)
        .select("genotypes")
    )
    _write_corrupt_chunk(store, "variant_DP", "0")

    df = lf.collect()

    assert df.height == 1
    assert df["genotypes"].to_list()[0] == {"DP": [2000]}


def test_scan_vcf_zarr_core_projection_prunes_info_and_format_chunks(tmp_path):
    store = _sampled_format_store(tmp_path)
    lf = (
        pb.scan_vcf_zarr(
            str(store), info_fields=["DP"], format_fields=["DP"], samples=["22"]
        )
        .select(["chrom", "start"])
        .head(2)
    )
    _write_corrupt_chunk(store, "variant_DP", "0")
    _write_corrupt_chunk(store, "call_DP", "0.0")

    df = lf.collect()

    assert df.columns == ["chrom", "start"]
    assert df.height == 2


def test_scan_vcf_zarr_sql_count_star_uses_empty_projection(tmp_path):
    from polars_bio.context import ctx
    from polars_bio.polars_bio import (
        InputFormat,
        ReadOptions,
        VcfZarrReadOptions,
        py_read_sql,
        py_register_table,
    )

    store = _sampled_format_store(tmp_path)
    _write_corrupt_chunk(store, "variant_DP", "0")
    _write_corrupt_chunk(store, "call_DP", "0.0")

    read_options = ReadOptions(
        vcf_zarr_read_options=VcfZarrReadOptions(
            info_fields=["DP"], format_fields=["DP"], samples=["22"]
        )
    )
    table = py_register_table(ctx, str(store), None, InputFormat.VcfZarr, read_options)
    result = py_read_sql(ctx, f"SELECT COUNT(*) FROM {table.name}")

    assert result.to_pydict()["count(*)"][0] == 1000


def test_scan_vcf_zarr_prunes_unrequested_info_arrays(tmp_path):
    store = tmp_path / "info_pruned.vcz"
    shutil.copytree(VCF_ZARR, store)
    _write_malformed_array_metadata(store, "variant_UNREADABLE")

    df = (
        pb.scan_vcf_zarr(str(store), info_fields=["DP"])
        .filter(pl.col("start") == 5_000_100)
        .select("DP")
        .collect()
    )

    assert df.columns == ["DP"]
    assert df.height == 1


def test_scan_vcf_zarr_filters_with_logical_columns():
    df = (
        pb.scan_vcf_zarr(str(VCF_ZARR))
        .filter(pl.col("start") >= 5_000_100)
        .select(["chrom", "start"])
        .head(2)
        .collect()
    )

    assert df.columns == ["chrom", "start"]
    assert df.height == 2
    assert df["start"].min() >= 5_000_100


def test_scan_vcf_zarr_filters_without_projecting_filter_column():
    df = (
        pb.scan_vcf_zarr(str(VCF_ZARR))
        .filter((pl.col("start") >= 5_000_200) & (pl.col("start") <= 5_000_300))
        .select(["chrom"])
        .collect()
    )

    assert df.columns == ["chrom"]
    assert df.height == 2


def test_scan_vcf_zarr_sets_source_metadata():
    metadata = pb.scan_vcf_zarr(str(VCF_ZARR)).config_meta.get_metadata()

    assert metadata["source_format"] == "vcf_zarr"
    assert metadata["source_path"] == str(VCF_ZARR)


def test_describe_vcf_zarr_returns_info_and_format_schema(tmp_path):
    store = _sampled_format_store(tmp_path)
    schema = pb.describe_vcf_zarr(str(store)).sort(["name", "field_type"])

    assert schema.columns == ["name", "field_type", "data_type", "description"]
    data_type_by_field = {
        (row["field_type"], row["name"]): row["data_type"] for row in schema.to_dicts()
    }
    assert data_type_by_field[("INFO", "AF")] == "Float"
    assert data_type_by_field[("INFO", "DB")] == "Flag"
    assert data_type_by_field[("INFO", "DP")] == "Integer"
    assert data_type_by_field[("FORMAT", "genotypes")] == "Struct"
    assert ("FORMAT", "DP") not in data_type_by_field
    assert ("FORMAT", "GT") not in data_type_by_field


def test_read_vcf_zarr_collects_dataframe():
    df = pb.read_vcf_zarr(str(VCF_ZARR))

    assert isinstance(df, pl.DataFrame)
    assert {"chrom", "start", "end", "ref", "alt"}.issubset(df.columns)
    assert df.height > 0


def test_scan_vcf_zarr_rejects_missing_format_array():
    with pytest.raises(Exception, match="call_GT|FORMAT field"):
        pb.scan_vcf_zarr(str(VCF_ZARR), format_fields=["GT"])


def test_scan_vcf_zarr_materializes_format_with_sample_pruning(tmp_path):
    store = _sampled_format_store(tmp_path)

    df = (
        pb.scan_vcf_zarr(
            str(store),
            format_fields=["GT", "DP"],
            samples=["22", "missing", "21"],
        )
        .filter(pl.col("start") == 5_000_100)
        .select("genotypes")
        .collect()
    )

    assert df.height == 1
    assert df["genotypes"].to_list()[0] == {
        "GT": [20000, 10000],
        "DP": [2000, 1000],
    }


def test_scan_vcf_zarr_auto_discovers_format_fields(tmp_path):
    store = _sampled_format_store(tmp_path)

    df = (
        pb.scan_vcf_zarr(str(store), samples=["22"])
        .filter(pl.col("start") == 5_000_100)
        .select("genotypes")
        .collect()
    )

    assert df.height == 1
    assert df["genotypes"].to_list()[0] == {"GT": [20000], "DP": [2000]}


def test_scan_vcf_zarr_defaults_spec_gt_to_raw_encoding(tmp_path):
    store = _spec_genotype_store(tmp_path)

    df = (
        pb.scan_vcf_zarr(str(store), format_fields=["GT"], samples=["22"])
        .filter(pl.col("start") == 5_000_100)
        .select("genotypes")
        .collect()
    )

    assert df.height == 1
    assert df["genotypes"].to_list()[0] == {
        "GT": [[1, 0]],
        "GT_phased": [True],
    }


def test_scan_vcf_zarr_can_request_string_gt_encoding(tmp_path):
    store = _spec_genotype_store(tmp_path)

    df = (
        pb.scan_vcf_zarr(
            str(store),
            format_fields=["GT"],
            samples=["22"],
            genotype_encoding_raw=False,
        )
        .filter(pl.col("start") == 5_000_100)
        .select("genotypes")
        .collect()
    )

    assert df.height == 1
    assert df["genotypes"].to_list()[0] == {"GT": ["1|0"]}


def test_scan_vcf_zarr_prunes_unrequested_format_arrays(tmp_path):
    store = _sampled_format_store(tmp_path)
    _write_malformed_array_metadata(store, "call_GT")

    df = (
        pb.scan_vcf_zarr(str(store), format_fields=["DP"], samples=["22"])
        .filter(pl.col("start") == 5_000_100)
        .select("genotypes")
        .collect()
    )

    assert df.height == 1
    assert df["genotypes"].to_list()[0] == {"DP": [2000]}


def test_register_vcf_zarr_queries_core_and_info_columns():
    pb.register_vcf_zarr(str(VCF_ZARR), name="test_vcz_register", info_fields=["DP"])

    df = (
        pb.sql(
            'SELECT chrom, start, "DP" FROM test_vcz_register WHERE start >= 5000100'
        )
        .head(2)
        .collect()
    )

    assert df.columns == ["chrom", "start", "DP"]
    assert df.height == 2
    assert df.schema["DP"] == pl.Int8


def test_register_vcf_zarr_queries_requested_format_fields(tmp_path):
    store = _sampled_format_store(tmp_path)
    pb.register_vcf_zarr(
        str(store),
        name="test_vcz_format_register",
        format_fields=["DP"],
        samples=["22"],
    )

    df = pb.sql(
        "SELECT genotypes FROM test_vcz_format_register WHERE start = 5000100"
    ).collect()

    assert df.height == 1
    assert df["genotypes"].to_list()[0] == {"DP": [2000]}
