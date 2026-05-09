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


def _write_malformed_array_metadata(root: Path, name: str):
    array_path = root / name
    if array_path.exists():
        shutil.rmtree(array_path)
    array_path.mkdir(parents=True)
    (array_path / ".zarray").write_text("{not valid json")


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


def test_scan_vcf_zarr_auto_discovers_info_field():
    df = pb.scan_vcf_zarr(str(VCF_ZARR)).select(["chrom", "DP"]).head(2).collect()

    assert df.columns == ["chrom", "DP"]
    assert df.height == 2


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
        "GT": ["20000", "10000"],
        "DP": ["2000", "1000"],
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
    assert df["genotypes"].to_list()[0] == {"GT": ["20000"], "DP": ["2000"]}


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
    assert df["genotypes"].to_list()[0] == {"DP": ["2000"]}
