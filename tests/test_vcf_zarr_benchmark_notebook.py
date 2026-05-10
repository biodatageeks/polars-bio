import json
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

NOTEBOOK = Path("notebooks/vcf_zarr_benchmark.ipynb")


def _notebook_source() -> str:
    notebook = json.loads(NOTEBOOK.read_text())
    return "\n".join(
        "".join(cell.get("source", []))
        for cell in notebook["cells"]
        if cell.get("cell_type") == "code"
    )


def _canonical_helpers() -> dict:
    notebook = json.loads(NOTEBOOK.read_text())
    source = next(
        "".join(cell.get("source", []))
        for cell in notebook["cells"]
        if "class ScenarioOutput" in "".join(cell.get("source", []))
    )
    source = source.split("\ndef validate_any", 1)[0]

    namespace = {
        "Any": object,
        "CONTIG_LOOKUP": {21: "chr22"},
        "SAMPLES_FOR_QUERY": ["HG00096"],
        "dataclass": __import__("dataclasses").dataclass,
        "hashlib": __import__("hashlib"),
        "json": __import__("json"),
        "np": np,
        "pd": pd,
        "pl": pl,
    }

    def decode_scalar(value):
        if hasattr(value, "item"):
            try:
                value = value.item()
            except Exception:
                pass
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return value

    def decode_list(values, limit=None):
        if values is None:
            return []
        if limit is not None:
            values = values[:limit]
        if hasattr(values, "tolist"):
            values = values.tolist()
        return [decode_scalar(value) for value in values]

    namespace["decode_scalar"] = decode_scalar
    namespace["decode_list"] = decode_list
    exec(source, namespace)
    return namespace


def test_vcf_zarr_benchmark_has_cross_tool_quality_gate():
    source = _notebook_source()

    assert "class ScenarioOutput" in source
    assert "canonical" in source
    assert "assert_comparable_results(raw_results_pd)" in source
    assert "VCZ_BENCH_CONSOLIDATE_METADATA" in source
    assert "consolidated=ZARR_METADATA_CONSOLIDATED" in source


def test_vcf_zarr_benchmark_sample_format_uses_same_region_and_samples():
    source = _notebook_source()

    zarr_start = source.index("def zarr_sample_format")
    sgkit_start = source.index("def sgkit_sample_format")
    zarr_source = source[zarr_start:sgkit_start]
    sgkit_source = source[sgkit_start:]

    assert "BENCH_REGION" in zarr_source
    assert "SAMPLES_FOR_QUERY" in zarr_source
    assert "BENCH_REGION" in sgkit_source
    assert "SAMPLES_FOR_QUERY" in sgkit_source


def test_canonical_gate_normalizes_logical_and_raw_alleles():
    helpers = _canonical_helpers()
    pb_df = pl.DataFrame(
        {"chrom": ["chr22"], "start": [10519265], "ref": ["CA"], "alt": ["C"]}
    )
    raw = {
        "contig": np.array([21], dtype=np.int8),
        "position": np.array([10519265], dtype=np.int32),
        "allele": np.array([["CA", "C"]]),
    }

    assert helpers["canonical_from_pb_df"](pb_df, include_alleles=True) == helpers[
        "canonical_from_raw_arrays"
    ](raw, include_alleles=True)


def test_canonical_gate_normalizes_missing_quality_values():
    helpers = _canonical_helpers()
    pb_df = pl.DataFrame(
        {"chrom": ["chr22", "chr22"], "start": [1, 2], "qual": [None, 10.0]}
    )
    raw = {
        "contig": np.array([21, 21], dtype=np.int8),
        "position": np.array([1, 2], dtype=np.int32),
        "value": np.array([np.nan, 10.0], dtype=np.float32),
    }

    assert helpers["canonical_from_pb_df"](pb_df, value_column="qual") == helpers[
        "canonical_from_raw_arrays"
    ](raw, value_name="variant_quality")


def test_canonical_gate_normalizes_single_alt_info_values():
    helpers = _canonical_helpers()
    pb_df = pl.DataFrame(
        {
            "chrom": ["chr22", "chr22"],
            "start": [1, 2],
            "AF": ["0.000312305", "0.0377889"],
        }
    )
    raw = {
        "contig": np.array([21, 21], dtype=np.int8),
        "position": np.array([1, 2], dtype=np.int32),
        "value": np.array([[0.000312305], [0.0377889]], dtype=np.float32),
    }

    assert helpers["canonical_from_pb_df"](pb_df, value_column="AF") == helpers[
        "canonical_from_raw_arrays"
    ](raw, value_name="variant_AF")
