#!/usr/bin/env python3
"""
Direct test of DataFusion execution plan validation for projection pushdown.
This test bypasses Polars and directly uses DataFusion to prove that column
projection works at the execution plan level.
"""

import re
from typing import List

import pytest

from tests._expected import DATA_DIR


def extract_projected_columns_from_plan(plan_str: str) -> List[str]:
    """Extract projected column names from DataFusion physical execution plan.

    Matches the DisplayAs format from datafusion-bio-formats PR #64, e.g.:
        VcfExec: projection=[chrom, start]
    """
    match = re.search(r"(?:Vcf|Bam|Cram)Exec: projection=\[(.*?)\]", plan_str)
    if match:
        cols_str = match.group(1).strip()
        if cols_str:
            return [col.strip() for col in cols_str.split(",")]
    return []


def test_datafusion_direct_projection_pushdown():
    """Test DataFusion projection pushdown directly without Polars integration."""
    vcf_path = f"{DATA_DIR}/io/vcf/vep.vcf.bgz"

    # Setup DataFusion table
    from polars_bio.context import ctx
    from polars_bio.polars_bio import (
        InputFormat,
        PyObjectStorageOptions,
        ReadOptions,
        VcfReadOptions,
        py_read_table,
        py_register_table,
    )

    object_storage_options = PyObjectStorageOptions(
        allow_anonymous=True,
        enable_request_payer=False,
        chunk_size=8,
        concurrent_fetches=1,
        max_retries=5,
        timeout=300,
        compression_type="auto",
    )

    vcf_read_options = VcfReadOptions(
        info_fields=None,
        object_storage_options=object_storage_options,
    )
    read_options = ReadOptions(vcf_read_options=vcf_read_options)

    table = py_register_table(ctx, vcf_path, None, InputFormat.Vcf, read_options)

    # Test 1: Full table scan (no projection)
    df_full = py_read_table(ctx, table.name)
    full_schema = df_full.schema().names
    full_plan = str(df_full.execution_plan())
    full_projected = extract_projected_columns_from_plan(full_plan)

    # Test 2: Column projection
    df_projected = df_full.select_columns("chrom", "start")
    proj_plan = str(df_projected.execution_plan())
    proj_projected = extract_projected_columns_from_plan(proj_plan)

    # Check that full scan projects all columns
    assert (
        full_projected == full_schema
    ), f"Full scan projection mismatch: expected {full_schema}, got {full_projected}"

    # Check that projected scan only projects requested columns
    assert proj_projected == [
        "chrom",
        "start",
    ], f"Column projection failed: expected ['chrom', 'start'], got {proj_projected}"

    # Check that projection reduces the number of columns at the exec level
    assert len(proj_projected) < len(
        full_projected
    ), f"Projection should reduce exec-level columns: full={full_projected}, projected={proj_projected}"


if __name__ == "__main__":
    test_datafusion_direct_projection_pushdown()
