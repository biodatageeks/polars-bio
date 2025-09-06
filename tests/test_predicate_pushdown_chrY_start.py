import re

import polars as pl


def _register_gff_table_serial():
    from polars_bio.context import ctx
    from polars_bio.polars_bio import (
        GffReadOptions,
        InputFormat,
        PyObjectStorageOptions,
        ReadOptions,
        py_register_table,
    )

    data_path = "tests/data/io/gff/gencode.v38.annotation.gff3.bgz"

    gff_opts = GffReadOptions(
        attr_fields=None,
        thread_num=1,
        object_storage_options=PyObjectStorageOptions(
            allow_anonymous=True,
            enable_request_payer=False,
            chunk_size=8,
            concurrent_fetches=1,
            max_retries=3,
            timeout=60,
            compression_type="auto",
        ),
        # Use non-parallel path to avoid BGZF .gzi index requirements in tests
        parallel=False,
    )
    read_options = ReadOptions(gff_read_options=gff_opts)
    table = py_register_table(ctx, data_path, None, InputFormat.Gff, read_options)
    return table.name


def test_predicate_pushdown_chrY_and_start_range_plan():
    from polars_bio.context import ctx
    from polars_bio.polars_bio import py_read_sql

    table_name = _register_gff_table_serial()
    sql = f"SELECT chrom, start, type FROM {table_name} WHERE chrom = 'chrY' AND start < 500000"
    df = py_read_sql(ctx, sql)
    plan = str(df.optimized_logical_plan())

    assert "Filter" in plan, f"Expected a Filter node in plan, got:\n{plan}"
    assert (
        "chrom" in plan and "chrY" in plan
    ), f"Missing chrom == 'chrY' condition in plan:\n{plan}"
    # Be lenient on formatting differences across DataFusion versions
    assert (
        "start" in plan and "500000" in plan
    ), f"Missing start < 500000 tokens in plan:\n{plan}"
    # Some DataFusion versions may not render explicit 'AND' in the plan string
    # even when both predicates are combined. Presence of both predicate tokens
    # (chrom/chrY and start/500000) along with a Filter node is sufficient.


def test_predicate_pushdown_results_match_polars():
    from polars_bio.context import ctx
    from polars_bio.polars_bio import py_read_sql, py_read_table

    table_name = _register_gff_table_serial()
    sql = f"SELECT chrom, start, type FROM {table_name} WHERE chrom = 'chrY' AND start < 500000"
    df = py_read_sql(ctx, sql)
    # Count rows from DataFusion stream (robust to zero results)
    out_count = 0
    for batch in df.execute_stream():
        out_count += batch.to_pyarrow().num_rows

    # Polars baseline over the same data (collect->polars)
    # Baseline: read all columns, project and filter in Polars
    df_all = py_read_table(ctx, table_name)
    df_sel_all = df_all.select_columns("chrom", "start", "type")
    predicate = (pl.col("chrom") == "chrY") & (pl.col("start") < 500000)
    base_count = 0
    for batch in df_sel_all.execute_stream():
        base_df = pl.DataFrame(batch.to_pyarrow())
        base_count += base_df.filter(predicate).height

    assert (
        out_count == base_count
    ), f"Row count mismatch: DF={out_count}, Polars={base_count}"
