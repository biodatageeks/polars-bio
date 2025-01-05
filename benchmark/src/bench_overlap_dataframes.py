import json
import os
import timeit

import numpy as np
import pandas as pd
import polars as pl
import pyranges as pr
from rich import print
from rich.box import MARKDOWN
from rich.table import Table

import polars_bio as pb

BENCH_DATA_ROOT = os.getenv("BENCH_DATA_ROOT")

if BENCH_DATA_ROOT is None:
    raise ValueError("BENCH_DATA_ROOT is not set")


pb.ctx.set_option("datafusion.optimizer.repartition_joins", "false")
pb.ctx.set_option("datafusion.execution.target_partitions", "1")

columns = ("contig", "pos_start", "pos_end")


num_repeats = 3
num_executions = 3


test_cases = [
    {
        "df_path_1": f"{BENCH_DATA_ROOT}/fBrain-DS14718/*.parquet",
        "df_path_2": f"{BENCH_DATA_ROOT}/exons/*.parquet",
        "name": "1-2",
    },
    {
        "df_path_1": f"{BENCH_DATA_ROOT}/exons/*.parquet",
        "df_path_2": f"{BENCH_DATA_ROOT}/ex-anno/*.parquet",
        "name": "2-7",
    },
    {
        "df_path_1": f"{BENCH_DATA_ROOT}/fBrain-DS14718/*.parquet",
        "df_path_2": f"{BENCH_DATA_ROOT}/chainRn4/*.parquet",
        "name": "1-0",
    },
    {
        "df_path_1": f"{BENCH_DATA_ROOT}/ex-anno/*.parquet",
        "df_path_2": f"{BENCH_DATA_ROOT}/chainRn4/*.parquet",
        "name": "7-0",
    },
    {
        "df_path_1": f"{BENCH_DATA_ROOT}/ex-anno/*.parquet",
        "df_path_2": f"{BENCH_DATA_ROOT}/chainOrnAna1/*.parquet",
        "name": "7-3",
    },
    {
        "df_path_1": f"{BENCH_DATA_ROOT}/chainRn4/*.parquet",
        "df_path_2": f"{BENCH_DATA_ROOT}/ex-rna/*.parquet",
        "name": "0-8",
    },
    {
        "df_path_1": f"{BENCH_DATA_ROOT}/chainVicPac2/*.parquet",
        "df_path_2": f"{BENCH_DATA_ROOT}/ex-rna/*.parquet",
        "name": "4-8",
    },
    {
        "df_path_1": f"{BENCH_DATA_ROOT}/ex-anno/*.parquet",
        "df_path_2": f"{BENCH_DATA_ROOT}/ex-rna/*.parquet",
        "name": "7-8",
    },
    # {
    #     "df_path_1": f"{BENCH_DATA_ROOT}/chainOrnAna1/*.parquet",
    #     "df_path_2": f"{BENCH_DATA_ROOT}/chainRn4/*.parquet",
    #     "name": "3-0",
    # },
    # {
    #     "df_path_1": f"{BENCH_DATA_ROOT}/chainRn4/*.parquet",
    #     "df_path_2": f"{BENCH_DATA_ROOT}/chainVicPac2/*.parquet",
    #     "name": "0-4",
    # },
    # {
    #     "df_path_1": f"{BENCH_DATA_ROOT}/chainRn4/*.parquet",
    #     "df_path_2": f"{BENCH_DATA_ROOT}/chainXenTro3Link/*.parquet",
    #     "name": "0-5",
    # },
]


# pyranges0
def df2pr0(df):
    return pr.PyRanges(
        chromosomes=df.contig,
        starts=df.pos_start,
        ends=df.pos_end,
    )


def polars_bio(df_path_1, df_path_2):
    pb.overlap(df_path_1, df_path_2, col1=columns, col2=columns).collect().count()


def polars_bio_pandas_lf(df1, df2):
    pb.overlap(df1, df2, col1=columns, col2=columns).collect().count()


def polars_bio_pandas_pd(df1, df2):
    len(
        pb.overlap(df1, df2, col1=columns, col2=columns, output_type="pandas.DataFrame")
    )


def polars_bio_polars_eager(df1, df2):
    pb.overlap(df1, df2, col1=columns, col2=columns).collect().count()


def polars_bio_polars_lazy(df1, df2):
    pb.overlap(df1, df2, col1=columns, col2=columns).collect().count()


functions = [
    polars_bio,
    polars_bio_pandas_lf,
    polars_bio_pandas_pd,
    polars_bio_polars_eager,
    polars_bio_polars_lazy,
]


# mkdir
# mkdir results directory if it does not exist

os.makedirs("results", exist_ok=True)

for t in test_cases:
    results = []
    df_1 = pd.read_parquet(t["df_path_1"].replace("*.parquet", ""), engine="pyarrow")
    df_2 = pd.read_parquet(t["df_path_2"].replace("*.parquet", ""), engine="pyarrow")
    df_pl_1 = pl.from_pandas(df_1)
    df_pl_2 = pl.from_pandas(df_2)
    df_pl_lazy_1 = df_pl_1.lazy()
    df_pl_lazy_2 = df_pl_2.lazy()

    for func in functions:
        times = None
        print(f"Running {func.__name__}...")
        if func == polars_bio:
            times = timeit.repeat(
                lambda: func(t["df_path_1"], t["df_path_2"]),
                repeat=num_repeats,
                number=num_executions,
            )
        elif func == polars_bio_pandas_lf:
            times = timeit.repeat(
                lambda: func(df_1, df_2),
                repeat=num_repeats,
                number=num_executions,
            )
        elif func == polars_bio_pandas_pd:
            times = timeit.repeat(
                lambda: func(df_1, df_2),
                repeat=num_repeats,
                number=num_executions,
            )
        elif func == polars_bio_polars_eager:
            times = timeit.repeat(
                lambda: func(df_pl_1, df_pl_2),
                repeat=num_repeats,
                number=num_executions,
            )
        elif func == polars_bio_polars_lazy:
            times = timeit.repeat(
                lambda: func(df_pl_lazy_1, df_pl_lazy_2),
                repeat=num_repeats,
                number=num_executions,
            )

        per_run_times = [
            time / num_executions for time in times
        ]  # Convert to per-run times
        results.append(
            {
                "name": func.__name__,
                "min": min(per_run_times),
                "max": max(per_run_times),
                "mean": np.mean(per_run_times),
            }
        )

    fastest_mean = min(result["mean"] for result in results)
    for result in results:
        result["speedup"] = fastest_mean / result["mean"]

    # Create Rich table
    table = Table(title="Benchmark Results", box=MARKDOWN)
    table.add_column("Library", justify="left", style="cyan", no_wrap=True)
    table.add_column("Min (s)", justify="right", style="green")
    table.add_column("Max (s)", justify="right", style="green")
    table.add_column("Mean (s)", justify="right", style="green")
    table.add_column("Speedup", justify="right", style="magenta")

    # Add rows to the table
    for result in results:
        table.add_row(
            result["name"],
            f"{result['min']:.6f}",
            f"{result['max']:.6f}",
            f"{result['mean']:.6f}",
            f"{result['speedup']:.2f}x",
        )

    # Display the table
    benchmark_results = {
        "inputs": {
            "df_1_num": len(df_1),
            "df_2_num": len(df_2),
        },
        # "output_num":
        #     pb.overlap(df_1, df_2, col1=columns, col2=columns).collect()
        # ,
        "results": results,
    }
    print(t["name"])
    print(json.dumps(benchmark_results, indent=4))
    json.dump(benchmark_results, open(f"results/{t['name']}.json", "w"))
    print(table)
