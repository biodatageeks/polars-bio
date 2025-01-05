import json
import os
import timeit

import numpy as np
import pandas as pd
import pyranges as pr
import pyranges1 as pr1
from rich import print
from rich.box import MARKDOWN
from rich.table import Table

import polars_bio as pb

BENCH_DATA_ROOT = os.getenv("BENCH_DATA_ROOT")

if BENCH_DATA_ROOT is None:
    raise ValueError("BENCH_DATA_ROOT is not set")

pb.ctx.set_option("datafusion.optimizer.repartition_joins", "true")
pb.ctx.set_option("datafusion.optimizer.repartition_file_scans", "true")
pb.ctx.set_option("datafusion.execution.coalesce_batches", "false")
pb.ctx.set_option("datafusion.execution.parquet.schema_force_view_types", "false")
columns = ("contig", "pos_start", "pos_end")

test_threads = [8]

num_repeats = 1
num_executions = 1

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
    # {
    #     "df_path_1": f"{BENCH_DATA_ROOT}/chainOrnAna1/*.parquet",
    #     "df_path_2": f"{BENCH_DATA_ROOT}/chainXenTro3Link/*.parquet",
    #     "name": "3-5",
    # },
]


# pyranges0
def df2pr0(df):
    return pr.PyRanges(
        chromosomes=df.contig,
        starts=df.pos_start,
        ends=df.pos_end,
    )


### pyranges1
def df2pr1(df):
    return pr1.PyRanges(
        {
            "Chromosome": df.contig,
            "Start": df.pos_start,
            "End": df.pos_end,
        }
    )


def polars_bio(df_path_1, df_path_2):
    # lf = pl.LazyFrame(
    #     {
    #         "a": ["a", "b", "a", "b", "b", "c"],
    #         "b": [1, 2, 3, 4, 5, 6],
    #         "c": [6, 5, 4, 3, 2, 1],
    #     }
    # )
    # print(lf.group_by("a", maintain_order=True).agg(pl.all().sum()).sort(
    #     "a"
    # ).explain(streaming=True, format="tree"))
    # print(pb.overlap(df_path_1, df_path_2, col1=columns, col2=columns).explain(streaming=True, format="tree"))
    len(
        pb.overlap(df_path_1, df_path_2, col1=columns, col2=columns).collect(
            streaming=True
        )
    )


def pyranges0(df_1_pr0, df_2_pr0):
    len(df_1_pr0.join(df_2_pr0))


def pyranges1(df_1_pr1, df_2_pr1):
    len(df_1_pr1.join_ranges(df_2_pr1))


functions = [
    pyranges0,
    pyranges1,
    polars_bio,
]


# mkdir
# mkdir results directory if it does not exist

os.makedirs("results", exist_ok=True)

for t in test_cases:
    results = []
    df_1 = pd.read_parquet(t["df_path_1"].replace("*.parquet", ""), engine="pyarrow")
    df_2 = pd.read_parquet(t["df_path_2"].replace("*.parquet", ""), engine="pyarrow")
    df_1_pr0 = df2pr0(df_1)
    df_2_pr0 = df2pr0(df_2)
    df_1_pr1 = df2pr1(df_1)
    df_2_pr1 = df2pr1(df_2)
    for p in test_threads:
        pb.ctx.set_option("datafusion.execution.target_partitions", str(p))
        for func in functions:
            times = None
            print(f"Running {func.__name__}...")
            if func == polars_bio:
                times = timeit.repeat(
                    lambda: func(t["df_path_1"], t["df_path_2"]),
                    repeat=num_repeats,
                    number=num_executions,
                )
            elif func == pyranges0 and p == 1:
                times = timeit.repeat(
                    lambda: func(df_1_pr0, df_2_pr0),
                    repeat=num_repeats,
                    number=num_executions,
                )
            elif func == pyranges1 and p == 1:
                times = timeit.repeat(
                    lambda: func(df_1_pr1, df_2_pr1),
                    repeat=num_repeats,
                    number=num_executions,
                )
            else:
                continue
            per_run_times = [
                time / num_executions for time in times
            ]  # Convert to per-run times
            results.append(
                {
                    "name": f"{func.__name__}-{p}",
                    "min": min(per_run_times),
                    "max": max(per_run_times),
                    "mean": np.mean(per_run_times),
                }
            )

        # fastest_mean = min(result["mean"] for result in results)
        fastest_mean = results[0]["mean"]
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
        #     pb.overlap(df_1, df_2, col1=columns, col2=columns).collect().count()
        # ,
        "results": results,
    }
    print(t["name"])
    # print(json.dumps(benchmark_results, indent=4))
    json.dump(benchmark_results, open(f"results/{t['name']}.json", "w"))
    print(table)
