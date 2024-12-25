import timeit

import bioframe as bf
import numpy as np
import pandas as pd
import pyranges as pr
from rich import print
from rich.table import Table

import polars_bio as pb

BENCH_DATA_ROOT = "/Users/mwiewior/research/git/openstack-bdg-runners/ansible/roles/gha_runner/files/databio"
# BENCH_DATA_ROOT = os.getenv("BENCH_DATA_ROOT")

# polars_bio
# df_path_1 = f"{BENCH_DATA_ROOT}/chainRn4/*.parquet"
# df_path_2 = f"{BENCH_DATA_ROOT}/fBrain-DS14718/*.parquet"
df_path_1 = f"{BENCH_DATA_ROOT}/chainRn4/*.parquet"
df_path_2 = f"{BENCH_DATA_ROOT}/ex-rna/*.parquet"

# df_path_1 = f"{BENCH_DATA_ROOT}/ex-anno/*.parquet"
# df_path_2 = f"{BENCH_DATA_ROOT}/ex-rna/*.parquet"
pb.ctx.set_option("datafusion.optimizer.repartition_joins", "false")

columns = ("contig", "pos_start", "pos_end")


# bioframe
df_1 = pd.read_parquet(df_path_1.replace("*.parquet", ""), engine="pyarrow")
df_2 = pd.read_parquet(df_path_2.replace("*.parquet", ""), engine="pyarrow")


# pyranges
def df2pr(df):
    return pr.PyRanges(
        chromosomes=df.contig,
        starts=df.pos_start,
        ends=df.pos_end,
    )


df_1_pr = df2pr(df_1)
df_2_pr = df2pr(df_2)


def bioframe():
    bf.overlap(df_1, df_2, cols1=columns, cols2=columns, how="inner").count()


def polars_bio():
    pb.overlap(df_path_1, df_path_2, col1=columns, col2=columns).collect().count()


def pyranges():
    df_1_pr.join(df_2_pr)


functions = [bioframe, polars_bio, pyranges]

num_repeats = 3
num_executions = 5

results = []
for func in functions:
    times = timeit.repeat(func, repeat=num_repeats, number=num_executions)
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
table = Table(title="Benchmark Results")
table.add_column("Function", justify="left", style="cyan", no_wrap=True)
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
print(table)
