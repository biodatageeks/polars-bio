import os

import pandas as pd
import pyranges as pr

import polars_bio as pb

os.environ["POLARS_MAX_THREADS"] = "1"
BENCH_DATA_ROOT = os.getenv("BENCH_DATA_ROOT")

if BENCH_DATA_ROOT is None:
    raise ValueError("BENCH_DATA_ROOT is not set")


pb.ctx.set_option("datafusion.optimizer.repartition_joins", "false")
pb.ctx.set_option("datafusion.execution.coalesce_batches", "false")
columns = ["contig", "pos_start", "pos_end"]


df_path_1 = f"{BENCH_DATA_ROOT}/ex-anno/*.parquet"
df_path_2 = f"{BENCH_DATA_ROOT}/ex-rna/*.parquet"


df_1 = pd.read_parquet(df_path_1.replace("*.parquet", ""), engine="pyarrow")
df_2 = pd.read_parquet(df_path_2.replace("*.parquet", ""), engine="pyarrow")

# def polars_bio(df_path_1, df_path_2):
#     return pb.overlap(df_path_1, df_path_2, col1=columns, col2=columns).collect()
#
# df = polars_bio(df_path_1, df_path_2)
# df.write_csv("output.csv")


# def polars_bio(df_path_1, df_path_2):
#     return pb.overlap(df_path_1, df_path_2, col1=columns, col2=columns, streaming=True)
#
# df = polars_bio(df_path_1, df_path_2)
# df.sink_csv("output.csv")


# def bioframe(df_1, df_2):
#     return bf.overlap(df_1, df_2, cols1=columns, cols2=columns, how="inner")
#
# df=bioframe(df_1, df_2)
# df.to_csv("output.csv")


def df2pr0(df):
    return pr.PyRanges(
        chromosomes=df.contig,
        starts=df.pos_start,
        ends=df.pos_end,
    )


df_1_pr0 = df2pr0(df_1)
df_2_pr0 = df2pr0(df_2)


#
def pyranges0(df_1_pr0, df_2_pr0):
    return df_1_pr0.join(df_2_pr0)


df = pyranges0(df_1_pr0, df_2_pr0)
df.to_csv("output.csv")


# def df2pr1(df):
#     return pr1.PyRanges(
#         {
#             "Chromosome": df.contig,
#             "Start": df.pos_start,
#             "End": df.pos_end,
#         }
#     )
#
# def pyranges1(df_1_pr1, df_2_pr1):
#     return df_1_pr1.join_ranges(df_2_pr1)
#
# df_1_pr1 = df2pr1(df_1)
# df_2_pr1 = df2pr1(df_2)
# df = pyranges1(df_1_pr1, df_2_pr1)
# df.to_csv("output.csv")
