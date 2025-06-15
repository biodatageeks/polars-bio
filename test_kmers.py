import json
import time
import subprocess
import os
import matplotlib.pyplot as plt
import pandas as pd
import tempfile
from pathlib import Path

from polars_bio.io import read_fastq
from polars_bio.kmer_analysis import kmer_count, visualize_kmers


SMALL_FASTQ_PATH = "tests/data/io/fastq/ERR194147.fastq"
LARGE_FASTQ_PATH = "tests/data/io/fastq/ERR194147.fastq"
FASTQC_RS_OUTPUT = "tests/data/io/fastq/output_big.json"


def run_fastqc_rs(fastq_path, k, output_json=None):
    if output_json is None:
        fd, output_json = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        temp_file = True
    else:
        temp_file = False
    cmd = [
        "fqc",
        "-k", str(k),
        "-q", fastq_path,
        "-s", output_json
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with open(output_json) as f:
            data = json.load(f)
        if temp_file:
            os.unlink(output_json)
        return data["values"]
    except Exception as e:
        print(f"fastqc-rs error: {e}")
        return None


def compare_with_fastqc_rs(k=3, fastq_path=SMALL_FASTQ_PATH):
    df = read_fastq(fastq_path)
    print("???")
    kmers_polars = kmer_count(k=k, df=df)
    print("my done")
    if k == 3 and os.path.exists(FASTQC_RS_OUTPUT) and fastq_path == SMALL_FASTQ_PATH:
        with open(FASTQC_RS_OUTPUT) as f:
            fastqc_rs_results = json.load(f)["values"]
        print("fastqc-rs done")
    else:
        fastqc_rs_results = run_fastqc_rs(fastq_path, k)
    if not fastqc_rs_results:
        return False
    fastqc_dict = {item["k_mer"]: item["count"] for item in fastqc_rs_results}
    polars_dict = {row["kmer"]: row["count"] for _, row in kmers_polars.iterrows()}
    return fastqc_dict == polars_dict


def performance_test(fastq_paths=None, k_values=None, num_threads=None):
    fastq_paths = fastq_paths or [SMALL_FASTQ_PATH, LARGE_FASTQ_PATH]
    k_values = k_values or [3, 5, 7]
    num_threads = num_threads or [1, 2, 4, 8]
    results = []
    for path in fastq_paths:
        df = read_fastq(path)
        for k in k_values:
            for threads in num_threads:
                import polars_bio as pb
                pb.ctx.set_option("datafusion.execution.target_partitions", str(threads))
                t0 = time.time()
                kmer_count(k=k, df=df)
                results.append({
                    'file': Path(path).name,
                    'k': k,
                    'threads': threads,
                    'time': time.time() - t0
                })
    return pd.DataFrame(results)


def visualize_both_outputs(k=3, fastq_path=SMALL_FASTQ_PATH, top_n=20):
    df = read_fastq(fastq_path)
    kmers_polars = kmer_count(k=k, df=df)
    if k == 3 and os.path.exists(FASTQC_RS_OUTPUT) and fastq_path == SMALL_FASTQ_PATH:
        with open(FASTQC_RS_OUTPUT) as f:
            fastqc_data = json.load(f)["values"]
    else:
        fastqc_data = run_fastqc_rs(fastq_path, k)
    if not fastqc_data:
        visualize_kmers(kmers_polars, top_n=top_n)
        return
    fastqc_df = pd.DataFrame(fastqc_data).rename(columns={'k_mer': 'kmer'})
    kmers_polars = kmers_polars.sort_values(by='count', ascending=False).head(top_n)
    fastqc_df = fastqc_df.sort_values(by='count', ascending=False).head(top_n)
    fig, axes = plt.subplots(2, 1)
    axes[0].barh(kmers_polars['kmer'][::-1], kmers_polars['count'][::-1])
    axes[1].barh(fastqc_df['kmer'][::-1], fastqc_df['count'][::-1])
    plt.tight_layout()
    plt.show()


def main():
    print(compare_with_fastqc_rs(k=3))
    # compare_with_fastqc_rs(k=5)
    # visualize_both_outputs(k=3, top_n=20)
    # performance_test()
    # run_fastqc_rs('ds', 3)


if __name__ == "__main__":
    main()
