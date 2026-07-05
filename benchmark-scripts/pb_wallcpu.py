#!/usr/bin/env python3
"""Measure polars-bio fastqc wall + total CPU (user+sys) for the 11-module
parallel path (kmer OFF). Fresh process per thread count (target_partitions is
cached after first use). POLARS_MAX_THREADS=1 isolates DataFusion parallelism.

Usage: POLARS_MAX_THREADS=1 python3 pb_wallcpu.py <file.fastq.gz> <target_partitions> [reps]
Prints one line per rep:  PB <t> <wall_s> <cpu_s>
"""
import resource
import sys
import time

import polars_bio as pb

PATH, p = sys.argv[1], sys.argv[2]
REPS = int(sys.argv[3]) if len(sys.argv) > 3 else 3
M = [
    "basic_stats",
    "per_base_quality",
    "per_seq_quality",
    "per_base_content",
    "per_seq_gc",
    "per_base_n",
    "seq_length",
    "overrepresented",
    "adapter_content",
    "dup_levels",
    "per_tile_quality",
]

pb.set_option("datafusion.execution.target_partitions", p)
pb.fastqc(PATH, modules=M).tidy.collect()  # warmup (JIT/page cache)
for _ in range(REPS):
    r0 = resource.getrusage(resource.RUSAGE_SELF)
    t0 = time.perf_counter()
    pb.fastqc(PATH, modules=M).tidy.collect()
    t1 = time.perf_counter()
    r1 = resource.getrusage(resource.RUSAGE_SELF)
    w = t1 - t0
    c = (r1.ru_utime - r0.ru_utime) + (r1.ru_stime - r0.ru_stime)
    print(f"PB {p} {w:.3f} {c:.3f}", flush=True)
