import resource
import sys
import time

import polars_bio as pb

PATH, p = sys.argv[1], sys.argv[2]
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
]  # 11, kmer OFF -> parallel path
pb.set_option("datafusion.execution.target_partitions", p)
pb.fastqc(PATH, modules=M).tidy.collect()  # warmup
best = None
for _ in range(2):
    r0 = resource.getrusage(resource.RUSAGE_SELF)
    t0 = time.perf_counter()
    pb.fastqc(PATH, modules=M).tidy.collect()
    t1 = time.perf_counter()
    r1 = resource.getrusage(resource.RUSAGE_SELF)
    wall = t1 - t0
    cpu = (r1.ru_utime - r0.ru_utime) + (r1.ru_stime - r0.ru_stime)
    if best is None or wall < best[0]:
        best = (wall, cpu)
print(f"{p:<4}{best[0]:<9.2f}{best[1]:<9.2f}{best[1]/best[0]:<11.1f}")
