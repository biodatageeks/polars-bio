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
]
pb.set_option("datafusion.execution.target_partitions", p)
pb.fastqc(PATH, modules=M).tidy.collect()  # warmup
for _ in range(3):
    r0 = resource.getrusage(resource.RUSAGE_SELF)
    t0 = time.perf_counter()
    pb.fastqc(PATH, modules=M).tidy.collect()
    t1 = time.perf_counter()
    r1 = resource.getrusage(resource.RUSAGE_SELF)
    w = t1 - t0
    c = (r1.ru_utime - r0.ru_utime) + (r1.ru_stime - r0.ru_stime)
    print(f"t={p:<3} wall={w:5.2f}  cpu={c:6.2f}  eff_cores={c/w:5.1f}")
