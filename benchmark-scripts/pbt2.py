import sys
import time

import polars_bio as pb

pb.set_option("datafusion.execution.target_partitions", sys.argv[2])
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
best = None
for _ in range(2):
    t = time.perf_counter()
    pb.fastqc(sys.argv[1], modules=M).tidy.collect()
    dt = time.perf_counter() - t
    best = dt if best is None else min(best, dt)
print(f"{best:.2f}")
