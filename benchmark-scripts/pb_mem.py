#!/usr/bin/env python3
"""Run pb.fastqc once (11-module parallel path) so /usr/bin/time -l can capture
the process peak footprint. Usage: pb_mem.py <file.fastq.gz> <target_partitions>"""
import sys

import polars_bio as pb

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
pb.set_option("datafusion.execution.target_partitions", sys.argv[2])
pb.fastqc(sys.argv[1], modules=M).tidy.collect()
