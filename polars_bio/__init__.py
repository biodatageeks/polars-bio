from polars_bio.polars_bio import InputFormat, ReadOptions, VcfReadOptions

from .context import ctx, set_option
from .io import (
    read_bam,
    read_fasta,
    read_fastq,
    read_table,
    read_vcf,
    register_vcf,
    sql,
)
from .polars_ext import PolarsRangesOperations as LazyFrame
from .range_op import FilterOp, count_overlaps, merge, nearest, overlap
from .range_viz import visualize_intervals

POLARS_BIO_MAX_THREADS = "datafusion.execution.target_partitions"


__version__ = "0.6.3"
__all__ = [
    "overlap",
    "nearest",
    "merge",
    "count_overlaps",
    "ctx",
    "FilterOp",
    "visualize_intervals",
    "read_bam",
    "read_vcf",
    "read_fasta",
    "read_fastq",
    "read_table",
    "register_vcf",
    "sql",
    "InputFormat",
    "LazyFrame",
    "ReadOptions",
    "VcfReadOptions",
    "set_option",
]
