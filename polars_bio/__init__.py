from polars_bio.polars_bio import InputFormat, ReadOptions, VcfReadOptions, FilterOp

from .context import ctx, set_option
from .io import (
    describe_vcf,
    from_polars,
    read_bam,
    read_fasta,
    read_fastq,
    read_table,
    read_vcf,
    register_vcf,
    register_view,
    sql,
)
from .polars_ext import PolarsRangesOperations as LazyFrame
from .count_overlaps import count_overlaps
from .coverage import coverage
from .merge import merge
from .nearest import nearest
from .overlap import overlap
from .range_viz import visualize_intervals

POLARS_BIO_MAX_THREADS = "datafusion.execution.target_partitions"


__version__ = "0.6.3"
__all__ = [
    "overlap",
    "nearest",
    "merge",
    "count_overlaps",
    "coverage",
    "ctx",
    "FilterOp",
    "visualize_intervals",
    "read_bam",
    "read_vcf",
    "read_fasta",
    "read_fastq",
    "read_table",
    "register_vcf",
    "describe_vcf",
    "register_view",
    "from_polars",
    "sql",
    "InputFormat",
    "LazyFrame",
    "ReadOptions",
    "VcfReadOptions",
    "set_option",
]
