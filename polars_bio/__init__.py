from polars_bio.polars_bio import InputFormat
from polars_bio.polars_bio import PyObjectStorageOptions as ObjectStorageOptions
from polars_bio.polars_bio import ReadOptions, VcfReadOptions

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
from .range_op import FilterOp, count_overlaps, coverage, merge, nearest, overlap
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
    "ObjectStorageOptions",
    "set_option",
]
