from polars_bio.polars_bio import GffReadOptions, InputFormat
from polars_bio.polars_bio import PyObjectStorageOptions as ObjectStorageOptions
from polars_bio.polars_bio import ReadOptions, VcfReadOptions

from .context import ctx, set_option
from .sql import SQL

register_gff = SQL.register_gff
register_vcf = SQL.register_vcf
register_fastq = SQL.register_fastq
register_bam = SQL.register_bam
register_bed = SQL.register_bed
register_view = SQL.register_view

sql = SQL.sql

from .io import (
    describe_vcf,
    from_polars,
    read_bam,
    read_bed,
    read_fasta,
    read_fastq,
    read_gff,
    read_table,
    read_vcf,
    scan_bam,
    scan_bed,
    scan_fasta,
    scan_fastq,
    scan_gff,
    scan_table,
    scan_vcf,
)
from .range_op import IntervalOperations

overlap = IntervalOperations.overlap
nearest = IntervalOperations.nearest
count_overlaps = IntervalOperations.count_overlaps
coverage = IntervalOperations.coverage
merge = IntervalOperations.merge

try:
    from .range_utils import Utils

    visualize_intervals = Utils.visualize_intervals
except ImportError:
    pass


from .logging import set_loglevel
from .polars_ext import PolarsRangesOperations as LazyFrame
from .range_op import FilterOp
from .range_op import IntervalOperations as range_operations
from .sql import SQL as data_processing

try:
    from .range_utils import Utils as utils
except ImportError:
    pass


POLARS_BIO_MAX_THREADS = "datafusion.execution.target_partitions"


__version__ = "0.12.0"
__all__ = [
    "ctx",
    "FilterOp",
    "InputFormat",
    "data_processing",
    "range_operations",
    # "LazyFrame",
    "utils",
    "ReadOptions",
    "VcfReadOptions",
    "ObjectStorageOptions",
    "set_option",
    "set_loglevel",
    "describe_vcf",
    "from_polars",
    "read_bam",
    "read_bed",
    "read_fasta",
    "read_fastq",
    "read_gff",
    "read_table",
    "read_vcf",
    "scan_bam",
    "scan_bed",
    "scan_fasta",
    "scan_fastq",
    "scan_gff",
    "scan_table",
    "scan_vcf",
]
