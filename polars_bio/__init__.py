from bioframe import count_overlaps

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

from .io import IOOperations

describe_vcf = IOOperations.describe_vcf
from_polars = IOOperations.from_polars
read_bam = IOOperations.read_bam
read_fastq = IOOperations.read_fastq
read_gff = IOOperations.read_gff
read_table = IOOperations.read_table
read_vcf = IOOperations.read_vcf
read_fastq = IOOperations.read_fastq
read_bed = IOOperations.read_bed

from .range_op import IntervalOperations

overlap = IntervalOperations.overlap
nearest = IntervalOperations.nearest
count_overlaps = IntervalOperations.count_overlaps
coverage = IntervalOperations.coverage
merge = IntervalOperations.merge

from . qc_op import QCOperations

sequence_quality_score = QCOperations.sequence_quality_score


from .range_utils import Utils
from .qc_utils import QCUtils

visualize_intervals = Utils.visualize_intervals
visualize_mean_quality = QCUtils.visualize_mean_quality
visualize_mean_quality_histogram = QCUtils.visualize_mean_quality_histogram

from .io import IOOperations as data_input
from .polars_ext import PolarsRangesOperations as LazyFrame
from .range_op import FilterOp
from .range_op import IntervalOperations as range_operations
from .range_utils import Utils as utils
from .sql import SQL as data_processing
from .qc_op import QCOperations as qc_operations

POLARS_BIO_MAX_THREADS = "datafusion.execution.target_partitions"


__version__ = "0.9.0"
__all__ = [
    "ctx",
    "FilterOp",
    "InputFormat",
    "data_processing",
    "range_operations",
    "qc_operations",
    # "LazyFrame",
    "data_input",
    "utils",
    "ReadOptions",
    "VcfReadOptions",
    "ObjectStorageOptions",
    "set_option",
]

print("Loaded polars_bio")
