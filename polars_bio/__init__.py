import os

from polars_bio.polars_bio import InputFormat
from polars_bio.polars_bio import PyObjectStorageOptions as ObjectStorageOptions
from polars_bio.polars_bio import ReadOptions, VcfReadOptions

from . import polars_ext  # registers pl.LazyFrame.pb namespace
from .context import ctx, set_option
from .io import IOOperations as data_input
from .logging import set_loglevel
from .range_op import FilterOp
from .range_op import IntervalOperations as range_operations
from .sql import SQL as data_processing

try:
    from .range_utils import Utils
    from .range_utils import Utils as utils

    visualize_intervals = Utils.visualize_intervals
except ImportError:
    pass

# Set POLARS_FORCE_NEW_STREAMING depending on installed Polars version
if "POLARS_FORCE_NEW_STREAMING" not in os.environ:
    try:
        import polars as _pl

        # New engine default on Polars >= 1.32 (safe for >1.31 too)
        _ver = tuple(int(x) for x in _pl.__version__.split(".")[:2])
        os.environ["POLARS_FORCE_NEW_STREAMING"] = "1" if _ver >= (1, 32) else "0"
    except Exception:
        os.environ["POLARS_FORCE_NEW_STREAMING"] = "0"

register_gff = data_processing.register_gff
register_vcf = data_processing.register_vcf
register_fastq = data_processing.register_fastq
register_bam = data_processing.register_bam
register_cram = data_processing.register_cram
register_bed = data_processing.register_bed
register_view = data_processing.register_view

sql = data_processing.sql

describe_vcf = data_input.describe_vcf
from_polars = data_input.from_polars
read_bam = data_input.read_bam
read_cram = data_input.read_cram
read_fastq = data_input.read_fastq
read_gff = data_input.read_gff
read_table = data_input.read_table
read_vcf = data_input.read_vcf
read_fastq = data_input.read_fastq
read_bed = data_input.read_bed
read_fasta = data_input.read_fasta
scan_bam = data_input.scan_bam
scan_cram = data_input.scan_cram
scan_bed = data_input.scan_bed
scan_fasta = data_input.scan_fasta
scan_fastq = data_input.scan_fastq
scan_gff = data_input.scan_gff
scan_table = data_input.scan_table
scan_vcf = data_input.scan_vcf

overlap = range_operations.overlap
nearest = range_operations.nearest
count_overlaps = range_operations.count_overlaps
coverage = range_operations.coverage
merge = range_operations.merge

POLARS_BIO_MAX_THREADS = "datafusion.execution.target_partitions"

__version__ = "0.19.0"
__all__ = [
    "ctx",
    "FilterOp",
    "InputFormat",
    "data_processing",
    "range_operations",
    # "LazyFrame",
    "data_input",
    "utils",
    "ReadOptions",
    "VcfReadOptions",
    "ObjectStorageOptions",
    "set_option",
    "set_loglevel",
    "describe_vcf",
    "from_polars",
    "read_bam",
    "read_cram",
    "read_bed",
    "read_fasta",
    "read_fastq",
    "read_gff",
    "read_table",
    "read_vcf",
    "scan_bam",
    "scan_cram",
    "scan_bed",
    "scan_fasta",
    "scan_fastq",
    "scan_gff",
    "scan_table",
    "scan_vcf",
    "register_gff",
    "register_vcf",
    "register_fastq",
    "register_bam",
    "register_cram",
    "register_bed",
    "register_view",
    "sql",
    "overlap",
    "nearest",
    "count_overlaps",
    "coverage",
    "merge",
    "visualize_intervals",
]
