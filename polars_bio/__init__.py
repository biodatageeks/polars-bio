import os

import polars_config_meta  # noqa: F401 - initializes DataFrame metadata support

from polars_bio.polars_bio import InputFormat
from polars_bio.polars_bio import PyObjectStorageOptions as ObjectStorageOptions
from polars_bio.polars_bio import ReadOptions, VcfReadOptions

from . import polars_ext  # registers pl.LazyFrame.pb namespace
from ._metadata import (
    get_metadata,
    print_metadata_json,
    print_metadata_summary,
    set_source_metadata,
)
from .constants import (
    POLARS_BIO_COORDINATE_SYSTEM_CHECK,
    POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED,
)
from .context import ctx, get_option, set_option
from .exceptions import CoordinateSystemMismatchError, MissingCoordinateSystemError
from .io import IOOperations as data_input
from .logging import set_loglevel
from .pileup_op import PileupOperations as pileup_operations
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
register_sam = data_processing.register_sam
register_cram = data_processing.register_cram
register_bed = data_processing.register_bed
register_pairs = data_processing.register_pairs
register_view = data_processing.register_view

sql = data_processing.sql

describe_vcf = data_input.describe_vcf
describe_bam = data_input.describe_bam
describe_sam = data_input.describe_sam
describe_cram = data_input.describe_cram
from_polars = data_input.from_polars
read_bam = data_input.read_bam
read_sam = data_input.read_sam
read_cram = data_input.read_cram
read_fastq = data_input.read_fastq
read_gff = data_input.read_gff
read_table = data_input.read_table
read_vcf = data_input.read_vcf
read_fastq = data_input.read_fastq
read_bed = data_input.read_bed
read_fasta = data_input.read_fasta
read_pairs = data_input.read_pairs
scan_bam = data_input.scan_bam
scan_sam = data_input.scan_sam
scan_cram = data_input.scan_cram
scan_bed = data_input.scan_bed
scan_fasta = data_input.scan_fasta
scan_fastq = data_input.scan_fastq
scan_gff = data_input.scan_gff
scan_pairs = data_input.scan_pairs
scan_table = data_input.scan_table
scan_vcf = data_input.scan_vcf
write_vcf = data_input.write_vcf
sink_vcf = data_input.sink_vcf
write_fastq = data_input.write_fastq
sink_fastq = data_input.sink_fastq
write_bam = data_input.write_bam
sink_bam = data_input.sink_bam
write_sam = data_input.write_sam
sink_sam = data_input.sink_sam
write_cram = data_input.write_cram
sink_cram = data_input.sink_cram

depth = pileup_operations.depth

overlap = range_operations.overlap
nearest = range_operations.nearest
count_overlaps = range_operations.count_overlaps
coverage = range_operations.coverage
merge = range_operations.merge
cluster = range_operations.cluster
complement = range_operations.complement
subtract = range_operations.subtract

POLARS_BIO_MAX_THREADS = "datafusion.execution.target_partitions"

__version__ = "0.23.0"
__all__ = [
    "ctx",
    "FilterOp",
    "InputFormat",
    "data_processing",
    "pileup_operations",
    "range_operations",
    # "LazyFrame",
    "data_input",
    "utils",
    "ReadOptions",
    "VcfReadOptions",
    "ObjectStorageOptions",
    "POLARS_BIO_COORDINATE_SYSTEM_CHECK",
    "POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED",
    "get_option",
    "set_option",
    "set_loglevel",
    # Exceptions
    "CoordinateSystemMismatchError",
    "MissingCoordinateSystemError",
    # Metadata functions
    "get_metadata",
    "set_source_metadata",
    "print_metadata_json",
    "print_metadata_summary",
    # I/O functions
    "describe_vcf",
    "describe_bam",
    "describe_sam",
    "describe_cram",
    "from_polars",
    "read_bam",
    "read_sam",
    "read_cram",
    "read_bed",
    "read_fasta",
    "read_fastq",
    "read_pairs",
    "read_gff",
    "read_table",
    "read_vcf",
    "scan_bam",
    "scan_sam",
    "scan_cram",
    "scan_bed",
    "scan_fasta",
    "scan_fastq",
    "scan_gff",
    "scan_pairs",
    "scan_table",
    "scan_vcf",
    "write_vcf",
    "sink_vcf",
    "write_fastq",
    "sink_fastq",
    "write_bam",
    "sink_bam",
    "write_sam",
    "sink_sam",
    "write_cram",
    "sink_cram",
    "register_gff",
    "register_vcf",
    "register_fastq",
    "register_bam",
    "register_sam",
    "register_cram",
    "register_bed",
    "register_pairs",
    "register_view",
    "sql",
    "depth",
    "overlap",
    "nearest",
    "count_overlaps",
    "coverage",
    "merge",
    "cluster",
    "complement",
    "subtract",
    "visualize_intervals",
]
