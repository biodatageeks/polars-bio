import logging

from polars_bio.polars_bio import InputFormat

from .context import ctx
from .io import (
    read_bam,
    read_bed,
    read_cram,
    read_fasta,
    read_fastq,
    read_gff,
    read_gtf,
    read_indexed_bam,
    read_indexed_vcf,
    read_vcf,
)
from .range_op import FilterOp, nearest, overlap
from .range_viz import visualize_intervals

logging.basicConfig()
logging.getLogger().setLevel(logging.WARN)
logger = logging.getLogger("polars_bio")
logger.setLevel(logging.INFO)


__version__ = "0.4.1"
__all__ = [
    "overlap",
    "nearest",
    "ctx",
    "FilterOp",
    "visualize_intervals",
    "read_bam",
    "read_indexed_bam",
    "read_vcf",
    "read_cram",
    "read_bed",
    "read_gff",
    "read_gtf",
    "read_fasta",
    "read_fastq",
    "read_indexed_vcf",
    "InputFormat",
]
