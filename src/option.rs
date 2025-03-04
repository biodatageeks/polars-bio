use std::fmt;

use pyo3::{pyclass, pymethods};

#[pyclass(name = "RangeOptions")]
#[derive(Clone, Debug)]
pub struct RangeOptions {
    #[pyo3(get, set)]
    pub range_op: RangeOp,
    #[pyo3(get, set)]
    pub filter_op: Option<FilterOp>,
    #[pyo3(get, set)]
    pub suffixes: Option<(String, String)>,
    #[pyo3(get, set)]
    pub columns_1: Option<Vec<String>>,
    #[pyo3(get, set)]
    pub columns_2: Option<Vec<String>>,
    #[pyo3(get, set)]
    on_cols: Option<Vec<String>>,
    #[pyo3(get, set)]
    pub overlap_alg: Option<String>,
    #[pyo3(get, set)]
    pub streaming: Option<bool>,
}

#[pymethods]
impl RangeOptions {
    #[allow(clippy::too_many_arguments)]
    #[new]
    #[pyo3(signature = (range_op, filter_op=None, suffixes=None, columns_1=None, columns_2=None, on_cols=None, overlap_alg=None, streaming=None))]
    pub fn new(
        range_op: RangeOp,
        filter_op: Option<FilterOp>,
        suffixes: Option<(String, String)>,
        columns_1: Option<Vec<String>>,
        columns_2: Option<Vec<String>>,
        on_cols: Option<Vec<String>>,
        overlap_alg: Option<String>,
        streaming: Option<bool>,
    ) -> Self {
        RangeOptions {
            range_op,
            filter_op,
            suffixes,
            columns_1,
            columns_2,
            on_cols,
            overlap_alg,
            streaming,
        }
    }
}
impl std::fmt::Display for RangeOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "streaming {}", self.streaming.unwrap_or(false))
    }
}

#[pyclass(eq, eq_int)]
#[derive(Clone, PartialEq, Debug)]
pub enum FilterOp {
    Weak = 0,
    Strict = 1,
}

#[pyclass(eq, eq_int)]
#[derive(Clone, PartialEq, Debug)]
pub enum RangeOp {
    Overlap = 0,
    Complement = 1,
    Cluster = 2,
    Nearest = 3,
    Coverage = 4,
    CountOverlaps = 5,
    CountOverlapsNaive = 6,
}

impl fmt::Display for RangeOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RangeOp::Overlap => write!(f, "Overlap"),
            RangeOp::Nearest => write!(f, "Nearest"),
            RangeOp::Complement => write!(f, "Complement"),
            RangeOp::Cluster => write!(f, "Cluster"),
            RangeOp::Coverage => write!(f, "Coverage"),
            RangeOp::CountOverlaps => write!(f, "Count overlaps"),
            RangeOp::CountOverlapsNaive => write!(f, "Count overlaps naive"),
        }
    }
}

#[pyclass(eq, eq_int)]
#[derive(Clone, PartialEq, Debug)]
pub enum InputFormat {
    Parquet,
    Csv,
    Bam,
    IndexedBam,
    Cram,
    Vcf,
    IndexedVcf,
    Fastq,
    Fasta,
    Bed,
    Gff,
    Gtf,
}

#[pyclass(eq, get_all)]
#[derive(Clone, PartialEq, Debug)]
pub struct BioTable {
    pub name: String,
    pub format: InputFormat,
    pub path: String,
}

impl fmt::Display for InputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            InputFormat::Parquet => "Parquet",
            InputFormat::Csv => "CSV",
            InputFormat::Bam => "BAM",
            InputFormat::Vcf => "VCF",
            InputFormat::Fastq => "FASTQ",
            InputFormat::Fasta => "FASTA",
            InputFormat::Bed => "BED",
            InputFormat::Gff => "GFF",
            InputFormat::Gtf => "GTF",
            InputFormat::IndexedBam => "INDEXED_BAM",
            InputFormat::IndexedVcf => "INDEXED_VCF",
            InputFormat::Cram => "CRAM",
        };
        write!(f, "{}", text)
    }
}
#[pyclass(name = "ReadOptions")]
#[derive(Clone, Debug)]
pub struct ReadOptions {
    #[pyo3(get, set)]
    pub vcf_read_options: Option<VcfReadOptions>,
}

#[pymethods]
impl ReadOptions {
    #[new]
    #[pyo3(signature = (vcf_read_options=None))]
    pub fn new(vcf_read_options: Option<VcfReadOptions>) -> Self {
        ReadOptions { vcf_read_options }
    }
}

#[pyclass(name = "VcfReadOptions")]
#[derive(Clone, Debug)]
pub struct VcfReadOptions {
    #[pyo3(get, set)]
    pub info_fields: Option<Vec<String>>,
    #[pyo3(get, set)]
    pub format_fields: Option<Vec<String>>,
    #[pyo3(get, set)]
    pub thread_num: Option<usize>,
    pub chunk_size: Option<usize>,
    pub concurrent_fetches: Option<usize>,
}

#[pymethods]
impl VcfReadOptions {
    #[new]
    #[pyo3(signature = (info_fields=None, format_fields=None, thread_num=None, chunk_size=None, concurrent_fetches=None))]
    pub fn new(
        info_fields: Option<Vec<String>>,
        format_fields: Option<Vec<String>>,
        thread_num: Option<usize>,
        chunk_size: Option<usize>,
        concurrent_fetches: Option<usize>,
    ) -> Self {
        VcfReadOptions {
            info_fields,
            format_fields,
            thread_num,
            chunk_size,
            concurrent_fetches,
        }
    }
    #[staticmethod]
    pub fn default() -> Self {
        VcfReadOptions {
            info_fields: None,
            format_fields: None,
            thread_num: Some(1),
            chunk_size: Some(64),
            concurrent_fetches: Some(8),
        }
    }
}
