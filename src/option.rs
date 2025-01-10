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
}

impl fmt::Display for RangeOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RangeOp::Overlap => write!(f, "Overlap"),
            RangeOp::Nearest => write!(f, "Nearest"),
            RangeOp::Complement => write!(f, "Complement"),
            RangeOp::Cluster => write!(f, "Cluster"),
            RangeOp::Coverage => write!(f, "Coverage"),
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

// impl BioTable {
//     pub
// }

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
