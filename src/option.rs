use std::fmt;

use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
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
    pub on_cols: Option<Vec<String>>,
    #[pyo3(get, set)]
    pub overlap_alg: Option<String>,
    #[pyo3(get, set)]
    pub overlap_low_memory: Option<bool>,
    #[pyo3(get, set)]
    pub nearest_k: Option<usize>,
    #[pyo3(get, set)]
    pub include_overlaps: Option<bool>,
    #[pyo3(get, set)]
    pub compute_distance: Option<bool>,
    #[pyo3(get, set)]
    pub min_dist: Option<i64>,
    #[pyo3(get, set)]
    pub view_table: Option<String>,
    #[pyo3(get, set)]
    pub view_columns: Option<Vec<String>>,
}

#[pymethods]
impl RangeOptions {
    #[allow(clippy::too_many_arguments)]
    #[new]
    #[pyo3(signature = (range_op, filter_op=None, suffixes=None, columns_1=None, columns_2=None, on_cols=None, overlap_alg=None, overlap_low_memory=None, nearest_k=None, include_overlaps=None, compute_distance=None, min_dist=None, view_table=None, view_columns=None))]
    pub fn new(
        range_op: RangeOp,
        filter_op: Option<FilterOp>,
        suffixes: Option<(String, String)>,
        columns_1: Option<Vec<String>>,
        columns_2: Option<Vec<String>>,
        on_cols: Option<Vec<String>>,
        overlap_alg: Option<String>,
        overlap_low_memory: Option<bool>,
        nearest_k: Option<usize>,
        include_overlaps: Option<bool>,
        compute_distance: Option<bool>,
        min_dist: Option<i64>,
        view_table: Option<String>,
        view_columns: Option<Vec<String>>,
    ) -> Self {
        RangeOptions {
            range_op,
            filter_op,
            suffixes,
            columns_1,
            columns_2,
            on_cols,
            overlap_alg,
            overlap_low_memory,
            nearest_k,
            include_overlaps,
            compute_distance,
            min_dist,
            view_table,
            view_columns,
        }
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
    Subtract = 5,
    CountOverlapsNaive = 6,
    Merge = 7,
}

impl fmt::Display for RangeOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RangeOp::Overlap => write!(f, "Overlap"),
            RangeOp::Nearest => write!(f, "Nearest"),
            RangeOp::Complement => write!(f, "Complement"),
            RangeOp::Cluster => write!(f, "Cluster"),
            RangeOp::Coverage => write!(f, "Coverage"),
            RangeOp::Subtract => write!(f, "Subtract"),
            RangeOp::CountOverlapsNaive => write!(f, "Count overlaps naive"),
            RangeOp::Merge => write!(f, "Merge"),
        }
    }
}

#[pyclass(eq, eq_int)]
#[derive(Clone, PartialEq, Debug)]
pub enum InputFormat {
    Parquet,
    Csv,
    Bam,
    Sam,
    Cram,
    Vcf,
    Fastq,
    Fasta,
    Bed,
    Gff,
    Gtf,
    Pairs,
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
            InputFormat::Sam => "SAM",
            InputFormat::Vcf => "VCF",
            InputFormat::Fastq => "FASTQ",
            InputFormat::Fasta => "FASTA",
            InputFormat::Bed => "BED",
            InputFormat::Gff => "GFF",
            InputFormat::Gtf => "GTF",
            InputFormat::Cram => "CRAM",
            InputFormat::Pairs => "PAIRS",
        };
        write!(f, "{}", text)
    }
}
#[pyclass(name = "ReadOptions")]
#[derive(Clone, Debug)]
pub struct ReadOptions {
    #[pyo3(get, set)]
    pub vcf_read_options: Option<VcfReadOptions>,
    #[pyo3(get, set)]
    pub gff_read_options: Option<GffReadOptions>,
    #[pyo3(get, set)]
    pub fastq_read_options: Option<FastqReadOptions>,
    #[pyo3(get, set)]
    pub bam_read_options: Option<BamReadOptions>,
    #[pyo3(get, set)]
    pub cram_read_options: Option<CramReadOptions>,
    #[pyo3(get, set)]
    pub bed_read_options: Option<BedReadOptions>,
    #[pyo3(get, set)]
    pub fasta_read_options: Option<FastaReadOptions>,
    #[pyo3(get, set)]
    pub pairs_read_options: Option<PairsReadOptions>,
}

#[pymethods]
impl ReadOptions {
    #[new]
    #[pyo3(signature = (vcf_read_options=None, gff_read_options=None, fastq_read_options=None, bam_read_options=None, cram_read_options=None, bed_read_options=None, fasta_read_options=None, pairs_read_options=None))]
    pub fn new(
        vcf_read_options: Option<VcfReadOptions>,
        gff_read_options: Option<GffReadOptions>,
        fastq_read_options: Option<FastqReadOptions>,
        bam_read_options: Option<BamReadOptions>,
        cram_read_options: Option<CramReadOptions>,
        bed_read_options: Option<BedReadOptions>,
        fasta_read_options: Option<FastaReadOptions>,
        pairs_read_options: Option<PairsReadOptions>,
    ) -> Self {
        ReadOptions {
            vcf_read_options,
            gff_read_options,
            fastq_read_options,
            bam_read_options,
            cram_read_options,
            bed_read_options,
            fasta_read_options,
            pairs_read_options,
        }
    }
}

#[pyclass(name = "PyObjectStorageOptions")]
#[derive(Clone, Debug)]
pub struct PyObjectStorageOptions {
    #[pyo3(get, set)]
    pub chunk_size: Option<usize>,
    #[pyo3(get, set)]
    pub concurrent_fetches: Option<usize>,
    #[pyo3(get, set)]
    pub allow_anonymous: bool,
    #[pyo3(get, set)]
    pub enable_request_payer: bool,
    #[pyo3(get, set)]
    pub max_retries: Option<usize>,
    #[pyo3(get, set)]
    pub timeout: Option<usize>,
    #[pyo3(get, set)]
    pub compression_type: String,
}

#[pymethods]
impl PyObjectStorageOptions {
    #[new]
    #[pyo3(signature = (allow_anonymous, enable_request_payer, compression_type, chunk_size=None, concurrent_fetches=None, max_retries=None, timeout=None, ))]
    pub fn new(
        allow_anonymous: bool,
        enable_request_payer: bool,
        compression_type: String,
        chunk_size: Option<usize>,
        concurrent_fetches: Option<usize>,
        max_retries: Option<usize>,
        timeout: Option<usize>,
    ) -> Self {
        PyObjectStorageOptions {
            allow_anonymous,
            enable_request_payer,
            compression_type,
            chunk_size,
            concurrent_fetches,
            max_retries,
            timeout,
        }
    }
}

pub fn pyobject_storage_options_to_object_storage_options(
    options: Option<PyObjectStorageOptions>,
) -> Option<ObjectStorageOptions> {
    options.map(|opts| ObjectStorageOptions {
        chunk_size: opts.chunk_size,
        concurrent_fetches: opts.concurrent_fetches,
        allow_anonymous: opts.allow_anonymous,
        enable_request_payer: opts.enable_request_payer,
        max_retries: opts.max_retries,
        timeout: opts.timeout,
        compression_type: Some(CompressionType::from_string(opts.compression_type)),
    })
}

#[pyclass(name = "FastqReadOptions")]
#[derive(Clone, Debug)]
pub struct FastqReadOptions {
    pub object_storage_options: Option<ObjectStorageOptions>,
}

#[pymethods]
impl FastqReadOptions {
    #[new]
    #[pyo3(signature = (object_storage_options=None))]
    pub fn new(object_storage_options: Option<PyObjectStorageOptions>) -> Self {
        FastqReadOptions {
            object_storage_options: pyobject_storage_options_to_object_storage_options(
                object_storage_options,
            ),
        }
    }
    #[staticmethod]
    pub fn default() -> Self {
        FastqReadOptions {
            object_storage_options: Some(ObjectStorageOptions {
                chunk_size: Some(1024 * 1024), // 1MB
                concurrent_fetches: Some(4),
                allow_anonymous: false,
                enable_request_payer: false,
                max_retries: Some(5),
                timeout: Some(300), // 300 seconds
                compression_type: Some(CompressionType::AUTO),
            }),
        }
    }
}

#[pyclass(name = "VcfReadOptions")]
#[derive(Clone, Debug)]
pub struct VcfReadOptions {
    #[pyo3(get, set)]
    pub info_fields: Option<Vec<String>>,
    #[pyo3(get, set)]
    pub format_fields: Option<Vec<String>>,
    pub object_storage_options: Option<ObjectStorageOptions>,
    /// If true (default), output 0-based half-open coordinates; if false, 1-based closed
    #[pyo3(get, set)]
    pub zero_based: bool,
}

#[pymethods]
impl VcfReadOptions {
    #[new]
    #[pyo3(signature = (info_fields=None, format_fields=None, object_storage_options=None, zero_based=true))]
    pub fn new(
        info_fields: Option<Vec<String>>,
        format_fields: Option<Vec<String>>,
        object_storage_options: Option<PyObjectStorageOptions>,
        zero_based: bool,
    ) -> Self {
        VcfReadOptions {
            info_fields,
            format_fields,
            object_storage_options: pyobject_storage_options_to_object_storage_options(
                object_storage_options,
            ),
            zero_based,
        }
    }
    #[staticmethod]
    pub fn default() -> Self {
        VcfReadOptions {
            info_fields: None,
            format_fields: None,
            object_storage_options: Some(ObjectStorageOptions {
                chunk_size: Some(1024 * 1024), // 1MB
                concurrent_fetches: Some(4),
                allow_anonymous: false,
                enable_request_payer: false,
                max_retries: Some(5),
                timeout: Some(300), // 300 seconds
                compression_type: Some(CompressionType::AUTO),
            }),
            zero_based: true,
        }
    }
}

#[pyclass(name = "GffReadOptions")]
#[derive(Clone, Debug)]
pub struct GffReadOptions {
    #[pyo3(get, set)]
    pub attr_fields: Option<Vec<String>>,
    pub object_storage_options: Option<ObjectStorageOptions>,
    /// If true (default), output 0-based half-open coordinates; if false, 1-based closed
    #[pyo3(get, set)]
    pub zero_based: bool,
}

#[pymethods]
impl GffReadOptions {
    #[new]
    #[pyo3(signature = (attr_fields=None, object_storage_options=None, zero_based=true))]
    pub fn new(
        attr_fields: Option<Vec<String>>,
        object_storage_options: Option<PyObjectStorageOptions>,
        zero_based: bool,
    ) -> Self {
        GffReadOptions {
            attr_fields,
            object_storage_options: pyobject_storage_options_to_object_storage_options(
                object_storage_options,
            ),
            zero_based,
        }
    }
    #[staticmethod]
    pub fn default() -> Self {
        GffReadOptions {
            attr_fields: None,
            object_storage_options: Some(ObjectStorageOptions {
                chunk_size: Some(1024 * 1024), // 1MB
                concurrent_fetches: Some(4),
                allow_anonymous: false,
                enable_request_payer: false,
                max_retries: Some(5),
                timeout: Some(300), // 300 seconds
                compression_type: Some(CompressionType::AUTO),
            }),
            zero_based: true,
        }
    }
}

#[pyclass(name = "BamReadOptions")]
#[derive(Clone, Debug)]
pub struct BamReadOptions {
    pub object_storage_options: Option<ObjectStorageOptions>,
    /// If true (default), output 0-based half-open coordinates; if false, 1-based closed
    #[pyo3(get, set)]
    pub zero_based: bool,
    /// Optional list of BAM tag names to include as columns (e.g., ["NM", "AS", "MD"])
    #[pyo3(get, set)]
    pub tag_fields: Option<Vec<String>>,
}

#[pymethods]
impl BamReadOptions {
    #[new]
    #[pyo3(signature = (object_storage_options=None, zero_based=true, tag_fields=None))]
    pub fn new(
        object_storage_options: Option<PyObjectStorageOptions>,
        zero_based: bool,
        tag_fields: Option<Vec<String>>,
    ) -> Self {
        BamReadOptions {
            object_storage_options: pyobject_storage_options_to_object_storage_options(
                object_storage_options,
            ),
            zero_based,
            tag_fields,
        }
    }
    #[staticmethod]
    pub fn default() -> Self {
        BamReadOptions {
            object_storage_options: Some(ObjectStorageOptions {
                chunk_size: Some(1024 * 1024), // 1MB
                concurrent_fetches: Some(4),
                allow_anonymous: false,
                enable_request_payer: false,
                max_retries: Some(5),
                timeout: Some(300), // 300 seconds
                compression_type: Some(CompressionType::AUTO),
            }),
            zero_based: true,
            tag_fields: None,
        }
    }
}

#[pyclass(name = "CramReadOptions")]
#[derive(Clone, Debug)]
pub struct CramReadOptions {
    #[pyo3(get, set)]
    pub reference_path: Option<String>,
    pub object_storage_options: Option<ObjectStorageOptions>,
    /// If true (default), output 0-based half-open coordinates; if false, 1-based closed
    #[pyo3(get, set)]
    pub zero_based: bool,
    /// Optional list of CRAM tag names to include as columns (e.g., ["NM", "AS", "MD"])
    #[pyo3(get, set)]
    pub tag_fields: Option<Vec<String>>,
}

#[pymethods]
impl CramReadOptions {
    #[new]
    #[pyo3(signature = (reference_path=None, object_storage_options=None, zero_based=true, tag_fields=None))]
    pub fn new(
        reference_path: Option<String>,
        object_storage_options: Option<PyObjectStorageOptions>,
        zero_based: bool,
        tag_fields: Option<Vec<String>>,
    ) -> Self {
        CramReadOptions {
            reference_path,
            object_storage_options: pyobject_storage_options_to_object_storage_options(
                object_storage_options,
            ),
            zero_based,
            tag_fields,
        }
    }
    #[staticmethod]
    pub fn default() -> Self {
        CramReadOptions {
            reference_path: None,
            object_storage_options: Some(ObjectStorageOptions {
                chunk_size: Some(1024 * 1024), // 1MB
                concurrent_fetches: Some(4),
                allow_anonymous: false,
                enable_request_payer: false,
                max_retries: Some(5),
                timeout: Some(300), // 300 seconds
                compression_type: Some(CompressionType::AUTO),
            }),
            zero_based: true,
            tag_fields: None,
        }
    }
}

#[pyclass(name = "BedReadOptions")]
#[derive(Clone, Debug)]
pub struct BedReadOptions {
    pub object_storage_options: Option<ObjectStorageOptions>,
    /// If true (default), output 0-based half-open coordinates; if false, 1-based closed
    #[pyo3(get, set)]
    pub zero_based: bool,
}

#[pymethods]
impl BedReadOptions {
    #[new]
    #[pyo3(signature = (object_storage_options=None, zero_based=true))]
    pub fn new(object_storage_options: Option<PyObjectStorageOptions>, zero_based: bool) -> Self {
        BedReadOptions {
            object_storage_options: pyobject_storage_options_to_object_storage_options(
                object_storage_options,
            ),
            zero_based,
        }
    }
    #[staticmethod]
    pub fn default() -> Self {
        BedReadOptions {
            object_storage_options: Some(ObjectStorageOptions {
                chunk_size: Some(1024 * 1024), // 1MB
                concurrent_fetches: Some(4),
                allow_anonymous: false,
                enable_request_payer: false,
                max_retries: Some(5),
                timeout: Some(300), // 300 seconds
                compression_type: Some(CompressionType::AUTO),
            }),
            zero_based: true,
        }
    }
}

#[pyclass(name = "FastaReadOptions")]
#[derive(Clone, Debug)]
pub struct FastaReadOptions {
    pub object_storage_options: Option<ObjectStorageOptions>,
    #[pyo3(get, set)]
    pub parallel: bool,
}

#[pymethods]
impl FastaReadOptions {
    #[new]
    #[pyo3(signature = (object_storage_options=None, parallel=false))]
    pub fn new(object_storage_options: Option<PyObjectStorageOptions>, parallel: bool) -> Self {
        FastaReadOptions {
            object_storage_options: pyobject_storage_options_to_object_storage_options(
                object_storage_options,
            ),
            parallel,
        }
    }
    #[staticmethod]
    pub fn default() -> Self {
        FastaReadOptions {
            object_storage_options: Some(ObjectStorageOptions {
                chunk_size: Some(1024 * 1024), // 1MB
                concurrent_fetches: Some(4),
                allow_anonymous: false,
                enable_request_payer: false,
                max_retries: Some(5),
                timeout: Some(300), // 300 seconds
                compression_type: Some(CompressionType::AUTO),
            }),
            parallel: true,
        }
    }
}

#[pyclass(name = "PairsReadOptions")]
#[derive(Clone, Debug)]
pub struct PairsReadOptions {
    pub object_storage_options: Option<ObjectStorageOptions>,
    /// If true, output 0-based half-open coordinates; if false (default), 1-based closed
    #[pyo3(get, set)]
    pub zero_based: bool,
}

#[pymethods]
impl PairsReadOptions {
    #[new]
    #[pyo3(signature = (object_storage_options=None, zero_based=false))]
    pub fn new(object_storage_options: Option<PyObjectStorageOptions>, zero_based: bool) -> Self {
        PairsReadOptions {
            object_storage_options: pyobject_storage_options_to_object_storage_options(
                object_storage_options,
            ),
            zero_based,
        }
    }
    #[staticmethod]
    pub fn default() -> Self {
        PairsReadOptions {
            object_storage_options: Some(ObjectStorageOptions {
                chunk_size: Some(1024 * 1024), // 1MB
                concurrent_fetches: Some(4),
                allow_anonymous: false,
                enable_request_payer: false,
                max_retries: Some(5),
                timeout: Some(300), // 300 seconds
                compression_type: Some(CompressionType::AUTO),
            }),
            zero_based: false,
        }
    }
}

// ============================================================================
// Pileup Options
// ============================================================================

#[pyclass(name = "PileupOptions")]
#[derive(Clone, Debug)]
pub struct PileupOptions {
    #[pyo3(get, set)]
    pub filter_flag: u32,
    #[pyo3(get, set)]
    pub min_mapping_quality: u32,
    #[pyo3(get, set)]
    pub binary_cigar: bool,
    #[pyo3(get, set)]
    pub dense_mode: String,
    /// If true, output 0-based half-open coordinates; if false (default), 1-based closed
    #[pyo3(get, set)]
    pub zero_based: bool,
    /// If true, emit one row per genomic position (like samtools depth -a)
    /// instead of RLE coverage blocks. Default: false.
    #[pyo3(get, set)]
    pub per_base: bool,
}

#[pymethods]
impl PileupOptions {
    #[new]
    // Default filter_flag 1796 = unmapped(4) + secondary(256) + qcfail(512) + duplicate(1024)
    #[pyo3(signature = (filter_flag=1796, min_mapping_quality=0, binary_cigar=true, dense_mode="auto".to_string(), zero_based=false, per_base=false))]
    pub fn new(
        filter_flag: u32,
        min_mapping_quality: u32,
        binary_cigar: bool,
        dense_mode: String,
        zero_based: bool,
        per_base: bool,
    ) -> Self {
        PileupOptions {
            filter_flag,
            min_mapping_quality,
            binary_cigar,
            dense_mode,
            zero_based,
            per_base,
        }
    }
}

// ============================================================================
// Write Options
// ============================================================================

/// Output format for write operations
#[pyclass(eq, eq_int)]
#[derive(Clone, PartialEq, Debug)]
pub enum OutputFormat {
    Vcf,
    Fastq,
    Bam,
    Sam,
    Cram,
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputFormat::Vcf => write!(f, "VCF"),
            OutputFormat::Fastq => write!(f, "FASTQ"),
            OutputFormat::Bam => write!(f, "BAM"),
            OutputFormat::Sam => write!(f, "SAM"),
            OutputFormat::Cram => write!(f, "CRAM"),
        }
    }
}

/// Options for writing VCF files
#[pyclass(name = "VcfWriteOptions")]
#[derive(Clone, Debug)]
pub struct VcfWriteOptions {
    /// Whether the source DataFrame uses 0-based coordinates
    #[pyo3(get, set)]
    pub zero_based: bool,
    /// INFO field metadata as JSON string: {"field_name": {"number": "A", "type": "Float", "description": "..."}}
    #[pyo3(get, set)]
    pub info_fields_metadata: Option<String>,
    /// FORMAT field metadata as JSON string: {"field_name": {"number": "1", "type": "String", "description": "..."}}
    #[pyo3(get, set)]
    pub format_fields_metadata: Option<String>,
    /// Sample names as JSON string: ["sample1", "sample2"]
    #[pyo3(get, set)]
    pub sample_names: Option<String>,
}

#[pymethods]
impl VcfWriteOptions {
    #[new]
    #[pyo3(signature = (zero_based=true, info_fields_metadata=None, format_fields_metadata=None, sample_names=None))]
    pub fn new(
        zero_based: bool,
        info_fields_metadata: Option<String>,
        format_fields_metadata: Option<String>,
        sample_names: Option<String>,
    ) -> Self {
        VcfWriteOptions {
            zero_based,
            info_fields_metadata,
            format_fields_metadata,
            sample_names,
        }
    }

    #[staticmethod]
    pub fn default() -> Self {
        VcfWriteOptions {
            zero_based: true,
            info_fields_metadata: None,
            format_fields_metadata: None,
            sample_names: None,
        }
    }
}

/// Options for writing FASTQ files (placeholder for future options)
#[pyclass(name = "FastqWriteOptions")]
#[derive(Clone, Debug, Default)]
pub struct FastqWriteOptions {}

#[pymethods]
impl FastqWriteOptions {
    #[new]
    pub fn new() -> Self {
        FastqWriteOptions {}
    }
}

/// Options for writing BAM files
#[pyclass(name = "BamWriteOptions")]
#[derive(Clone, Debug)]
pub struct BamWriteOptions {
    #[pyo3(get, set)]
    pub zero_based: bool,
    #[pyo3(get, set)]
    pub tag_fields: Option<Vec<String>>,
    #[pyo3(get, set)]
    pub header_metadata: Option<String>,
    #[pyo3(get, set)]
    pub sort_on_write: bool,
}

#[pymethods]
impl BamWriteOptions {
    #[new]
    #[pyo3(signature = (zero_based=true, tag_fields=None, header_metadata=None, sort_on_write=false))]
    pub fn new(
        zero_based: bool,
        tag_fields: Option<Vec<String>>,
        header_metadata: Option<String>,
        sort_on_write: bool,
    ) -> Self {
        BamWriteOptions {
            zero_based,
            tag_fields,
            header_metadata,
            sort_on_write,
        }
    }
}

/// Options for writing CRAM files
#[pyclass(name = "CramWriteOptions")]
#[derive(Clone, Debug)]
pub struct CramWriteOptions {
    #[pyo3(get, set)]
    pub zero_based: bool,
    #[pyo3(get, set)]
    pub reference_path: Option<String>,
    #[pyo3(get, set)]
    pub tag_fields: Option<Vec<String>>,
    #[pyo3(get, set)]
    pub header_metadata: Option<String>,
    #[pyo3(get, set)]
    pub sort_on_write: bool,
}

#[pymethods]
impl CramWriteOptions {
    #[new]
    #[pyo3(signature = (reference_path=None, zero_based=true, tag_fields=None, header_metadata=None, sort_on_write=false))]
    pub fn new(
        reference_path: Option<String>,
        zero_based: bool,
        tag_fields: Option<Vec<String>>,
        header_metadata: Option<String>,
        sort_on_write: bool,
    ) -> Self {
        CramWriteOptions {
            zero_based,
            reference_path,
            tag_fields,
            header_metadata,
            sort_on_write,
        }
    }
}

/// Container for write options for different formats
#[pyclass(name = "WriteOptions")]
#[derive(Clone, Debug)]
pub struct WriteOptions {
    #[pyo3(get, set)]
    pub vcf_write_options: Option<VcfWriteOptions>,
    #[pyo3(get, set)]
    pub fastq_write_options: Option<FastqWriteOptions>,
    #[pyo3(get, set)]
    pub bam_write_options: Option<BamWriteOptions>,
    #[pyo3(get, set)]
    pub cram_write_options: Option<CramWriteOptions>,
}

#[pymethods]
impl WriteOptions {
    #[new]
    #[pyo3(signature = (vcf_write_options=None, fastq_write_options=None, bam_write_options=None, cram_write_options=None))]
    pub fn new(
        vcf_write_options: Option<VcfWriteOptions>,
        fastq_write_options: Option<FastqWriteOptions>,
        bam_write_options: Option<BamWriteOptions>,
        cram_write_options: Option<CramWriteOptions>,
    ) -> Self {
        WriteOptions {
            vcf_write_options,
            fastq_write_options,
            bam_write_options,
            cram_write_options,
        }
    }
}
