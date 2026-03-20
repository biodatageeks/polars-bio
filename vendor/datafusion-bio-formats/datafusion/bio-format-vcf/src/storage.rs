use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use datafusion::arrow;
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, StorageType, get_compression_type, get_remote_stream,
    get_remote_stream_bgzf_async, get_remote_stream_gz_async, get_storage_type,
};
use datafusion_bio_format_core::record_filter::RecordFieldAccessor;
use futures::stream::BoxStream;
use futures::{StreamExt, stream};
use log::debug;
use log::info;
use noodles_bgzf::{AsyncReader, Reader as BgzfReader};
use noodles_vcf as vcf;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::Error;
use std::sync::Arc;
use tokio::io::BufReader;
use tokio_util::io::StreamReader;
use vcf::io::Reader;
use vcf::{Header, Record};

/// Creates a remote BGZF-compressed VCF reader for cloud storage.
///
/// # Arguments
///
/// * `file_path` - Path or URI to the remote VCF file (e.g., `s3://bucket/file.vcf.bgz`)
/// * `object_storage_options` - Configuration for cloud storage access
///
/// # Returns
///
/// An async VCF reader for BGZF-compressed data
pub async fn get_remote_vcf_bgzf_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> vcf::r#async::io::Reader<AsyncReader<StreamReader<FuturesBytesStream, Bytes>>> {
    let inner = get_remote_stream_bgzf_async(file_path.clone(), object_storage_options)
        .await
        .unwrap();
    vcf::r#async::io::Reader::new(inner)
}

/// Creates a remote GZIP-compressed VCF reader for cloud storage.
///
/// # Arguments
///
/// * `file_path` - Path or URI to the remote VCF file (e.g., `s3://bucket/file.vcf.gz`)
/// * `object_storage_options` - Configuration for cloud storage access
///
/// # Returns
///
/// An async VCF reader for GZIP-compressed data
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or the stream cannot be created
pub async fn get_remote_vcf_gz_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    vcf::r#async::io::Reader<
        tokio::io::BufReader<
            async_compression::tokio::bufread::GzipDecoder<StreamReader<FuturesBytesStream, Bytes>>,
        >,
    >,
    Error,
> {
    let stream = tokio::io::BufReader::new(
        get_remote_stream_gz_async(file_path.clone(), object_storage_options).await?,
    );
    let reader = vcf::r#async::io::Reader::new(stream);
    Ok(reader)
}

/// Creates a remote uncompressed VCF reader for cloud storage.
///
/// # Arguments
///
/// * `file_path` - Path or URI to the remote VCF file (e.g., `s3://bucket/file.vcf`)
/// * `object_storage_options` - Configuration for cloud storage access
///
/// # Returns
///
/// An async VCF reader for uncompressed data
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or the stream cannot be created
pub async fn get_remote_vcf_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<vcf::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>, std::io::Error> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options, None)
        .await
        .map_err(std::io::Error::other)?;
    let inner = StreamReader::new(stream);
    Ok(vcf::r#async::io::Reader::new(inner))
}

/// Creates a local BGZF-compressed VCF reader with multithreading support.
///
/// # Arguments
///
/// * `file_path` - Path to the local VCF file
///
/// # Returns
///
/// A VCF reader with multithreaded BGZF decompression
///
/// # Errors
///
/// Returns an error if the file cannot be opened
pub fn get_local_vcf_bgzf_reader(file_path: String) -> Result<Reader<BgzfReader<File>>, Error> {
    debug!("Reading VCF file from local storage");
    File::open(file_path)
        .map(BgzfReader::new)
        .map(vcf::io::Reader::new)
}

/// Creates a local GZIP-compressed VCF reader.
///
/// # Arguments
///
/// * `file_path` - Path to the local VCF file
///
/// # Returns
///
/// An async VCF reader for GZIP-compressed data
///
/// # Errors
///
/// Returns an error if the file cannot be opened
pub async fn get_local_vcf_gz_reader(
    file_path: String,
) -> Result<
    vcf::r#async::io::Reader<
        tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
    >,
    Error,
> {
    tokio::fs::File::open(file_path)
        .await
        .map(tokio::io::BufReader::new)
        .map(GzipDecoder::new)
        .map(tokio::io::BufReader::new)
        .map(vcf::r#async::io::Reader::new)
}

/// Creates a local uncompressed VCF reader.
///
/// # Arguments
///
/// * `file_path` - Path to the local VCF file
///
/// # Returns
///
/// An async VCF reader for uncompressed data
///
/// # Errors
///
/// Returns an error if the file cannot be opened
pub async fn get_local_vcf_reader(
    file_path: String,
) -> Result<vcf::r#async::io::Reader<BufReader<tokio::fs::File>>, Error> {
    debug!("Reading VCF file from local storage with async reader");
    let reader = tokio::fs::File::open(file_path)
        .await
        .map(BufReader::new)
        .map(vcf::r#async::io::Reader::new)?;
    Ok(reader)
}

/// Gets the VCF header from a local file, auto-detecting compression.
///
/// # Arguments
///
/// * `file_path` - Path to the local VCF file
/// * `object_storage_options` - Configuration for compression detection
///
/// # Returns
///
/// The parsed VCF header
///
/// # Errors
///
/// Returns an error if the file cannot be read or the header is invalid
pub async fn get_local_vcf_header(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<vcf::Header, Error> {
    let compression_type = get_compression_type(
        file_path.clone(),
        object_storage_options.compression_type.clone(),
        object_storage_options.clone(),
    )
    .await
    .map_err(std::io::Error::other)?;
    let header = match compression_type {
        CompressionType::BGZF => {
            let mut reader = get_local_vcf_bgzf_reader(file_path)?;
            reader.read_header()?
        }
        CompressionType::GZIP => {
            let mut reader = get_local_vcf_gz_reader(file_path).await?;
            reader.read_header().await?
        }
        CompressionType::NONE => {
            let mut reader = get_local_vcf_reader(file_path).await?;
            reader.read_header().await?
        }
        _ => panic!("Compression type not supported."),
    };
    Ok(header)
}

/// Gets the VCF header from a remote file, auto-detecting compression.
///
/// # Arguments
///
/// * `file_path` - Path or URI to the remote VCF file
/// * `object_storage_options` - Configuration for cloud storage and compression detection
///
/// # Returns
///
/// The parsed VCF header
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or the header is invalid
pub async fn get_remote_vcf_header(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<vcf::Header, Error> {
    info!("Getting remote VCF header with options: {object_storage_options}");
    let compression_type = get_compression_type(
        file_path.clone(),
        object_storage_options.clone().compression_type,
        object_storage_options.clone(),
    )
    .await
    .map_err(std::io::Error::other)?;
    let header = match compression_type {
        CompressionType::BGZF => {
            let mut reader = get_remote_vcf_bgzf_reader(file_path, object_storage_options).await;
            reader.read_header().await?
        }
        CompressionType::GZIP => {
            let mut reader = get_remote_vcf_gz_reader(file_path, object_storage_options).await?;
            reader.read_header().await?
        }
        CompressionType::NONE => {
            let mut reader = get_remote_vcf_reader(file_path, object_storage_options).await?;
            reader.read_header().await?
        }
        _ => panic!("Compression type not supported."),
    };
    Ok(header)
}

/// Gets the VCF header from either a local or remote file, auto-detecting storage type and compression.
///
/// # Arguments
///
/// * `file_path` - Path to the VCF file (local path or cloud URI)
/// * `object_storage_options` - Configuration for cloud storage and compression detection
///
/// # Returns
///
/// The parsed VCF header
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or the header is invalid
pub async fn get_header(
    file_path: String,
    object_storage_options: Option<ObjectStorageOptions>,
) -> Result<vcf::Header, Error> {
    let storage_type = get_storage_type(file_path.clone());
    let opts = object_storage_options.unwrap_or_default();
    let header = match storage_type {
        StorageType::LOCAL => get_local_vcf_header(file_path, opts.clone()).await?,
        _ => get_remote_vcf_header(file_path, opts.clone()).await?,
    };
    Ok(header)
}

/// Unified reader for remote VCF files with multiple compression format support.
///
/// This enum handles BGZF, GZIP, and uncompressed remote VCF files from cloud storage.
/// The appropriate variant is created based on the detected compression type.
pub enum VcfRemoteReader {
    /// Reader for BGZF-compressed remote VCF files.
    BGZF(vcf::r#async::io::Reader<AsyncReader<StreamReader<FuturesBytesStream, Bytes>>>),
    /// Reader for GZIP-compressed remote VCF files.
    GZIP(vcf::r#async::io::Reader<BufReader<GzipDecoder<StreamReader<FuturesBytesStream, Bytes>>>>),
    /// Reader for uncompressed remote VCF files.
    PLAIN(vcf::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>),
}

impl VcfRemoteReader {
    /// Creates a new remote VCF reader with automatic compression detection.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path or URI to the remote VCF file
    /// * `object_storage_options` - Configuration for cloud storage access
    ///
    /// # Returns
    ///
    /// A `VcfRemoteReader` instance with the appropriate variant for the detected compression
    pub async fn new(file_path: String, object_storage_options: ObjectStorageOptions) -> Self {
        info!("Creating remote VCF reader: {object_storage_options}");
        let compression_type = get_compression_type(
            file_path.clone(),
            object_storage_options.clone().compression_type,
            object_storage_options.clone(),
        )
        .await
        .unwrap_or(CompressionType::NONE);
        match compression_type {
            CompressionType::BGZF => {
                let reader = get_remote_vcf_bgzf_reader(file_path, object_storage_options).await;
                VcfRemoteReader::BGZF(reader)
            }
            CompressionType::GZIP => {
                let reader = get_remote_vcf_gz_reader(file_path, object_storage_options)
                    .await
                    .unwrap();
                VcfRemoteReader::GZIP(reader)
            }
            CompressionType::NONE => {
                let reader = get_remote_vcf_reader(file_path, object_storage_options)
                    .await
                    .unwrap();
                VcfRemoteReader::PLAIN(reader)
            }
            _ => panic!("Compression type not supported."),
        }
    }

    /// Reads the VCF header.
    ///
    /// # Returns
    ///
    /// The parsed VCF header
    ///
    /// # Errors
    ///
    /// Returns an error if the header cannot be read
    pub async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match self {
            VcfRemoteReader::BGZF(reader) => reader.read_header().await,
            VcfRemoteReader::GZIP(reader) => reader.read_header().await,
            VcfRemoteReader::PLAIN(reader) => reader.read_header().await,
        }
    }

    /// Reads INFO field metadata from the VCF header.
    ///
    /// # Returns
    ///
    /// A RecordBatch containing field names, types, and descriptions
    ///
    /// # Errors
    ///
    /// Returns an error if the header cannot be read
    pub async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error> {
        match self {
            VcfRemoteReader::BGZF(reader) => {
                let header = reader.read_header().await?;
                Ok(get_info_fields(&header).await)
            }
            VcfRemoteReader::GZIP(reader) => {
                let header = reader.read_header().await?;
                Ok(get_info_fields(&header).await)
            }
            VcfRemoteReader::PLAIN(reader) => {
                let header = reader.read_header().await?;
                Ok(get_info_fields(&header).await)
            }
        }
    }

    /// Reads VCF records as an async stream.
    ///
    /// # Returns
    ///
    /// A boxed async stream of VCF records
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            VcfRemoteReader::BGZF(reader) => reader.records().boxed(),
            VcfRemoteReader::GZIP(reader) => reader.records().boxed(),
            VcfRemoteReader::PLAIN(reader) => reader.records().boxed(),
        }
    }
}

/// Unified reader for local VCF files with multiple compression format support.
///
/// This enum handles BGZF (multithreaded), GZIP, and uncompressed local VCF files.
/// The appropriate variant is created based on the detected compression type.
#[allow(clippy::large_enum_variant)]
pub enum VcfLocalReader {
    /// Reader for BGZF-compressed local VCF files.
    BGZF(Reader<BgzfReader<File>>),
    /// Reader for GZIP-compressed local VCF files.
    GZIP(vcf::r#async::io::Reader<BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>>),
    /// Reader for uncompressed local VCF files.
    PLAIN(vcf::r#async::io::Reader<BufReader<tokio::fs::File>>),
}

impl VcfLocalReader {
    /// Creates a new local VCF reader with automatic compression detection.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the local VCF file
    /// * `object_storage_options` - Configuration for compression detection
    ///
    /// # Returns
    ///
    /// A `VcfLocalReader` instance with the appropriate variant for the detected compression
    pub async fn new(file_path: String, object_storage_options: ObjectStorageOptions) -> Self {
        let compression_type = get_compression_type(
            file_path.clone(),
            object_storage_options.clone().compression_type,
            object_storage_options.clone(),
        )
        .await
        .unwrap_or(CompressionType::NONE);
        match compression_type {
            CompressionType::BGZF => {
                let reader = get_local_vcf_bgzf_reader(file_path).unwrap();
                VcfLocalReader::BGZF(reader)
            }
            CompressionType::GZIP => {
                let reader = get_local_vcf_gz_reader(file_path).await.unwrap();
                VcfLocalReader::GZIP(reader)
            }
            CompressionType::NONE => {
                let reader = get_local_vcf_reader(file_path).await.unwrap();
                VcfLocalReader::PLAIN(reader)
            }
            _ => panic!("Compression type not supported."),
        }
    }

    /// Reads the VCF header.
    ///
    /// # Returns
    ///
    /// The parsed VCF header
    ///
    /// # Errors
    ///
    /// Returns an error if the header cannot be read
    pub async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match self {
            VcfLocalReader::BGZF(reader) => reader.read_header(),
            VcfLocalReader::GZIP(reader) => reader.read_header().await,
            VcfLocalReader::PLAIN(reader) => reader.read_header().await,
        }
    }

    /// Reads VCF records as a stream.
    ///
    /// # Returns
    ///
    /// A boxed stream of VCF records
    pub fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            VcfLocalReader::BGZF(reader) => stream::iter(reader.records()).boxed(),
            VcfLocalReader::GZIP(reader) => reader.records().boxed(),
            VcfLocalReader::PLAIN(reader) => reader.records().boxed(),
        }
    }

    /// Reads INFO field metadata from the VCF header.
    ///
    /// # Returns
    ///
    /// A RecordBatch containing field names, types, and descriptions
    ///
    /// # Errors
    ///
    /// Returns an error if the header cannot be read
    pub async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error> {
        match self {
            VcfLocalReader::BGZF(reader) => {
                let header = reader.read_header()?;
                Ok(get_info_fields(&header).await)
            }
            VcfLocalReader::GZIP(reader) => {
                let header = reader.read_header().await?;
                Ok(get_info_fields(&header).await)
            }
            VcfLocalReader::PLAIN(reader) => {
                let header = reader.read_header().await?;
                Ok(get_info_fields(&header).await)
            }
        }
    }
}

/// Synchronous local VCF reader for BGZF and uncompressed files.
/// Used by the buffer-reuse read path to avoid per-record clone allocations.
pub enum VcfSyncReader {
    /// BGZF-compressed reader.
    Bgzf(vcf::io::Reader<BgzfReader<File>>),
    /// Uncompressed reader with buffered I/O.
    Plain(vcf::io::Reader<std::io::BufReader<File>>),
}

impl VcfSyncReader {
    /// Reads the VCF header.
    pub fn read_header(&mut self) -> std::io::Result<Header> {
        match self {
            VcfSyncReader::Bgzf(r) => r.read_header(),
            VcfSyncReader::Plain(r) => r.read_header(),
        }
    }

    /// Reads the next VCF record into the provided buffer.
    /// Returns 0 at EOF.
    pub fn read_record(&mut self, record: &mut Record) -> std::io::Result<usize> {
        match self {
            VcfSyncReader::Bgzf(r) => r.read_record(record),
            VcfSyncReader::Plain(r) => r.read_record(record),
        }
    }
}

/// Opens a local VCF file synchronously and returns the reader + header.
/// Supports BGZF and uncompressed formats for the buffer-reuse read path.
pub fn open_local_vcf_sync(
    file_path: &str,
    compression_type: CompressionType,
) -> std::io::Result<(VcfSyncReader, Header)> {
    let mut reader = match compression_type {
        CompressionType::BGZF => {
            VcfSyncReader::Bgzf(get_local_vcf_bgzf_reader(file_path.to_string())?)
        }
        CompressionType::NONE => {
            let file = File::open(file_path)?;
            VcfSyncReader::Plain(vcf::io::Reader::new(std::io::BufReader::new(file)))
        }
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                format!("Sync VCF reader does not support compression: {compression_type:?}"),
            ));
        }
    };
    let header = reader.read_header()?;
    Ok((reader, header))
}

/// Extracts INFO field metadata from a VCF header into a RecordBatch.
///
/// # Arguments
///
/// * `header` - The VCF header to extract INFO fields from
///
/// # Returns
///
/// A RecordBatch with columns: name (String), type (String), description (String)
pub async fn get_info_fields(header: &Header) -> arrow::array::RecordBatch {
    let info_fields = header.infos();
    let mut field_names = StringBuilder::new();
    let mut field_types = StringBuilder::new();
    let mut field_descriptions = StringBuilder::new();
    for (field_name, field) in info_fields {
        field_names.append_value(field_name);
        field_types.append_value(field.ty());
        field_descriptions.append_value(field.description());
    }
    // build RecordBatch
    let field_names = field_names.finish();
    let field_types = field_types.finish();
    let field_descriptions = field_descriptions.finish();
    let schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("type", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("description", arrow::datatypes::DataType::Utf8, false),
    ]);
    arrow::record_batch::RecordBatch::try_new(
        SchemaRef::from(schema.clone()),
        vec![
            Arc::new(field_names),
            Arc::new(field_types),
            Arc::new(field_descriptions),
        ],
    )
    .unwrap()
}

/// Unified reader for both local and remote VCF files.
///
/// This is the primary entry point for reading VCF files from any source (local, S3, GCS, etc).
/// It automatically detects the storage type and compression format.
pub enum VcfReader {
    /// Reader for local VCF files.
    Local(VcfLocalReader),
    /// Reader for remote VCF files.
    Remote(VcfRemoteReader),
}

impl VcfReader {
    /// Creates a new VCF reader with automatic storage type and compression detection.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the VCF file (local path or cloud URI)
    /// * `object_storage_options` - Optional configuration for cloud storage and compression
    ///
    /// # Returns
    ///
    /// A `VcfReader` instance configured for the detected storage and compression type
    pub async fn new(
        file_path: String,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> Self {
        let storage_type = get_storage_type(file_path.clone());
        info!("Storage type for VCF file {file_path}: {storage_type:?}");
        let opts = object_storage_options.unwrap_or_default();
        match storage_type {
            StorageType::LOCAL => VcfReader::Local(VcfLocalReader::new(file_path, opts).await),
            _ => VcfReader::Remote(VcfRemoteReader::new(file_path, opts).await),
        }
    }

    /// Reads the VCF header.
    ///
    /// # Returns
    ///
    /// The parsed VCF header
    ///
    /// # Errors
    ///
    /// Returns an error if the header cannot be read
    pub async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match self {
            VcfReader::Local(reader) => reader.read_header().await,
            VcfReader::Remote(reader) => reader.read_header().await,
        }
    }

    /// Reads INFO field metadata from the VCF header.
    ///
    /// # Returns
    ///
    /// A RecordBatch containing field names, types, and descriptions
    ///
    /// # Errors
    ///
    /// Returns an error if the header cannot be read
    pub async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error> {
        match self {
            VcfReader::Local(reader) => reader.describe().await,
            VcfReader::Remote(reader) => reader.describe().await,
        }
    }

    /// Reads VCF records as an async stream.
    ///
    /// # Returns
    ///
    /// A boxed async stream of VCF records
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            VcfReader::Local(reader) => reader.read_records(),
            VcfReader::Remote(reader) => reader.read_records().await,
        }
    }
}

/// A local indexed VCF reader for region-based queries.
///
/// Uses noodles' `IndexedReader::Builder` to support random-access queries using TBI/CSI indexes.
/// This is used when an index file is available and genomic region filters are present.
pub struct IndexedVcfReader {
    reader: vcf::io::IndexedReader<noodles_bgzf_vcf::io::Reader<File>>,
    header: vcf::Header,
}

impl IndexedVcfReader {
    /// Creates a new indexed VCF reader.
    ///
    /// # Arguments
    /// * `file_path` - Path to the BGZF-compressed VCF file (.vcf.gz)
    /// * `index_path` - Path to the TBI or CSI index file
    pub fn new(file_path: &str, index_path: &str) -> Result<Self, std::io::Error> {
        let index = noodles_tabix::fs::read(index_path)?;
        let mut reader = vcf::io::indexed_reader::Builder::default()
            .set_index(index)
            .build_from_path(file_path)
            .map_err(std::io::Error::other)?;
        let header = reader.read_header()?;
        Ok(Self { reader, header })
    }

    /// Returns a reference to the VCF header.
    pub fn header(&self) -> &vcf::Header {
        &self.header
    }

    /// Returns contig names from the header.
    pub fn contig_names(&self) -> Vec<String> {
        self.header
            .contigs()
            .keys()
            .map(|k| k.to_string())
            .collect()
    }

    /// Query records overlapping a genomic region.
    pub fn query(
        &mut self,
        region: &noodles_core::Region,
    ) -> Result<impl Iterator<Item = Result<Record, std::io::Error>> + '_, std::io::Error> {
        self.reader.query(&self.header, region)
    }
}

/// Estimate compressed byte sizes per region from a TBI (tabix) index.
///
/// For each region, this reads the tabix index and finds the min/max compressed
/// byte offsets across all bins for the corresponding reference sequence. The
/// difference gives a rough estimate of how many compressed bytes that region spans.
///
/// # Arguments
///
/// * `index_path` - Path to the TBI index file
/// * `regions` - Genomic regions to estimate sizes for
/// * `contig_names` - Contig names from the VCF header (in order)
/// * `contig_lengths` - Contig lengths from the VCF header (for sub-region splitting)
pub fn estimate_sizes_from_tbi(
    index_path: &str,
    regions: &[datafusion_bio_format_core::genomic_filter::GenomicRegion],
    contig_names: &[String],
    contig_lengths: &[u64],
) -> Vec<datafusion_bio_format_core::partition_balancer::RegionSizeEstimate> {
    use datafusion_bio_format_core::partition_balancer::RegionSizeEstimate;
    use noodles_csi::binning_index::BinningIndex;

    let index = match noodles_tabix::fs::read(index_path) {
        Ok(idx) => idx,
        Err(e) => {
            log::debug!("Failed to read TBI index for size estimation: {e}");
            return regions
                .iter()
                .map(|r| RegionSizeEstimate {
                    region: r.clone(),
                    estimated_bytes: 1,
                    contig_length: None,
                    unmapped_count: 0,
                    nonempty_bin_positions: Vec::new(),
                    leaf_bin_span: 0,
                })
                .collect();
        }
    };

    // Prefer index header reference names for chrom -> ref_idx mapping.
    // This avoids positional mismatches when schema/header contigs contain
    // additional sequences not present in the index.
    let index_ref_names: Vec<String> = index
        .header()
        .map(|h| {
            h.reference_sequence_names()
                .iter()
                .map(|n| n.to_string())
                .collect()
        })
        .unwrap_or_default();
    let index_name_to_idx: std::collections::HashMap<&str, usize> = index_ref_names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i))
        .collect();

    let contig_name_to_idx: std::collections::HashMap<&str, usize> = contig_names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i))
        .collect();
    let contig_length_by_name: std::collections::HashMap<&str, u64> = contig_names
        .iter()
        .enumerate()
        .filter_map(|(i, n)| {
            contig_lengths
                .get(i)
                .copied()
                .filter(|&len| len > 0)
                .map(|len| (n.as_str(), len))
        })
        .collect();

    regions
        .iter()
        .map(|region| {
            let ref_idx = index_name_to_idx
                .get(region.chrom.as_str())
                .copied()
                .or_else(|| {
                    contig_name_to_idx
                        .get(region.chrom.as_str())
                        .copied()
                        .filter(|&idx| idx < index.reference_sequences().len())
                });
            let estimated_bytes = ref_idx
                .and_then(|idx| index.reference_sequences().get(idx))
                .map(|ref_seq| {
                    let mut min_offset = u64::MAX;
                    let mut max_offset = 0u64;
                    for (_bin_id, bin) in ref_seq.bins() {
                        for chunk in bin.chunks() {
                            let start = chunk.start().compressed();
                            let end = chunk.end().compressed();
                            min_offset = min_offset.min(start);
                            max_offset = max_offset.max(end);
                        }
                    }
                    max_offset.saturating_sub(min_offset)
                })
                .unwrap_or(1);

            // Collect non-empty leaf bin positions for data-aware splitting.
            // TBI uses the same binning scheme as BAI (min_shift=14, depth=5).
            const TBI_LEAF_FIRST: usize = 4681;
            const TBI_LEAF_LAST: usize = 37448;
            const TBI_LEAF_SPAN: u64 = 16384;

            let mut nonempty_bin_positions: Vec<u64> = ref_idx
                .and_then(|idx| index.reference_sequences().get(idx))
                .map(|ref_seq| {
                    ref_seq
                        .bins()
                        .keys()
                        .copied()
                        .filter(|&bin_id| (TBI_LEAF_FIRST..=TBI_LEAF_LAST).contains(&bin_id))
                        .map(|bin_id| ((bin_id - TBI_LEAF_FIRST) as u64) * TBI_LEAF_SPAN + 1)
                        .collect()
                })
                .unwrap_or_default();
            nonempty_bin_positions.sort_unstable();

            let contig_length = contig_length_by_name
                .get(region.chrom.as_str())
                .copied()
                .or_else(|| {
                    // First try leaf bins (finest granularity).
                    nonempty_bin_positions
                        .last()
                        .map(|&max_pos| max_pos + TBI_LEAF_SPAN - 1)
                })
                .or_else(|| {
                    // Fall back to any bin level to infer length when only
                    // higher-level bins are populated (common for small
                    // coordinate ranges that don't reach leaf bins).
                    // BAI binning levels: offsets [0, 1, 9, 73, 585, 4681]
                    // with spans [2^29, 2^26, 2^23, 2^20, 2^17, 2^14].
                    const LEVEL_OFFSETS: [(usize, u64); 6] = [
                        (0, 1 << 29),
                        (1, 1 << 26),
                        (9, 1 << 23),
                        (73, 1 << 20),
                        (585, 1 << 17),
                        (4681, 1 << 14),
                    ];
                    ref_idx
                        .and_then(|idx| index.reference_sequences().get(idx))
                        .and_then(|ref_seq| {
                            ref_seq
                                .bins()
                                .keys()
                                .copied()
                                .filter_map(|bin_id| {
                                    LEVEL_OFFSETS.iter().rev().find_map(|&(offset, span)| {
                                        if bin_id >= offset
                                            && bin_id
                                                < LEVEL_OFFSETS
                                                    .iter()
                                                    .find(|&&(o, _)| o > offset)
                                                    .map_or(37449, |&(o, _)| o)
                                        {
                                            let idx_in_level = (bin_id - offset) as u64;
                                            Some((idx_in_level + 1) * span)
                                        } else {
                                            None
                                        }
                                    })
                                })
                                .max()
                        })
                });

            RegionSizeEstimate {
                region: region.clone(),
                estimated_bytes,
                contig_length,
                unmapped_count: 0,
                nonempty_bin_positions,
                leaf_bin_span: TBI_LEAF_SPAN,
            }
        })
        .collect()
}

/// Record field accessor for VCF records, used for record-level filter evaluation.
pub struct VcfRecordFields {
    /// Chromosome name
    pub chrom: Option<String>,
    /// Start position (in the output coordinate system)
    pub start: Option<u32>,
    /// End position (in the output coordinate system)
    pub end: Option<u32>,
}

impl RecordFieldAccessor for VcfRecordFields {
    fn get_string_field(&self, name: &str) -> Option<String> {
        match name {
            "chrom" => self.chrom.clone(),
            _ => None,
        }
    }

    fn get_u32_field(&self, name: &str) -> Option<u32> {
        match name {
            "start" => self.start,
            "end" => self.end,
            _ => None,
        }
    }

    fn get_f32_field(&self, _name: &str) -> Option<f32> {
        None
    }

    fn get_f64_field(&self, _name: &str) -> Option<f64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_bio_format_core::genomic_filter::GenomicRegion;
    use noodles_csi::binning_index::BinningIndex;

    fn test_tbi_path() -> String {
        let manifest = env!("CARGO_MANIFEST_DIR");
        format!("{manifest}/tests/multi_chrom.vcf.gz.tbi")
    }

    /// Read contig names from the TBI header (same as production code does).
    fn read_tbi_contig_names(tbi_path: &str) -> Vec<String> {
        let index = noodles_tabix::fs::read(tbi_path).expect("failed to read TBI");
        index
            .header()
            .map(|h| {
                h.reference_sequence_names()
                    .iter()
                    .map(|n| n.to_string())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn vcf_regions(contig_names: &[String]) -> Vec<GenomicRegion> {
        contig_names
            .iter()
            .map(|name| GenomicRegion {
                chrom: name.clone(),
                start: None,
                end: None,
                unmapped_tail: false,
            })
            .collect()
    }

    #[test]
    fn tbi_infers_contig_length_when_header_omits_it() {
        let tbi_path = test_tbi_path();
        let contig_names = read_tbi_contig_names(&tbi_path);
        assert!(!contig_names.is_empty(), "TBI should have contig names");
        let regions = vcf_regions(&contig_names);

        let estimates = estimate_sizes_from_tbi(&tbi_path, &regions, &contig_names, &[]);

        assert_eq!(estimates.len(), contig_names.len());
        for est in &estimates {
            assert!(
                est.contig_length.is_some(),
                "contig_length should be inferred from TBI for {}",
                est.region.chrom
            );
            let len = est.contig_length.unwrap();
            assert!(
                len > 10_000,
                "inferred contig length for {} should be > 10000 bp, got {}",
                est.region.chrom,
                len
            );
        }
    }

    #[test]
    fn tbi_header_length_takes_priority() {
        let tbi_path = test_tbi_path();
        let contig_names = read_tbi_contig_names(&tbi_path);
        assert!(!contig_names.is_empty(), "TBI should have contig names");
        let regions = vcf_regions(&contig_names);
        let contig_lengths: Vec<u64> = contig_names
            .iter()
            .enumerate()
            .map(|(i, _)| 999 - i as u64)
            .collect();

        let estimates =
            estimate_sizes_from_tbi(&tbi_path, &regions, &contig_names, &contig_lengths);

        assert_eq!(estimates.len(), contig_names.len());
        for (i, est) in estimates.iter().enumerate() {
            assert_eq!(
                est.contig_length,
                Some(contig_lengths[i]),
                "header length should take priority for {}",
                est.region.chrom
            );
        }
    }

    #[test]
    fn tbi_estimation_uses_index_name_mapping_even_with_misaligned_contigs() {
        let tbi_path = test_tbi_path();
        let index_contig_names = read_tbi_contig_names(&tbi_path);
        assert!(
            !index_contig_names.is_empty(),
            "TBI should have contig names"
        );
        let regions = vcf_regions(&index_contig_names);

        let baseline = estimate_sizes_from_tbi(&tbi_path, &regions, &index_contig_names, &[]);

        let mut misaligned_contigs = vec![
            "__extra_before_1".to_string(),
            "__extra_before_2".to_string(),
        ];
        misaligned_contigs.extend(index_contig_names.iter().cloned());
        let misaligned_lengths = vec![0u64; misaligned_contigs.len()];
        let with_misaligned = estimate_sizes_from_tbi(
            &tbi_path,
            &regions,
            &misaligned_contigs,
            &misaligned_lengths,
        );

        assert_eq!(baseline.len(), with_misaligned.len());
        for (base, shifted) in baseline.iter().zip(with_misaligned.iter()) {
            assert_eq!(base.region.chrom, shifted.region.chrom);
            assert_eq!(
                base.estimated_bytes, shifted.estimated_bytes,
                "estimated bytes should not depend on caller contig ordering for {}",
                base.region.chrom
            );
            assert_eq!(
                base.nonempty_bin_positions, shifted.nonempty_bin_positions,
                "non-empty bin positions should not depend on caller contig ordering for {}",
                base.region.chrom
            );
        }
    }
}
