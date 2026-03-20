use async_compression::tokio::bufread::GzipDecoder;
use futures::StreamExt;
use log;
use log::debug;
use noodles::bgzf;
use noodles_bgzf::AsyncReader;
use opendal::layers::{LoggingLayer, RetryLayer, TimeoutLayer};
use opendal::services::{Azblob, Gcs, S3};
use opendal::{FuturesBytesStream, Operator};
use std::env;
use std::fmt::Display;
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;
use url::Url;

/// Configuration options for object storage operations
#[derive(Clone, Debug)]
pub struct ObjectStorageOptions {
    /// Chunk size in MB for reading data
    pub chunk_size: Option<usize>,
    /// Number of concurrent fetch operations
    pub concurrent_fetches: Option<usize>,
    /// Allow anonymous access to cloud storage
    pub allow_anonymous: bool,
    /// Enable request payer for S3
    pub enable_request_payer: bool,
    /// Maximum number of retry attempts
    pub max_retries: Option<usize>,
    /// Timeout in seconds for operations
    pub timeout: Option<usize>,
    /// Type of compression to use
    pub compression_type: Option<CompressionType>,
}

impl Display for ObjectStorageOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ObjectStorageOptions {{ chunk_size: {:?}, concurrent_fetches: {:?}, allow_anonymous: {}, enable_request_payer: {}, max_retries: {:?}, timeout: {:?}, compression_type: {:?} }}",
            self.chunk_size,
            self.concurrent_fetches,
            self.allow_anonymous,
            self.enable_request_payer,
            self.max_retries,
            self.timeout,
            self.compression_type
        )
    }
}

/// Type of compression used for data files
#[derive(Clone, Debug, PartialEq)]
pub enum CompressionType {
    /// Standard GZIP compression
    GZIP,
    /// BGZF (Block GZIP Format) compression for parallel reading
    BGZF,
    /// No compression
    NONE,
    /// Automatically detect compression type
    AUTO,
}

impl CompressionType {
    /// Creates a CompressionType from a string representation
    ///
    /// # Arguments
    ///
    /// * `compression_type` - String representing the compression type ("gz", "bgz", "none", "auto")
    ///
    /// # Panics
    ///
    /// Panics if the compression type string is not recognized
    pub fn from_string(compression_type: String) -> Self {
        match compression_type.to_lowercase().as_str() {
            "gz" => CompressionType::GZIP,
            "bgz" => CompressionType::BGZF,
            "none" => CompressionType::NONE,
            "auto" => CompressionType::AUTO,
            _ => panic!("Invalid compression type: {compression_type}"),
        }
    }
}

impl Default for ObjectStorageOptions {
    fn default() -> Self {
        ObjectStorageOptions {
            chunk_size: Some(8),                           // Default chunk size in MB
            concurrent_fetches: Some(1),                   // Default concurrent fetches
            allow_anonymous: true, // Default to not allowing anonymous access
            enable_request_payer: false, // Default to not enabling request payer
            max_retries: Some(5),  // Default max retries
            timeout: Some(300),    // Default timeout in seconds
            compression_type: Some(CompressionType::AUTO), // Default compression type
        }
    }
}
/// Type of storage backend for data files
#[derive(Debug)]
pub enum StorageType {
    /// Google Cloud Storage
    GCS,
    /// Amazon S3
    S3,
    /// Azure Blob Storage
    AZBLOB,
    /// HTTP/HTTPS endpoint
    HTTP,
    /// Local filesystem
    LOCAL,
}

impl StorageType {
    /// Creates a StorageType from a URL prefix
    ///
    /// # Arguments
    ///
    /// * `object_storage_type` - URL scheme prefix ("gs", "s3", "abfs", "local", "file", "http", "https")
    ///
    /// # Panics
    ///
    /// Panics if the storage type prefix is not recognized
    pub fn from_prefix(object_storage_type: String) -> Self {
        match object_storage_type.to_lowercase().as_str() {
            "gs" => StorageType::GCS,
            "s3" => StorageType::S3,
            "abfs" => StorageType::AZBLOB,
            "local" => StorageType::LOCAL,
            "file" => StorageType::LOCAL,
            "http" | "https" => StorageType::HTTP,
            _ => panic!("Invalid object storage type"),
        }
    }
}

fn get_file_path(file_path: String) -> String {
    //extract the file path from the file path
    let file_path = file_path
        .split("://")
        .last()
        .unwrap()
        .split('/')
        .skip(1)
        .collect::<Vec<&str>>()
        .join("/");
    //return the file path
    file_path.to_string()
}

/// Detects the compression type of a file by examining its header
///
/// # Arguments
///
/// * `file_path` - Path to the file (local or remote URL)
/// * `compression_type` - Optional compression type hint; if AUTO or None, detection will be performed
/// * `object_storage_options` - Configuration options for accessing remote files
///
/// # Returns
///
/// The detected compression type (GZIP, BGZF, or NONE)
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or read
pub async fn get_compression_type(
    file_path: String,
    compression_type: Option<CompressionType>,
    object_storage_options: ObjectStorageOptions,
) -> Result<CompressionType, opendal::Error> {
    debug!(
        "get_compression_type called with file_path: {file_path}, compression_type: {compression_type:?}"
    );
    if let Some(ct) = compression_type {
        if ct != CompressionType::AUTO {
            return Ok(ct);
        }
    }

    let storage_type = get_storage_type(file_path.clone());
    let buffer = if matches!(storage_type, StorageType::LOCAL) {
        let local_path = file_path.strip_prefix("file://").unwrap_or(&file_path);
        // For local files, read directly
        let mut file = tokio::fs::File::open(local_path).await.unwrap();
        let mut buffer = vec![0; 18];
        let n = file.read(&mut buffer).await.unwrap();
        buffer.truncate(n);
        buffer
    } else {
        // For remote files, read only the minimum bytes needed for compression detection (18 bytes)
        match get_remote_stream(file_path.clone(), object_storage_options.clone(), Some(18)).await {
            Ok(mut stream) => {
                let mut buffer = Vec::with_capacity(18);
                while let Some(chunk_result) = stream.next().await {
                    match chunk_result {
                        Ok(chunk) => {
                            buffer.extend_from_slice(&chunk);
                            if buffer.len() >= 18 {
                                break;
                            }
                        }
                        Err(_) => {
                            // If we get an error but have some data, use what we have
                            break;
                        }
                    }
                }
                buffer
            }
            Err(e) => {
                log::error!("Failed to get remote stream for compression detection: {e}");
                return Ok(CompressionType::NONE);
            }
        }
    };

    if buffer.len() < 4 {
        return Ok(CompressionType::NONE);
    }

    // GZIP magic number: 0x1f 0x8b
    if buffer.len() >= 2 && buffer[0] == 0x1f && buffer[1] == 0x8b {
        // FLG byte is at index 3
        if buffer.len() >= 10 && (buffer[3] & 0x04) != 0 {
            if buffer.len() < 12 {
                return Ok(CompressionType::GZIP); // Not enough data for BGZF check
            }
            // XLEN is at index 10, little-endian
            let xlen = u16::from_le_bytes([buffer[10], buffer[11]]);
            if buffer.len() >= 12 + xlen as usize {
                // BGZF subfield identifier is 'B' 'C'
                let mut i = 12;
                while i < 12 + xlen as usize {
                    let si1 = buffer[i];
                    let si2 = buffer[i + 1];
                    let slen = u16::from_le_bytes([buffer[i + 2], buffer[i + 3]]);
                    if si1 == b'B' && si2 == b'C' && slen == 2 {
                        return Ok(CompressionType::BGZF);
                    }
                    i += (slen + 4) as usize;
                }
            }
        }
        return Ok(CompressionType::GZIP);
    }

    Ok(CompressionType::NONE)
}

/// Creates a BGZF-decompressing async reader for a remote file
///
/// # Arguments
///
/// * `file_path` - Path to the BGZF-compressed file (local or remote URL)
/// * `object_storage_options` - Configuration options for accessing the file
///
/// # Returns
///
/// An async reader that decompresses BGZF data on the fly
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or if stream creation fails
pub async fn get_remote_stream_bgzf_async(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<AsyncReader<StreamReader<FuturesBytesStream, bytes::Bytes>>, opendal::Error> {
    let remote_stream = StreamReader::new(
        get_remote_stream(file_path.clone(), object_storage_options, None).await?,
    );
    Ok(bgzf::AsyncReader::new(remote_stream))
}

/// Creates a GZIP-decompressing async reader for a remote file
///
/// # Arguments
///
/// * `file_path` - Path to the GZIP-compressed file (local or remote URL)
/// * `object_storage_options` - Configuration options for accessing the file
///
/// # Returns
///
/// An async reader that decompresses GZIP data on the fly
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or if stream creation fails
pub async fn get_remote_stream_gz_async(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    async_compression::tokio::bufread::GzipDecoder<StreamReader<FuturesBytesStream, bytes::Bytes>>,
    opendal::Error,
> {
    let remote_stream = StreamReader::new(
        get_remote_stream(file_path.clone(), object_storage_options, None).await?,
    );
    Ok(GzipDecoder::new(remote_stream))
}

/// Determines the storage type from a file path or URL
///
/// # Arguments
///
/// * `file_path` - File path or URL to analyze
///
/// # Returns
///
/// The detected storage type (GCS, S3, AZBLOB, HTTP, or LOCAL)
pub fn get_storage_type(file_path: String) -> StorageType {
    //extract the file system prefix from the file path
    let file_system_prefix = file_path.split("://").next();
    let file_system_prefix = if file_path == file_system_prefix.unwrap() {
        None
    } else {
        file_system_prefix
    };
    match file_system_prefix {
        Some(prefix)
            if prefix.to_lowercase().starts_with("http") & is_azure_blob_url(&file_path) =>
        {
            StorageType::AZBLOB
        }
        Some(prefix) => StorageType::from_prefix(prefix.to_string()),
        None => StorageType::LOCAL,
    }
}

fn get_bucket_name(file_path: String) -> String {
    //extract the bucket name from the file path
    let bucket_name = file_path
        .split("://")
        .last()
        .unwrap()
        .split('/')
        .next()
        .unwrap();
    //return the bucket name
    bucket_name.to_string()
}

#[derive(Debug)]
struct BlobInfo {
    account: String,
    container: String,
    endpoint: String,
    relative_path: String,
}

fn extract_account_and_container(url_str: &str) -> BlobInfo {
    // 1) Parse with `url::Url`
    let url = Url::parse(url_str).unwrap();
    let scheme = url.scheme();
    let host = url.host_str().ok_or("URL is missing a host").unwrap();
    // If there’s an explicit port (e.g. emulator), include it; otherwise, empty.
    let port = match url.port() {
        Some(p) => format!("{p}"),
        None => String::new(),
    };
    let mut segments = url
        .path_segments()
        .ok_or("Unable to split path segments")
        .unwrap();
    let (account, container) = if host.ends_with(".blob.core.windows.net") {
        // For “real Azure”, the account is the subdomain before ".blob.core.windows.net"
        let account = host.trim_end_matches(".blob.core.windows.net").to_string();

        // The first path segment is the container
        let container = segments
            .next()
            .ok_or("URL is missing container segment")
            .unwrap()
            .to_string();

        (account, container)
    } else {
        // For emulator style (e.g. "127.0.0.1:10000/devstoreaccount1/dataset/"),
        // the first path segment is the account, the second is the container.

        let account = segments
            .next()
            .ok_or("URL is missing account segment")
            .unwrap()
            .to_string();
        let container = segments
            .next()
            .ok_or("URL is missing container segment")
            .unwrap()
            .to_string();

        (account, container)
    };
    let endpoint = if !host.ends_with(".blob.core.windows.net") {
        // For Azure Blob Storage, the endpoint is the full URL without the path
        format!("{scheme}://{host}:{port}/{account}")
    } else {
        format!("{scheme}://{host}:{port}")
    };
    let remaining: Vec<&str> = segments.collect();
    // Join by "/" (no leading slash). If empty, relative_path = ""
    let relative_path = if remaining.is_empty() {
        String::new()
    } else {
        remaining.join("/")
    };
    BlobInfo {
        account,
        container,
        endpoint,
        relative_path,
    }
}

fn is_azure_blob_url(url_str: &str) -> bool {
    if let Ok(url) = Url::parse(url_str) {
        if let Some(host) = url.host_str() {
            // Check if the host ends with the Azure Blob Storage domain
            if host.ends_with(".blob.core.windows.net") {
                // Ensure the path has at least two segments: container and blob
                if let Some(segments) = url.path_segments() {
                    let segments: Vec<_> = segments.collect();
                    return segments.len() >= 2;
                }
            } else if !&env::var("AZURE_ENDPOINT_URL")
                .unwrap_or("".parse().unwrap())
                .is_empty()
                && url
                    .to_string()
                    .starts_with(&env::var("AZURE_ENDPOINT_URL").unwrap())
            //FIXME: this is a workaround for Azure Blob Storage emulator
            {
                return true;
            }
        }
    }
    false
}
/// Creates a byte stream for reading from a file (local or remote)
///
/// # Arguments
///
/// * `file_path` - Path to the file (local path or remote URL)
/// * `object_storage_options` - Configuration options for accessing remote files
/// * `byte_limit` - Optional limit on number of bytes to read
///
/// # Returns
///
/// A byte stream for reading file contents
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or if the storage backend is not supported
pub async fn get_remote_stream(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
    byte_limit: Option<usize>,
) -> Result<FuturesBytesStream, opendal::Error> {
    let storage_type = get_storage_type(file_path.clone());
    let bucket_name = get_bucket_name(file_path.clone());
    let relative_file_path = get_file_path(file_path.clone());
    let chunk_size = object_storage_options.clone().chunk_size.unwrap_or(64);
    let concurrent_fetches = object_storage_options
        .clone()
        .concurrent_fetches
        .unwrap_or(8);
    let allow_anonymous = object_storage_options.allow_anonymous;
    let enable_request_payer = object_storage_options.enable_request_payer;
    let max_retries = object_storage_options.max_retries.unwrap_or(5);
    let timeout = object_storage_options.timeout.unwrap_or(300);

    match storage_type {
        StorageType::S3 => {
            log::info!(
                "Using S3 storage type with parameters: \
                bucket_name: {bucket_name}, \
                allow_anonymous: {allow_anonymous}, \
                enable_request_payer: {enable_request_payer}, \
                max_retries: {max_retries}, \
                timeout: {timeout}"
            );
            let mut builder = S3::default()
                .region(
                    &env::var("AWS_REGION").unwrap_or(
                        env::var("AWS_DEFAULT_REGION").unwrap_or(
                            S3::detect_region("https://s3.amazonaws.com", bucket_name.as_str())
                                .await
                                .unwrap_or("us-east-1".to_string()),
                        ),
                    ),
                )
                .bucket(bucket_name.as_str())
                .endpoint(&env::var("AWS_ENDPOINT_URL").unwrap_or_default());
            if allow_anonymous {
                builder = builder.disable_ec2_metadata().allow_anonymous();
            };
            if enable_request_payer {
                builder = builder.enable_request_payer();
            }
            let operator = Operator::new(builder)?
                .layer(
                    TimeoutLayer::new()
                        .with_io_timeout(std::time::Duration::from_secs(timeout as u64)),
                ) // 5 minutes
                .layer(RetryLayer::new().with_max_times(max_retries)) // Retry up to 5 times
                .layer(LoggingLayer::default())
                .finish();

            //FIXME: disable because of AWS S3 bug
            // Reduce chunk size and increase concurrency for better reliability
            // let adjusted_chunk_size = chunk_size.min(8 * 1024 * 1024); // Max 8MB chunks
            // let adjusted_concurrency = concurrent_fetches.max(4); // Min 4 concurrent fetches

            match byte_limit {
                Some(limit) => {
                    operator
                        .reader_with(relative_file_path.as_str())
                        .concurrent(1)
                        .await?
                        .into_bytes_stream(0u64..limit as u64)
                        .await
                }
                None => {
                    operator
                        .reader_with(relative_file_path.as_str())
                        .concurrent(1)
                        .await?
                        .into_bytes_stream(..)
                        .await
                }
            }
        }
        //FIXME: Currently, Azure Blob Storage does not support anonymous access
        StorageType::AZBLOB => {
            let blob_info = extract_account_and_container(&file_path);
            log::info!(
                "Using Azure Blob Storage type with parameters: \
                account_name: {}, \
                container_name: {}, \
                endpoint: {}, \
                chunk_size: {}, \
                concurrent_fetches: {}, \
                allow_anonymous: {}, \
                max_retries: {}, \
                timeout: {}",
                blob_info.account,
                blob_info.container.clone(),
                blob_info.endpoint,
                chunk_size,
                concurrent_fetches,
                allow_anonymous,
                max_retries,
                timeout,
            );

            let builder = Azblob::default()
                .root("/")
                .container(&blob_info.container)
                .endpoint(&blob_info.endpoint)
                .account_name(&env::var("AZURE_STORAGE_ACCOUNT").unwrap_or_default())
                .account_key(&env::var("AZURE_STORAGE_KEY").unwrap_or_default());
            let operator = Operator::new(builder)?
                .layer(
                    TimeoutLayer::new()
                        .with_io_timeout(std::time::Duration::from_secs(timeout as u64)),
                ) // 5 minutes
                .layer(RetryLayer::new().with_max_times(max_retries)) // Retry up to 5 times
                .layer(LoggingLayer::default())
                .finish();
            match byte_limit {
                Some(limit) => {
                    operator
                        .reader_with(blob_info.relative_path.as_str())
                        .chunk(chunk_size * 1024 * 1024)
                        .concurrent(1)
                        .await?
                        .into_bytes_stream(0u64..limit as u64)
                        .await
                }
                None => {
                    operator
                        .reader_with(blob_info.relative_path.as_str())
                        .chunk(chunk_size * 1024 * 1024)
                        .concurrent(1)
                        .await?
                        .into_bytes_stream(..)
                        .await
                }
            }
        }
        StorageType::HTTP => unimplemented!("HTTP storage type is not implemented yet"),

        StorageType::GCS => {
            log::info!(
                "Using GCS storage type with parameters: \
                bucket_name: {bucket_name}, \
                chunk_size: {chunk_size}, \
                concurrent_fetches: {concurrent_fetches}, \
                allow_anonymous: {allow_anonymous}, \
                max_retries: {max_retries}, \
                timeout: {timeout}",
            );
            let mut builder = Gcs::default().bucket(bucket_name.as_str());
            if allow_anonymous {
                builder = builder.disable_vm_metadata().allow_anonymous();
            } else if let Ok(service_account_key) = env::var("GOOGLE_APPLICATION_CREDENTIALS") {
                builder = builder.credential_path(service_account_key.as_str());
            } else {
                log::warn!(
                    "GOOGLE_APPLICATION_CREDENTIALS environment variable is not set. Using default credentials."
                );
            };
            let operator = Operator::new(builder)?
                .layer(
                    TimeoutLayer::new()
                        .with_io_timeout(std::time::Duration::from_secs(timeout as u64)),
                ) // 5 minutes
                .layer(RetryLayer::new().with_max_times(max_retries)) // Retry up to 5 times
                .layer(LoggingLayer::default())
                .finish();
            match byte_limit {
                Some(limit) => {
                    operator
                        .reader_with(relative_file_path.as_str())
                        .chunk(chunk_size * 1024 * 1024)
                        .concurrent(concurrent_fetches)
                        .await?
                        .into_bytes_stream(0u64..limit as u64)
                        .await
                }
                None => {
                    operator
                        .reader_with(relative_file_path.as_str())
                        .chunk(chunk_size * 1024 * 1024)
                        .concurrent(concurrent_fetches)
                        .await?
                        .into_bytes_stream(..)
                        .await
                }
            }
        }
        _ => panic!("Invalid object storage type"),
    }
}
