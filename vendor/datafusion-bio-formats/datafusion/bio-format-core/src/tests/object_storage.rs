use crate::object_storage::{CompressionType, ObjectStorageOptions, get_compression_type};
use std::io::Write;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_get_compression_type_gzip() {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(&[0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff])
        .unwrap();
    let path = format!("file://{}", file.path().to_str().unwrap());

    let compression_type = get_compression_type(path, None, ObjectStorageOptions::default()).await;
    assert_eq!(compression_type.unwrap(), CompressionType::GZIP);
}

#[tokio::test]
async fn test_get_compression_type_bgzf() {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(&[
        0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02,
        0x00, 0x00, 0x00,
    ])
    .unwrap();
    let path = format!("file://{}", file.path().to_str().unwrap());

    let compression_type = get_compression_type(path, None, ObjectStorageOptions::default()).await;
    assert_eq!(compression_type.unwrap(), CompressionType::BGZF);
}

#[tokio::test]
async fn test_get_compression_type_none() {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(b"this is not compressed").unwrap();
    let path = format!("file://{}", file.path().to_str().unwrap());

    let compression_type = get_compression_type(path, None, ObjectStorageOptions::default()).await;
    assert_eq!(compression_type.unwrap(), CompressionType::NONE);
}

#[tokio::test]
async fn test_get_compression_type_empty() {
    let file = NamedTempFile::new().unwrap();
    let path = format!("file://{}", file.path().to_str().unwrap());

    let compression_type = get_compression_type(path, None, ObjectStorageOptions::default()).await;
    assert_eq!(compression_type.unwrap(), CompressionType::NONE);
}
