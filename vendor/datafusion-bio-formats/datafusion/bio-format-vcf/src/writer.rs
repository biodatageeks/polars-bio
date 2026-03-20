//! Writer for VCF files with compression support
//!
//! This module provides writers for VCF files with support for:
//! - Uncompressed (plain) VCF
//! - GZIP compression
//! - BGZF compression (default, recommended)

use crate::header_builder::{build_vcf_column_header, build_vcf_header_lines};
use crate::serializer::VcfRecordLine;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use flate2::Compression;
use flate2::write::GzEncoder;
use noodles_bgzf as bgzf;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Compression type for VCF output files
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VcfCompressionType {
    /// No compression (plain text VCF)
    Plain,
    /// Standard GZIP compression
    Gzip,
    /// BGZF compression (block-gzipped, allows random access)
    /// This is the default and recommended compression format
    #[default]
    Bgzf,
}

impl VcfCompressionType {
    /// Determines compression type from file extension
    ///
    /// # Arguments
    ///
    /// * `path` - File path to analyze
    ///
    /// # Returns
    ///
    /// The detected compression type based on extension:
    /// - `.bgz` or `.bgzf` -> BGZF
    /// - `.gz` (but not `.bgz`) -> GZIP
    /// - Otherwise -> Plain
    pub fn from_path<P: AsRef<Path>>(path: P) -> Self {
        let path = path.as_ref();
        let path_str = path.to_string_lossy().to_lowercase();

        if path_str.ends_with(".bgz") || path_str.ends_with(".bgzf") {
            VcfCompressionType::Bgzf
        } else if path_str.ends_with(".gz") {
            VcfCompressionType::Gzip
        } else {
            VcfCompressionType::Plain
        }
    }
}

/// A unified writer for VCF files supporting multiple compression formats
///
/// This enum provides a single interface for writing VCF data regardless
/// of the underlying compression format. Use `VcfCompressionType::from_path()`
/// to automatically detect the appropriate format from the file extension.
pub enum VcfLocalWriter {
    /// Writer for uncompressed VCF files
    Plain(BufWriter<File>),
    /// Writer for GZIP-compressed VCF files
    Gzip(GzEncoder<BufWriter<File>>),
    /// Writer for BGZF-compressed VCF files (recommended)
    Bgzf(bgzf::Writer<BufWriter<File>>),
}

impl VcfLocalWriter {
    /// Creates a new VCF writer for the given path with automatic compression detection
    ///
    /// # Arguments
    ///
    /// * `path` - The output file path
    ///
    /// # Returns
    ///
    /// A new `VcfLocalWriter` configured for the appropriate compression format
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let compression = VcfCompressionType::from_path(&path);
        Self::with_compression(path, compression)
    }

    /// Creates a new VCF writer with explicit compression type
    ///
    /// # Arguments
    ///
    /// * `path` - The output file path
    /// * `compression` - The compression format to use
    ///
    /// # Returns
    ///
    /// A new `VcfLocalWriter` configured for the specified compression format
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created
    pub fn with_compression<P: AsRef<Path>>(
        path: P,
        compression: VcfCompressionType,
    ) -> Result<Self> {
        let file = File::create(path.as_ref()).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create output file: {e}"))
        })?;
        let buf_writer = BufWriter::new(file);

        match compression {
            VcfCompressionType::Plain => Ok(VcfLocalWriter::Plain(buf_writer)),
            VcfCompressionType::Gzip => {
                let encoder = GzEncoder::new(buf_writer, Compression::default());
                Ok(VcfLocalWriter::Gzip(encoder))
            }
            VcfCompressionType::Bgzf => {
                let bgzf_writer = bgzf::Writer::new(buf_writer);
                Ok(VcfLocalWriter::Bgzf(bgzf_writer))
            }
        }
    }

    /// Writes the VCF header to the file
    ///
    /// This must be called before writing any records.
    ///
    /// # Arguments
    ///
    /// * `schema` - The Arrow schema for determining INFO/FORMAT types
    /// * `info_fields` - Names of INFO fields
    /// * `format_fields` - Names of FORMAT fields
    /// * `sample_names` - Names of samples
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails
    pub fn write_header(
        &mut self,
        schema: &SchemaRef,
        info_fields: &[String],
        format_fields: &[String],
        sample_names: &[String],
    ) -> Result<()> {
        // Build header lines
        let header_lines =
            build_vcf_header_lines(schema, info_fields, format_fields, sample_names)?;

        // Write each header line
        for line in header_lines {
            self.write_line(&line)?;
        }

        // Write column header
        let column_header = build_vcf_column_header(sample_names);
        self.write_line(&column_header)?;

        Ok(())
    }

    /// Writes a single line to the file
    fn write_line(&mut self, line: &str) -> Result<()> {
        let line_with_newline = format!("{line}\n");
        let bytes = line_with_newline.as_bytes();

        match self {
            VcfLocalWriter::Plain(writer) => writer
                .write_all(bytes)
                .map_err(|e| DataFusionError::Execution(format!("Failed to write VCF line: {e}"))),
            VcfLocalWriter::Gzip(writer) => writer
                .write_all(bytes)
                .map_err(|e| DataFusionError::Execution(format!("Failed to write VCF line: {e}"))),
            VcfLocalWriter::Bgzf(writer) => writer
                .write_all(bytes)
                .map_err(|e| DataFusionError::Execution(format!("Failed to write VCF line: {e}"))),
        }
    }

    /// Writes a single VCF record line to the file
    ///
    /// # Arguments
    ///
    /// * `record` - The VCF record line to write
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails
    pub fn write_record(&mut self, record: &VcfRecordLine) -> Result<()> {
        self.write_line(&record.line)
    }

    /// Writes multiple VCF record lines to the file
    ///
    /// # Arguments
    ///
    /// * `records` - Slice of VCF record lines to write
    ///
    /// # Errors
    ///
    /// Returns an error if writing any record fails
    pub fn write_records(&mut self, records: &[VcfRecordLine]) -> Result<()> {
        for record in records {
            self.write_record(record)?;
        }
        Ok(())
    }

    /// Flushes the writer, ensuring all data is written to the file
    ///
    /// # Errors
    ///
    /// Returns an error if flushing fails
    pub fn flush(&mut self) -> Result<()> {
        match self {
            VcfLocalWriter::Plain(writer) => writer
                .flush()
                .map_err(|e| DataFusionError::Execution(format!("Failed to flush writer: {e}"))),
            VcfLocalWriter::Gzip(writer) => writer
                .flush()
                .map_err(|e| DataFusionError::Execution(format!("Failed to flush writer: {e}"))),
            VcfLocalWriter::Bgzf(writer) => writer
                .flush()
                .map_err(|e| DataFusionError::Execution(format!("Failed to flush writer: {e}"))),
        }
    }

    /// Finishes writing and closes the file
    ///
    /// For compressed formats, this properly finalizes the compression stream.
    /// This consumes the writer.
    ///
    /// # Errors
    ///
    /// Returns an error if finishing fails
    pub fn finish(self) -> Result<()> {
        match self {
            VcfLocalWriter::Plain(mut writer) => writer
                .flush()
                .map_err(|e| DataFusionError::Execution(format!("Failed to flush writer: {e}"))),
            VcfLocalWriter::Gzip(encoder) => {
                encoder.finish().map_err(|e| {
                    DataFusionError::Execution(format!("Failed to finish GZIP stream: {e}"))
                })?;
                Ok(())
            }
            VcfLocalWriter::Bgzf(bgzf_writer) => {
                bgzf_writer.finish().map_err(|e| {
                    DataFusionError::Execution(format!("Failed to finish BGZF stream: {e}"))
                })?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serializer::batch_to_vcf_lines;
    use datafusion::arrow::array::{Float64Array, StringArray, UInt32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::io::Read;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch() -> RecordBatch {
        let schema = create_test_schema();

        let chroms = StringArray::from(vec!["chr1"]);
        let starts = UInt32Array::from(vec![99u32]); // 0-based
        let ends = UInt32Array::from(vec![100u32]);
        let ids = StringArray::from(vec![Some("rs123")]);
        let refs = StringArray::from(vec!["A"]);
        let alts = StringArray::from(vec!["G"]);
        let quals = Float64Array::from(vec![Some(30.0)]);
        let filters = StringArray::from(vec![Some("PASS")]);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(chroms),
                Arc::new(starts),
                Arc::new(ends),
                Arc::new(ids),
                Arc::new(refs),
                Arc::new(alts),
                Arc::new(quals),
                Arc::new(filters),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_compression_type_from_path() {
        assert_eq!(
            VcfCompressionType::from_path("test.vcf"),
            VcfCompressionType::Plain
        );
        assert_eq!(
            VcfCompressionType::from_path("test.vcf.gz"),
            VcfCompressionType::Gzip
        );
        assert_eq!(
            VcfCompressionType::from_path("test.vcf.bgz"),
            VcfCompressionType::Bgzf
        );
        assert_eq!(
            VcfCompressionType::from_path("test.vcf.bgzf"),
            VcfCompressionType::Bgzf
        );
    }

    #[test]
    fn test_write_plain_vcf() -> Result<()> {
        let temp_file = NamedTempFile::with_suffix(".vcf").unwrap();
        let path = temp_file.path();

        let schema = create_test_schema();
        let batch = create_test_batch();

        // Convert batch to VCF lines
        let lines = batch_to_vcf_lines(&batch, &[], &[], &[], true)?;

        {
            let mut writer = VcfLocalWriter::new(path)?;
            writer.write_header(&schema, &[], &[], &[])?;
            writer.write_records(&lines)?;
            writer.finish()?;
        }

        // Read back and verify
        let mut content = String::new();
        let mut file = File::open(path).unwrap();
        file.read_to_string(&mut content).unwrap();

        assert!(content.contains("##fileformat=VCF"));
        assert!(content.contains("#CHROM"));
        assert!(content.contains("chr1"));
        assert!(content.contains("100")); // Position (1-based)

        Ok(())
    }

    #[test]
    fn test_write_gzip_vcf() -> Result<()> {
        let temp_file = NamedTempFile::with_suffix(".vcf.gz").unwrap();
        let path = temp_file.path();

        let schema = create_test_schema();
        let batch = create_test_batch();
        let lines = batch_to_vcf_lines(&batch, &[], &[], &[], true)?;

        {
            let mut writer = VcfLocalWriter::new(path)?;
            writer.write_header(&schema, &[], &[], &[])?;
            writer.write_records(&lines)?;
            writer.finish()?;
        }

        // Verify file exists and is not empty
        let metadata = std::fs::metadata(path).unwrap();
        assert!(metadata.len() > 0);

        Ok(())
    }
}
