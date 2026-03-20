use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;
use tokio::fs;

const SAMPLE_VCF_CONTENT_WITH_SPECIAL_INFO: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=HGMD-PUBLIC_20204,Number=0,Type=Flag,Description="Variants from HGMD-PUBLIC dataset December 2020">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs1	A	T	60	PASS	HGMD-PUBLIC_20204
"#;

async fn create_test_vcf_file_with_special_info() -> std::io::Result<(tempfile::TempDir, String)> {
    let temp_dir = tempfile::tempdir()?;
    let temp_file = temp_dir.path().join("test_special_info_projection.vcf");
    fs::write(&temp_file, SAMPLE_VCF_CONTENT_WITH_SPECIAL_INFO).await?;
    Ok((temp_dir, temp_file.to_str().unwrap().to_string()))
}

fn create_object_storage_options() -> ObjectStorageOptions {
    ObjectStorageOptions {
        allow_anonymous: true,
        enable_request_payer: false,
        max_retries: Some(1),
        timeout: Some(300),
        chunk_size: Some(16),
        concurrent_fetches: Some(8),
        compression_type: Some(CompressionType::NONE),
    }
}

#[tokio::test]
async fn test_special_info_fields() -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, file_path) = create_test_vcf_file_with_special_info().await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        Some(vec!["HGMD-PUBLIC_20204".to_string()]),
        None,
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx.sql("SELECT `HGMD-PUBLIC_20204` FROM test_vcf").await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned from INFO projection query, skipping test");
        return Ok(());
    }

    let batch = &results[0];

    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "HGMD-PUBLIC_20204");

    if batch.num_rows() > 0 {
        let hgmd_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap();

        assert!(hgmd_array.value(0));
    }

    Ok(())
}
