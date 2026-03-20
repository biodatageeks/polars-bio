use datafusion::arrow;
use datafusion::catalog::TableProvider;
use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    println!("Starting GnomAD query test...");

    // Create session context
    let ctx = SessionContext::new();

    // Configure object storage options for GCS
    let mut object_storage_options = ObjectStorageOptions {
        chunk_size: Some(8),                           // Default chunk size in MB
        concurrent_fetches: Some(8),                   // Default concurrent fetches
        allow_anonymous: true,                         // Default to not allowing anonymous access
        enable_request_payer: false,                   // Default to not enabling request payer
        max_retries: Some(5),                          // Default max retries
        timeout: Some(300),                            // Default timeout in seconds
        compression_type: Some(CompressionType::AUTO), // Default compression type
    };
    object_storage_options.allow_anonymous = true;

    // Register the table
    let file_path = "gs://polars-bio-it/vep.vcf.bgz";
    println!("Registering table from: {file_path}");

    let table_provider = Arc::new(VcfTableProvider::new(
        file_path.to_string(),
        Some(Vec::from(["CSQ".to_string()])), // info_fields
        None,                                 // format_fields
        Some(object_storage_options.clone()),
        true, // Use 0-based coordinates (default)
    )?) as Arc<dyn TableProvider>;

    ctx.register_table("gnomad", table_provider)?;
    println!("✅ Table registered successfully!");

    // Run the query
    let sql = r#"
        SELECT *
        FROM gnomad
    "#;

    println!("Running query:");
    println!("{sql}");

    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;

    println!("Query results:");
    for batch in results {
        println!("{}", arrow::util::pretty::pretty_format_batches(&[batch])?);
    }

    Ok(())
}
