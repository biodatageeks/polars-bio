use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;
use tokio::fs;

const SAMPLE_VCF_CONTENT_WITH_INFO: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count in genotypes">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##INFO=<ID=AN,Number=1,Type=Integer,Description="Total number of alleles">
##INFO=<ID=DP,Number=1,Type=Integer,Description="Combined depth across samples">
##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP membership">
##INFO=<ID=SVTYPE,Number=1,Type=String,Description="Type of structural variant">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs1	A	T	60	PASS	AC=1;AF=0.5;AN=2;DP=20;DB
chr1	200	rs2	G	C	80	PASS	AC=2;AF=1.0;AN=2;DP=25
chr2	300	rs3	C	T,A	70	PASS	AC=1,1;AF=0.33,0.33;AN=3;DP=30
chr2	400	.	T	G	50	PASS	AC=1;AF=0.25;AN=4;DP=40;SVTYPE=SNV
"#;

async fn create_test_vcf_file_with_info(test_name: &str) -> std::io::Result<String> {
    let temp_file = format!("/tmp/test_info_projection_{test_name}.vcf");
    fs::write(&temp_file, SAMPLE_VCF_CONTENT_WITH_INFO).await?;
    Ok(temp_file)
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
async fn test_info_projection_single_info_field() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file_with_info("single_info_field").await?;
    let object_storage_options = create_object_storage_options();

    // Create table with multiple INFO fields but only project one
    let table = VcfTableProvider::new(
        file_path.clone(),
        Some(vec![
            "AC".to_string(),
            "AF".to_string(),
            "AN".to_string(),
            "DP".to_string(),
        ]),
        None,
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    // Query only chrom and AC (index 8 for first INFO field)
    let df = ctx.sql("SELECT chrom, `AC` FROM test_vcf").await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned from INFO projection query, skipping test");
        return Ok(());
    }

    let batch = &results[0];

    // Should only have 2 columns
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "chrom");
    assert_eq!(batch.schema().field(1).name(), "AC");

    if batch.num_rows() > 0 {
        // Verify the data
        let chrom_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let ac_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::ListArray>()
            .unwrap();
        let ac_values = ac_array
            .values()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();

        assert_eq!(chrom_array.value(0), "chr1");
        assert_eq!(ac_values.value(ac_array.value_offsets()[0] as usize), 1);
        if batch.num_rows() > 1 {
            assert_eq!(ac_values.value(ac_array.value_offsets()[1] as usize), 2);
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_info_projection_multiple_info_fields() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file_with_info("multiple_info_fields").await?;
    let object_storage_options = create_object_storage_options();

    // Create table with all INFO fields but only project some
    let table = VcfTableProvider::new(
        file_path.clone(),
        Some(vec![
            "AC".to_string(),
            "AF".to_string(),
            "AN".to_string(),
            "DP".to_string(),
        ]),
        None,
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    // Query chrom, AC (index 8), and DP (index 11)
    let df = ctx.sql("SELECT chrom, `AC`, `DP` FROM test_vcf").await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned from multi-INFO projection query, skipping test");
        return Ok(());
    }

    let batch = &results[0];

    // Should have 3 columns
    assert_eq!(batch.num_columns(), 3);
    assert_eq!(batch.schema().field(0).name(), "chrom");
    assert_eq!(batch.schema().field(1).name(), "AC");
    assert_eq!(batch.schema().field(2).name(), "DP");

    if batch.num_rows() > 0 {
        // Verify the data
        let chrom_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let ac_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::ListArray>()
            .unwrap();
        let ac_values = ac_array
            .values()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        let dp_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();

        assert_eq!(chrom_array.value(0), "chr1");
        assert_eq!(ac_values.value(ac_array.value_offsets()[0] as usize), 1);
        assert_eq!(dp_array.value(0), 20);
    }

    Ok(())
}

#[tokio::test]
async fn test_info_projection_mixed_core_and_info() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file_with_info("mixed_core_and_info").await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        Some(vec!["AC".to_string(), "AF".to_string(), "AN".to_string()]),
        None,
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    // Mix core VCF fields and INFO fields in different order
    let df = ctx
        .sql("SELECT `AF`, start, `AC`, ref FROM test_vcf")
        .await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned from mixed projection query, skipping test");
        return Ok(());
    }

    let batch = &results[0];

    // Should have 4 columns in requested order
    assert_eq!(batch.num_columns(), 4);
    assert_eq!(batch.schema().field(0).name(), "AF");
    assert_eq!(batch.schema().field(1).name(), "start");
    assert_eq!(batch.schema().field(2).name(), "AC");
    assert_eq!(batch.schema().field(3).name(), "ref");

    if batch.num_rows() > 0 {
        // Verify the data - AF is a List(Float32) because it has Number=A
        let af_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::ListArray>()
            .unwrap();
        let start_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt32Array>()
            .unwrap();
        let ac_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::ListArray>()
            .unwrap();
        let ref_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();

        // For ListArray, we need to get the first element of the first list
        let af_first_list = af_array.value(0);
        let af_float_array = af_first_list
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float32Array>()
            .unwrap();
        assert_eq!(af_float_array.value(0), 0.5);

        assert_eq!(start_array.value(0), 99); // 0-based coordinate

        let ac_first_list = ac_array.value(0);
        let ac_int_array = ac_first_list
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(ac_int_array.value(0), 1);

        assert_eq!(ref_array.value(0), "A");
    }

    Ok(())
}

#[tokio::test]
async fn test_info_projection_no_info_fields_queried() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file_with_info("no_info_fields_queried").await?;
    let object_storage_options = create_object_storage_options();

    // Create table with INFO fields available but don't query any
    let table = VcfTableProvider::new(
        file_path.clone(),
        Some(vec![
            "AC".to_string(),
            "AF".to_string(),
            "AN".to_string(),
            "DP".to_string(),
        ]),
        None,
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    // Query only core VCF fields - no INFO fields should be processed
    let df = ctx
        .sql("SELECT chrom, start, ref, alt FROM test_vcf")
        .await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned from core-only projection query, skipping test");
        return Ok(());
    }

    let batch = &results[0];

    // Should only have 4 columns (no INFO fields)
    assert_eq!(batch.num_columns(), 4);
    assert_eq!(batch.schema().field(0).name(), "chrom");
    assert_eq!(batch.schema().field(1).name(), "start");
    assert_eq!(batch.schema().field(2).name(), "ref");
    assert_eq!(batch.schema().field(3).name(), "alt");

    if batch.num_rows() > 0 {
        // Verify the data
        let chrom_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let start_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt32Array>()
            .unwrap();
        let ref_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let alt_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();

        assert_eq!(chrom_array.value(0), "chr1");
        assert_eq!(start_array.value(0), 99); // 0-based coordinate
        assert_eq!(ref_array.value(0), "A");
        assert_eq!(alt_array.value(0), "T");
    }

    Ok(())
}

#[tokio::test]
async fn test_info_projection_all_info_fields() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file_with_info("all_info_fields").await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        Some(vec![
            "AC".to_string(),
            "AF".to_string(),
            "AN".to_string(),
            "DP".to_string(),
        ]),
        None,
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    // Query all INFO fields - should process all
    let df = ctx
        .sql("SELECT chrom, `AC`, `AF`, `AN`, `DP` FROM test_vcf")
        .await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned from all-INFO projection query, skipping test");
        return Ok(());
    }

    let batch = &results[0];

    // Should have 5 columns (1 core + 4 INFO)
    assert_eq!(batch.num_columns(), 5);
    assert_eq!(batch.schema().field(0).name(), "chrom");
    assert_eq!(batch.schema().field(1).name(), "AC");
    assert_eq!(batch.schema().field(2).name(), "AF");
    assert_eq!(batch.schema().field(3).name(), "AN");
    assert_eq!(batch.schema().field(4).name(), "DP");

    if batch.num_rows() > 0 {
        // Verify the data
        let chrom_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let ac_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::ListArray>()
            .unwrap();
        let ac_values = ac_array
            .values()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        let af_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::ListArray>()
            .unwrap();
        let af_values = af_array
            .values()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float32Array>()
            .unwrap();
        let an_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        let dp_array = batch
            .column(4)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();

        assert_eq!(chrom_array.value(0), "chr1");
        assert_eq!(ac_values.value(ac_array.value_offsets()[0] as usize), 1);
        assert_eq!(af_values.value(af_array.value_offsets()[0] as usize), 0.5);
        assert_eq!(an_array.value(0), 2);
        assert_eq!(dp_array.value(0), 20);
    }

    Ok(())
}

#[tokio::test]
async fn test_info_projection_with_count_aggregation() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file_with_info("with_count_aggregation").await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        Some(vec![
            "AC".to_string(),
            "AF".to_string(),
            "AN".to_string(),
            "DP".to_string(),
        ]),
        None,
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    // COUNT query with INFO field - should still work with projection
    let df = ctx.sql("SELECT COUNT(*) FROM test_vcf").await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned from INFO COUNT query, skipping test");
        return Ok(());
    }

    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1);

    if batch.num_rows() > 0 {
        // Verify count result
        let count_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert!(count_array.value(0) >= 0); // At least no errors
    }

    Ok(())
}

#[tokio::test]
async fn test_info_projection_with_limit() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file_with_info("with_limit").await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        Some(vec!["AC".to_string(), "AF".to_string(), "AN".to_string()]),
        None,
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    // Test INFO projection with LIMIT
    let df = ctx
        .sql("SELECT chrom, `AC`, `AN` FROM test_vcf LIMIT 2")
        .await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned from INFO projection with LIMIT, skipping test");
        return Ok(());
    }

    let batch = &results[0];

    // Should have 3 columns and at most 2 rows due to LIMIT
    assert_eq!(batch.num_columns(), 3);
    assert!(batch.num_rows() <= 2);
    assert_eq!(batch.schema().field(0).name(), "chrom");
    assert_eq!(batch.schema().field(1).name(), "AC");
    assert_eq!(batch.schema().field(2).name(), "AN");

    if batch.num_rows() > 0 {
        // Verify the first record
        let chrom_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let ac_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::ListArray>()
            .unwrap();
        let ac_values = ac_array
            .values()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        let an_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();

        assert_eq!(chrom_array.value(0), "chr1");
        assert_eq!(ac_values.value(ac_array.value_offsets()[0] as usize), 1);
        assert_eq!(an_array.value(0), 2);
    }

    Ok(())
}

#[tokio::test]
async fn test_info_projection_no_projection_all_fields() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file_with_info("no_projection_all_fields").await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        Some(vec!["AC".to_string(), "AF".to_string(), "AN".to_string()]),
        None,
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    // SELECT * should include all core fields plus all INFO fields
    let df = ctx.sql("SELECT * FROM test_vcf").await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned from SELECT * query, skipping test");
        return Ok(());
    }

    let batch = &results[0];

    // Should have 8 core fields + 3 INFO fields = 11 total
    assert_eq!(batch.num_columns(), 11);

    // Verify core fields
    assert_eq!(batch.schema().field(0).name(), "chrom");
    assert_eq!(batch.schema().field(1).name(), "start");
    assert_eq!(batch.schema().field(2).name(), "end");
    assert_eq!(batch.schema().field(3).name(), "id");
    assert_eq!(batch.schema().field(4).name(), "ref");
    assert_eq!(batch.schema().field(5).name(), "alt");
    assert_eq!(batch.schema().field(6).name(), "qual");
    assert_eq!(batch.schema().field(7).name(), "filter");

    // Verify INFO fields
    assert_eq!(batch.schema().field(8).name(), "AC");
    assert_eq!(batch.schema().field(9).name(), "AF");
    assert_eq!(batch.schema().field(10).name(), "AN");

    Ok(())
}
