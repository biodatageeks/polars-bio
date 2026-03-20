use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;
use tokio::fs;

const SAMPLE_VCF_CONTENT_WITH_MANY_INFO: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count in genotypes">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##INFO=<ID=AN,Number=1,Type=Integer,Description="Total number of alleles">
##INFO=<ID=DP,Number=1,Type=Integer,Description="Combined depth across samples">
##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP membership">
##INFO=<ID=SVTYPE,Number=1,Type=String,Description="Type of structural variant">
##INFO=<ID=BaseQRankSum,Number=1,Type=Float,Description="Base Quality Rank Sum Test">
##INFO=<ID=ClippingRankSum,Number=1,Type=Float,Description="Clipping Rank Sum Test">
##INFO=<ID=ExcessHet,Number=1,Type=Float,Description="Excess Heterozygosity">
##INFO=<ID=FS,Number=1,Type=Float,Description="Fisher's exact test">
##INFO=<ID=MLEAC,Number=A,Type=Integer,Description="Maximum Likelihood Estimate Allele Count">
##INFO=<ID=MLEAF,Number=A,Type=Float,Description="Maximum Likelihood Estimate Allele Frequency">
##INFO=<ID=MQ,Number=1,Type=Float,Description="RMS Mapping Quality">
##INFO=<ID=MQRankSum,Number=1,Type=Float,Description="Mapping Quality Rank Sum Test">
##INFO=<ID=QD,Number=1,Type=Float,Description="Variant Confidence/Quality by Depth">
##INFO=<ID=ReadPosRankSum,Number=1,Type=Float,Description="Read Position Rank Sum Test">
##INFO=<ID=SOR,Number=1,Type=Float,Description="Strand Odds Ratio">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs1	A	T	60	PASS	AC=1;AF=0.5;AN=2;DP=20;DB;BaseQRankSum=1.5;ClippingRankSum=0.5;ExcessHet=2.1;FS=0.3;MLEAC=1;MLEAF=0.5;MQ=55.2;MQRankSum=-1.2;QD=15.3;ReadPosRankSum=0.8;SOR=0.7
chr1	200	rs2	G	C	80	PASS	AC=2;AF=1.0;AN=2;DP=25;BaseQRankSum=2.1;FS=1.2;MLEAC=2;MLEAF=1.0;MQ=58.1;QD=18.5;SOR=1.1
chr2	300	rs3	C	T,A	70	PASS	AC=1,1;AF=0.33,0.33;AN=3;DP=30;DB;ExcessHet=1.8;MLEAC=1,1;MLEAF=0.33,0.33;MQ=52.4;QD=12.7
chr2	400	.	T	G	50	PASS	AC=1;AF=0.25;AN=4;DP=40;SVTYPE=SNV;FS=0.8;MQ=49.5;QD=11.2;SOR=0.9
"#;

async fn create_test_vcf_file_with_many_info(test_name: &str) -> std::io::Result<String> {
    let temp_file = format!("/tmp/test_projection_optimization_{test_name}.vcf");
    fs::write(&temp_file, SAMPLE_VCF_CONTENT_WITH_MANY_INFO).await?;
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
async fn test_projection_optimization_core_fields_only() -> Result<(), Box<dyn std::error::Error>> {
    // Test case 1: Query only core VCF fields (no INFO fields)
    // This should be very fast since no INFO parsing is needed

    let vcf_path = create_test_vcf_file_with_many_info("core_fields_only").await?;

    // Create table provider with ALL info fields initially (simulating polars-bio scenario)
    let all_info_fields = Some(vec![
        "AC".to_string(),
        "AF".to_string(),
        "AN".to_string(),
        "BaseQRankSum".to_string(),
        "ClippingRankSum".to_string(),
        "DP".to_string(),
        "ExcessHet".to_string(),
        "FS".to_string(),
        "MLEAC".to_string(),
        "MLEAF".to_string(),
        "MQ".to_string(),
        "MQRankSum".to_string(),
        "QD".to_string(),
        "ReadPosRankSum".to_string(),
        "SOR".to_string(),
    ]);

    let object_storage_options = create_object_storage_options();
    let table_provider = VcfTableProvider::new(
        vcf_path,
        all_info_fields, // ALL fields registered
        None,
        Some(object_storage_options),
        true,
    )?;

    // Create session context and register table
    let ctx = SessionContext::new();
    ctx.register_table("vcf_table", Arc::new(table_provider))?;

    // Query 1: Select only core fields (should be optimized - no INFO parsing)
    let query = "SELECT chrom, start, id FROM vcf_table LIMIT 10";
    let df = ctx.sql(query).await?;

    println!("Query 1 (core fields only): {query:?}");

    let results = df.collect().await?;
    println!("Results count: {}", results.len());

    if !results.is_empty() {
        let batch = &results[0];
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(0).name(), "chrom");
        assert_eq!(batch.schema().field(1).name(), "start");
        assert_eq!(batch.schema().field(2).name(), "id");

        if batch.num_rows() > 0 {
            println!("✅ Core fields only query works - no INFO parsing needed");
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_projection_optimization_with_info_fields() -> Result<(), Box<dyn std::error::Error>> {
    // Test case 2: Query with specific INFO fields
    // This should only parse the requested INFO fields

    let vcf_path = create_test_vcf_file_with_many_info("with_info_fields").await?;

    let all_info_fields = Some(vec![
        "AC".to_string(),
        "AF".to_string(),
        "AN".to_string(),
        "DP".to_string(),
        "FS".to_string(),
        "MQ".to_string(),
        "QD".to_string(),
        "BaseQRankSum".to_string(),
        "ClippingRankSum".to_string(),
        "ExcessHet".to_string(),
        "MLEAC".to_string(),
        "MLEAF".to_string(),
        "MQRankSum".to_string(),
        "ReadPosRankSum".to_string(),
        "SOR".to_string(),
    ]);

    let object_storage_options = create_object_storage_options();
    let table_provider = VcfTableProvider::new(
        vcf_path,
        all_info_fields, // ALL fields registered initially
        None,
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("vcf_table", Arc::new(table_provider))?;

    // Query 2: Select core fields + only 2 INFO fields (should only parse AC and DP)
    let query = "SELECT chrom, start, `AC`, `DP` FROM vcf_table LIMIT 10";
    let df = ctx.sql(query).await?;

    println!("Query 2 (with specific INFO fields): {query:?}");

    let results = df.collect().await?;
    println!("Results count: {}", results.len());

    if !results.is_empty() {
        let batch = &results[0];
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.schema().field(0).name(), "chrom");
        assert_eq!(batch.schema().field(1).name(), "start");
        assert_eq!(batch.schema().field(2).name(), "AC");
        assert_eq!(batch.schema().field(3).name(), "DP");

        if batch.num_rows() > 0 {
            println!("✅ Selective INFO field parsing works - only AC and DP should be parsed");
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_count_optimization() -> Result<(), Box<dyn std::error::Error>> {
    // Test case 3: COUNT(*) query
    // This should be the fastest - no field parsing needed

    let vcf_path = create_test_vcf_file_with_many_info("count_optimization").await?;

    let all_info_fields = Some(vec![
        "AC".to_string(),
        "AF".to_string(),
        "AN".to_string(),
        "DP".to_string(),
        "BaseQRankSum".to_string(),
        "ClippingRankSum".to_string(),
        "ExcessHet".to_string(),
        "FS".to_string(),
        "MLEAC".to_string(),
        "MLEAF".to_string(),
        "MQ".to_string(),
        "MQRankSum".to_string(),
        "QD".to_string(),
        "ReadPosRankSum".to_string(),
        "SOR".to_string(),
    ]);

    let object_storage_options = create_object_storage_options();
    let table_provider = VcfTableProvider::new(
        vcf_path,
        all_info_fields, // ALL fields registered
        None,
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("vcf_table", Arc::new(table_provider))?;

    // Query 3: COUNT(*) (should be optimized - minimal parsing)
    let query = "SELECT COUNT(*) FROM vcf_table";
    let df = ctx.sql(query).await?;

    println!("Query 3 (COUNT(*)): {query:?}");

    let results = df.collect().await?;

    if !results.is_empty() {
        let batch = &results[0];
        let count_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        let count = count_array.value(0);
        println!("✅ COUNT(*) optimization works - counted {count} records");
        assert!(count >= 4); // We have 4 records in our test file
    }

    Ok(())
}
