//! Integration tests for LIMIT pushdown and indexed projection optimizations.
//!
//! Tests verify:
//! - LIMIT is enforced in sequential scan paths (plain VCF)
//! - LIMIT is enforced in indexed scan paths (VCF.gz with TBI)
//! - Projection pushdown works correctly in indexed scans
//! - Edge cases: LIMIT 1, LIMIT > total_rows

use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;
use tokio::fs;

const SAMPLE_VCF_CONTENT: &str = r#"##fileformat=VCFv4.2
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##contig=<ID=chr1>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs1	A	G	30	PASS	DP=10
chr1	200	rs2	C	T	40	PASS	DP=20
chr1	300	rs3	G	A	50	PASS	DP=15
chr1	400	rs4	T	C	60	PASS	DP=25
chr1	500	rs5	A	T	70	PASS	DP=30
chr1	600	rs6	C	G	80	PASS	DP=35
chr1	700	rs7	G	T	90	PASS	DP=40
chr1	800	rs8	T	A	100	PASS	DP=45
chr1	900	rs9	A	C	110	PASS	DP=50
chr1	1000	rs10	C	A	120	PASS	DP=55
"#;

async fn create_test_vcf_file(test_name: &str) -> std::io::Result<String> {
    let temp_file = format!("/tmp/test_limit_{test_name}.vcf");
    fs::write(&temp_file, SAMPLE_VCF_CONTENT).await?;
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

/// Helper: execute a SQL query and return total row count across all batches.
async fn count_rows(ctx: &SessionContext, sql: &str) -> u64 {
    let df = ctx.sql(sql).await.expect("SQL execution failed");
    let batches = df.collect().await.expect("collect failed");
    batches.iter().map(|b| b.num_rows() as u64).sum()
}

// ============================================================================
// Sequential scan LIMIT tests (plain VCF)
// ============================================================================

#[tokio::test]
async fn test_limit_sequential_basic() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("seq_basic").await?;

    let table = VcfTableProvider::new(
        file_path.clone(),
        None,
        None,
        Some(create_object_storage_options()),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("vcf", Arc::new(table))?;

    let count = count_rows(&ctx, "SELECT chrom FROM vcf LIMIT 5").await;
    assert_eq!(count, 5, "LIMIT 5 should return exactly 5 rows");

    Ok(())
}

#[tokio::test]
async fn test_limit_sequential_one() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("seq_one").await?;

    let table = VcfTableProvider::new(
        file_path.clone(),
        None,
        None,
        Some(create_object_storage_options()),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("vcf", Arc::new(table))?;

    let count = count_rows(&ctx, "SELECT chrom FROM vcf LIMIT 1").await;
    assert_eq!(count, 1, "LIMIT 1 should return exactly 1 row");

    Ok(())
}

#[tokio::test]
async fn test_limit_sequential_exceeds_total() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("seq_exceeds").await?;

    let table = VcfTableProvider::new(
        file_path.clone(),
        None,
        None,
        Some(create_object_storage_options()),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("vcf", Arc::new(table))?;

    let count = count_rows(&ctx, "SELECT chrom FROM vcf LIMIT 9999").await;
    assert_eq!(count, 10, "LIMIT > total rows should return all 10 rows");

    Ok(())
}

#[tokio::test]
async fn test_limit_sequential_with_projection() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("seq_proj").await?;

    let table = VcfTableProvider::new(
        file_path.clone(),
        Some(vec!["DP".to_string()]),
        None,
        Some(create_object_storage_options()),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("vcf", Arc::new(table))?;

    // Select specific columns with LIMIT
    let df = ctx.sql("SELECT chrom, start FROM vcf LIMIT 3").await?;
    let batches = df.collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    assert_eq!(
        total_rows, 3,
        "LIMIT 3 with projection should return 3 rows"
    );
    // Verify we only have the projected columns
    assert_eq!(
        batches[0].num_columns(),
        2,
        "Should have 2 columns (chrom, start)"
    );

    Ok(())
}

/// Regression test: LIMIT equal to batch_size must not drop the final batch.
/// When LIMIT == batch_size, the limit check breaks before the modulo-based
/// flush, and the tail flush `record_num % batch_size != 0` is also false,
/// silently dropping all accumulated rows.
#[tokio::test]
async fn test_limit_at_batch_boundary() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("batch_boundary").await?;

    let table = VcfTableProvider::new(
        file_path.clone(),
        None,
        None,
        Some(create_object_storage_options()),
        true,
    )?;

    // Set batch_size = 5 so LIMIT 5 hits the exact boundary
    let config = SessionConfig::new().with_batch_size(5);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("vcf", Arc::new(table))?;

    let count = count_rows(&ctx, "SELECT chrom FROM vcf LIMIT 5").await;
    assert_eq!(
        count, 5,
        "LIMIT equal to batch_size must return all rows, not drop the batch"
    );

    Ok(())
}

/// Regression test: LIMIT equal to 2*batch_size — verifies the second batch boundary.
#[tokio::test]
async fn test_limit_at_double_batch_boundary() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("double_batch_boundary").await?;

    let table = VcfTableProvider::new(
        file_path.clone(),
        None,
        None,
        Some(create_object_storage_options()),
        true,
    )?;

    // 10 records total, batch_size = 5, LIMIT 10 = 2 * batch_size
    let config = SessionConfig::new().with_batch_size(5);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("vcf", Arc::new(table))?;

    let count = count_rows(&ctx, "SELECT chrom FROM vcf LIMIT 10").await;
    assert_eq!(
        count, 10,
        "LIMIT equal to 2*batch_size must return all 10 rows"
    );

    Ok(())
}

// ============================================================================
// Indexed scan LIMIT tests (VCF.gz with TBI)
// ============================================================================

/// Create a session context with the multi_chrom.vcf.gz test file registered.
async fn setup_indexed_ctx() -> datafusion::error::Result<SessionContext> {
    let ctx = SessionContext::new();
    let provider = VcfTableProvider::new(
        "tests/multi_chrom.vcf.gz".to_string(),
        None,
        None,
        None,
        true,
    )?;
    ctx.register_table("vcf", Arc::new(provider))?;
    Ok(ctx)
}

#[tokio::test]
async fn test_limit_indexed_basic() -> datafusion::error::Result<()> {
    let ctx = setup_indexed_ctx().await?;

    let count = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '21' LIMIT 5").await;
    assert_eq!(count, 5, "Indexed LIMIT 5 should return exactly 5 rows");

    Ok(())
}

#[tokio::test]
async fn test_limit_indexed_one() -> datafusion::error::Result<()> {
    let ctx = setup_indexed_ctx().await?;

    let count = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '22' LIMIT 1").await;
    assert_eq!(count, 1, "Indexed LIMIT 1 should return exactly 1 row");

    Ok(())
}

#[tokio::test]
async fn test_limit_indexed_exceeds_total() -> datafusion::error::Result<()> {
    let ctx = setup_indexed_ctx().await?;

    // chr 21 has 500 variants
    let count = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '21' LIMIT 9999").await;
    assert_eq!(
        count, 500,
        "Indexed LIMIT > total should return all 500 chr21 variants"
    );

    Ok(())
}

#[tokio::test]
async fn test_limit_indexed_across_chromosomes() -> datafusion::error::Result<()> {
    let ctx = setup_indexed_ctx().await?;

    // Full scan with limit
    let count = count_rows(&ctx, "SELECT chrom FROM vcf LIMIT 10").await;
    assert_eq!(
        count, 10,
        "Full-scan LIMIT 10 should return exactly 10 rows"
    );

    Ok(())
}

// ============================================================================
// Indexed projection pushdown tests
// ============================================================================

#[tokio::test]
async fn test_indexed_projection_single_column() -> datafusion::error::Result<()> {
    let ctx = setup_indexed_ctx().await?;

    let df = ctx.sql("SELECT chrom FROM vcf WHERE chrom = '21'").await?;
    let batches = df.collect().await?;

    // Should only have 1 column
    for batch in &batches {
        assert_eq!(
            batch.num_columns(),
            1,
            "Indexed scan with single column projection should return 1 column"
        );
    }

    let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    assert_eq!(
        total_rows, 500,
        "Should return all 500 chr21 variants with single column projection"
    );

    Ok(())
}

#[tokio::test]
async fn test_indexed_projection_two_columns() -> datafusion::error::Result<()> {
    let ctx = setup_indexed_ctx().await?;

    let df = ctx
        .sql("SELECT chrom, start FROM vcf WHERE chrom = '22'")
        .await?;
    let batches = df.collect().await?;

    for batch in &batches {
        assert_eq!(
            batch.num_columns(),
            2,
            "Indexed scan with two-column projection should return 2 columns"
        );
    }

    let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    assert_eq!(
        total_rows, 500,
        "Should return all 500 chr22 variants with two-column projection"
    );

    Ok(())
}

#[tokio::test]
async fn test_indexed_projection_with_limit() -> datafusion::error::Result<()> {
    let ctx = setup_indexed_ctx().await?;

    let df = ctx
        .sql("SELECT chrom, start FROM vcf WHERE chrom = '21' LIMIT 3")
        .await?;
    let batches = df.collect().await?;

    let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    assert_eq!(
        total_rows, 3,
        "Indexed projection + LIMIT should return exactly 3 rows"
    );

    for batch in &batches {
        assert_eq!(batch.num_columns(), 2, "Should have 2 projected columns");
    }

    Ok(())
}
