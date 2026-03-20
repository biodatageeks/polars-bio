//! Large integration tests for index-based predicate pushdown with VCF data.
//!
//! Uses multi_chrom_large.vcf.gz: 10000 variants across 21(5000), 22(5000).

use datafusion::arrow::array::Array;
use datafusion::prelude::*;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::collections::HashSet;
use std::sync::Arc;

async fn count_rows(ctx: &SessionContext, sql: &str) -> u64 {
    let df = ctx.sql(sql).await.expect("SQL execution failed");
    let batches = df.collect().await.expect("collect failed");
    batches.iter().map(|b| b.num_rows() as u64).sum()
}

async fn collect_distinct_chroms(ctx: &SessionContext, sql: &str) -> HashSet<String> {
    let df = ctx.sql(sql).await.expect("SQL execution failed");
    let batches = df.collect().await.expect("collect failed");
    let mut chroms = HashSet::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("expected StringArray");
        for i in 0..col.len() {
            if !col.is_null(i) {
                chroms.insert(col.value(i).to_string());
            }
        }
    }
    chroms
}

async fn setup_vcf_ctx() -> datafusion::error::Result<SessionContext> {
    let ctx = SessionContext::new();
    let provider = VcfTableProvider::new(
        "tests/multi_chrom_large.vcf.gz".to_string(),
        None,
        None,
        None,
        true,
    )?;
    ctx.register_table("vcf", Arc::new(provider))?;
    Ok(ctx)
}

#[tokio::test]
async fn test_vcf_single_region_query_large() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let chroms =
        collect_distinct_chroms(&ctx, "SELECT DISTINCT chrom FROM vcf WHERE chrom = '21'").await;
    assert_eq!(chroms.len(), 1);
    assert!(chroms.contains("21"));

    let count = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '21'").await;
    assert_eq!(count, 5000, "Expected 5000 variants on chr 21");

    Ok(())
}

#[tokio::test]
async fn test_vcf_multi_chromosome_query_large() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let chroms = collect_distinct_chroms(
        &ctx,
        "SELECT DISTINCT chrom FROM vcf WHERE chrom IN ('21', '22')",
    )
    .await;
    assert!(chroms.contains("21"));
    assert!(chroms.contains("22"));

    let count = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom IN ('21', '22')").await;
    assert_eq!(count, 5000 + 5000, "Expected 10000 variants on chr 21 + 22");

    Ok(())
}

#[tokio::test]
async fn test_vcf_full_scan_total_count_large() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM vcf").await;
    assert_eq!(total, 10000, "Expected 10000 total variants");

    let chr21 = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '21'").await;
    let chr22 = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '22'").await;

    assert_eq!(total, chr21 + chr22);

    Ok(())
}

#[tokio::test]
async fn test_vcf_record_level_filter_large() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM vcf").await;
    let filtered = count_rows(&ctx, "SELECT chrom FROM vcf WHERE qual >= 50").await;

    assert!(filtered > 0);
    assert!(filtered < total);

    Ok(())
}

#[tokio::test]
async fn test_vcf_combined_filters_large() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let chr21_total = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '21'").await;
    let combined = count_rows(
        &ctx,
        "SELECT chrom FROM vcf WHERE chrom = '21' AND qual >= 50",
    )
    .await;

    assert!(combined > 0);
    assert!(combined <= chr21_total);

    Ok(())
}

#[tokio::test]
async fn test_vcf_region_with_positions_large() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let chr21_total = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '21'").await;

    let region_count = count_rows(
        &ctx,
        "SELECT chrom FROM vcf WHERE chrom = '21' AND start >= 5009999 AND start <= 5029999",
    )
    .await;

    assert!(region_count > 0);
    assert!(region_count < chr21_total);

    Ok(())
}

#[tokio::test]
async fn test_vcf_indexed_correctness_large() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let indexed_count = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '21'").await;

    let df = ctx.sql("SELECT chrom FROM vcf").await?;
    let batches = df.collect().await?;
    let mut manual_count: u64 = 0;
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("expected StringArray");
        for i in 0..col.len() {
            if !col.is_null(i) && col.value(i) == "21" {
                manual_count += 1;
            }
        }
    }

    assert_eq!(indexed_count, manual_count);

    Ok(())
}
