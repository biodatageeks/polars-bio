use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let vcf_file = "/tmp/homo_sapiens-chr1.vcf.bgz";

    if !std::path::Path::new(vcf_file).exists() {
        eprintln!("❌ VCF file not found: {vcf_file}");
        return Ok(());
    }

    println!("🧬 Baseline VCF Performance Test (Regular VcfTableProvider)");
    println!("📁 File: {vcf_file}");

    let metadata = std::fs::metadata(vcf_file)?;
    let file_size_mb = metadata.len() as f64 / (1024.0 * 1024.0);
    println!("💾 File size: {file_size_mb:.2} MB");
    println!();

    {
        println!("🔄 Testing regular VCF reading...");

        // Create session
        let ctx = SessionContext::new();

        // Create regular VCF table provider
        let table_provider = VcfTableProvider::new(
            vcf_file.to_string(),
            None, // No specific info fields
            None, // No specific format fields
            Some(ObjectStorageOptions::default()),
            true, // Use 0-based coordinates (default)
        )?;

        ctx.register_table("vcf_data", Arc::new(table_provider))?;

        // Test 1: Count with limit
        println!("   Running COUNT query...");
        let start = Instant::now();
        let df = ctx
            .sql("SELECT COUNT(*) as total_records FROM vcf_data LIMIT 1000000")
            .await?;
        let result = df.collect().await?;
        let count_duration = start.elapsed();

        let record_count = if let Some(batch) = result.first() {
            if let Some(array) = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
            {
                array.value(0)
            } else {
                0
            }
        } else {
            0
        };

        // Test 2: Simple projection
        println!("   Running projection query...");
        let start = Instant::now();
        let df = ctx
            .sql("SELECT chrom, start FROM vcf_data LIMIT 100000")
            .await?;
        let proj_result = df.collect().await?;
        let projection_duration = start.elapsed();
        let projection_rows = proj_result
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>();

        println!("   📊 Records: {record_count}");
        println!("   🎯 Projection rows: {projection_rows}");
        println!("   ⏱️  COUNT time: {count_duration:?}");
        println!("   🔍 Projection time: {projection_duration:?}");

        let records_per_sec = record_count as f64 / count_duration.as_secs_f64();
        println!("   🚀 Throughput: {records_per_sec:.0} records/sec");

        // Show sample data
        if let Some(batch) = proj_result.first() {
            if batch.num_rows() > 0 {
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

                println!("   📋 Sample data:");
                for i in 0..std::cmp::min(3, batch.num_rows()) {
                    println!(
                        "      {}: {} {}",
                        i + 1,
                        chrom_array.value(i),
                        start_array.value(i)
                    );
                }
            }
        }
        println!();
    }

    println!("🎉 Baseline benchmark completed!");
    println!("This shows the performance of the regular VcfTableProvider");
    println!("with MultithreadedReader for BGZF decompression.");

    Ok(())
}
