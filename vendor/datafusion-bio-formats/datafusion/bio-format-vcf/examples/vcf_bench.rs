use std::sync::Arc;
use std::time::Instant;

use datafusion::catalog::TableProvider;
use datafusion::execution::context::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion_bio_format_core::metadata::{VCF_SAMPLE_NAMES_KEY, from_json_string};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use futures::StreamExt;

struct Args {
    file: String,
    samples_file: Option<String>,
    sample_limit: Option<usize>,
    target_partitions: usize,
    batch_size: usize,
    sql: Option<String>,
}

fn parse_args() -> Result<Args, String> {
    let mut file = None::<String>;
    let mut samples_file = None::<String>;
    let mut sample_limit = None::<usize>;
    let mut target_partitions = 4usize;
    let mut batch_size = 8192usize;
    let mut sql = None::<String>;

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--file" => {
                file = Some(it.next().ok_or("--file requires a value")?);
            }
            "--samples-file" => {
                samples_file = Some(it.next().ok_or("--samples-file requires a value")?);
            }
            "--sample-limit" => {
                let v = it.next().ok_or("--sample-limit requires a value")?;
                sample_limit = Some(
                    v.parse::<usize>()
                        .map_err(|e| format!("invalid --sample-limit '{v}': {e}"))?,
                );
            }
            "--target-partitions" => {
                let v = it.next().ok_or("--target-partitions requires a value")?;
                target_partitions = v
                    .parse::<usize>()
                    .map_err(|e| format!("invalid --target-partitions '{v}': {e}"))?;
                if target_partitions == 0 {
                    return Err("--target-partitions must be > 0".to_string());
                }
            }
            "--batch-size" => {
                let v = it.next().ok_or("--batch-size requires a value")?;
                batch_size = v
                    .parse::<usize>()
                    .map_err(|e| format!("invalid --batch-size '{v}': {e}"))?;
                if batch_size == 0 {
                    return Err("--batch-size must be > 0".to_string());
                }
            }
            "--sql" => {
                sql = Some(it.next().ok_or("--sql requires a value")?);
            }
            "--help" | "-h" => {
                return Err(
                    "usage: vcf_bench --file <path> [--samples-file <path>] [--sample-limit N] [--target-partitions N] [--batch-size N] [--sql <query>]".to_string()
                );
            }
            other => {
                return Err(format!("unknown argument '{other}'"));
            }
        }
    }

    Ok(Args {
        file: file.ok_or("--file is required")?,
        samples_file,
        sample_limit,
        target_partitions,
        batch_size,
        sql,
    })
}

fn load_sample_names_from_file(path: &str) -> Result<Vec<String>, String> {
    let contents =
        std::fs::read_to_string(path).map_err(|e| format!("failed to read '{path}': {e}"))?;
    Ok(contents
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect())
}

fn resolve_samples(
    file: &str,
    samples_file: &Option<String>,
    sample_limit: Option<usize>,
) -> datafusion::common::Result<Option<Vec<String>>> {
    if let Some(sf) = samples_file {
        let names = load_sample_names_from_file(sf)
            .map_err(|e| datafusion::common::DataFusionError::External(e.into()))?;
        return Ok(Some(names));
    }

    let Some(limit) = sample_limit else {
        return Ok(None);
    };

    let provider = VcfTableProvider::new(
        file.to_string(),
        None,
        None,
        Some(ObjectStorageOptions::default()),
        true,
    )?;

    let all_samples = provider
        .schema()
        .metadata()
        .get(VCF_SAMPLE_NAMES_KEY)
        .and_then(|raw| from_json_string::<Vec<String>>(raw))
        .unwrap_or_default();

    Ok(Some(all_samples.into_iter().take(limit).collect()))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args =
        parse_args().map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();

    let samples_to_include = resolve_samples(&args.file, &args.samples_file, args.sample_limit)?;

    // No INFO fields, only GT/GQ/DP FORMAT fields — matches polars-bio scan_vcf defaults
    let info_fields: Option<Vec<String>> = Some(vec![]);
    let format_fields: Option<Vec<String>> =
        Some(vec!["GT".to_string(), "GQ".to_string(), "DP".to_string()]);

    let provider = VcfTableProvider::new_with_samples(
        args.file.clone(),
        info_fields,
        format_fields,
        samples_to_include.clone(),
        Some(ObjectStorageOptions::default()),
        true,
    )?;

    let selected_samples_count = provider
        .schema()
        .metadata()
        .get(VCF_SAMPLE_NAMES_KEY)
        .and_then(|raw| from_json_string::<Vec<String>>(raw))
        .map(|v| v.len())
        .unwrap_or(0);

    let config = SessionConfig::new()
        .with_batch_size(args.batch_size)
        .with_target_partitions(args.target_partitions);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("vcf_table", Arc::new(provider))?;

    let sql = args.sql.as_deref().unwrap_or(
        "SELECT chrom, start, \"end\", ref, alt, qual, filter, id, genotypes FROM vcf_table",
    );

    eprintln!("file={}", args.file);
    eprintln!("sql={sql}");
    eprintln!("batch_size={}", args.batch_size);
    eprintln!("target_partitions={}", args.target_partitions);
    eprintln!("selected_samples={selected_samples_count}");

    let start = Instant::now();
    let mut stream = ctx.sql(sql).await?.execute_stream().await?;
    let mut total_rows = 0usize;
    let mut num_batches = 0usize;

    while let Some(batch_res) = stream.next().await {
        let batch = batch_res?;
        num_batches += 1;
        total_rows += batch.num_rows();
    }

    let elapsed = start.elapsed();
    let secs = elapsed.as_secs_f64();
    let rows_per_sec = if secs > 0.0 {
        total_rows as f64 / secs
    } else {
        0.0
    };

    eprintln!("---");
    eprintln!("rows={total_rows}");
    eprintln!("batches={num_batches}");
    eprintln!("elapsed={secs:.3}s");
    eprintln!("rows/sec={rows_per_sec:.0}");

    // Also print machine-parseable line to stdout
    println!(
        "partitions={}\trows={}\tbatches={}\telapsed_s={:.3}\trows_per_sec={:.0}\tsamples={}",
        args.target_partitions, total_rows, num_batches, secs, rows_per_sec, selected_samples_count
    );

    Ok(())
}
