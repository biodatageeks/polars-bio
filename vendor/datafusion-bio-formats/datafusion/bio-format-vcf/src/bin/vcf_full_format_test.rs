use datafusion::catalog::TableProvider;
use datafusion::execution::context::SessionConfig;
use datafusion::prelude::*;
use datafusion_bio_format_core::metadata::{VCF_SAMPLE_NAMES_KEY, from_json_string};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use datafusion_bio_format_vcf::udfs::register_vcf_udfs;
use std::sync::Arc;
use std::time::Instant;

const VCF: &str = "/Users/mwiewior/research/data/polars-bio/20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr_12_22_X.recalibrated_variants.exome.vcf.gz";
const SAMPLES: &str = "/Users/mwiewior/research/data/polars-bio/samples_rand_2000.txt";

// All FORMAT fields in alphabetical order (matching schema), with GT masking
const FILTER_SQL: &str = "\
SELECT chrom, start, \"end\", id, ref, alt, qual, filter, \
  named_struct(\
    'AB', genotypes.\"AB\", \
    'AD', genotypes.\"AD\", \
    'DP', genotypes.\"DP\", \
    'GQ', genotypes.\"GQ\", \
    'GT', vcf_set_gts(genotypes.\"GT\", \
      list_and(list_and(list_gte(genotypes.\"GQ\", 10), list_gte(genotypes.\"DP\", 10)), \
               list_lte(genotypes.\"DP\", 200))), \
    'MIN_DP', genotypes.\"MIN_DP\", \
    'MQ0', genotypes.\"MQ0\", \
    'PGT', genotypes.\"PGT\", \
    'PID', genotypes.\"PID\", \
    'PL', genotypes.\"PL\", \
    'RGQ', genotypes.\"RGQ\", \
    'SB', genotypes.\"SB\"\
  ) AS genotypes \
FROM vcf_table \
WHERE qual >= 20 \
  AND list_avg(genotypes.\"GQ\") >= 15.0 \
  AND list_avg(genotypes.\"DP\") >= 15.0 \
  AND list_avg(genotypes.\"DP\") <= 150.0";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();

    let samples: Vec<String> = std::fs::read_to_string(SAMPLES)?
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();
    // Read ALL format fields (pass None)
    let source_provider = VcfTableProvider::new_with_samples(
        VCF.to_string(),
        Some(vec![]),
        None, // all FORMAT fields
        Some(samples),
        Some(ObjectStorageOptions::default()),
        true,
    )?;

    let source_schema = source_provider.schema();
    let source_meta = source_schema.metadata().clone();
    let sample_names = source_schema
        .metadata()
        .get(VCF_SAMPLE_NAMES_KEY)
        .and_then(|raw| from_json_string::<Vec<String>>(raw))
        .unwrap_or_default();

    // Get FORMAT field names from the genotypes struct
    if let Ok(genotypes_field) = source_schema.field_with_name("genotypes") {
        if let datafusion::arrow::datatypes::DataType::Struct(fields) = genotypes_field.data_type()
        {
            let field_names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
            eprintln!("FORMAT fields from schema: {field_names:?}");
        }
    }

    let format_fields: Vec<String> = vec![
        "AB", "AD", "DP", "GQ", "GT", "MIN_DP", "MQ0", "PGT", "PID", "PL", "RGQ", "SB",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    let info_fields: Vec<String> = vec![];

    let config = SessionConfig::new()
        .with_batch_size(8192)
        .with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config.clone());
    register_vcf_udfs(&ctx);
    ctx.register_table("vcf_table", Arc::new(source_provider))?;

    let vcf_path = "/tmp/vcf_full_format_output/output.vcf";
    std::fs::create_dir_all("/tmp/vcf_full_format_output")?;

    ctx.sql(&format!("CREATE VIEW vcf_filtered AS {FILTER_SQL}"))
        .await?;

    let dest_provider = VcfTableProvider::new_for_write_with_source_metadata(
        vcf_path.to_string(),
        source_schema.clone(),
        info_fields,
        format_fields,
        sample_names,
        true,
        source_meta,
    );
    ctx.register_table("vcf_dest", Arc::new(dest_provider))?;

    eprintln!("[vcf] writing to {vcf_path}...");
    let start = Instant::now();
    ctx.sql("INSERT OVERWRITE vcf_dest SELECT * FROM vcf_filtered")
        .await?
        .collect()
        .await?;
    let elapsed = start.elapsed();

    let line_count: usize = std::fs::read_to_string(vcf_path)?
        .lines()
        .filter(|l| !l.starts_with('#'))
        .count();
    eprintln!(
        "[vcf] rows={line_count}  elapsed={:.3}s",
        elapsed.as_secs_f64()
    );

    Ok(())
}
