use datafusion::prelude::SessionContext;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> datafusion::error::Result<()> {
    env_logger::init();
    let path = "/Users/mwiewior/research/git/polars-bio/tests/data/io/vcf/ensembl.vcf".to_string();
    let infos = None;
    // Create a new context with the default configuration
    let ctx = SessionContext::new();
    let table_provider =
        VcfTableProvider::new(path.clone(), infos, None, Some(Default::default()), true)?;
    ctx.register_table("vcf_table", Arc::new(table_provider))
        .expect("Failed to register table");

    let df = ctx.sql("SELECT * FROM vcf_table LIMIT 10").await?;
    df.show().await?;

    Ok(())
}
