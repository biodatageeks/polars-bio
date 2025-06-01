use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::PyArrowType;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use exon::ExonSession;
use log::info;
use tokio::runtime::Runtime;
use tracing::debug;

use crate::context::PyBioSessionContext;
use crate::option::{InputFormat, ReadOptions, VcfReadOptions};

const MAX_IN_MEMORY_ROWS: usize = 1024 * 1024;

pub(crate) fn register_frame(
    py_ctx: &PyBioSessionContext,
    df: PyArrowType<ArrowArrayStreamReader>,
    table_name: String,
) {
    let batches =
        df.0.collect::<Result<Vec<RecordBatch>, ArrowError>>()
            .unwrap();
    let schema = batches[0].schema();
    let ctx = &py_ctx.ctx;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let table_source = MemTable::try_new(schema, vec![batches]).unwrap();
    ctx.session.deregister_table(&table_name).unwrap();
    ctx.session
        .register_table(&table_name, Arc::new(table_source))
        .unwrap();
    let df = rt.block_on(ctx.session.table(&table_name)).unwrap();
    let table_size = rt.block_on(df.clone().count()).unwrap();
    if table_size > MAX_IN_MEMORY_ROWS {
        let path = format!("{}/{}.parquet", py_ctx.catalog_dir, table_name);
        ctx.session.deregister_table(&table_name).unwrap();
        rt.block_on(df.write_parquet(&path, DataFrameWriteOptions::new(), None))
            .unwrap();
        ctx.session.deregister_table(&table_name).unwrap();
        rt.block_on(register_table(
            ctx,
            &path,
            &table_name,
            InputFormat::Parquet,
            None,
        ));
    }
}

pub(crate) fn get_input_format(path: &str) -> InputFormat {
    let path = path.to_lowercase();
    if path.ends_with(".parquet") {
        InputFormat::Parquet
    } else if path.ends_with(".csv") {
        InputFormat::Csv
    } else if path.ends_with(".bed") {
        InputFormat::Bed
    } else if path.ends_with(".vcf") || path.ends_with(".vcf.gz") || path.ends_with(".vcf.bgz") {
        InputFormat::Vcf
    } else {
        panic!("Unsupported format")
    }
}

pub(crate) async fn register_table(
    ctx: &ExonSession,
    path: &str,
    table_name: &str,
    format: InputFormat,
    read_options: Option<ReadOptions>,
) -> String {
    ctx.session.deregister_table(table_name).unwrap();
    match format {
        InputFormat::Parquet => ctx
            .session
            .register_parquet(table_name, path, ParquetReadOptions::new())
            .await
            .unwrap(),
        InputFormat::Csv => {
            let csv_read_options = CsvReadOptions::new() //FIXME: expose
                .delimiter(b',')
                .has_header(true);
            ctx.session
                .register_csv(table_name, path, csv_read_options)
                .await
                .unwrap()
        },
        InputFormat::Vcf => {
            let vcf_read_options = match &read_options {
                Some(options) => match options.clone().vcf_read_options {
                    Some(vcf_read_options) => vcf_read_options,
                    _ => VcfReadOptions::default(),
                },
                _ => VcfReadOptions::default(),
            };
            info!(
                "Registering VCF table {} with options: {:?}",
                table_name, vcf_read_options
            );
            let table_provider = VcfTableProvider::new(
                path.to_string(),
                vcf_read_options.info_fields,
                vcf_read_options.format_fields,
                vcf_read_options.thread_num,
                vcf_read_options.object_storage_options.clone(),
            )
            .unwrap();
            ctx.session
                .register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register VCF table");
        },
        InputFormat::Bam
        | InputFormat::Cram
        | InputFormat::Fastq
        | InputFormat::Fasta
        | InputFormat::Bed
        | InputFormat::Gff
        | InputFormat::Gtf => ctx
            .register_exon_table(table_name, path, &format.to_string())
            .await
            .unwrap(),
        InputFormat::IndexedVcf | InputFormat::IndexedBam => {
            todo!("Indexed formats are not supported")
        },
    };
    table_name.to_string()
}

pub(crate) fn maybe_register_table(
    df_path_or_table: String,
    default_table: &String,
    read_options: Option<ReadOptions>,
    ctx: &ExonSession,
    rt: &Runtime,
) -> String {
    let ext: Vec<&str> = df_path_or_table.split('.').collect();
    debug!("ext: {:?}", ext);
    if ext.len() == 1 {
        return df_path_or_table;
    }
    match ext.last() {
        Some(_ext) => {
            rt.block_on(register_table(
                ctx,
                &df_path_or_table,
                default_table,
                get_input_format(&df_path_or_table),
                read_options,
            ));
            default_table.to_string()
        },
        _ => df_path_or_table,
    }
    .to_string()
}
