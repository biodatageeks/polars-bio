use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::PyArrowType;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext};
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use datafusion_bio_format_bed::table_provider::{BEDFields, BedTableProvider};
use datafusion_bio_format_fasta::table_provider::FastaTableProvider;
use datafusion_bio_format_fastq::bgzf_parallel_reader::BgzfFastqTableProvider as BgzfParallelFastqTableProvider;
use datafusion_bio_format_fastq::table_provider::FastqTableProvider;
use datafusion_bio_format_gff::table_provider::GffTableProvider;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use log::info;
use tokio::runtime::Runtime;
use tracing::debug;

use crate::context::PyBioSessionContext;
use crate::option::{
    BamReadOptions, BedReadOptions, FastaReadOptions, FastqReadOptions, GffReadOptions,
    InputFormat, ReadOptions, VcfReadOptions,
};

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
    ctx.deregister_table(&table_name).unwrap();
    ctx.register_table(&table_name, Arc::new(table_source))
        .unwrap();
    let df = rt.block_on(ctx.table(&table_name)).unwrap();
    let table_size = rt.block_on(df.clone().count()).unwrap();
    if table_size > MAX_IN_MEMORY_ROWS {
        let path = format!("{}/{}.parquet", py_ctx.catalog_dir, table_name);
        ctx.deregister_table(&table_name).unwrap();
        rt.block_on(df.write_parquet(&path, DataFrameWriteOptions::new(), None))
            .unwrap();
        ctx.deregister_table(&table_name).unwrap();
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
    } else if path.ends_with(".gff") || path.ends_with(".gff.gz") || path.ends_with(".gff.bgz") {
        InputFormat::Gff
    } else {
        panic!("Unsupported format")
    }
}

pub(crate) async fn register_table(
    ctx: &SessionContext,
    path: &str,
    table_name: &str,
    format: InputFormat,
    read_options: Option<ReadOptions>,
) -> String {
    ctx.deregister_table(table_name).unwrap();
    match format {
        InputFormat::Parquet => ctx
            .register_parquet(table_name, path, ParquetReadOptions::new())
            .await
            .unwrap(),
        InputFormat::Csv => {
            let csv_read_options = CsvReadOptions::new() //FIXME: expose
                .delimiter(b',')
                .has_header(true);
            ctx.register_csv(table_name, path, csv_read_options)
                .await
                .unwrap()
        },
        InputFormat::Fastq => {
            let fastq_read_options = match &read_options {
                Some(options) => match options.clone().fastq_read_options {
                    Some(fastq_read_options) => fastq_read_options,
                    _ => FastqReadOptions::default(),
                },
                _ => FastqReadOptions::default(),
            };
            info!(
                "Registering FASTQ table {} with options: {:?}",
                table_name, fastq_read_options
            );

            if fastq_read_options.parallel && path.ends_with(".bgz") {
                let table_provider = BgzfParallelFastqTableProvider::try_new(path).unwrap();
                ctx.register_table(table_name, Arc::new(table_provider))
                    .expect("Failed to register parallel FASTQ table");
            } else {
                let table_provider = FastqTableProvider::new(
                    path.to_string(),
                    None,
                    fastq_read_options.object_storage_options.clone(),
                )
                .unwrap();
                ctx.register_table(table_name, Arc::new(table_provider))
                    .expect("Failed to register FASTQ table");
            }
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
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register VCF table");
        },
        InputFormat::Gff => {
            let gff_read_options = match &read_options {
                Some(options) => match options.clone().gff_read_options {
                    Some(gff_read_options) => gff_read_options,
                    _ => GffReadOptions::default(),
                },
                _ => GffReadOptions::default(),
            };
            info!(
                "Registering GFF table {} with options: {:?}",
                table_name, gff_read_options
            );
            let table_provider = GffTableProvider::new(
                path.to_string(),
                gff_read_options.attr_fields,
                gff_read_options.thread_num,
                gff_read_options.object_storage_options.clone(),
            )
            .unwrap();
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register GFF table");
        },
        InputFormat::Bam => {
            let bam_read_options = match &read_options {
                Some(options) => match options.clone().bam_read_options {
                    Some(bam_read_options) => bam_read_options,
                    _ => BamReadOptions::default(),
                },
                _ => BamReadOptions::default(),
            };
            info!(
                "Registering BAM table {} with options: {:?}",
                table_name, bam_read_options
            );
            let table_provider = BamTableProvider::new(
                path.to_string(),
                bam_read_options.thread_num,
                bam_read_options.object_storage_options.clone(),
            )
            .unwrap();
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register BAM table");
        },
        InputFormat::Bed => {
            let bed_read_options = match &read_options {
                Some(options) => match options.clone().bed_read_options {
                    Some(bed_read_options) => bed_read_options,
                    _ => BedReadOptions::default(),
                },
                _ => BedReadOptions::default(),
            };
            info!(
                "Registering BED table {} with options: {:?}",
                table_name, bed_read_options
            );
            let table_provider = BedTableProvider::new(
                path.to_string(),
                BEDFields::BED4,
                bed_read_options.thread_num,
                bed_read_options.object_storage_options.clone(),
            )
            .unwrap();
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register BED table");
        },

        InputFormat::Fasta => {
            let fasta_read_options = match &read_options {
                Some(options) => match options.clone().fasta_read_options {
                    Some(fasta_read_options) => fasta_read_options,
                    _ => FastaReadOptions::default(),
                },
                _ => FastaReadOptions::default(),
            };
            info!(
                "Registering FASTA table {} with options: {:?}",
                table_name, fasta_read_options
            );

            let table_provider = FastaTableProvider::new(
                path.to_string(),
                None,
                fasta_read_options.object_storage_options.clone(),
            )
            .unwrap();
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register FASTA table");
        },
        InputFormat::IndexedVcf | InputFormat::IndexedBam => {
            todo!("Indexed formats are not supported")
        },
        InputFormat::Cram | InputFormat::Gtf => {
            todo!("Cram and Gtf formats are not supported")
        },
    };
    table_name.to_string()
}

pub(crate) fn maybe_register_table(
    df_path_or_table: String,
    default_table: &String,
    read_options: Option<ReadOptions>,
    ctx: &SessionContext,
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
