use std::sync::Arc;

use datafusion::catalog_common::TableReference;
use exon::ExonSession;
use log::{debug, info};
use sequila_core::session_context::{Algorithm, SequilaConfig};
use tokio::runtime::Runtime;

use crate::context::set_option_internal;
use crate::option::{FilterOp, RangeOp, RangeOptions, QCOptions, QCOp};
use crate::query::{nearest_query, overlap_query, mean_quality_query};
use crate::udtf::CountOverlapsProvider;
use crate::utils::default_cols_to_string;
use crate::DEFAULT_COLUMN_NAMES;

pub(crate) struct QueryParams {
    pub sign: String,
    pub suffixes: (String, String),
    pub columns_1: Vec<String>,
    pub columns_2: Vec<String>,
    pub other_columns_1: Vec<String>,
    pub other_columns_2: Vec<String>,
    pub left_table: String,
    pub right_table: String,
}
pub(crate) fn do_range_operation(
    ctx: &ExonSession,
    rt: &Runtime,
    range_options: RangeOptions,
    left_table: String,
    right_table: String,
) -> datafusion::dataframe::DataFrame {
    // defaults
    match &range_options.overlap_alg {
        Some(alg) if alg == "coitreesnearest" => {
            panic!("CoitreesNearest is an internal algorithm for nearest operation. Can't be set explicitly.");
        },
        Some(alg) => {
            set_option_internal(ctx, "sequila.interval_join_algorithm", alg);
        },
        _ => {
            set_option_internal(
                ctx,
                "sequila.interval_join_algorithm",
                &Algorithm::Coitrees.to_string(),
            );
        },
    }
    let streaming = range_options.streaming.unwrap_or(false);
    if streaming {
        info!("Running in streaming mode...");
    }
    info!(
        "Running {} operation with algorithm {} and {} thread(s)...",
        range_options.range_op,
        ctx.session
            .state()
            .config()
            .options()
            .extensions
            .get::<SequilaConfig>()
            .unwrap()
            .interval_join_algorithm,
        ctx.session
            .state()
            .config()
            .options()
            .execution
            .target_partitions
    );
    match range_options.range_op {
        RangeOp::Overlap => rt.block_on(do_overlap(ctx, range_options, left_table, right_table)),
        RangeOp::Nearest => {
            set_option_internal(ctx, "sequila.interval_join_algorithm", "coitreesnearest");
            rt.block_on(do_nearest(ctx, range_options, left_table, right_table))
        },
        RangeOp::CountOverlapsNaive => rt.block_on(do_count_overlaps_coverage_naive(
            ctx,
            range_options,
            left_table,
            right_table,
            false,
        )),
        RangeOp::Coverage => rt.block_on(do_count_overlaps_coverage_naive(
            ctx,
            range_options,
            left_table,
            right_table,
            true,
        )),

        _ => panic!("Unsupported operation"),
    }
}

async fn do_nearest(
    ctx: &ExonSession,
    range_opts: RangeOptions,
    left_table: String,
    right_table: String,
) -> datafusion::dataframe::DataFrame {
    let query = prepare_query(nearest_query, range_opts, ctx, left_table, right_table)
        .await
        .to_string();
    debug!("Query: {}", query);
    ctx.sql(&query).await.unwrap()
}

async fn do_overlap(
    ctx: &ExonSession,
    range_opts: RangeOptions,
    left_table: String,
    right_table: String,
) -> datafusion::dataframe::DataFrame {
    let query = prepare_query(overlap_query, range_opts, ctx, left_table, right_table)
        .await
        .to_string();
    debug!("Query: {}", query);
    debug!(
        "{}",
        ctx.session
            .state()
            .config()
            .options()
            .execution
            .target_partitions
    );
    ctx.sql(&query).await.unwrap()
}

async fn do_count_overlaps_coverage_naive(
    ctx: &ExonSession,
    range_opts: RangeOptions,
    left_table: String,
    right_table: String,
    coverage: bool,
) -> datafusion::dataframe::DataFrame {
    let columns_1 = range_opts.columns_1.unwrap();
    let columns_2 = range_opts.columns_2.unwrap();
    let session = &ctx.session;
    let right_table_ref = TableReference::from(right_table.clone());
    let right_schema = session
        .table(right_table_ref.clone())
        .await
        .unwrap()
        .schema()
        .as_arrow()
        .clone();
    let count_overlaps_provider = CountOverlapsProvider::new(
        Arc::new(session.clone()),
        left_table,
        right_table,
        right_schema,
        columns_1,
        columns_2,
        range_opts.filter_op.unwrap(),
        coverage,
    );
    let table_name = "count_overlaps_coverage".to_string();
    session.deregister_table(table_name.clone()).unwrap();
    session
        .register_table(table_name.clone(), Arc::new(count_overlaps_provider))
        .unwrap();
    let query = format!("SELECT * FROM {}", table_name);
    debug!("Query: {}", query);
    ctx.sql(&query).await.unwrap()
}


use arrow::array::{LargeStringArray, ArrayRef, Float64Builder};
use arrow_array::Array;
use arrow::datatypes::{Field, Schema,DataType};
use arrow::record_batch::RecordBatch;

// /// Dodaje kolumnę `mean_c` z obliczoną średnią jakością do każdego rekordu
// pub fn add_mean_quality_column(batch: &RecordBatch) -> RecordBatch {
//     let quality_col = batch
//         .column_by_name("quality_scores")
//         .expect("Column 'quality_scores' not found");

//     let quality_scores = quality_col
//         .as_any()
//         .downcast_ref::<StringArray>()
//         .expect("Expected StringArray");

//     let mut means = Vec::with_capacity(quality_scores.len());

//     for i in 0..quality_scores.len() {
//         if quality_scores.is_null(i) {
//             means.push(None);
//         } else {
//             let quality_str = quality_scores.value(i);
//             let sum: u64 = quality_str.chars().map(|c| (c as u8 - 33) as u64).sum();
//             let len = quality_str.len() as f64;
//             let mean = if len > 0.0 {
//                 Some(sum as f64 / len)
//             } else {
//                 None
//             };
//             means.push(mean);
//         }
//     }

//     // Nowa kolumna
//     let mean_array = Arc::new(Float64Array::from(means)) as ArrayRef;

//     // Nowy batch z dodatkową kolumną
//     let mut columns = batch.columns().to_vec();
//     columns.push(mean_array);


//     let mut fields: Vec<Field> = batch
//         .schema()
//         .fields()
//         .iter()
//         .map(|f| f.as_ref().clone())
//         .collect();

//     fields.push(Field::new("mean_c", arrow::datatypes::DataType::Float64, true));

//     let new_schema = Arc::new(Schema::new(fields));

//     RecordBatch::try_new(new_schema, columns).unwrap()
// }





pub fn compute_mean_c(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    batches
        .into_iter()
        .map(|batch| {
            // println!("Columns in batch: {:?}", batch.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());
            let array = batch
                .column_by_name("quality_scores")
                .expect("Column not found");

            println!(
                "quality_scores column type: {:?}",
                array.data_type()
            );
            let quality_col = batch
                .column_by_name("quality_scores")
                .expect("Column 'quality_scores' not found in RecordBatch")
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("Column 'quality_scores' is not a LargeStringArray");

            let mut means = Float64Builder::with_capacity(quality_col.len());
            for i in 0..quality_col.len() {
                if quality_col.is_null(i) {
                    means.append_null();
                } else {
                    let qstr = quality_col.value(i);
                    let mean = qstr
                        .as_bytes()
                        .iter()
                        .map(|b| (*b as i32 - 33) as f64)
                        .sum::<f64>()
                        / qstr.len() as f64;
                    means.append_value(mean);
                }
            }

            let mean_array: ArrayRef = Arc::new(means.finish());

            // dodajemy nową kolumnę
            let mut cols = batch.columns().to_vec();
            cols.push(mean_array);

            // konwersja Arc<Field> na Field
            let mut fields: Vec<Field> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect();
            fields.push(Field::new("mean_c", DataType::Float64, true));

            let schema = Arc::new(Schema::new(fields));
            RecordBatch::try_new(schema, cols).unwrap()
        })
        .collect()
}



// fn phred_mean(quality: &str) -> f64 {
//     let sum: u32 = quality.chars().map(|c| (c as u8 - 33) as u32).sum();
//     sum as f64 / quality.len() as f64
// }


// use polars_arrow::conversion::to_arrow::to_record_batches;

// use polars_arrow::conversion::from_arrow::to_polars;


// fn arrow_batches_to_polars(batches: &[RecordBatch]) -> polars::prelude::DataFrame {
//     let dfs: Vec<_> = batches.iter()
//         .map(|batch| to_polars(batch).unwrap())
//         .collect();
//     concat_df(&dfs).unwrap()
// }

pub fn do_qc_operation(
    ctx: &ExonSession,
    rt: &Runtime,
    qc_options: QCOptions,
    table_name: String,
) -> datafusion::dataframe::DataFrame {

    info!(
        "Running MeanQuality operation with {} thread(s)...",
        ctx.session
            .state()
            .config()
            .options()
            .execution
            .target_partitions
    );


    match qc_options.qc_op {
        QCOp::MeanQuality => rt.block_on(do_mean_quality(ctx, qc_options, table_name)),
        _ => panic!("Unsupported operation"),

        // QCOp::MeanQuality => {
        //     rt.block_on(do_mean_quality(ctx, rt, qc_options, table_name)),
        //     let df_df = rt.block_on(ctx.session.table(&table_name)).unwrap();

        //     // Pobierz dane jako RecordBatch
        //     let batches = rt.block_on(df_df.clone().collect()).unwrap();
        //     let polars_df = arrow_batches_to_polars(&batches);

        //     // Oblicz średnie jako kolumnę
        //     let mean_quality_series = polars_df
        //         .column("quality_scores")
        //         .unwrap()
        //         .str()
        //         .unwrap()
        //         .into_iter()
        //         .map(|opt_qs| opt_qs.map(|qs| phred_mean(qs)))
        //         .collect::<Float64Chunked>()
        //         .into_series()
        //         .rename("mean_quality".into());

        //     let mut new_polars_df = polars_df.clone();
        //     new_polars_df.with_column(mean_quality_series).unwrap();

        //     // Zamień na Arrow
        //     let arrow_batches = to_record_batches(&new_polars_df).unwrap();
        //     let mem_table = MemTable::try_new(
        //         arrow_batches[0].schema(),
        //         vec![arrow_batches],
        //     ).unwrap();

        //     // Zarejestruj
        //     let new_table_name = "qc_table".to_string();
        //     ctx.session.deregister_table(&new_table_name).ok();
        //     ctx.session.register_table(new_table_name.clone(), Arc::new(mem_table)).unwrap();

        //     // Zwróć wynik SQL
        //     let sql = format!("SELECT AVG(mean_quality) AS mean_quality FROM {}", new_table_name);
        //     rt.block_on(ctx.session.sql(&sql)).unwrap()
        // }
    }
}

// pub(crate) fn do_qc_operation(
//     ctx: &ExonSession,
//     rt: &Runtime,
//     qc_options: QcOptions,
//     input_table: String,
// ) -> datafusion::dataframe::DataFrame {
//     // Możesz dorzucić logi lub ustawienia jak w range
//     info!(
//         "Running QC operation '{}' on table '{}'",
//         qc_options.qc_op,
//         input_table
//     );

//     match qc_options.qc_op {
//         QcOp::MeanQuality => rt.block_on(do_mean_quality(ctx, input_table)),
//         _ => panic!("Unsupported QC operation: {:?}", qc_options.qc_op),
//     }
// }



// pub fn do_qc_operation(
//     ctx: &ExonSession,
//     rt: &Runtime,
//     qc_options: QCOptions,
//     table: String,
// ) -> datafusion::dataframe::DataFrame {
//     match qc_options.qc_op {
//         QCOp::MeanQuality => {
//             let sql = format!("SELECT AVG(quality) AS mean_quality FROM {}", table);
//             rt.block_on(ctx.sql(&sql)).unwrap()
//         }
//     }
// }

pub async fn do_mean_quality(
    ctx: &ExonSession,
    qc_options: QCOptions,
    table: String,
)  -> datafusion::dataframe::DataFrame {
    let bin_size: u32 = 1 as u32; // szerokość bina histogramu, spakować w qc_options
    // qc_options.quality_col to nazwa kolumny dodanej w rust, która zawiera uśrednioną jakość dla każdego odczytu
    // table to nazwa zarejestrowanej tabeli
    let mean_qual_col = String::from("mean_c");
    let query = mean_quality_query(table, bin_size,  mean_qual_col)
        .to_string();
    debug!("Query: {}", query);
    debug!(
        "{}",
        ctx.session
            .state()
            .config()
            .options()
            .execution
            .target_partitions
    );
    println!("Query : {}", query);
    ctx.sql(&query).await.unwrap() // zwróci datafusion::dataframe::DataFrame z histogramem
}


async fn get_non_join_columns(
    table_name: String,
    join_columns: Vec<String>,
    ctx: &ExonSession,
) -> Vec<String> {
    let table_ref = TableReference::from(table_name);
    let table = ctx.session.table(table_ref).await.unwrap();
    table
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .filter(|f| !join_columns.contains(f))
        .collect::<Vec<String>>()
}

pub(crate) fn format_non_join_tables(
    columns: Vec<String>,
    table_alias: String,
    suffix: String,
) -> String {
    if columns.is_empty() {
        return "".to_string();
    }
    columns
        .iter()
        .map(|c| format!("{}.{} as {}{}", table_alias, c, c, suffix))
        .collect::<Vec<String>>()
        .join(", ")
}

pub(crate) async fn prepare_query(
    query: fn(QueryParams) -> String,
    range_opts: RangeOptions,
    ctx: &ExonSession,
    left_table: String,
    right_table: String,
) -> String {
    let sign = match range_opts.filter_op.unwrap() {
        FilterOp::Weak => "=".to_string(),
        _ => "".to_string(),
    };
    let suffixes = match range_opts.suffixes {
        Some((s1, s2)) => (s1, s2),
        _ => ("_1".to_string(), "_2".to_string()),
    };
    let columns_1 = match range_opts.columns_1 {
        Some(cols) => cols,
        _ => default_cols_to_string(&DEFAULT_COLUMN_NAMES),
    };
    let columns_2 = match range_opts.columns_2 {
        Some(cols) => cols,
        _ => default_cols_to_string(&DEFAULT_COLUMN_NAMES),
    };

    let left_table_columns =
        get_non_join_columns(left_table.to_string(), columns_1.clone(), ctx).await;
    let right_table_columns =
        get_non_join_columns(right_table.to_string(), columns_2.clone(), ctx).await;

    let query_params = QueryParams {
        sign,
        suffixes,
        columns_1,
        columns_2,
        other_columns_1: left_table_columns,
        other_columns_2: right_table_columns,
        left_table,
        right_table,
    };

    query(query_params)
}
