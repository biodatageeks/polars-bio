use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use arrow::array::{Array, LargeStringArray};
use arrow_array::ffi_stream::ArrowArrayStreamReader;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use datafusion::dataframe::DataFrame;
use datafusion::arrow::{
    array::{Int32Array, Float64Array, ArrayRef},
    record_batch::RecordBatch,
};
use datafusion::prelude::SessionContext;
use exon::ExonSession;
use futures_util::StreamExt;
use pyo3::PyErr;
use tokio::runtime::Runtime;
use crate::context::PyBioSessionContext;

fn quartiles(hist: &[usize]) -> [f64; 5] {
    let sum = hist.iter().sum::<usize>();
    assert!(sum != 0);
    if sum == 1 {
        let value = hist.iter().enumerate().fold(
            0_usize,
            |acc, (value, &count)| if count > 0 { value } else { acc },
        ) as f64;
        return [value, value, value, value, value];
    }
    let mut ret = [0_f64; 5];
    // compute the quartiles
    for (i, quantile) in [0.25, 0.5, 0.75].iter().enumerate() {
        let rank = quantile * (sum - 1) as f64;
        let rank_ = rank.floor();
        let delta = rank - rank_;
        let n = rank_ as usize + 1;
        let mut acc = 0;
        let mut lo = None;
        for (hi, &count) in hist.iter().enumerate().filter(|(_, &count)| count > 0) {
            if acc == n && lo.is_some() {
                let lo = lo.unwrap() as f64;
                ret[i + 1] = (lo + (hi as f64 - lo) * delta) as f64;
                break;
            } else if acc + count > n {
                ret[i + 1] = hi as f64;
                break;
            }
            acc += count;
            lo = Some(hi);
        }
    }
    // compute lower, upper fences
    // TODO(lhepler): the UI reports these as min/max, which is incorrect.
    // If that's what we want, we can return those values.
    let iqr = ret[3] - ret[1];
    ret[0] = ret[1] - 1.5 * iqr;
    ret[4] = ret[3] + 1.5 * iqr;
    ret
}

async fn do_compute(df: DataFrame)->Result<HashMap<usize, Vec<usize>>, Box<dyn Error>> {
    let mut base_quality_count: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut stream = df.execute_stream().await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;                                // Arrow RecordBatch
        let qual_arr = batch
            .column_by_name("quality_scores")
            .or_else(|| batch.column_by_name("quality_scores"))
            .ok_or("column «quality_scores» not found")?
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .ok_or("quality_scores must be LargeStringArray")?;

        for row in 0..qual_arr.len() {
            let qs = qual_arr.value(row).as_bytes();
            for (pos, &byte) in qs.iter().enumerate() {
                // Phred + 33 encoding
                let q = (byte as usize) - 33;
                base_quality_count
                    .entry(pos)
                    .or_insert_with(|| vec![0; 94])[q] += 1;
            }
        }
    }
    Ok(base_quality_count)
}
/// Main routine – gathers histograms *from an Arrow stream*,
/// then produces a Polars `DataFrame`
pub fn compute_base_quality(
    ctx: &ExonSession,
    rt: &Runtime,
    table: String,
) -> Result<DataFrame, Box<dyn Error>> {
    let query = format!(r#"SELECT * FROM {}"#, table);
    let df = rt.block_on(ctx.sql(&query))?;
    let base_quality_count = rt.block_on(do_compute(df))?;

    // ── 2. derive statistics for every position
    let mut pos_vec =   Vec::with_capacity(base_quality_count.len());
    let mut avg_vec =   Vec::with_capacity(base_quality_count.len());
    let mut lower_vec = Vec::with_capacity(base_quality_count.len());
    let mut q1_vec =    Vec::with_capacity(base_quality_count.len());
    let mut median_vec =Vec::with_capacity(base_quality_count.len());
    let mut q3_vec =    Vec::with_capacity(base_quality_count.len());
    let mut upper_vec = Vec::with_capacity(base_quality_count.len());

    for (pos, hist) in base_quality_count.into_iter() {
        let (sum, total) = hist.iter().enumerate()
            .fold((0usize,0usize), |(s,t),(q,c)| (s+q*c,t+c));
        let avg = sum as f64 / total as f64;
        let qs  = quartiles(&hist);

        pos_vec.push(pos as i32);
        avg_vec.push(avg);
        lower_vec .push(qs[0]);
        q1_vec    .push(qs[1]);
        median_vec.push(qs[2]);
        q3_vec    .push(qs[3]);
        upper_vec .push(qs[4]);
    }
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("pos",     DataType::Int32,   false),
        Field::new("average", DataType::Float64, false),
        Field::new("lower",   DataType::Float64, false),
        Field::new("q1",      DataType::Float64, false),
        Field::new("median",  DataType::Float64, false),
        Field::new("q3",      DataType::Float64, false),
        Field::new("upper",   DataType::Float64, false),
    ]));
    // ── 3. build a Polars DataFrame
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(pos_vec)),
            Arc::new(Float64Array::from(avg_vec)),
            Arc::new(Float64Array::from(lower_vec)),
            Arc::new(Float64Array::from(q1_vec)),
            Arc::new(Float64Array::from(median_vec)),
            Arc::new(Float64Array::from(q3_vec)),
            Arc::new(Float64Array::from(upper_vec)),
        ],
    )?;
    //TODO this should result in dataframe not RecordBatch
    ctx.session.register_batch("b", batch).ok();
    let df = rt.block_on(ctx.session.table("b"))?;
    Ok(df)
}
