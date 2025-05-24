use arrow::array::StringArray;
use arrow::record_batch::RecordBatch;
use polars::prelude::*;
use std::collections::HashMap;

fn compute_quartiles(sorted: &mut [i32]) -> (f64, f64, f64, f64, f64) {
    let len = sorted.len();
    if len == 0 {
        return (0.0, 0.0, 0.0, 0.0, 0.0);
    }
    sorted.sort_unstable();
    let median = |slice: &[i32]| -> f64 {
        let mid = slice.len() / 2;
        if slice.len() % 2 == 0 {
            (slice[mid - 1] + slice[mid]) as f64 / 2.0
        } else {
            slice[mid] as f64
        }
    };
    let q2 = median(sorted);
    let q1 = median(&sorted[..len / 2]);
    let q3 = median(&sorted[(len + 1) / 2..]);
    (
        *sorted.first().unwrap() as f64,
        q1,
        q2,
        q3,
        *sorted.last().unwrap() as f64,
    )
}

pub fn compute_base_quality(rb: &RecordBatch) -> DataFrame {
    let qual_array = rb
        .column_by_name("qual")
        .expect("QUAL column not found")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("QUAL column is not a StringArray");

    let mut pos_quality_map: HashMap<usize, Vec<i32>> = HashMap::new();

    qual_array
        .iter()
        .enumerate()
        .filter_map(|(i, opt_qual)| opt_qual.map(|qual| (i, qual)))
        .for_each(|(i, qual)| {
            let quality_values = qual
                .chars()
                .map(|ch| ch as i32) // or subtract 33 if using Phred+33 encoding
                .collect::<Vec<_>>();

            pos_quality_map
                .entry(i)
                .or_insert_with(Vec::new)
                .extend(quality_values);
        });

    let mut positions: Vec<u32> = Vec::new();
    let mut averages: Vec<f64> = Vec::new();
    let mut q1s: Vec<f64> = Vec::new();
    let mut medians: Vec<f64> = Vec::new();
    let mut q3s: Vec<f64> = Vec::new();
    let mut mins: Vec<f64> = Vec::new();
    let mut maxs: Vec<f64> = Vec::new();
    let mut warning_status: Vec<&str> = Vec::new();

    for (pos, values) in pos_quality_map.iter_mut() {
        let avg = values.iter().copied().sum::<i32>() as f64 / values.len() as f64;
        let (min, q1, median, q3, max) = compute_quartiles(values);
        positions.push(*pos as u32);
        averages.push(avg);
        q1s.push(q1);
        medians.push(median);
        q3s.push(q3);
        mins.push(min);
        maxs.push(max);
        warning_status.push(if avg < 20.0 {
            "Low"
        } else if avg > 40.0 {
            "High"
        } else {
            "Normal"
        });
    }

    let df = DataFrame::new(vec![
        Series::new("position".into(), positions).into(),
        Series::new("average".into(), averages).into(),
        Series::new("q1".into(), q1s).into(),
        Series::new("median".into(), medians).into(),
        Series::new("q3".into(), q3s).into(),
        Series::new("min".into(), mins).into(),
        Series::new("max".into(), maxs).into(),
        Series::new("warning_status".into(), warning_status).into(),
    ]);

    df.expect("REASON")
}
