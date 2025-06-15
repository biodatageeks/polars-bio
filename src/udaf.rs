use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, UInt64Array, UInt8Array};
use arrow_schema::{DataType, Field, Fields};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::error::Result;
use datafusion::logical_expr::{create_udaf, AggregateUDF, Volatility};
use datafusion::physical_plan::Accumulator;

// region Base Sequence Quality

pub(crate) type PhredHist = [u64; 94];

const QUALITY_STATS_COLUMNS: [&str; 6] = ["avg", "lower", "q1", "median", "q3", "upper"];

#[derive(Debug)]
pub struct QualityQuartilesAccumulator {
    hist: PhredHist,
}

impl QualityQuartilesAccumulator {
    pub fn new() -> Self {
        Self { hist: [0u64; 94] }
    }

    fn stats(hist: &PhredHist) -> Option<[f64; 6]> {
        let (sum, total_count) = hist
            .iter()
            .enumerate()
            .fold((0u64, 0u64), |(s, t), (q, c)| (s + (q as u64) * c, t + c));

        if total_count == 0 {
            return None;
        }

        if total_count == 1 {
            let value = hist.iter().enumerate().fold(
                0_usize,
                |acc, (value, &count)| if count > 0 { value } else { acc },
            ) as f64;

            return Some([value; 6]);
        }

        let mut quantile = [0f64; 3];

        for (i, quant) in [0.25, 0.5, 0.75].iter().enumerate() {
            let rank = quant * (total_count - 1) as f64;
            let rank_ = rank.floor();
            let delta = rank - rank_;
            let n = rank_ as u64 + 1;
            let mut acc = 0;
            let mut lo = None;
            for (hi, &count) in hist.iter().enumerate().filter(|(_, &count)| count > 0) {
                if acc == n && lo.is_some() {
                    let lo = lo.unwrap() as f64;
                    quantile[i] = (lo + (hi as f64 - lo) * delta) as f64;
                    break;
                } else if acc + count > n {
                    quantile[i] = hi as f64;
                    break;
                }
                acc += count;
                lo = Some(hi);
            }
        }

        let avg = sum as f64 / total_count as f64;
        let q1 = quantile[0];
        let median = quantile[1];
        let q3 = quantile[2];
        let iqr = q3 - q1;
        let lower = q1 - 1.5 * iqr;
        let upper = q3 + 1.5 * iqr;

        Some([avg, lower, q1, median, q3, upper])
    }
}

impl Accumulator for QualityQuartilesAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        if values.len() != 2 {
            return Err(DataFusionError::Execution(
                "Expected 2 columns: score, count".to_string(),
            ));
        }

        let score_array = values[0]
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| DataFusionError::Execution("score must be UInt8".to_string()))?;

        let count_array = values[1]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| DataFusionError::Execution("count must be UInt64".to_string()))?;

        for i in 0..score_array.len() {
            if score_array.is_null(i) || count_array.is_null(i) {
                continue;
            }

            let score = score_array.value(i);
            let count = count_array.value(i);

            self.hist[score as usize] += count;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut builder = ScalarStructBuilder::new();

        if let Some(stats) = Self::stats(&self.hist) {
            for (name, stat) in QUALITY_STATS_COLUMNS.iter().zip(stats.iter()) {
                builder = builder.with_scalar(
                    Field::new(*name, DataType::Float64, false),
                    ScalarValue::Float64(Some(*stat)),
                )
            }
        }

        builder.build()
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let scores: Vec<ScalarValue> = self
            .hist
            .iter()
            .enumerate()
            .map(|(score, _)| ScalarValue::UInt8(Some(score as u8)))
            .collect();

        let counts: Vec<ScalarValue> = self
            .hist
            .iter()
            .map(|count| ScalarValue::UInt64(Some(*count)))
            .collect();

        let scores_list = ScalarValue::new_list(&*scores, &DataType::UInt8, false);
        let counts_list = ScalarValue::new_list(&counts, &DataType::UInt64, false);

        Ok(vec![
            ScalarValue::List(scores_list),
            ScalarValue::List(counts_list),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 2 {
            return Err(DataFusionError::Execution(
                "Expected 2 arrays (score, count) in merge_batch".to_string(),
            ));
        }

        let scores_list = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("Expected ListArray for scores".to_string())
            })?;

        let counts_list = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("Expected ListArray for counts".to_string())
            })?;

        let scores = scores_list.value(0);
        let counts = counts_list.value(0);

        self.update_batch(&[scores, counts])
    }
}

pub(crate) fn create_quality_quartiles_udaf() -> AggregateUDF {
    let input_types = vec![DataType::UInt8, DataType::UInt64];

    let return_fields = QUALITY_STATS_COLUMNS
        .iter()
        .map(|c| Field::new(*c, DataType::Float64, false))
        .collect::<Vec<_>>();
    let return_type = DataType::Struct(Fields::from(return_fields));

    let state_types = vec![
        DataType::List(Arc::new(Field::new("item", DataType::UInt8, false))),
        DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
    ];

    create_udaf(
        "quality_quartiles",
        input_types,
        Arc::new(return_type),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(QualityQuartilesAccumulator::new()))),
        Arc::new(state_types),
    )
}

// endregion
