use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, UInt64Array, UInt8Array};
use arrow_schema::{DataType, Field, Fields};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::error::Result;
use datafusion::logical_expr::{create_udaf, AggregateUDF, Volatility};
use datafusion::physical_plan::Accumulator;

// region Base Sequence Quality

type PhredHist = [u64; 94];

#[derive(Debug)]
pub struct QualityQuartilesAccumulator {
    hist: PhredHist,
}

impl QualityQuartilesAccumulator {
    pub fn new() -> Self {
        Self { hist: [0u64; 94] }
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

        // @TODO: calc quartiles
        // @TODO: extend response struct

        builder = builder.with_scalar(
            Field::new("avg", DataType::Float64, false),
            ScalarValue::Float64(Some(self.hist.len() as f64)),
        );

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

    let return_type = DataType::Struct(Fields::from(vec![Field::new(
        "avg",
        DataType::Float64,
        false,
    )]));

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
