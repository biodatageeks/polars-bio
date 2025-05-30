use std::collections::HashMap;
use std::sync::Arc;

use arrow::buffer::OffsetBuffer;
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, ListArray, StringArray, StructArray};
use arrow_schema::{DataType, Field, Fields};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;

#[derive(Debug)]
pub(crate) struct QualityScoresStats {
    values_per_pos: HashMap<usize, Vec<u8>>, // key: position, value: decoded quality scores
}

impl QualityScoresStats {
    pub fn new() -> Self {
        Self {
            values_per_pos: HashMap::new(),
        }
    }

    fn decode_score(c: char) -> Option<u8> {
        let ascii = c as u8;
        if ascii >= 33 {
            Some(ascii - 33)
        } else {
            None
        }
    }

    fn calc_stats(values: &mut Vec<u8>) -> (f64, f64, f64, f64, f64, f64) {
        values.sort_unstable();
        let n = values.len();
        let average = values.iter().map(|&v| v as f64).sum::<f64>() / n as f64;
        let median = if n % 2 == 0 {
            (values[n / 2 - 1] as f64 + values[n / 2] as f64) / 2.0
        } else {
            values[n / 2] as f64
        };
        let q1 = values[n / 4] as f64;
        let q3 = values[(3 * n) / 4] as f64;
        let iqr = q3 - q1;
        let lower = q1 - 1.5 * iqr;
        let upper = q3 + 1.5 * iqr;
        (average, median, q1, q3, lower, upper)
    }
}

impl Accumulator for QualityScoresStats {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        #[derive(Default)]
        struct StatColumns {
            pos: Vec<i64>,
            avg: Vec<f64>,
            median: Vec<f64>,
            q1: Vec<f64>,
            q3: Vec<f64>,
            lower: Vec<f64>,
            upper: Vec<f64>,
        }

        let mut cols = StatColumns::default();
        let mut base_quality_warn = "pass";

        for (&pos, values) in &mut self.values_per_pos {
            if values.is_empty() {
                continue;
            }

            let (avg, median, q1, q3, lower, upper) = Self::calc_stats(values);

            cols.pos.push(pos as i64);
            cols.avg.push(avg);
            cols.median.push(median);
            cols.q1.push(q1);
            cols.q3.push(q3);
            cols.lower.push(lower);
            cols.upper.push(upper);

            base_quality_warn = match (q1 <= 20.0, q1 <= 25.0, base_quality_warn) {
                (true, _, _) => "fail",
                (false, true, "pass") => "warn",
                _ => base_quality_warn,
            };
        }

        let result_type = base_quality_result_type();

        let fields = match result_type {
            DataType::Struct(ref fields) => fields.clone(),
            _ => {
                return Err(DataFusionError::Execution(
                    "Unexpected result type".to_string(),
                ))
            },
        };

        let base_quality_warn_field = fields[0].clone();
        let base_per_pos_data_field = fields[1].clone();

        let base_per_pos_data_element_field = match base_per_pos_data_field.data_type() {
            DataType::List(inner_field) => inner_field.as_ref().clone(),
            _ => return Err(DataFusionError::Execution("Expected List type".to_string())),
        };

        let struct_fields = match base_per_pos_data_element_field.data_type() {
            DataType::Struct(inner_fields) => inner_fields.clone(),
            _ => {
                return Err(DataFusionError::Execution(
                    "Expected Struct type inside list".to_string(),
                ))
            },
        };

        let to_array = |vec: Vec<f64>| Arc::new(Float64Array::from(vec)) as ArrayRef;

        let struct_array = Arc::new(StructArray::new(
            struct_fields.clone(),
            vec![
                Arc::new(Int64Array::from(cols.pos)) as ArrayRef,
                to_array(cols.avg),
                to_array(cols.median),
                to_array(cols.q1),
                to_array(cols.q3),
                to_array(cols.lower),
                to_array(cols.upper),
            ],
            None,
        )) as ArrayRef;

        let list_array = Arc::new(ListArray::new(
            Arc::new(base_per_pos_data_element_field),
            OffsetBuffer::new(vec![0, struct_array.len() as i32].into()),
            struct_array,
            None,
        ));

        Ok(ScalarValue::Struct(Arc::new(StructArray::from(vec![
            (
                base_quality_warn_field,
                Arc::new(StringArray::from(vec![base_quality_warn])) as ArrayRef,
            ),
            (base_per_pos_data_field, list_array),
        ]))))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = values[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal("Expected StringArray".to_string())
            })?;

        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let val = arr.value(i);

            for (j, c) in val.chars().enumerate() {
                if let Some(decoded) = Self::decode_score(c) {
                    self.values_per_pos.entry(j).or_default().push(decoded);
                }
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

pub fn base_quality_result_type() -> DataType {
    let per_pos_fields = Fields::from(vec![
        Field::new("pos", DataType::Int64, false),
        Field::new("average", DataType::Float64, false),
        Field::new("median", DataType::Float64, false),
        Field::new("q1", DataType::Float64, false),
        Field::new("q3", DataType::Float64, false),
        Field::new("lower", DataType::Float64, false),
        Field::new("upper", DataType::Float64, false),
    ]);

    let base_per_pos_element_field = Field::new(
        "base_per_pos_data_element",
        DataType::Struct(per_pos_fields),
        false,
    );

    DataType::Struct(Fields::from(vec![
        Field::new("base_quality_warn", DataType::Utf8, false),
        Field::new(
            "base_per_pos_data",
            DataType::List(Arc::new(base_per_pos_element_field)),
            false,
        ),
    ]))
}
