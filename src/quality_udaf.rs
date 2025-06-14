use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use arrow_array::{Array, ArrayRef, LargeStringArray, ListArray, UInt64Array};
use arrow_schema::Fields;
use datafusion::common::DataFusionError;
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::error::Result;
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use polars_arrow::array::StructArray;

type PhredHist = [u64; 94];

#[derive(Debug)]
pub struct QuartilesAccumulator {
    hist: Vec<PhredHist>,
}

impl QuartilesAccumulator {
    pub fn new() -> Self {
        Self { hist: Vec::new() }
    }

    #[inline]
    fn grow_to(&mut self, len: usize) {
        while self.hist.len() < len {
            // @TODO: can resize() pushing default value
            self.hist.push([0; 94]);
        }
    }

    fn add_quality(&mut self, arr: &str) {
        self.grow_to(arr.len());

        for (pos, byte) in arr.bytes().enumerate() {
            let q = (byte as usize) - 33; // Phred + 33 encoding
            self.hist[pos][q] += 1;
        }
    }

    fn merge_hist(&mut self, other: &[PhredHist]) {
        self.grow_to(other.len());

        for (self_pos, other_pos) in self.hist.iter_mut().zip(other) {
            for dig in 0..94 {
                self_pos[dig] += other_pos[dig];
            }
        }
    }

    fn quartiles(hist: &PhredHist) -> [f64; 5] {
        // @TODO: extract to separate function
        // @TODO: include calculating sum, total and average
        // @TODO: optimize calculating quartiles (can make single loop instead of 3)
        //      @TODO: (optional) benchmark
        // @TODO: unit tests

        let sum = hist.iter().sum::<u64>();
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
            let n = rank_ as u64 + 1;
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
}

impl Accumulator for QuartilesAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = values[0]
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }

            let s = arr.value(i);
            self.add_quality(&s);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let inner_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Float64,
            false,
        )));

        let mut columns = Vec::<Vec<ScalarValue>>::new();
        let fields = vec![
            Field::new("pos", inner_type.clone(), false),
            Field::new("avg", inner_type.clone(), false),
            Field::new("lower", inner_type.clone(), false),
            Field::new("q1", inner_type.clone(), false),
            Field::new("mean", inner_type.clone(), false),
            Field::new("q2", inner_type.clone(), false),
            Field::new("upper", inner_type.clone(), false),
        ];

        for (pos, hist) in self.hist.iter().enumerate() {
            let res = Self::quartiles(hist);
            let (sum, total) = hist
                .iter()
                .enumerate()
                .fold((0u64, 0u64), |(s, t), (q, c)| (s + (q as u64) * c, t + c));
            let avg = sum as f64 / total as f64;

            columns[0].push(ScalarValue::Float64(Some(pos as f64)));
            columns[1].push(ScalarValue::Float64(Some(avg)));
            
            for (i, r) in res.iter().enumerate() {
                columns[i + 2].push(ScalarValue::Float64(Some(*r)));
            }
        }

        let lists: Vec<_> = columns.iter().map(|c| ScalarValue::new_list(c, &DataType::Float64, false)).collect();

        let mut builder = ScalarStructBuilder::new();

        for (l, f) in lists.iter().to_owned().zip(fields) {
            builder = builder.with_array(f, l);
        }

        let s1 = builder.build();

        s1
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.hist.len() * size_of::<PhredHist>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let per_pos: Vec<ScalarValue> = self
            .hist
            .iter()
            .map(|buckets| {
                let scalars: Vec<ScalarValue> = buckets
                    .iter()
                    .map(|&c| ScalarValue::UInt64(Some(c)))
                    .collect();
                ScalarValue::List(ScalarValue::new_list(&scalars, &DataType::UInt64, false))
            })
            .collect();

        let outer_type = DataType::List(Arc::new(Field::new("item", DataType::UInt64, false)));

        Ok(vec![ScalarValue::List(ScalarValue::new_list(
            &per_pos,
            &outer_type,
            false,
        ))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let outer = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("state column must be List<List<UInt64>>".into())
            })?;

        let mut buckets = [0u64; 94];
        let mut position_histograms = Vec::<PhredHist>::new();

        for row in 0..outer.len() {
            let positions_ref = outer.value(row);
            let positions = positions_ref
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("expected inner List<UInt64> in state".into())
                })?;

            position_histograms.clear();
            position_histograms.reserve(positions.len());

            for pos in 0..positions.len() {
                let counts_ref = positions.value(pos);
                let counts = counts_ref
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("expected UInt64Array in histogram".into())
                    })?;

                let len = counts.len().min(94);
                buckets[..len].copy_from_slice(&counts.values()[..len]);
                buckets[len..].fill(0);
                position_histograms.push(buckets);
            }

            self.merge_hist(&position_histograms);
        }

        Ok(())
    }
}
