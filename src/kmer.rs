use arrow_array::{ArrayRef, StringArray};
use datafusion::common::{ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::Accumulator;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use arrow_array::Array;

#[derive(Debug, Serialize, Deserialize)]
pub struct KmerCounter {
    kmer_len: usize,
    frequency: HashMap<String, usize>,
}

impl KmerCounter {
    pub fn with_k(k: usize) -> Self {
        KmerCounter {
            kmer_len: k,
            frequency: HashMap::new(),
        }
    }

    fn count_kmers(&mut self, sequence: &str) {
        let len = sequence.len();
        if len < self.kmer_len {
            return;
        }

        for i in 0..=(len - self.kmer_len) {
            let candidate = &sequence[i..i + self.kmer_len];
            if candidate.contains('N') {
                continue;
            }
            *self.frequency.entry(candidate.to_owned()).or_insert(0) += 1;
        }
    }

    fn merge_map(&mut self, other: HashMap<String, usize>) {
        for (kmer, count) in other {
            *self.frequency.entry(kmer).or_insert(0) += count;
        }
    }
}

impl Accumulator for KmerCounter {
    fn update_batch(&mut self, inputs: &[ArrayRef]) -> Result<()> {
        let input_array = inputs.get(0)
            .ok_or_else(|| DataFusionError::Execution("No input array provided".into()))?;
        
        let strings = input_array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected StringArray input".into()))?;
        
        for i in 0..strings.len() {
            if strings.is_valid(i) {
                let seq = strings.value(i);
                self.count_kmers(seq);
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let state_array = states.get(0)
            .ok_or_else(|| DataFusionError::Execution("No state array provided".into()))?;
        
        let string_array = state_array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected StringArray in state merge".into()))?;
        
        for i in 0..string_array.len() {
            if string_array.is_valid(i) {
                let json = string_array.value(i);
                let parsed: HashMap<String, usize> = serde_json::from_str(json)
                    .map_err(|e| DataFusionError::Execution(format!("JSON parsing error: {}", e)))?;
                self.merge_map(parsed);
            }
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let json = serde_json::to_string(&self.frequency)
            .map_err(|e| DataFusionError::Execution(format!("JSON serialization error: {}", e)))?;
        Ok(vec![ScalarValue::Utf8(Some(json))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let json = serde_json::to_string(&self.frequency)
            .map_err(|e| DataFusionError::Execution(format!("JSON serialization error: {}", e)))?;
        Ok(ScalarValue::Utf8(Some(json)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
