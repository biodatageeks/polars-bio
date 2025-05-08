

use arrow::array::{ArrayRef, Float64Array, UInt32Array};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::scalar::ScalarValue;
use serde_json::{Value, json};

#[derive(Debug)]
pub struct BaseQualityMetrics {
    position_counts: Vec<PositionStats>,
    warning_status: String,
}

#[derive(Debug, Default)]
struct PositionStats {
    sum: u64,
    count: u64,
    quality_counts: [u32; 41], // Phred scores 0-40
    min: u8,
    max: u8,
}

impl BaseQualityMetrics {
    pub fn new() -> Self {
        Self {
            position_counts: Vec::new(),
            warning_status: "pass".to_string(),
        }
    }

    pub fn update(&mut self, position: usize, quality_scores: &[u8]) {
        if position >= self.position_counts.len() {
            self.position_counts.resize(position + 1, PositionStats::default());
        }

        let stats = &mut self.position_counts[position];
        for &q in quality_scores {
            let phred = q - 33; // Convert ASCII to Phred score
            stats.sum += phred as u64;
            stats.count += 1;
            stats.quality_counts[phred as usize] += 1;
            stats.min = stats.min.min(phred);
            stats.max = stats.max.max(phred);
        }
    }

    pub fn finalize(mut self) -> Value {
        let mut base_per_pos_data = Vec::new();
        
        for (pos, stats) in self.position_counts.iter().enumerate() {
            let values = self.calculate_quartiles(stats);
            let median = values[2];
            
            if median <= 20.0 {
                self.warning_status = "fail".to_string();
            } else if median <= 25.0 && self.warning_status != "fail" {
                self.warning_status = "warn".to_string();
            }

            base_per_pos_data.push(json!({
                "pos": pos,
                "average": stats.sum as f64 / stats.count as f64,
                "upper": values[4],
                "lower": values[0],
                "q1": values[1],
                "q3": values[3],
                "median": median
            }));
        }

        json!({
            "base_quality_warn": self.warning_status,
            "base_per_pos_data": base_per_pos_data,
            "plots": self.generate_plots()
        })
    }

    fn calculate_quartiles(&self, stats: &PositionStats) -> [f64; 5] {
        // Weighted percentile calculation without expanding array
        let mut positions = Vec::new();
        let total = stats.count as f64;
        
        for (score, &count) in stats.quality_counts.iter().enumerate() {
            if count > 0 {
                positions.push((score as f64, count as f64));
            }
        }
        
        // Implement linear interpolation for percentiles
        [0.0, 25.0, 50.0, 75.0, 100.0].map(|p| {
            let rank = (p / 100.0) * (total - 1.0);
            // ... percentile calculation logic ...
        })
    }

    fn generate_plots(&self) -> Value {
        // Load template from specs file
        let mut specs: Value = serde_json::from_str(
            include_str!("report/quality_per_pos_specs.json")
        ).unwrap();
        
        specs["data"]["values"] = json!(self.base_per_pos_data);
        json!({ /* Plot configuration */ })
    }
}
