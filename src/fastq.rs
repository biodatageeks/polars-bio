use rayon::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::collections::HashMap;
use pyo3::prelude::*;
use pyo3::types::PyDict;

pub fn process_fastq(path: &str, partitions: usize) -> PyResult<QualityStats> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let lines: Vec<_> = reader.lines().filter_map(Result::ok).collect(); 
    let chunks: Vec<_> = lines.chunks(4).collect();

    let chunk_size = (chunks.len() + partitions - 1) / partitions.max(1);

    let partitioned: Vec<Vec<&[String]>> = 
        chunks
        .chunks(chunk_size)
        .map(|slice| slice.to_vec())
        .collect();
    
    let stats: Vec<_> = partitioned
        .into_par_iter()
        .map(|reads| {
            let mut per_pos: HashMap<usize, Vec<u8>> = HashMap::new();

            for chunk in reads {
                if chunk.len() < 4 {
                    continue;
                }

                let quality_line = &chunk[3];
                
                for (i, c) in quality_line.chars().enumerate() {
                    per_pos.entry(i).or_default().push(c as u8 - 33);
                }
            }

            per_pos
        })
        .collect();

    let mut merged: HashMap<usize, Vec<u8>> = HashMap::new();
    for stat in stats {
        for (pos, values) in stat{
            merged.entry(pos).or_default().extend(values);
        }
    }

    Ok(QualityStats::from_map(merged))
}

pub struct QualityStats {
    pub stats: Vec<(usize, u8, f64, f64, f64, u8)>
}

impl QualityStats {
    pub fn from_map(data: HashMap<usize, Vec<u8>>) -> Self {
        let mut stats = vec![];
        for (pos, scores) in data{
            let mut sorted = scores.clone();

            sorted.sort_unstable();//?

            let q1 = percentile(&sorted, 25);
            let q3 = percentile(&sorted, 75);
            let mean = sorted.iter().copied().map(f64::from).sum::<f64>();
            let min = *sorted.first().unwrap();
            let max = *sorted.last().unwrap();

            stats.push((pos, min, q1, mean, q3, max));
        }

        stats.sort_by_key(|x| x.0); //?

        Self {stats}
    }

    pub fn to_py_dict<'py>(&self, py: Python<'py>) -> Bound<'py, PyDict>{
        let dict = PyDict::new_bound(py);

        dict.set_item("position", self.stats.iter().map(|x| x.0).collect::<Vec<_>>()).unwrap();
        dict.set_item("min", self.stats.iter().map(|x| x.1).collect::<Vec<_>>()).unwrap();
        dict.set_item("q1", self.stats.iter().map(|x| x.2).collect::<Vec<_>>()).unwrap();
        dict.set_item("mean", self.stats.iter().map(|x| x.3).collect::<Vec<_>>()).unwrap();
        dict.set_item("q3", self.stats.iter().map(|x| x.4).collect::<Vec<_>>()).unwrap();
        dict.set_item("max", self.stats.iter().map(|x| x.5).collect::<Vec<_>>()).unwrap();

        dict
    }
}

fn percentile(sorted: &Vec<u8>, p: usize) -> f64{
    let idx = (p *  (sorted.len() - 1)) / 100;
    sorted[idx] as f64
}