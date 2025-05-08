impl AggregateUDF for BaseQualityAggregator {
    fn accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(BaseQualityAccumulator::new()))
    }

    fn data_type(&self) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
}

struct BaseQualityAccumulator {
    metrics: BaseQualityMetrics,
}

impl Accumulator for BaseQualityAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let positions = values[0].as_any().downcast_ref::<UInt32Array>().unwrap();
        let qualities = values[1].as_any().downcast_ref::<StringArray>().unwrap();
        
        for (pos, qual) in positions.iter().zip(qualities.iter()) {
            if let (Some(pos), Some(qual)) = (pos, qual) {
                self.metrics.update(pos as usize, qual.as_bytes());
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Utf8(Some(self.metrics.finalize().to_string())))
    }
}
