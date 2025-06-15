use std::sync::Arc;

use arrow::array::{ArrayBuilder, ListBuilder, StructBuilder, UInt64Builder};
use arrow::datatypes::UInt64Type;
use arrow_array::{Array, ArrayRef, ListArray, StringArray, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field};
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{create_udaf, Volatility};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use exon::ExonSession;

use std::fs::OpenOptions;
use std::io::Write;

#[derive(Debug)]
struct BaseSequenceContent {
    a_counts: Vec<Option<u64>>,
    c_counts: Vec<Option<u64>>,
    t_counts: Vec<Option<u64>>,
    g_counts: Vec<Option<u64>>,
    n_counts: Vec<Option<u64>>,
    max_position_seen: usize,
}

impl BaseSequenceContent {
    pub fn new() -> Self {
        BaseSequenceContent {
            a_counts: Vec::new(),
            c_counts: Vec::new(),
            t_counts: Vec::new(),
            g_counts: Vec::new(),
            n_counts: Vec::new(),
            max_position_seen: 0,
        }
    }

    fn ensure_capacity(&mut self, desired_len: usize) {
        if desired_len > self.max_position_seen {
            self.a_counts.resize(desired_len, Some(0));
            self.c_counts.resize(desired_len, Some(0));
            self.g_counts.resize(desired_len, Some(0));
            self.t_counts.resize(desired_len, Some(0));
            self.n_counts.resize(desired_len, Some(0));
            self.max_position_seen = desired_len;
        }
    }
}

impl Accumulator for BaseSequenceContent {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let a_counts_list = ScalarValue::List(Arc::new(
            ListArray::from_iter_primitive::<UInt64Type, _, _>(vec![Some(
                self.a_counts.clone(),
            )]),
        ));
        let c_counts_list = ScalarValue::List(Arc::new(
            ListArray::from_iter_primitive::<UInt64Type, _, _>(vec![Some(
                self.c_counts.clone(),
            )]),
        ));
        let g_counts_list = ScalarValue::List(Arc::new(
            ListArray::from_iter_primitive::<UInt64Type, _, _>(vec![Some(
                self.g_counts.clone(),
            )]),
        ));
        let t_counts_list = ScalarValue::List(Arc::new(
            ListArray::from_iter_primitive::<UInt64Type, _, _>(vec![Some(
                self.t_counts.clone(),
            )]),
        ));
        let n_counts_list = ScalarValue::List(Arc::new(
            ListArray::from_iter_primitive::<UInt64Type, _, _>(vec![Some(
                self.n_counts.clone(),
            )]),
        ));
        let max_position_seen = ScalarValue::from(self.max_position_seen as u64);

        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("/home/sh4dqw/dev/studies/Polars-UDF/rust_logs.txt") {
            writeln!(file, "[STATE] Returning serialized state (A): {}", a_counts_list.data_type());
            writeln!(file, "[STATE] Returning serialized state (C): {}", c_counts_list.data_type());
            writeln!(file, "[STATE] Returning serialized state (G): {}", g_counts_list.data_type());
            writeln!(file, "[STATE] Returning serialized state (T): {}", t_counts_list.data_type());
            writeln!(file, "[STATE] Returning serialized state (N): {}", n_counts_list.data_type());
            writeln!(file, "[STATE] Returning serialized state (max_position_seen): {}", max_position_seen.data_type());
        }
        
        let state = vec![
            a_counts_list,
            c_counts_list,
            g_counts_list,
            t_counts_list,
            n_counts_list,
            max_position_seen
        ];
        
        Ok(state)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let struct_fields = vec![
            Arc::new(Field::new("a_count", DataType::UInt64, false)),
            Arc::new(Field::new("c_count", DataType::UInt64, false)),
            Arc::new(Field::new("g_count", DataType::UInt64, false)),
            Arc::new(Field::new("t_count", DataType::UInt64, false)),
            Arc::new(Field::new("n_count", DataType::UInt64, false)),
        ];
        let struct_builders: Vec<Box<dyn ArrayBuilder>> = vec![
            Box::new(UInt64Builder::new()),
            Box::new(UInt64Builder::new()),
            Box::new(UInt64Builder::new()),
            Box::new(UInt64Builder::new()),
            Box::new(UInt64Builder::new()),
        ];
        let struct_builder = StructBuilder::new(struct_fields, struct_builders);
        let mut list_builder = ListBuilder::new(struct_builder);

        for i in 0..self.max_position_seen {
            let a_count = self.a_counts.get(i).copied().unwrap().unwrap_or(0);
            let c_count = self.c_counts.get(i).copied().unwrap().unwrap_or(0);
            let g_count = self.g_counts.get(i).copied().unwrap().unwrap_or(0);
            let t_count = self.t_counts.get(i).copied().unwrap().unwrap_or(0);
            let n_count = self.n_counts.get(i).copied().unwrap().unwrap_or(0);

            let struct_builder_ref = list_builder
                .values()
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .unwrap();
            struct_builder_ref
                .field_builder::<UInt64Builder>(0)
                .unwrap()
                .append_value(a_count);
            struct_builder_ref
                .field_builder::<UInt64Builder>(1)
                .unwrap()
                .append_value(c_count);
            struct_builder_ref
                .field_builder::<UInt64Builder>(2)
                .unwrap()
                .append_value(g_count);
            struct_builder_ref
                .field_builder::<UInt64Builder>(3)
                .unwrap()
                .append_value(t_count);
            struct_builder_ref
                .field_builder::<UInt64Builder>(4)
                .unwrap()
                .append_value(n_count);
            struct_builder_ref.append(true);
            list_builder.append(true);
        }

        Ok(ScalarValue::List(Arc::new(list_builder.finish())))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let sequences = values[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("Argument must be string array".to_string())
            })?;

        
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("/home/sh4dqw/dev/studies/Polars-UDF/rust_logs.txt") {
            writeln!(file, "[UPDATE_BATCH] Start processing batch. Num values: {}. Current max_pos: {}", values[0].len(), self.max_position_seen);
        }

        for i in 0..sequences.len() {
            if !sequences.is_null(i) {
                let seq = sequences.value(i);
                let current_seq_len = seq.len();
                self.ensure_capacity(current_seq_len);

                for (pos, base_char) in seq.chars().enumerate() {
                    if pos < self.max_position_seen {
                        match base_char.to_ascii_uppercase() {
                            'A' => {
                                self.a_counts[pos] =
                                    self.a_counts[pos].map_or(Some(1), |val| Some(val + 1))
                            },
                            'C' => {
                                self.c_counts[pos] =
                                    self.c_counts[pos].map_or(Some(1), |val| Some(val + 1))
                            },
                            'G' => {
                                self.g_counts[pos] =
                                    self.g_counts[pos].map_or(Some(1), |val| Some(val + 1))
                            },
                            'T' => {
                                self.t_counts[pos] =
                                    self.t_counts[pos].map_or(Some(1), |val| Some(val + 1))
                            },
                            'N' => {
                                self.n_counts[pos] =
                                    self.n_counts[pos].map_or(Some(1), |val| Some(val + 1))
                            },
                            _ => {},
                        }
                    }
                }
            }
        }

        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("/home/sh4dqw/dev/studies/Polars-UDF/rust_logs.txt") {
            writeln!(file, "[UPDATE_BATCH] Finished processing batch. Num values: {}. Current max_pos: {}", values[0].len(), self.max_position_seen);
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("/home/sh4dqw/dev/studies/Polars-UDF/rust_logs.txt") {
            writeln!(file, "[MERGE_BATCH] Started merging batch. Num A counts: {}. Current max_pos: {}", states[0].len(), self.max_position_seen);
            writeln!(file, "[MERGE_BATCH] Started merging batch. Num C counts: {}. Current max_pos: {}", states[1].len(), self.max_position_seen);
            writeln!(file, "[MERGE_BATCH] Started merging batch. Num G counts: {}. Current max_pos: {}", states[2].len(), self.max_position_seen);
            writeln!(file, "[MERGE_BATCH] Started merging batch. Num T counts: {}. Current max_pos: {}", states[3].len(), self.max_position_seen);
            writeln!(file, "[MERGE_BATCH] Started merging batch. Num N counts: {}. Current max_pos: {}", states[4].len(), self.max_position_seen);
        }
        
        let a_counts_array = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("First column must be list array of A counts".to_string())
            })?;
        let c_counts_array = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "Second column must be list array of C counts".to_string(),
                )
            })?;
        let g_counts_array = states[2]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("Third column must be list array of G counts".to_string())
            })?;
        let t_counts_array = states[3]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "Fourth column must be list array of T counts".to_string(),
                )
            })?;
        let n_counts_array = states[4]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("Fifth column must be list array of N counts".to_string())
            })?;
        let max_pos_array = states[5]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "Sixth column must be u64 array of max positions".to_string(),
                )
            })?;

        for i in 0..a_counts_array.len() {
            let a_counts_array_ref = a_counts_array.value(i);
            let other_a_counts = a_counts_array_ref
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let c_counts_array_ref = c_counts_array.value(i);
            let other_c_counts = c_counts_array_ref
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let g_counts_array_ref = g_counts_array.value(i);
            let other_g_counts = g_counts_array_ref
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let t_counts_array_ref = t_counts_array.value(i);
            let other_t_counts = t_counts_array_ref
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let n_counts_array_ref = n_counts_array.value(i);
            let other_n_counts = n_counts_array_ref
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let other_max_pos = max_pos_array.value(i) as usize;

            self.ensure_capacity(other_max_pos);

            for pos in 0..other_max_pos {
                self.a_counts[pos] =
                    self.a_counts[pos].map_or(Some(1), |val| Some(val + other_a_counts.value(pos)));
                self.c_counts[pos] =
                    self.c_counts[pos].map_or(Some(1), |val| Some(val + other_c_counts.value(pos)));
                self.g_counts[pos] =
                    self.g_counts[pos].map_or(Some(1), |val| Some(val + other_g_counts.value(pos)));
                self.t_counts[pos] =
                    self.t_counts[pos].map_or(Some(1), |val| Some(val + other_t_counts.value(pos)));
                self.n_counts[pos] =
                    self.n_counts[pos].map_or(Some(1), |val| Some(val + other_n_counts.value(pos)));
            }
        }

        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("/home/sh4dqw/dev/studies/Polars-UDF/rust_logs.txt") {
            writeln!(file, "[MERGE_BATCH] Stopped merging batch. Num values: {}. Current max_pos: {}", states[0].len(), self.max_position_seen);
        }
        
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.a_counts.capacity() * std::mem::size_of::<u64>()
            + self.c_counts.capacity() * std::mem::size_of::<u64>()
            + self.g_counts.capacity() * std::mem::size_of::<u64>()
            + self.t_counts.capacity() * std::mem::size_of::<u64>()
            + self.n_counts.capacity() * std::mem::size_of::<u64>()
    }
}

pub(crate) fn register_base_sequence_content(ctx: &ExonSession) {
    let udaf = create_udaf(
        "base_sequence_content",
        vec![DataType::Utf8],
        Arc::new(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(
                vec![
                    Field::new("a_count", DataType::UInt64, false),
                    Field::new("c_count", DataType::UInt64, false),
                    Field::new("g_count", DataType::UInt64, false),
                    Field::new("t_count", DataType::UInt64, false),
                    Field::new("n_count", DataType::UInt64, false),
                ]
                .into(),
            ),
            true,
        )))),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(BaseSequenceContent::new()))),
        Arc::new(vec![
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            DataType::UInt64,
        ]),
    );
    ctx.session.register_udaf(udaf);
}

pub(crate) async fn do_base_sequence_content(ctx: &ExonSession, table_name: String) -> DataFrame {
    let query = format!(
        r#"
        WITH struct_column AS (
            SELECT 
                array_any_value(base_sequence_content(sequence)) AS my_struct
            FROM {}
        )
        SELECT
            my_struct.a_count AS A_count,
            my_struct.c_count AS C_count,
            my_struct.g_count AS G_count,
            my_struct.t_count AS T_count,
            my_struct.n_count AS N_count
        FROM struct_column;
        "#,
        table_name
    );

    ctx.sql(&query).await.unwrap()
}
