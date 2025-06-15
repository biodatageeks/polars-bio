use std::mem;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use polars::prelude::{PlSmallStr, PolarsError};
use polars_core::prelude::{CompatLevel, DataFrame, Series};

pub(crate) fn default_cols_to_string(s: &[&str; 3]) -> Vec<String> {
    s.iter().map(|x| x.to_string()).collect()
}

fn convert_arrow_rs_field_to_polars_arrow_field(
    arrow_rs_field: &arrow_schema::Field,
) -> Result<polars_arrow::datatypes::Field, &str> {
    let arrow_rs_dtype = arrow_rs_field.data_type();
    let arrow_c_schema = arrow_array::ffi::FFI_ArrowSchema::try_from(arrow_rs_dtype).unwrap();
    let polars_c_schema: polars_arrow::ffi::ArrowSchema = unsafe { mem::transmute(arrow_c_schema) };
    Ok(unsafe { polars_arrow::ffi::import_field_from_c(&polars_c_schema) }.unwrap())
}

pub fn convert_arrow_rb_schema_to_polars_df_schema(
    arrow_schema: &arrow_schema::Schema,
) -> Result<polars::prelude::Schema, PolarsError> {
    let polars_df_fields: Result<Vec<polars::prelude::Field>, PolarsError> = arrow_schema
        .fields()
        .iter()
        .map(|arrow_rs_field| {
            let polars_arrow_field: polars_arrow::datatypes::Field =
                convert_arrow_rs_field_to_polars_arrow_field(arrow_rs_field).unwrap();
            Ok(polars::prelude::Field::new(
                PlSmallStr::from(arrow_rs_field.name()),
                polars::datatypes::DataType::from_arrow_dtype(polars_arrow_field.dtype()),
            ))
        })
        .collect();
    Ok(polars::prelude::Schema::from_iter(polars_df_fields?))
}

fn convert_arrow_rs_array_to_polars_arrow_array(
    arrow_rs_array: &Arc<dyn arrow_array::Array>,
    polars_arrow_dtype: polars::datatypes::ArrowDataType,
) -> Result<Box<dyn polars_arrow::array::Array>, PolarsError> {
    let export_arrow = arrow_array::ffi::to_ffi(&arrow_rs_array.to_data()).unwrap();
    let arrow_c_array = export_arrow.0;
    let polars_c_array: polars_arrow::ffi::ArrowArray = unsafe { mem::transmute(arrow_c_array) };
    Ok(unsafe { polars_arrow::ffi::import_array_from_c(polars_c_array, polars_arrow_dtype) }?)
}

pub fn convert_arrow_rb_to_polars_df(
    arrow_rb: &RecordBatch,
    polars_schema: &polars::prelude::Schema,
) -> Result<DataFrame, PolarsError> {
    let mut columns: Vec<Series> = Vec::with_capacity(arrow_rb.num_columns());

    for (i, column) in arrow_rb.columns().iter().enumerate() {
        let polars_df_dtype = polars_schema.try_get_at_index(i)?.1;
        let mut polars_arrow_dtype = polars_df_dtype.to_arrow(CompatLevel::oldest());
        if polars_arrow_dtype == polars::datatypes::ArrowDataType::LargeUtf8 {
            polars_arrow_dtype = polars::datatypes::ArrowDataType::Utf8;
        }
        let polars_array =
            convert_arrow_rs_array_to_polars_arrow_array(column, polars_arrow_dtype)?;
        let series =
            Series::from_arrow(polars_schema.try_get_at_index(i)?.0.clone(), polars_array)?;
        columns.push(series);
    }

    Ok(DataFrame::from_iter(columns))
}
