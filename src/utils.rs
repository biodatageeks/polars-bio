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
    // Pre-calculate all schema info to avoid repeated lookups
    let schema_info: Vec<_> = (0..arrow_rb.num_columns())
        .map(|i| polars_schema.try_get_at_index(i))
        .collect::<Result<Vec<_>, _>>()?;

    // Pre-calculate Arrow dtypes to avoid repeated conversions
    let arrow_dtypes: Vec<_> = schema_info
        .iter()
        .map(|(_, polars_df_dtype)| {
            let mut polars_arrow_dtype = polars_df_dtype.to_arrow(CompatLevel::oldest());
            if polars_arrow_dtype == polars::datatypes::ArrowDataType::LargeUtf8 {
                polars_arrow_dtype = polars::datatypes::ArrowDataType::Utf8;
            }
            polars_arrow_dtype
        })
        .collect();

    // Use iterator with collect for better optimization - process all columns in parallel
    let columns: Result<Vec<_>, _> = arrow_rb
        .columns()
        .iter()
        .zip(schema_info.iter())
        .zip(arrow_dtypes.iter())
        .map(|((column, (name, _)), arrow_dtype)| {
            let polars_array =
                convert_arrow_rs_array_to_polars_arrow_array(column, arrow_dtype.clone())?;
            Series::from_arrow((*name).clone(), polars_array)
        })
        .collect();

    // Use from_iter which accepts Series (new expects Column)
    Ok(DataFrame::from_iter(columns?))
}
