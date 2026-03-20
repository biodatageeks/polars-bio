use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayRef, BooleanBuilder, Float32Builder, Int32Builder, ListBuilder,
    StringBuilder, StructBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::arrow::error::ArrowError;
use std::sync::Arc;

/// Represents a key-value attribute pair used in bioinformatics file formats
pub struct Attribute {
    /// The attribute name/key
    pub tag: String,
    /// The optional attribute value
    pub value: Option<String>,
}

/// Builder wrapper for optional fields in Arrow record batches
///
/// Supports various data types including scalar and array types for building Arrow columns
#[derive(Debug)]
pub enum OptionalField {
    /// Builder for Int32 scalar values
    Int32Builder(Int32Builder),
    /// Builder for Int32 array values
    ArrayInt32Builder(ListBuilder<Int32Builder>),
    /// Builder for Float32 scalar values
    Float32Builder(Float32Builder),
    /// Builder for Float32 array values
    ArrayFloat32Builder(ListBuilder<Float32Builder>),
    /// Builder for Boolean scalar values
    BooleanBuilder(BooleanBuilder),
    /// Builder for Boolean array values
    ArrayBooleanBuilder(ListBuilder<BooleanBuilder>),
    /// Builder for UTF8 string scalar values
    Utf8Builder(StringBuilder),
    /// Builder for UTF8 string array values
    ArrayUtf8Builder(ListBuilder<StringBuilder>),
    /// Builder for structured array values (list of structs)
    ArrayStructBuilder(ListBuilder<StructBuilder>),
}

impl OptionalField {
    /// Creates a new OptionalField builder for the specified data type
    ///
    /// # Arguments
    ///
    /// * `data_type` - Arrow data type to build
    /// * `batch_size` - Initial capacity for the builder
    ///
    /// # Returns
    ///
    /// A new OptionalField builder or an error if the data type is unsupported
    ///
    /// # Errors
    ///
    /// Returns an error if the data type is not supported
    pub fn new(data_type: &DataType, batch_size: usize) -> Result<OptionalField, ArrowError> {
        match data_type {
            DataType::Int32 => Ok(OptionalField::Int32Builder(Int32Builder::with_capacity(
                batch_size,
            ))),
            DataType::Float32 => Ok(OptionalField::Float32Builder(
                Float32Builder::with_capacity(batch_size),
            )),
            DataType::Utf8 => Ok(OptionalField::Utf8Builder(StringBuilder::with_capacity(
                batch_size,
                batch_size * 10,
            ))),
            DataType::Boolean => Ok(OptionalField::BooleanBuilder(
                BooleanBuilder::with_capacity(batch_size),
            )),

            DataType::List(f) => match f.data_type() {
                DataType::Int32 => Ok(OptionalField::ArrayInt32Builder(
                    ListBuilder::with_capacity(Int32Builder::with_capacity(batch_size), batch_size),
                )),
                DataType::Float32 => Ok(OptionalField::ArrayFloat32Builder(
                    ListBuilder::with_capacity(
                        Float32Builder::with_capacity(batch_size),
                        batch_size,
                    ),
                )),
                DataType::Utf8 => Ok(OptionalField::ArrayUtf8Builder(ListBuilder::with_capacity(
                    StringBuilder::with_capacity(batch_size, batch_size * 10),
                    batch_size,
                ))),
                DataType::Boolean => Ok(OptionalField::ArrayBooleanBuilder(
                    ListBuilder::with_capacity(
                        BooleanBuilder::with_capacity(batch_size),
                        batch_size,
                    ),
                )),
                DataType::Struct(_) => {
                    let tag_builder = StringBuilder::with_capacity(batch_size, batch_size);
                    let value_builder = StringBuilder::with_capacity(batch_size, batch_size);

                    let struct_fields = Fields::from(vec![
                        Field::new("tag", DataType::Utf8, false),  // non-null
                        Field::new("value", DataType::Utf8, true), // nullable
                    ]);
                    let struct_builder = StructBuilder::new(
                        struct_fields,
                        vec![
                            Box::new(tag_builder) as Box<dyn ArrayBuilder>,
                            Box::new(value_builder) as Box<dyn ArrayBuilder>,
                        ],
                    );

                    // 3) wrap StructBuilder in a ListBuilder
                    Ok(OptionalField::ArrayStructBuilder(
                        ListBuilder::with_capacity(struct_builder, batch_size),
                    ))
                }

                _ => Err(ArrowError::SchemaError(
                    "Unsupported list inner data type".into(),
                )),
            },

            _ => Err(ArrowError::SchemaError("Unsupported data type".into())),
        }
    }

    /// Appends a list of structured attribute items to an ArrayStructBuilder
    ///
    /// # Arguments
    ///
    /// * `items` - Vector of Attribute key-value pairs to append
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an ArrayStructBuilder
    pub fn append_array_struct(&mut self, items: Vec<Attribute>) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayStructBuilder(list_builder) => {
                // 1) start a new list slot

                // 2) grab the StructBuilder inside
                let struct_builder = list_builder.values();

                // 3) push each Attribute
                for Attribute { tag, value } in items {
                    // field 0: tag (non-null)
                    struct_builder
                        .field_builder::<StringBuilder>(0)
                        .unwrap()
                        .append_value(&tag);
                    // field 1: value (nullable)
                    struct_builder
                        .field_builder::<StringBuilder>(1)
                        .unwrap()
                        .append_option(value.as_deref());
                    struct_builder.append(true);
                }

                list_builder.append(true);
                Ok(())
            }
            other => Err(ArrowError::SchemaError(format!(
                "Expected ArrayStructBuilder, found {other:?}"
            ))),
        }
    }
    /// Appends an integer value to the builder
    ///
    /// # Arguments
    ///
    /// * `value` - Int32 value to append
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an Int32Builder or ArrayInt32Builder
    pub fn append_int(&mut self, value: i32) -> Result<(), ArrowError> {
        match self {
            OptionalField::Int32Builder(builder) => {
                builder.append_value(value);
                Ok(())
            }
            OptionalField::ArrayInt32Builder(builder) => {
                builder.values().append_value(value);
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError("Invalid builder".into())),
        }
    }

    /// Appends a boolean value to the builder
    ///
    /// # Arguments
    ///
    /// * `value` - Boolean value to append
    ///
    /// # Errors
    ///
    /// Returns an error if this is not a BooleanBuilder or ArrayBooleanBuilder
    pub fn append_boolean(&mut self, value: bool) -> Result<(), ArrowError> {
        match self {
            OptionalField::BooleanBuilder(builder) => {
                builder.append_value(value);
                Ok(())
            }
            OptionalField::ArrayBooleanBuilder(builder) => {
                builder.values().append_value(value);
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError("Expected BooleanBuilder".into())),
        }
    }

    /// Appends integer values from an iterator as an array element, avoiding temporary Vec allocation
    ///
    /// # Arguments
    ///
    /// * `iter` - Iterator yielding i32 values to append as a single array element
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an ArrayInt32Builder
    pub fn append_array_int_iter(
        &mut self,
        iter: impl Iterator<Item = i32>,
    ) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayInt32Builder(builder) => {
                for v in iter {
                    builder.values().append_value(v);
                }
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError("Expected ArrayInt32Builder".into())),
        }
    }

    /// Appends float values from an iterator as an array element, avoiding temporary Vec allocation
    ///
    /// # Arguments
    ///
    /// * `iter` - Iterator yielding f32 values to append as a single array element
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an ArrayFloat32Builder
    pub fn append_array_float_iter(
        &mut self,
        iter: impl Iterator<Item = f32>,
    ) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayFloat32Builder(builder) => {
                for v in iter {
                    builder.values().append_value(v);
                }
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError(
                "Expected ArrayFloat32Builder".into(),
            )),
        }
    }

    /// Appends string values from an iterator as an array element, avoiding temporary Vec allocation
    ///
    /// # Arguments
    ///
    /// * `iter` - Iterator yielding &str values to append as a single array element
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an ArrayUtf8Builder
    pub fn append_array_string_iter(
        &mut self,
        iter: impl Iterator<Item = impl AsRef<str>>,
    ) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayUtf8Builder(builder) => {
                for v in iter {
                    builder.values().append_value(v.as_ref());
                }
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError("Expected ArrayUtf8Builder".into())),
        }
    }

    /// Appends a vector of integers as an array element
    ///
    /// # Arguments
    ///
    /// * `value` - Vector of Int32 values to append as a single array element
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an ArrayInt32Builder
    pub fn append_array_int(&mut self, value: Vec<i32>) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayInt32Builder(builder) => {
                builder.values().append_slice(&value);
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError("Expected ArrayInt32Builder".into())),
        }
    }

    /// Appends a vector of nullable integers as an array element
    ///
    /// Preserves null entries in the array (e.g., for VCF AD=10,. -> [10, null])
    ///
    /// # Arguments
    ///
    /// * `value` - Vector of `Option<i32>` values to append as a single array element
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an ArrayInt32Builder
    pub fn append_array_int_nullable(&mut self, value: Vec<Option<i32>>) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayInt32Builder(builder) => {
                for v in value {
                    match v {
                        Some(i) => builder.values().append_value(i),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError("Expected ArrayInt32Builder".into())),
        }
    }

    /// Appends nullable integer values from an iterator as an array element.
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an ArrayInt32Builder
    pub fn append_array_int_nullable_iter(
        &mut self,
        iter: impl Iterator<Item = Option<i32>>,
    ) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayInt32Builder(builder) => {
                for v in iter {
                    match v {
                        Some(i) => builder.values().append_value(i),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError("Expected ArrayInt32Builder".into())),
        }
    }

    /// Appends a float value to the builder
    ///
    /// # Arguments
    ///
    /// * `value` - Float32 value to append
    ///
    /// # Errors
    ///
    /// Returns an error if this is not a Float32Builder or ArrayFloat32Builder
    pub fn append_float(&mut self, value: f32) -> Result<(), ArrowError> {
        match self {
            OptionalField::Float32Builder(builder) => {
                builder.append_value(value);
                Ok(())
            }
            OptionalField::ArrayFloat32Builder(builder) => {
                builder.values().append_value(value);
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError("Expected Float32Builder".into())),
        }
    }

    /// Appends a vector of floats as an array element
    ///
    /// # Arguments
    ///
    /// * `value` - Vector of Float32 values to append as a single array element
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an ArrayFloat32Builder
    pub fn append_array_float(&mut self, value: Vec<f32>) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayFloat32Builder(builder) => {
                builder.values().append_slice(&value);
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError(
                "Expected ArrayFloat32Builder".into(),
            )),
        }
    }

    /// Appends a vector of nullable floats as an array element
    ///
    /// Preserves null entries in the array (e.g., for VCF AF=0.5,. -> [0.5, null])
    ///
    /// # Arguments
    ///
    /// * `value` - Vector of `Option<f32>` values to append as a single array element
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an ArrayFloat32Builder
    pub fn append_array_float_nullable(
        &mut self,
        value: Vec<Option<f32>>,
    ) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayFloat32Builder(builder) => {
                for v in value {
                    match v {
                        Some(f) => builder.values().append_value(f),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError(
                "Expected ArrayFloat32Builder".into(),
            )),
        }
    }

    /// Appends nullable float values from an iterator as an array element.
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an ArrayFloat32Builder
    pub fn append_array_float_nullable_iter(
        &mut self,
        iter: impl Iterator<Item = Option<f32>>,
    ) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayFloat32Builder(builder) => {
                for v in iter {
                    match v {
                        Some(f) => builder.values().append_value(f),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError(
                "Expected ArrayFloat32Builder".into(),
            )),
        }
    }

    /// Appends a string value to the builder
    ///
    /// # Arguments
    ///
    /// * `value` - String value to append
    ///
    /// # Errors
    ///
    /// Returns an error if this is not a Utf8Builder or ArrayUtf8Builder
    pub fn append_string(&mut self, value: &str) -> Result<(), ArrowError> {
        match self {
            OptionalField::Utf8Builder(builder) => {
                builder.append_value(value);
                Ok(())
            }
            OptionalField::ArrayUtf8Builder(builder) => {
                builder.values().append_value(value);
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError("Expected Utf8Builder".into())),
        }
    }

    /// Appends a vector of strings as an array element
    ///
    /// # Arguments
    ///
    /// * `value` - Vector of String values to append as a single array element
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an ArrayUtf8Builder
    pub fn append_array_string(&mut self, value: Vec<String>) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayUtf8Builder(builder) => {
                for v in value {
                    builder.values().append_value(&v);
                }
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError("Expected ArrayUtf8Builder".into())),
        }
    }

    /// Appends a vector of nullable strings as an array element
    ///
    /// Preserves null entries in the array
    ///
    /// # Arguments
    ///
    /// * `value` - Vector of `Option<String>` values to append as a single array element
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an ArrayUtf8Builder
    pub fn append_array_string_nullable(
        &mut self,
        value: Vec<Option<String>>,
    ) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayUtf8Builder(builder) => {
                for v in value {
                    match v {
                        Some(s) => builder.values().append_value(&s),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError("Expected ArrayUtf8Builder".into())),
        }
    }

    /// Appends nullable string values from an iterator as an array element.
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an ArrayUtf8Builder
    pub fn append_array_string_nullable_iter(
        &mut self,
        iter: impl Iterator<Item = Option<String>>,
    ) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayUtf8Builder(builder) => {
                for v in iter {
                    match v {
                        Some(s) => builder.values().append_value(&s),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
                Ok(())
            }
            _ => Err(ArrowError::SchemaError("Expected ArrayUtf8Builder".into())),
        }
    }

    /// Appends a null value to the builder
    ///
    /// # Errors
    ///
    /// This method does not return errors in practice
    pub fn append_null(&mut self) -> Result<(), ArrowError> {
        match self {
            OptionalField::Int32Builder(builder) => {
                builder.append_null();
                Ok(())
            }
            OptionalField::ArrayInt32Builder(builder) => {
                builder.append_null();
                Ok(())
            }
            OptionalField::Utf8Builder(builder) => {
                builder.append_null();
                Ok(())
            }
            OptionalField::ArrayUtf8Builder(builder) => {
                builder.append_null();
                Ok(())
            }
            OptionalField::Float32Builder(builder) => {
                builder.append_null();
                Ok(())
            }
            OptionalField::ArrayFloat32Builder(builder) => {
                builder.append_null();
                Ok(())
            }
            OptionalField::BooleanBuilder(builder) => {
                builder.append_null();
                Ok(())
            }
            OptionalField::ArrayBooleanBuilder(builder) => {
                builder.append_null();
                Ok(())
            }
            OptionalField::ArrayStructBuilder(builder) => {
                builder.append_null();
                Ok(())
            }
        }
    }

    /// Finalizes the builder and returns the built Arrow array
    ///
    /// # Returns
    ///
    /// The completed Arrow array
    ///
    /// # Errors
    ///
    /// This method does not return errors in practice
    pub fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        match self {
            OptionalField::Int32Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::ArrayInt32Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::Utf8Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::ArrayUtf8Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::Float32Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::ArrayFloat32Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::BooleanBuilder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::ArrayBooleanBuilder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::ArrayStructBuilder(builder) => {
                let struct_array = builder.finish();
                Ok(Arc::new(struct_array))
            }
        }
    }
}

/// Converts a vector of OptionalField builders to a vector of Arrow arrays
///
/// # Arguments
///
/// * `builders` - Mutable reference to slice of OptionalField builders
///
/// # Returns
///
/// Vector of finalized Arrow arrays
///
/// # Panics
///
/// Panics if any builder fails to finish (which should not happen in normal operation)
pub fn builders_to_arrays(builders: &mut [OptionalField]) -> Vec<Arc<dyn Array>> {
    builders
        .iter_mut()
        .map(|f| f.finish())
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}
