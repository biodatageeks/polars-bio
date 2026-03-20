//! Scalar UDFs for analytical queries on VCF columnar genotype lists.
//!
//! These UDFs operate on `List<T>` columns produced by the multi-sample columnar
//! genotypes schema, enabling bcftools-style filtering in SQL.

use datafusion::arrow::array::{
    Array, AsArray, BooleanArray, BooleanBuilder, Float32Array, Float64Builder, Int32Array,
    ListArray, ListBuilder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::TypeSignature::Exact;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

// ============================================================================
// list_avg — average of non-null elements in a list
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct ListAvgUdf {
    signature: Signature,
}

impl ListAvgUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Int32,
                        true,
                    )))]),
                    Exact(vec![DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Float32,
                        true,
                    )))]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ListAvgUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "list_avg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = args.args[0].clone().into_array(1)?;
        let list = array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("list_avg expects List input".to_string()))?;
        let mut builder = Float64Builder::with_capacity(list.len());
        for i in 0..list.len() {
            if list.is_null(i) {
                builder.append_null();
                continue;
            }
            let inner = list.value(i);
            let (sum, count) = compute_sum_count(&inner);
            if count == 0 {
                builder.append_null();
            } else {
                builder.append_value(sum / count as f64);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

fn compute_sum_count(array: &dyn Array) -> (f64, usize) {
    let mut sum = 0.0_f64;
    let mut count = 0_usize;
    if let Some(int_arr) = array.as_any().downcast_ref::<Int32Array>() {
        for i in 0..int_arr.len() {
            if !int_arr.is_null(i) {
                sum += int_arr.value(i) as f64;
                count += 1;
            }
        }
    } else if let Some(float_arr) = array.as_any().downcast_ref::<Float32Array>() {
        for i in 0..float_arr.len() {
            if !float_arr.is_null(i) {
                sum += float_arr.value(i) as f64;
                count += 1;
            }
        }
    }
    (sum, count)
}

/// Creates the `list_avg` scalar UDF.
pub fn list_avg_udf() -> ScalarUDF {
    ScalarUDF::from(ListAvgUdf::new())
}

/// Parses a GT string into allele indices.
/// Returns None for entirely missing genotypes (".", "./.", ".|.").
/// Splits on "/" or "|" and parses each allele as usize.
/// Non-integer alleles (e.g., ".") are treated as missing (None).
fn parse_gt_alleles(gt: &str) -> Option<Vec<Option<usize>>> {
    let gt = gt.trim();
    if gt == "." || gt == "./." || gt == ".|." {
        return None; // entirely missing
    }
    let alleles: Vec<Option<usize>> = gt
        .split(['/', '|'])
        .map(|a| {
            let a = a.trim();
            if a == "." {
                None
            } else {
                a.parse::<usize>().ok()
            }
        })
        .collect();
    if alleles.is_empty() {
        None
    } else {
        Some(alleles)
    }
}

/// Counts the number of ALT alleles from a pipe-separated ALT string.
///
/// E.g., `"A|T"` → 2, `"A"` → 1, `""` / `"."` → 0.
fn count_alt_alleles(alt: &str) -> usize {
    let alt = alt.trim();
    if alt.is_empty() || alt == "." {
        0
    } else {
        alt.split('|').count()
    }
}

// ============================================================================
// vcf_an — allele number: count of called (non-missing) alleles
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct VcfAnUdf {
    signature: Signature,
}

impl VcfAnUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                )))],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for VcfAnUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "vcf_an"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = args.args[0].clone().into_array(1)?;
        let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            DataFusionError::Execution("vcf_an expects List<Utf8> input".to_string())
        })?;
        let mut builder = datafusion::arrow::array::Int32Builder::with_capacity(list.len());
        for i in 0..list.len() {
            if list.is_null(i) {
                builder.append_null();
                continue;
            }
            let inner = list.value(i);
            let gt_strings = inner
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("vcf_an expects List<Utf8> input".to_string())
                })?;
            let mut an = 0i32;
            for j in 0..gt_strings.len() {
                if gt_strings.is_null(j) {
                    continue;
                }
                if let Some(alleles) = parse_gt_alleles(gt_strings.value(j)) {
                    an += alleles.iter().filter(|a| a.is_some()).count() as i32;
                }
            }
            builder.append_value(an);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Creates the `vcf_an` scalar UDF.
pub fn vcf_an_udf() -> ScalarUDF {
    ScalarUDF::from(VcfAnUdf::new())
}

// ============================================================================
// vcf_ac — allele count per ALT allele
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct VcfAcUdf {
    signature: Signature,
}

impl VcfAcUdf {
    fn new() -> Self {
        let gt_list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        Self {
            signature: Signature::one_of(
                vec![
                    // 1-arg: vcf_ac(gt_list)
                    Exact(vec![gt_list.clone()]),
                    // 2-arg: vcf_ac(gt_list, alt)
                    Exact(vec![gt_list, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for VcfAcUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "vcf_ac"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = args.args[0].clone().into_array(1)?;
        let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            DataFusionError::Execution("vcf_ac expects List<Utf8> input".to_string())
        })?;

        // Optional 2nd arg: alt column (pipe-separated ALT alleles)
        let alt_array = if args.args.len() > 1 {
            let alt_arr = args.args[1].clone().into_array(list.len())?;
            Some(
                alt_arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "vcf_ac 2nd argument must be Utf8 (alt column)".to_string(),
                        )
                    })?
                    .clone(),
            )
        } else {
            None
        };

        let mut builder = ListBuilder::new(datafusion::arrow::array::Int32Builder::new());
        for i in 0..list.len() {
            if list.is_null(i) {
                builder.append(false);
                continue;
            }
            let inner = list.value(i);
            let gt_strings = inner
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("vcf_ac expects List<Utf8> input".to_string())
                })?;

            // Determine num_alt from the alt column (if provided)
            let num_alt_from_col = alt_array
                .as_ref()
                .filter(|a| !a.is_null(i))
                .map(|a| count_alt_alleles(a.value(i)));

            // First pass: find max allele index
            let mut max_allele: usize = 0;
            let mut has_called = false;
            for j in 0..gt_strings.len() {
                if gt_strings.is_null(j) {
                    continue;
                }
                if let Some(alleles) = parse_gt_alleles(gt_strings.value(j)) {
                    for idx in alleles.iter().flatten() {
                        has_called = true;
                        if *idx > max_allele {
                            max_allele = *idx;
                        }
                    }
                }
            }

            // Output vector length: max(num_alt_from_col, max_allele)
            let vec_len = match num_alt_from_col {
                Some(n) => n.max(max_allele),
                None => max_allele,
            };

            if vec_len == 0 {
                // No ALT alleles — empty list
                builder.append(true);
                continue;
            }

            // Second pass: count each ALT allele (1..=vec_len)
            let mut counts = vec![0i32; vec_len];
            if has_called {
                for j in 0..gt_strings.len() {
                    if gt_strings.is_null(j) {
                        continue;
                    }
                    if let Some(alleles) = parse_gt_alleles(gt_strings.value(j)) {
                        for idx in alleles.iter().flatten() {
                            if *idx >= 1 && *idx <= vec_len {
                                counts[*idx - 1] += 1;
                            }
                        }
                    }
                }
            }

            for c in &counts {
                builder.values().append_value(*c);
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Creates the `vcf_ac` scalar UDF.
///
/// Signatures:
/// - `vcf_ac(List<Utf8>) -> List<Int32>` — infer output length from max GT allele
/// - `vcf_ac(List<Utf8>, Utf8) -> List<Int32>` — use ALT column for output length
pub fn vcf_ac_udf() -> ScalarUDF {
    ScalarUDF::from(VcfAcUdf::new())
}

// ============================================================================
// vcf_af — allele frequency per ALT allele (AC / AN)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct VcfAfUdf {
    signature: Signature,
}

impl VcfAfUdf {
    fn new() -> Self {
        let gt_list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        Self {
            signature: Signature::one_of(
                vec![
                    // 1-arg: vcf_af(gt_list)
                    Exact(vec![gt_list.clone()]),
                    // 2-arg: vcf_af(gt_list, alt)
                    Exact(vec![gt_list, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for VcfAfUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "vcf_af"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Float64,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = args.args[0].clone().into_array(1)?;
        let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            DataFusionError::Execution("vcf_af expects List<Utf8> input".to_string())
        })?;

        // Optional 2nd arg: alt column (pipe-separated ALT alleles)
        let alt_array = if args.args.len() > 1 {
            let alt_arr = args.args[1].clone().into_array(list.len())?;
            Some(
                alt_arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "vcf_af 2nd argument must be Utf8 (alt column)".to_string(),
                        )
                    })?
                    .clone(),
            )
        } else {
            None
        };

        let mut builder = ListBuilder::new(Float64Builder::new());
        for i in 0..list.len() {
            if list.is_null(i) {
                builder.append(false);
                continue;
            }
            let inner = list.value(i);
            let gt_strings = inner
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("vcf_af expects List<Utf8> input".to_string())
                })?;

            // Determine num_alt from the alt column (if provided)
            let num_alt_from_col = alt_array
                .as_ref()
                .filter(|a| !a.is_null(i))
                .map(|a| count_alt_alleles(a.value(i)));

            // First pass: find max_allele and AN
            let mut an = 0i32;
            let mut max_allele: usize = 0;
            let mut has_called = false;
            for j in 0..gt_strings.len() {
                if gt_strings.is_null(j) {
                    continue;
                }
                if let Some(alleles) = parse_gt_alleles(gt_strings.value(j)) {
                    for idx in alleles.iter().flatten() {
                        has_called = true;
                        an += 1;
                        if *idx > max_allele {
                            max_allele = *idx;
                        }
                    }
                }
            }

            // Output vector length: max(num_alt_from_col, max_allele)
            let vec_len = match num_alt_from_col {
                Some(n) => n.max(max_allele),
                None => max_allele,
            };

            if vec_len == 0 {
                // No ALT alleles — empty list
                builder.append(true);
                continue;
            }

            if an == 0 {
                // All missing — produce NULLs of length vec_len
                for _ in 0..vec_len {
                    builder.values().append_null();
                }
                builder.append(true);
                continue;
            }

            // Second pass: count each ALT allele
            let mut counts = vec![0i32; vec_len];
            if has_called {
                for j in 0..gt_strings.len() {
                    if gt_strings.is_null(j) {
                        continue;
                    }
                    if let Some(alleles) = parse_gt_alleles(gt_strings.value(j)) {
                        for idx in alleles.iter().flatten() {
                            if *idx >= 1 && *idx <= vec_len {
                                counts[*idx - 1] += 1;
                            }
                        }
                    }
                }
            }

            for c in &counts {
                builder.values().append_value(*c as f64 / an as f64);
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Creates the `vcf_af` scalar UDF.
///
/// Signatures:
/// - `vcf_af(List<Utf8>) -> List<Float64>` — infer output length from max GT allele
/// - `vcf_af(List<Utf8>, Utf8) -> List<Float64>` — use ALT column for output length
pub fn vcf_af_udf() -> ScalarUDF {
    ScalarUDF::from(VcfAfUdf::new())
}

// ============================================================================
// list_gte — element-wise >= comparison: List<Int32>, Int32 → List<Boolean>
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct ListGteUdf {
    signature: Signature,
}

impl ListGteUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![
                        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                        DataType::Int32,
                    ]),
                    Exact(vec![
                        DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                        DataType::Float32,
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ListGteUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "list_gte"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Boolean,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let list_arr = args.args[0].clone().into_array(1)?;
        let threshold_arr = args.args[1].clone().into_array(1)?;
        let list = list_arr.as_list::<i32>();
        let mut builder = ListBuilder::new(BooleanBuilder::new());

        for i in 0..list.len() {
            if list.is_null(i) {
                builder.append(false);
                continue;
            }
            let inner = list.value(i);
            if let Some(int_arr) = inner.as_any().downcast_ref::<Int32Array>() {
                let threshold = threshold_arr
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .map(|a| a.value(if a.len() == 1 { 0 } else { i }))
                    .unwrap_or(0);
                for j in 0..int_arr.len() {
                    if int_arr.is_null(j) {
                        builder.values().append_null();
                    } else {
                        builder.values().append_value(int_arr.value(j) >= threshold);
                    }
                }
            } else if let Some(float_arr) = inner.as_any().downcast_ref::<Float32Array>() {
                let threshold = threshold_arr
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .map(|a| a.value(if a.len() == 1 { 0 } else { i }))
                    .unwrap_or(0.0);
                for j in 0..float_arr.len() {
                    if float_arr.is_null(j) {
                        builder.values().append_null();
                    } else {
                        builder
                            .values()
                            .append_value(float_arr.value(j) >= threshold);
                    }
                }
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Creates the `list_gte` scalar UDF.
pub fn list_gte_udf() -> ScalarUDF {
    ScalarUDF::from(ListGteUdf::new())
}

// ============================================================================
// list_lte — element-wise <= comparison: List<Int32>, Int32 → List<Boolean>
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct ListLteUdf {
    signature: Signature,
}

impl ListLteUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![
                        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                        DataType::Int32,
                    ]),
                    Exact(vec![
                        DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                        DataType::Float32,
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ListLteUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "list_lte"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Boolean,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let list_arr = args.args[0].clone().into_array(1)?;
        let threshold_arr = args.args[1].clone().into_array(1)?;
        let list = list_arr.as_list::<i32>();
        let mut builder = ListBuilder::new(BooleanBuilder::new());

        for i in 0..list.len() {
            if list.is_null(i) {
                builder.append(false);
                continue;
            }
            let inner = list.value(i);
            if let Some(int_arr) = inner.as_any().downcast_ref::<Int32Array>() {
                let threshold = threshold_arr
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .map(|a| a.value(if a.len() == 1 { 0 } else { i }))
                    .unwrap_or(0);
                for j in 0..int_arr.len() {
                    if int_arr.is_null(j) {
                        builder.values().append_null();
                    } else {
                        builder.values().append_value(int_arr.value(j) <= threshold);
                    }
                }
            } else if let Some(float_arr) = inner.as_any().downcast_ref::<Float32Array>() {
                let threshold = threshold_arr
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .map(|a| a.value(if a.len() == 1 { 0 } else { i }))
                    .unwrap_or(0.0);
                for j in 0..float_arr.len() {
                    if float_arr.is_null(j) {
                        builder.values().append_null();
                    } else {
                        builder
                            .values()
                            .append_value(float_arr.value(j) <= threshold);
                    }
                }
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Creates the `list_lte` scalar UDF.
pub fn list_lte_udf() -> ScalarUDF {
    ScalarUDF::from(ListLteUdf::new())
}

// ============================================================================
// list_and — element-wise AND: List<Boolean>, List<Boolean> → List<Boolean>
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct ListAndUdf {
    signature: Signature,
}

impl ListAndUdf {
    fn new() -> Self {
        let bool_list = DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)));
        Self {
            signature: Signature::exact(vec![bool_list.clone(), bool_list], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ListAndUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "list_and"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Boolean,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let left_arr = args.args[0].clone().into_array(1)?;
        let right_arr = args.args[1].clone().into_array(1)?;
        let left = left_arr.as_list::<i32>();
        let right = right_arr.as_list::<i32>();
        let mut builder = ListBuilder::new(BooleanBuilder::new());

        for i in 0..left.len() {
            if left.is_null(i) || right.is_null(i) {
                builder.append(false);
                continue;
            }
            let left_inner = left.value(i);
            let right_inner = right.value(i);
            let left_bools = left_inner.as_any().downcast_ref::<BooleanArray>().unwrap();
            let right_bools = right_inner.as_any().downcast_ref::<BooleanArray>().unwrap();
            let len = left_bools.len().min(right_bools.len());
            for j in 0..len {
                let l_null = left_bools.is_null(j);
                let r_null = right_bools.is_null(j);
                if l_null && r_null {
                    builder.values().append_null();
                } else if l_null {
                    // SQL 3VL: null AND false = false, null AND true = null
                    if right_bools.value(j) {
                        builder.values().append_null();
                    } else {
                        builder.values().append_value(false);
                    }
                } else if r_null {
                    // SQL 3VL: false AND null = false, true AND null = null
                    if left_bools.value(j) {
                        builder.values().append_null();
                    } else {
                        builder.values().append_value(false);
                    }
                } else {
                    builder
                        .values()
                        .append_value(left_bools.value(j) && right_bools.value(j));
                }
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Creates the `list_and` scalar UDF.
pub fn list_and_udf() -> ScalarUDF {
    ScalarUDF::from(ListAndUdf::new())
}

// ============================================================================
// vcf_set_gts — replace GT values where mask is false (like bcftools +setGT)
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct VcfSetGtsUdf {
    signature: Signature,
}

impl VcfSetGtsUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
                    DataType::Utf8,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for VcfSetGtsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "vcf_set_gts"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let gt_arr = args.args[0].clone().into_array(1)?;
        let mask_arr = args.args[1].clone().into_array(1)?;
        let rep_arr = args.args[2].clone().into_array(1)?;
        let replacement = rep_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(0).to_string())
            .unwrap_or_else(|| "./.".to_string());

        let gt_list = gt_arr.as_list::<i32>();
        let mask_list = mask_arr.as_list::<i32>();
        let mut builder = ListBuilder::new(StringBuilder::new());

        for i in 0..gt_list.len() {
            if gt_list.is_null(i) {
                builder.append(false);
                continue;
            }
            let gt_inner = gt_list.value(i);
            let gt_strings = gt_inner.as_any().downcast_ref::<StringArray>().unwrap();

            let mask_bools = if !mask_list.is_null(i) {
                let mask_inner = mask_list.value(i);
                Some(
                    mask_inner
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .clone(),
                )
            } else {
                None
            };

            for j in 0..gt_strings.len() {
                // null mask → keep original GT (matches bcftools: missing filter
                // values don't trigger the exclude condition)
                let keep = mask_bools
                    .as_ref()
                    .map(|m| j >= m.len() || m.is_null(j) || m.value(j))
                    .unwrap_or(true);
                if keep {
                    if gt_strings.is_null(j) {
                        // Preserve null GT as null — serializer writes "."
                        builder.values().append_null();
                    } else {
                        builder.values().append_value(gt_strings.value(j));
                    }
                } else {
                    builder.values().append_value(&replacement);
                }
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Creates the `vcf_set_gts` scalar UDF.
pub fn vcf_set_gts_udf() -> ScalarUDF {
    ScalarUDF::from(VcfSetGtsUdf::new())
}

// ============================================================================
// Registration helper
// ============================================================================

/// Registers all VCF analytical UDFs with the given SessionContext.
///
/// Available UDFs:
/// - `list_avg(List<Int32|Float32>) -> Float64` — average of non-null list elements
/// - `list_gte(List<Int32|Float32>, T) -> List<Boolean>` — element-wise >=
/// - `list_lte(List<Int32|Float32>, T) -> List<Boolean>` — element-wise <=
/// - `list_and(List<Boolean>, List<Boolean>) -> List<Boolean>` — element-wise AND
/// - `vcf_set_gts(List<Utf8>, List<Boolean>, Utf8) -> List<Utf8>` — replace GT where mask is false with the given replacement string
/// - `vcf_an(List<Utf8>) -> Int32` — allele number (count of called alleles)
/// - `vcf_ac(List<Utf8> [, Utf8]) -> List<Int32>` — per-ALT-allele call count (optional ALT column for correct cardinality)
/// - `vcf_af(List<Utf8> [, Utf8]) -> List<Float64>` — per-ALT-allele frequency (optional ALT column for correct cardinality)
pub fn register_vcf_udfs(ctx: &datafusion::prelude::SessionContext) {
    ctx.register_udf(list_avg_udf());
    ctx.register_udf(list_gte_udf());
    ctx.register_udf(list_lte_udf());
    ctx.register_udf(list_and_udf());
    ctx.register_udf(vcf_set_gts_udf());
    ctx.register_udf(vcf_an_udf());
    ctx.register_udf(vcf_ac_udf());
    ctx.register_udf(vcf_af_udf());
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Builder, ListBuilder};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::*;

    async fn make_test_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        register_vcf_udfs(&ctx);

        // Create a test batch with List<Int32> columns
        let mut gq_builder = ListBuilder::new(Int32Builder::new());
        gq_builder.values().append_value(30);
        gq_builder.values().append_value(20);
        gq_builder.values().append_value(10);
        gq_builder.append(true);
        gq_builder.values().append_value(5);
        gq_builder.values().append_null();
        gq_builder.values().append_value(15);
        gq_builder.append(true);

        let mut dp_builder = ListBuilder::new(Int32Builder::new());
        dp_builder.values().append_value(50);
        dp_builder.values().append_value(30);
        dp_builder.values().append_value(20);
        dp_builder.append(true);
        dp_builder.values().append_value(10);
        dp_builder.values().append_value(200);
        dp_builder.values().append_value(100);
        dp_builder.append(true);

        let mut gt_builder = ListBuilder::new(StringBuilder::new());
        gt_builder.values().append_value("0/1");
        gt_builder.values().append_value("1/1");
        gt_builder.values().append_value("0/0");
        gt_builder.append(true);
        gt_builder.values().append_value("./.");
        gt_builder.values().append_value("0/1");
        gt_builder.values().append_value("1/1");
        gt_builder.append(true);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "GQ",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new(
                "DP",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new(
                "GT",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(gq_builder.finish()),
                Arc::new(dp_builder.finish()),
                Arc::new(gt_builder.finish()),
            ],
        )
        .unwrap();

        ctx.register_batch("test_data", batch).unwrap();
        ctx
    }

    #[tokio::test]
    async fn test_list_avg_int32() {
        let ctx = make_test_ctx().await;
        let df = ctx
            .sql(r#"SELECT list_avg("GQ") as avg_gq FROM test_data"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let result = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        // Row 0: avg(30, 20, 10) = 20.0
        assert!((result.value(0) - 20.0).abs() < 0.001);
        // Row 1: avg(5, 15) = 10.0 (null skipped)
        assert!((result.value(1) - 10.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_list_gte() {
        let ctx = make_test_ctx().await;
        let df = ctx
            .sql(r#"SELECT list_gte("GQ", 15) as gq_gte FROM test_data"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        // Row 0: [30>=15, 20>=15, 10>=15] = [true, true, false]
        let inner0 = list.value(0);
        let bools0 = inner0.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bools0.value(0));
        assert!(bools0.value(1));
        assert!(!bools0.value(2));
    }

    #[tokio::test]
    async fn test_vcf_set_gts() {
        let ctx = make_test_ctx().await;
        let df = ctx
            .sql(r#"SELECT vcf_set_gts("GT", list_gte("GQ", 15), './.') as masked FROM test_data"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        // Row 0: GQ=[30,20,10], gte(15)=[T,T,F] → GT=["0/1","1/1","./.""]
        let inner0 = list.value(0);
        let strings0 = inner0.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings0.value(0), "0/1");
        assert_eq!(strings0.value(1), "1/1");
        assert_eq!(strings0.value(2), "./.");
        // Row 1: GQ=[5,null,15], gte(15)=[F,null,T] → GT=["./.", "0/1", "1/1"]
        // null mask → keep original GT (matches bcftools: missing values don't trigger filter)
        let inner1 = list.value(1);
        let strings1 = inner1.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings1.value(0), "./.");
        assert_eq!(strings1.value(1), "0/1"); // null mask → preserve original
        assert_eq!(strings1.value(2), "1/1");
    }

    #[tokio::test]
    async fn test_list_and() {
        let ctx = make_test_ctx().await;
        let df = ctx
            .sql(r#"SELECT list_and(list_gte("GQ", 10), list_lte("DP", 100)) as combined FROM test_data"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        // Row 0: GQ=[30,20,10], DP=[50,30,20]
        // gte(GQ,10)=[T,T,T], lte(DP,100)=[T,T,T] → and=[T,T,T]
        let inner0 = list.value(0);
        let bools0 = inner0.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bools0.value(0));
        assert!(bools0.value(1));
        assert!(bools0.value(2));
        // Row 1: GQ=[5,null,15], DP=[10,200,100]
        // gte(GQ,10)=[F,null,T], lte(DP,100)=[T,F,T]
        // SQL 3VL: F AND T=F, null AND F=F, T AND T=T → and=[F,F,T]
        let inner1 = list.value(1);
        let bools1 = inner1.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!bools1.value(0));
        assert!(!bools1.value(1)); // null AND false = false (SQL 3VL)
        assert!(bools1.value(2));
    }

    #[test]
    fn test_parse_gt_alleles() {
        // Standard diploid
        let r = parse_gt_alleles("0/1").unwrap();
        assert_eq!(r, vec![Some(0), Some(1)]);

        // Phased
        let r = parse_gt_alleles("1|0").unwrap();
        assert_eq!(r, vec![Some(1), Some(0)]);

        // Entirely missing
        assert!(parse_gt_alleles(".").is_none());
        assert!(parse_gt_alleles("./.").is_none());
        assert!(parse_gt_alleles(".|.").is_none());

        // Haploid
        let r = parse_gt_alleles("0").unwrap();
        assert_eq!(r, vec![Some(0)]);

        // Triploid / multiallelic
        let r = parse_gt_alleles("0/1/2").unwrap();
        assert_eq!(r, vec![Some(0), Some(1), Some(2)]);

        // Partial missing
        let r = parse_gt_alleles("./1").unwrap();
        assert_eq!(r, vec![None, Some(1)]);
    }

    #[tokio::test]
    async fn test_vcf_an() {
        let ctx = make_test_ctx().await;
        let df = ctx
            .sql(r#"SELECT vcf_an("GT") as an FROM test_data"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let result = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        // Row 0: GT=["0/1","1/1","0/0"] → AN=6
        assert_eq!(result.value(0), 6);
        // Row 1: GT=["./.","0/1","1/1"] → AN=4
        assert_eq!(result.value(1), 4);
    }

    #[tokio::test]
    async fn test_vcf_ac() {
        let ctx = make_test_ctx().await;
        let df = ctx
            .sql(r#"SELECT vcf_ac("GT") as ac FROM test_data"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        // Row 0: GT=["0/1","1/1","0/0"] → AC=[3] (allele 1 appears 3 times)
        let inner0 = list.value(0);
        let ints0 = inner0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints0.len(), 1);
        assert_eq!(ints0.value(0), 3);

        // Row 1: GT=["./.","0/1","1/1"] → AC=[3]
        let inner1 = list.value(1);
        let ints1 = inner1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints1.len(), 1);
        assert_eq!(ints1.value(0), 3);
    }

    #[tokio::test]
    async fn test_vcf_af() {
        let ctx = make_test_ctx().await;
        let df = ctx
            .sql(r#"SELECT vcf_af("GT") as af FROM test_data"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        // Row 0: AN=6, AC=[3] → AF=[0.5]
        let inner0 = list.value(0);
        let floats0 = inner0
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(floats0.len(), 1);
        assert!((floats0.value(0) - 0.5).abs() < 0.001);

        // Row 1: AN=4, AC=[3] → AF=[0.75]
        let inner1 = list.value(1);
        let floats1 = inner1
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(floats1.len(), 1);
        assert!((floats1.value(0) - 0.75).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_allele_stats_multiallelic() {
        let ctx = SessionContext::new();
        register_vcf_udfs(&ctx);

        let mut gt_builder = ListBuilder::new(StringBuilder::new());
        gt_builder.values().append_value("0/2");
        gt_builder.values().append_value("1/2");
        gt_builder.values().append_value("0/1");
        gt_builder.append(true);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "GT",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(gt_builder.finish())]).unwrap();
        ctx.register_batch("multi", batch).unwrap();

        // AN = 6 (all alleles called)
        let df = ctx
            .sql(r#"SELECT vcf_an("GT") as an FROM multi"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let an = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(an.value(0), 6);

        // AC = [2, 2] (allele 1 appears 2 times, allele 2 appears 2 times)
        let df = ctx
            .sql(r#"SELECT vcf_ac("GT") as ac FROM multi"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let inner = list.value(0);
        let ints = inner.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints.len(), 2);
        assert_eq!(ints.value(0), 2); // allele 1
        assert_eq!(ints.value(1), 2); // allele 2

        // AF = [2/6, 2/6] ≈ [0.333, 0.333]
        let df = ctx
            .sql(r#"SELECT vcf_af("GT") as af FROM multi"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let inner = list.value(0);
        let floats = inner
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(floats.len(), 2);
        assert!((floats.value(0) - 1.0 / 3.0).abs() < 0.001);
        assert!((floats.value(1) - 1.0 / 3.0).abs() < 0.001);

        // 2-arg form with ALT column — same result since all alleles are observed
        let df = ctx
            .sql(r#"SELECT vcf_ac("GT", 'A|T') as ac FROM multi"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let inner = list.value(0);
        let ints = inner.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints.len(), 2);
        assert_eq!(ints.value(0), 2);
        assert_eq!(ints.value(1), 2);

        let df = ctx
            .sql(r#"SELECT vcf_af("GT", 'A|T') as af FROM multi"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let inner = list.value(0);
        let floats = inner
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(floats.len(), 2);
        assert!((floats.value(0) - 1.0 / 3.0).abs() < 0.001);
        assert!((floats.value(1) - 1.0 / 3.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_allele_stats_all_missing() {
        let ctx = SessionContext::new();
        register_vcf_udfs(&ctx);

        let mut gt_builder = ListBuilder::new(StringBuilder::new());
        gt_builder.values().append_value("./.");
        gt_builder.values().append_value("./.");
        gt_builder.append(true);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "GT",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(gt_builder.finish())]).unwrap();
        ctx.register_batch("missing", batch).unwrap();

        // AN = 0
        let df = ctx
            .sql(r#"SELECT vcf_an("GT") as an FROM missing"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let an = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(an.value(0), 0);

        // AC = [] (empty list)
        let df = ctx
            .sql(r#"SELECT vcf_ac("GT") as ac FROM missing"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let inner = list.value(0);
        assert_eq!(inner.len(), 0);

        // AF = [] (empty list)
        let df = ctx
            .sql(r#"SELECT vcf_af("GT") as af FROM missing"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let inner = list.value(0);
        assert_eq!(inner.len(), 0);

        // 2-arg form: ALT="A|T", all GT=./. → AC=[0, 0]
        let df = ctx
            .sql(r#"SELECT vcf_ac("GT", 'A|T') as ac FROM missing"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let inner = list.value(0);
        let ints = inner.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints.len(), 2);
        assert_eq!(ints.value(0), 0);
        assert_eq!(ints.value(1), 0);

        // 2-arg form: ALT="A|T", all GT=./. → AF=[null, null]
        let df = ctx
            .sql(r#"SELECT vcf_af("GT", 'A|T') as af FROM missing"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let inner = list.value(0);
        let floats = inner
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(floats.len(), 2);
        assert!(floats.is_null(0));
        assert!(floats.is_null(1));
    }

    /// Reproduces #103: ALT=A|T but GTs only reference allele 1.
    /// Output length must still be 2 (matching the number of ALT alleles).
    #[tokio::test]
    async fn test_ac_af_two_arg_unobserved_alt() {
        let ctx = SessionContext::new();
        register_vcf_udfs(&ctx);

        // 3 samples, all carrying only allele 1 — allele 2 never observed in GT
        let mut gt_builder = ListBuilder::new(StringBuilder::new());
        gt_builder.values().append_value("0/1");
        gt_builder.values().append_value("0/1");
        gt_builder.values().append_value("1/1");
        gt_builder.append(true);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "GT",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(gt_builder.finish())]).unwrap();
        ctx.register_batch("unobs", batch).unwrap();

        // 1-arg form: only allele 1 observed → AC=[4] (length 1)
        let df = ctx
            .sql(r#"SELECT vcf_ac("GT") as ac FROM unobs"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let inner = list.value(0);
        let ints = inner.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints.len(), 1);
        assert_eq!(ints.value(0), 4);

        // 2-arg form: ALT="A|T" → AC must be [4, 0] (length 2)
        let df = ctx
            .sql(r#"SELECT vcf_ac("GT", 'A|T') as ac FROM unobs"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let inner = list.value(0);
        let ints = inner.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints.len(), 2);
        assert_eq!(ints.value(0), 4);
        assert_eq!(ints.value(1), 0);

        // 2-arg form: ALT="A|T" → AF must be [4/6, 0/6] = [0.667, 0.0]
        let df = ctx
            .sql(r#"SELECT vcf_af("GT", 'A|T') as af FROM unobs"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let inner = list.value(0);
        let floats = inner
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(floats.len(), 2);
        assert!((floats.value(0) - 4.0 / 6.0).abs() < 0.001);
        assert!((floats.value(1) - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_count_alt_alleles() {
        assert_eq!(count_alt_alleles("A"), 1);
        assert_eq!(count_alt_alleles("A|T"), 2);
        assert_eq!(count_alt_alleles("A|T|C"), 3);
        assert_eq!(count_alt_alleles(""), 0);
        assert_eq!(count_alt_alleles("."), 0);
        assert_eq!(count_alt_alleles("  A|T  "), 2);
    }

    #[tokio::test]
    async fn test_vcf_set_gts_dot_replacement() {
        let ctx = make_test_ctx().await;
        let df = ctx
            .sql(r#"SELECT vcf_set_gts("GT", list_gte("GQ", 15), '.') as masked FROM test_data"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        // Row 0: GQ=[30,20,10], gte(15)=[T,T,F], GT=["0/1","1/1","0/0"]
        // keep 0/1, keep 1/1, replace 0/0 with literal "."
        let inner0 = list.value(0);
        let strings0 = inner0.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings0.value(0), "0/1");
        assert_eq!(strings0.value(1), "1/1");
        assert_eq!(strings0.value(2), ".");
    }
}
