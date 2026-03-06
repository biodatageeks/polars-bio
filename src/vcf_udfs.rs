//! Local VCF genotype processing UDFs.
//!
//! - `list_normalize_gt(List<Utf8>) -> List<Utf8>`: normalize haploid GT calls
//! - `vcf_process_genotypes(GT, GQ, DP, PL, gq_min, dp_min, dp_max, calc_ds_min_gq)
//!    -> Struct<GT, GQ, DP, PL, DS>`: full per-sample genotype processing in one pass

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float32Builder, Int32Array, Int32Builder, ListArray, ListBuilder,
    StringArray, StringBuilder, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::TypeSignature::Exact;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

// ============================================================================
// list_normalize_gt — normalize haploid GT strings
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct ListNormalizeGtUdf {
    signature: Signature,
}

impl ListNormalizeGtUdf {
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

impl ScalarUDFImpl for ListNormalizeGtUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "list_normalize_gt"
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
        let array = args.args[0].clone().into_array(1)?;
        let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            DataFusionError::Execution("list_normalize_gt expects List<Utf8> input".into())
        })?;

        let mut builder = ListBuilder::new(StringBuilder::new());
        for i in 0..list.len() {
            if list.is_null(i) {
                builder.append(false);
                continue;
            }
            let inner = list.value(i);
            let strings = inner.as_any().downcast_ref::<StringArray>().unwrap();
            for j in 0..strings.len() {
                if strings.is_null(j) {
                    builder.values().append_null();
                } else {
                    let gt = strings.value(j);
                    let normalized = match gt {
                        "0" => "0/0",
                        "1" => "1/1",
                        "2" => "2/2",
                        other => other,
                    };
                    builder.values().append_value(normalized);
                }
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

pub fn list_normalize_gt_udf() -> ScalarUDF {
    ScalarUDF::from(ListNormalizeGtUdf::new())
}

// ============================================================================
// vcf_process_genotypes — monolithic per-sample genotype processing
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct VcfProcessGenotypesUdf {
    signature: Signature,
}

impl VcfProcessGenotypesUdf {
    fn new() -> Self {
        let list_utf8 = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let list_i32 = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let list_list_i32 = DataType::List(Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )));

        Self {
            signature: Signature::one_of(
                vec![
                    // GT, GQ, DP, PL, gq_min, dp_min, dp_max, calc_ds_min_gq
                    Exact(vec![
                        list_utf8.clone(),
                        list_i32.clone(),
                        list_i32.clone(),
                        list_list_i32.clone(),
                        DataType::Int64,
                        DataType::Int64,
                        DataType::Int64,
                        DataType::Int64,
                    ]),
                    Exact(vec![
                        list_utf8,
                        list_i32.clone(),
                        list_i32,
                        list_list_i32,
                        DataType::Int32,
                        DataType::Int32,
                        DataType::Int32,
                        DataType::Int32,
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

fn genotypes_return_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new(
            "GT",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
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
            "PL",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ))),
            true,
        ),
        Field::new(
            "DSG",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        ),
    ]))
}

fn get_scalar_i64(args: &[ColumnarValue], idx: usize) -> Result<i64> {
    let arr = args[idx].clone().into_array(1)?;
    if let Some(i64_arr) = arr
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
    {
        Ok(i64_arr.value(0))
    } else if let Some(i32_arr) = arr.as_any().downcast_ref::<Int32Array>() {
        Ok(i32_arr.value(0) as i64)
    } else {
        Err(DataFusionError::Execution(format!(
            "vcf_process_genotypes: arg {} must be Int32 or Int64",
            idx
        )))
    }
}

impl ScalarUDFImpl for VcfProcessGenotypesUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "vcf_process_genotypes"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(genotypes_return_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let gq_min = get_scalar_i64(&args.args, 4)?;
        let dp_min = get_scalar_i64(&args.args, 5)?;
        let dp_max = get_scalar_i64(&args.args, 6)?;
        let calc_ds_min_gq = get_scalar_i64(&args.args, 7)?;

        let gt_arr = args.args[0].clone().into_array(1)?;
        let gq_arr = args.args[1].clone().into_array(1)?;
        let dp_arr = args.args[2].clone().into_array(1)?;
        let pl_arr = args.args[3].clone().into_array(1)?;

        let gt_list = gt_arr.as_list::<i32>();
        let gq_list = gq_arr.as_list::<i32>();
        let dp_list = dp_arr.as_list::<i32>();
        let pl_list = pl_arr.as_list::<i32>(); // outer list: per-row, inner: per-sample ListArray of Int32

        let nrows = gt_list.len();

        let mut gt_builder = ListBuilder::new(StringBuilder::new());
        let mut gq_builder = ListBuilder::new(Int32Builder::new());
        let mut dp_builder = ListBuilder::new(Int32Builder::new());
        let mut pl_builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
        let mut ds_builder = ListBuilder::new(Float32Builder::new());

        for row in 0..nrows {
            if gt_list.is_null(row) {
                gt_builder.append(false);
                gq_builder.append(false);
                dp_builder.append(false);
                pl_builder.append(false);
                ds_builder.append(false);
                continue;
            }

            let gt_inner = gt_list.value(row);
            let gt_strings = gt_inner.as_any().downcast_ref::<StringArray>().unwrap();

            let gq_inner = gq_list.value(row);
            let gq_ints = gq_inner.as_any().downcast_ref::<Int32Array>().unwrap();

            let dp_inner = dp_list.value(row);
            let dp_ints = dp_inner.as_any().downcast_ref::<Int32Array>().unwrap();

            let pl_inner = pl_list.value(row);
            let pl_per_sample = pl_inner.as_any().downcast_ref::<ListArray>().unwrap();

            let nsamples = gt_strings.len();

            for s in 0..nsamples {
                // --- Get raw values ---
                let gt_raw = if gt_strings.is_null(s) {
                    None
                } else {
                    Some(gt_strings.value(s))
                };
                let gq_val = if gq_ints.is_null(s) {
                    None
                } else {
                    Some(gq_ints.value(s) as i64)
                };
                let dp_val = if dp_ints.is_null(s) {
                    None
                } else {
                    Some(dp_ints.value(s) as i64)
                };

                // Extract PL triplet
                let (pl0, pl1, pl2, pl_valid) = if pl_per_sample.is_null(s) {
                    (None, None, None, false)
                } else {
                    let pl_sample = pl_per_sample.value(s);
                    let pl_vals = pl_sample.as_any().downcast_ref::<Int32Array>().unwrap();
                    if pl_vals.len() >= 3 {
                        let p0 = if pl_vals.is_null(0) {
                            None
                        } else {
                            Some(pl_vals.value(0) as f64)
                        };
                        let p1 = if pl_vals.is_null(1) {
                            None
                        } else {
                            Some(pl_vals.value(1) as f64)
                        };
                        let p2 = if pl_vals.is_null(2) {
                            None
                        } else {
                            Some(pl_vals.value(2) as f64)
                        };
                        (p0, p1, p2, true)
                    } else {
                        (None, None, None, false)
                    }
                };

                // --- Normalize GT ---
                let gt_norm = match gt_raw {
                    Some("0") => "0/0",
                    Some("1") => "1/1",
                    Some("2") => "2/2",
                    Some(other) => other,
                    None => ".",
                };

                // --- Quality mask ---
                let good = match (gq_val, dp_val) {
                    (Some(gq), Some(dp)) => gq >= gq_min && dp >= dp_min && dp <= dp_max,
                    _ => false,
                };

                // --- GT masking ---
                let gt_final = if good { gt_norm } else { "./." };

                // --- PL correction & DS calculation ---
                let is_hom_ref = gt_norm == "0/0" || gt_norm == "0|0";
                let gq_sufficient = gq_val.is_some() && gq_val.unwrap() >= calc_ds_min_gq;

                let is_pl_all_zero =
                    pl_valid && pl0 == Some(0.0) && pl1 == Some(0.0) && pl2 == Some(0.0);
                let needs_correction = is_hom_ref && is_pl_all_zero && gq_sufficient;

                let (final_pl0, final_pl1, final_pl2) = if needs_correction {
                    let gq_f = gq_val.unwrap_or(0) as f64;
                    let p_wrong = 10.0_f64.powf(-gq_f / 10.0);
                    let discriminant = 1.0 + 4.0 * p_wrong;
                    let x = (-1.0 + discriminant.sqrt()) / 2.0;

                    let l_het = x;
                    let l_alt = x * x;
                    let l_ref = 1.0 - l_het - l_alt;

                    let pl_ref = if l_ref > 0.0 {
                        (-10.0 * l_ref.log10()).round().clamp(0.0, 255.0)
                    } else {
                        255.0
                    };
                    let pl_het = if l_het > 0.0 {
                        (-10.0 * l_het.log10()).round().clamp(0.0, 255.0)
                    } else {
                        255.0
                    };
                    let pl_alt = if l_alt > 0.0 {
                        (-10.0 * l_alt.log10()).round().clamp(0.0, 255.0)
                    } else {
                        255.0
                    };
                    (pl_ref, pl_het, pl_alt)
                } else {
                    (pl0.unwrap_or(0.0), pl1.unwrap_or(0.0), pl2.unwrap_or(0.0))
                };

                // DS calculation
                let ds_val: Option<f32> = if pl_valid && gq_sufficient {
                    let l_ref = if final_pl0 < 255.0 {
                        10.0_f64.powf(-final_pl0 / 10.0)
                    } else {
                        0.0
                    };
                    let l_het = if final_pl1 < 255.0 {
                        10.0_f64.powf(-final_pl1 / 10.0)
                    } else {
                        0.0
                    };
                    let l_alt = if final_pl2 < 255.0 {
                        10.0_f64.powf(-final_pl2 / 10.0)
                    } else {
                        0.0
                    };
                    let l_sum = l_ref + l_het + l_alt;
                    if l_sum > 0.0 {
                        let dosage = (l_het / l_sum) + 2.0 * (l_alt / l_sum);
                        let cleaned = if dosage > 0.0 && dosage < 0.0001 {
                            0.0
                        } else {
                            dosage
                        };
                        Some(cleaned as f32)
                    } else {
                        Some(0.0)
                    }
                } else {
                    None
                };

                // --- Append to builders ---
                gt_builder.values().append_value(gt_final);

                // GQ: passthrough
                match gq_val {
                    Some(v) => gq_builder.values().append_value(v as i32),
                    None => gq_builder.values().append_null(),
                }

                // DP: passthrough
                match dp_val {
                    Some(v) => dp_builder.values().append_value(v as i32),
                    None => dp_builder.values().append_null(),
                }

                // PL: output corrected triplet or original
                if pl_valid {
                    let inner = pl_builder.values();
                    inner.values().append_value(final_pl0 as i32);
                    inner.values().append_value(final_pl1 as i32);
                    inner.values().append_value(final_pl2 as i32);
                    inner.append(true);
                } else if !pl_per_sample.is_null(s) {
                    // Pass through original PL (< 3 elements)
                    let pl_sample = pl_per_sample.value(s);
                    let pl_vals = pl_sample.as_any().downcast_ref::<Int32Array>().unwrap();
                    let inner = pl_builder.values();
                    for k in 0..pl_vals.len() {
                        if pl_vals.is_null(k) {
                            inner.values().append_null();
                        } else {
                            inner.values().append_value(pl_vals.value(k));
                        }
                    }
                    inner.append(true);
                } else {
                    pl_builder.values().append(false); // null inner list
                }

                // DS
                match ds_val {
                    Some(v) => ds_builder.values().append_value(v),
                    None => ds_builder.values().append_null(),
                }
            }

            gt_builder.append(true);
            gq_builder.append(true);
            dp_builder.append(true);
            pl_builder.append(true);
            ds_builder.append(true);
        }

        let gt_array: ArrayRef = Arc::new(gt_builder.finish());
        let gq_array: ArrayRef = Arc::new(gq_builder.finish());
        let dp_array: ArrayRef = Arc::new(dp_builder.finish());
        let pl_array: ArrayRef = Arc::new(pl_builder.finish());
        let ds_array: ArrayRef = Arc::new(ds_builder.finish());

        let fields = Fields::from(vec![
            Field::new("GT", gt_array.data_type().clone(), true),
            Field::new("GQ", gq_array.data_type().clone(), true),
            Field::new("DP", dp_array.data_type().clone(), true),
            Field::new("PL", pl_array.data_type().clone(), true),
            Field::new("DSG", ds_array.data_type().clone(), true),
        ]);

        let struct_array = StructArray::new(
            fields,
            vec![gt_array, gq_array, dp_array, pl_array, ds_array],
            None,
        );

        Ok(ColumnarValue::Array(Arc::new(struct_array)))
    }
}

pub fn vcf_process_genotypes_udf() -> ScalarUDF {
    ScalarUDF::from(VcfProcessGenotypesUdf::new())
}

pub fn register_local_vcf_udfs(ctx: &datafusion::prelude::SessionContext) {
    ctx.register_udf(list_normalize_gt_udf());
    ctx.register_udf(vcf_process_genotypes_udf());
}
