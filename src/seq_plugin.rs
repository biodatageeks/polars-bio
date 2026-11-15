//! Polars expression plugin behind the `seq` namespace (`polars_bio/seq.py`).
//!
//! Each function is a thin adapter over `datafusion_bio_function_seq::kernels`.
//! The `seq_*` SQL functions registered in `context.rs` call the same kernels,
//! so the expression and SQL results cannot drift apart. pyo3-polars exports
//! each function as `_polars_plugin_<name>` from this extension module, and
//! `seq.py` points Polars at it.

use datafusion_bio_function_seq::kernels::{
    self, Alphabet, GcDenominator, KernelError, QualityMethod, Translator,
};
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;

fn compute_err(e: KernelError) -> PolarsError {
    polars_err!(ComputeError: "{}", e)
}

/// Runs a kernel that appends bytes to a Polars string buffer.
fn append_kernel_output(
    buf: &mut String,
    kernel: impl FnOnce(&mut Vec<u8>) -> Result<(), KernelError>,
) -> PolarsResult<()> {
    // SAFETY: the kernels keep valid UTF-8 valid. Complement maps ASCII to
    // ASCII and copies every other byte unchanged; reverse complement moves a
    // multi-byte character as a whole; translation writes ASCII only. The input
    // is a Polars string, so it is valid UTF-8. On error the kernels write
    // nothing.
    let bytes = unsafe { buf.as_mut_vec() };
    let result = kernel(bytes);
    debug_assert!(std::str::from_utf8(bytes).is_ok());
    result.map_err(compute_err)
}

#[derive(Deserialize)]
struct ComplementKwargs {
    alphabet: String,
    strict: bool,
}

#[derive(Deserialize)]
struct GcKwargs {
    denominator: String,
}

#[derive(Deserialize)]
struct TranslateKwargs {
    frame: i64,
    table: i64,
    to_stop: bool,
}

#[derive(Deserialize)]
struct QualityKwargs {
    offset: u8,
    method: String,
}

#[polars_expr(output_type=String)]
fn seq_revcomp(inputs: &[Series], kwargs: ComplementKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let alphabet = Alphabet::parse(&kwargs.alphabet).map_err(compute_err)?;
    let out = ca.try_apply_into_string_amortized(|s, buf| {
        append_kernel_output(buf, |out| {
            kernels::revcomp_into(s.as_bytes(), alphabet, kwargs.strict, out)
        })
    })?;
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn seq_complement(inputs: &[Series], kwargs: ComplementKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let alphabet = Alphabet::parse(&kwargs.alphabet).map_err(compute_err)?;
    let out = ca.try_apply_into_string_amortized(|s, buf| {
        append_kernel_output(buf, |out| {
            kernels::complement_into(s.as_bytes(), alphabet, kwargs.strict, out)
        })
    })?;
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn seq_translate(inputs: &[Series], kwargs: TranslateKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut translator =
        Translator::new(kwargs.frame, kwargs.table, kwargs.to_stop).map_err(compute_err)?;
    let out = ca.try_apply_into_string_amortized(|s, buf| {
        append_kernel_output(buf, |out| {
            translator.translate_into(s.as_bytes(), out);
            Ok(())
        })
    })?;
    Ok(out.into_series())
}

#[polars_expr(output_type=Float64)]
fn seq_gc_content(inputs: &[Series], kwargs: GcKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let denominator = GcDenominator::parse(&kwargs.denominator).map_err(compute_err)?;
    let out: Float64Chunked = ca
        .downcast_iter()
        .flat_map(|arr| arr.iter())
        .map(|s| s.and_then(|s| kernels::gc_content(s.as_bytes(), denominator)))
        .collect();
    Ok(out.with_name(ca.name().clone()).into_series())
}

#[polars_expr(output_type=Float64)]
fn seq_mean_quality(inputs: &[Series], kwargs: QualityKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let method = QualityMethod::parse(&kwargs.method).map_err(compute_err)?;
    let out: Float64Chunked = ca
        .downcast_iter()
        .flat_map(|arr| arr.iter())
        .map(|q| match q {
            Some(q) => kernels::phred_mean(q.as_bytes(), kwargs.offset, method),
            None => Ok(None),
        })
        .collect::<Result<_, _>>()
        .map_err(compute_err)?;
    Ok(out.with_name(ca.name().clone()).into_series())
}
