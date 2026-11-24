//! Polars expression plugin behind the `seq` namespace (`polars_bio/seq.py`).
//!
//! Each function is a thin adapter over `datafusion_bio_function_seq::kernels`.
//! The `seq_*` SQL functions registered in `context.rs` call the same kernels,
//! so the expression and SQL results cannot drift apart. pyo3-polars exports
//! each function as `_polars_plugin_<name>` from this extension module, and
//! `seq.py` points Polars at it.

use datafusion_bio_function_seq::kernels::{
    self, Alphabet, Ambiguity, CdsError, GcDenominator, KernelError, MolecularWeight, Motif,
    OrfFinder, QualityMethod, QualityTrimmer, StartCodons, Strands, Translator, Windows,
};
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use pyo3_polars::export::polars_arrow::array::Array;
use pyo3_polars::export::polars_arrow::bitmap::Bitmap;
use pyo3_polars::export::polars_arrow::offset::OffsetsBuffer;
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
    // ASCII and copies every other byte unchanged; reverse and reverse
    // complement move a multi-byte character as a whole; translation writes
    // ASCII only. The input is a Polars string, so it is valid UTF-8.
    // On error the kernels write nothing.
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

fn default_stop_symbol() -> String {
    "*".to_string()
}

fn default_ambiguous() -> String {
    "x".to_string()
}

/// The defaults keep plans serialized before `stop_symbol`, `cds` and
/// `ambiguous` existed loadable.
#[derive(Deserialize)]
struct TranslateKwargs {
    frame: i64,
    table: i64,
    to_stop: bool,
    #[serde(default = "default_stop_symbol")]
    stop_symbol: String,
    #[serde(default)]
    cds: bool,
    #[serde(default = "default_ambiguous")]
    ambiguous: String,
}

#[derive(Deserialize)]
struct CdsStatusKwargs {
    frame: i64,
    table: i64,
}

#[derive(Deserialize)]
struct OffsetKwargs {
    offset: u8,
}

#[derive(Deserialize)]
struct OrfKwargs {
    table: i64,
    min_len: i64,
    strands: String,
    start_codons: String,
    allow_partial: bool,
    include_protein: bool,
    zero_based: bool,
}

#[derive(Deserialize)]
struct MotifKwargs {
    pattern: String,
    both_strands: bool,
    overlapping: bool,
    zero_based: bool,
}

#[derive(Deserialize)]
struct WindowKwargs {
    window: i64,
    step: Option<i64>,
    #[serde(default = "default_denominator")]
    denominator: String,
}

fn default_denominator() -> String {
    "length".to_string()
}

#[derive(Deserialize)]
struct WeightKwargs {
    seq_type: String,
    double_stranded: bool,
    circular: bool,
    monoisotopic: bool,
}

#[derive(Deserialize)]
struct HashKwargs {
    canonical: bool,
}

#[derive(Deserialize)]
struct TrimKwargs {
    leading: Option<i64>,
    trailing: Option<i64>,
    window_size: Option<i64>,
    window_quality: Option<f64>,
    offset: u8,
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
fn seq_reverse(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let out = ca.try_apply_into_string_amortized(|s, buf| {
        append_kernel_output(buf, |out| {
            kernels::reverse_into(s.as_bytes(), out);
            Ok(())
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
    let ambiguity = Ambiguity::parse(&kwargs.ambiguous).map_err(compute_err)?;
    let mut translator = Translator::new(kwargs.frame, kwargs.table, kwargs.to_stop)
        .map_err(compute_err)?
        .with_stop_symbol(&kwargs.stop_symbol)
        .with_cds(kwargs.cds)
        .with_ambiguity(ambiguity);
    let out = ca.try_apply_into_string_amortized(|s, buf| {
        append_kernel_output(buf, |out| translator.translate_into(s.as_bytes(), out))
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

#[polars_expr(output_type=String)]
fn seq_cds_status(inputs: &[Series], kwargs: CdsStatusKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut translator = Translator::new(kwargs.frame, kwargs.table, false).map_err(compute_err)?;
    let out: StringChunked = ca
        .iter()
        .map(|s| {
            s.and_then(|s| translator.cds_status(s.as_bytes()))
                .map(CdsError::as_str)
        })
        .collect();
    Ok(out.with_name(ca.name().clone()).into_series())
}

#[polars_expr(output_type=String)]
fn seq_transcribe(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let out = ca.try_apply_into_string_amortized(|s, buf| {
        append_kernel_output(buf, |out| {
            kernels::transcribe_into(s.as_bytes(), out);
            Ok(())
        })
    })?;
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn seq_back_transcribe(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let out = ca.try_apply_into_string_amortized(|s, buf| {
        append_kernel_output(buf, |out| {
            kernels::back_transcribe_into(s.as_bytes(), out);
            Ok(())
        })
    })?;
    Ok(out.into_series())
}

#[polars_expr(output_type=Float64)]
fn seq_expected_errors(inputs: &[Series], kwargs: OffsetKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let out: Float64Chunked = ca
        .iter()
        .map(|q| {
            q.map(|q| kernels::expected_errors(q.as_bytes(), kwargs.offset))
                .transpose()
        })
        .collect::<Result<_, _>>()
        .map_err(compute_err)?;
    Ok(out.with_name(ca.name().clone()).into_series())
}

fn quality_extreme(inputs: &[Series], offset: u8, max: bool) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let out: Int32Chunked = ca
        .iter()
        .map(|q| match q {
            Some(q) => kernels::phred_min_max(q.as_bytes(), offset)
                .map(|r| r.map(|(lo, hi)| i32::from(if max { hi } else { lo }))),
            None => Ok(None),
        })
        .collect::<Result<_, _>>()
        .map_err(compute_err)?;
    Ok(out.with_name(ca.name().clone()).into_series())
}

#[polars_expr(output_type=Int32)]
fn seq_min_quality(inputs: &[Series], kwargs: OffsetKwargs) -> PolarsResult<Series> {
    quality_extreme(inputs, kwargs.offset, false)
}

#[polars_expr(output_type=Int32)]
fn seq_max_quality(inputs: &[Series], kwargs: OffsetKwargs) -> PolarsResult<Series> {
    quality_extreme(inputs, kwargs.offset, true)
}

#[polars_expr(output_type=Float64)]
fn seq_molecular_weight(inputs: &[Series], kwargs: WeightKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let weigher = MolecularWeight::new(
        &kwargs.seq_type,
        kwargs.double_stranded,
        kwargs.circular,
        kwargs.monoisotopic,
    )
    .map_err(compute_err)?;
    let out: Float64Chunked = ca
        .iter()
        .map(|s| s.and_then(|s| weigher.weight(s.as_bytes())))
        .collect();
    Ok(out.with_name(ca.name().clone()).into_series())
}

#[polars_expr(output_type=UInt64)]
fn seq_hash(inputs: &[Series], kwargs: HashKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut scratch = Vec::new();
    let out: UInt64Chunked = ca
        .iter()
        .map(|s| s.map(|s| kernels::seq_hash(s.as_bytes(), kwargs.canonical, &mut scratch)))
        .collect();
    Ok(out.with_name(ca.name().clone()).into_series())
}

fn list_of(inputs: &[Field], inner: DataType) -> PolarsResult<Field> {
    Ok(Field::new(
        inputs[0].name().clone(),
        DataType::List(Box::new(inner)),
    ))
}

fn int64_list(inputs: &[Field]) -> PolarsResult<Field> {
    list_of(inputs, DataType::Int64)
}

fn float64_list(inputs: &[Field]) -> PolarsResult<Field> {
    list_of(inputs, DataType::Float64)
}

#[polars_expr(output_type_func=int64_list)]
fn seq_find_motif(inputs: &[Series], kwargs: MotifKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let motif = Motif::new(&kwargs.pattern, kwargs.both_strands, kwargs.overlapping)
        .map_err(compute_err)?;
    let first = i64::from(!kwargs.zero_based);
    let mut hits = Vec::new();
    let mut values = Vec::new();
    let mut builder = ListPrimitiveChunkedBuilder::<Int64Type>::new(
        ca.name().clone(),
        ca.len(),
        0,
        DataType::Int64,
    );
    for s in ca.iter() {
        match s {
            Some(s) => {
                motif.find_into(s.as_bytes(), &mut hits);
                values.clear();
                values.extend(hits.iter().map(|&h| h as i64 + first));
                builder.append_slice(&values);
            },
            None => builder.append_null(),
        }
    }
    Ok(builder.finish().into_series())
}

fn float_lists(
    ca: &StringChunked,
    mut f: impl FnMut(&[u8], &mut Vec<Option<f64>>),
) -> PolarsResult<Series> {
    let mut values = Vec::new();
    let mut builder = ListPrimitiveChunkedBuilder::<Float64Type>::new(
        ca.name().clone(),
        ca.len(),
        0,
        DataType::Float64,
    );
    for s in ca.iter() {
        match s {
            Some(s) => {
                values.clear();
                f(s.as_bytes(), &mut values);
                builder.append_iter(values.iter().copied());
            },
            None => builder.append_null(),
        }
    }
    Ok(builder.finish().into_series())
}

#[polars_expr(output_type_func=float64_list)]
fn seq_gc_content_windows(inputs: &[Series], kwargs: WindowKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let windows = Windows::new(kwargs.window, kwargs.step).map_err(compute_err)?;
    let denominator = GcDenominator::parse(&kwargs.denominator).map_err(compute_err)?;
    float_lists(ca, |s, out| {
        kernels::gc_content_windows_into(s, windows, denominator, out)
    })
}

#[polars_expr(output_type_func=float64_list)]
fn seq_gc_skew(inputs: &[Series], kwargs: WindowKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let windows = Windows::new(kwargs.window, kwargs.step).map_err(compute_err)?;
    let mut skews = Vec::new();
    float_lists(ca, |s, out| {
        skews.clear();
        kernels::gc_skew_into(s, windows, &mut skews);
        out.extend(skews.iter().copied().map(Some));
    })
}

/// The struct of one ORF, as `seq.orfs` returns it.
fn orf_struct(include_protein: bool) -> DataType {
    let mut fields = vec![
        Field::new("start".into(), DataType::Int64),
        Field::new("end".into(), DataType::Int64),
        Field::new("strand".into(), DataType::String),
        Field::new("frame".into(), DataType::Int32),
        Field::new("aa_len".into(), DataType::Int64),
        Field::new("has_start".into(), DataType::Boolean),
        Field::new("has_stop".into(), DataType::Boolean),
    ];
    if include_protein {
        fields.push(Field::new("protein".into(), DataType::String));
    }
    DataType::Struct(fields)
}

fn orfs_output(inputs: &[Field], kwargs: OrfKwargs) -> PolarsResult<Field> {
    list_of(inputs, orf_struct(kwargs.include_protein))
}

#[polars_expr(output_type_func_with_kwargs=orfs_output)]
fn seq_orfs(inputs: &[Series], kwargs: OrfKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut finder = OrfFinder::new(
        kwargs.table,
        kwargs.min_len,
        Strands::parse(&kwargs.strands).map_err(compute_err)?,
        StartCodons::parse(&kwargs.start_codons).map_err(compute_err)?,
        kwargs.allow_partial,
        kwargs.include_protein,
    )
    .map_err(compute_err)?;
    let first = i64::from(!kwargs.zero_based);

    let (mut start, mut end, mut aa_len) = (Vec::new(), Vec::new(), Vec::new());
    let (mut frame, mut has_start, mut has_stop) = (Vec::new(), Vec::new(), Vec::new());
    let mut strand = StringChunkedBuilder::new("strand".into(), 0);
    let mut protein = StringChunkedBuilder::new("protein".into(), 0);
    let mut offsets = Vec::with_capacity(ca.len() + 1);
    offsets.push(0i64);
    let mut validity = Vec::with_capacity(ca.len());
    for s in ca.iter() {
        if let Some(s) = s {
            finder.find(s.as_bytes());
            for orf in finder.orfs() {
                start.push(orf.start as i64 + first);
                end.push(orf.end as i64);
                strand.append_value(if orf.reverse { "-" } else { "+" });
                frame.push(i32::from(orf.frame));
                aa_len.push(orf.aa_len as i64);
                has_start.push(orf.has_start);
                has_stop.push(orf.has_stop);
                if kwargs.include_protein {
                    // Translations are ASCII.
                    protein.append_value(std::str::from_utf8(finder.protein(orf)).unwrap_or("X"));
                }
            }
        }
        offsets.push(start.len() as i64);
        validity.push(s.is_some());
    }
    let total = start.len();
    let mut fields = vec![
        Int64Chunked::from_vec("start".into(), start).into_series(),
        Int64Chunked::from_vec("end".into(), end).into_series(),
        strand.finish().into_series(),
        Int32Chunked::from_vec("frame".into(), frame).into_series(),
        Int64Chunked::from_vec("aa_len".into(), aa_len).into_series(),
        BooleanChunked::from_slice("has_start".into(), &has_start).into_series(),
        BooleanChunked::from_slice("has_stop".into(), &has_stop).into_series(),
    ];
    if kwargs.include_protein {
        fields.push(protein.finish().into_series());
    }
    let orfs = StructChunked::from_series("".into(), total, fields.iter())?;
    let orfs = orfs.rechunk().into_owned();
    let values = orfs
        .downcast_iter()
        .next()
        .cloned()
        .ok_or_else(|| polars_err!(ComputeError: "seq_orfs: empty struct column"))?;
    let item_dtype = orfs.dtype().clone();
    let arrow_item = values.dtype().clone();
    let list = LargeListArray::new(
        LargeListArray::default_datatype(arrow_item),
        // SAFETY: the offsets start at 0 and never decrease.
        unsafe { OffsetsBuffer::new_unchecked(offsets.into()) },
        Box::new(values),
        Some(Bitmap::from_iter(validity)),
    );
    // SAFETY: the dtype is the list of the struct the values were built from.
    let out = unsafe {
        ListChunked::from_chunks_and_dtype(
            ca.name().clone(),
            vec![Box::new(list)],
            DataType::List(Box::new(item_dtype)),
        )
    };
    Ok(out.into_series())
}

fn trim_output(inputs: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(
        inputs[0].name().clone(),
        DataType::Struct(vec![
            Field::new("sequence".into(), DataType::String),
            Field::new("quality_scores".into(), DataType::String),
        ]),
    ))
}

#[polars_expr(output_type_func=trim_output)]
fn seq_trim_quality(inputs: &[Series], kwargs: TrimKwargs) -> PolarsResult<Series> {
    let seqs = inputs[0].str()?;
    let quals = inputs[1].str()?;
    polars_ensure!(
        seqs.len() == quals.len(),
        ShapeMismatch: "seq_trim_quality: {} sequences but {} quality strings", seqs.len(), quals.len()
    );
    let window = match (kwargs.window_size, kwargs.window_quality) {
        (Some(size), Some(quality)) => Some((size, quality)),
        _ => None,
    };
    let trimmer = QualityTrimmer::new(kwargs.leading, kwargs.trailing, window, kwargs.offset)
        .map_err(compute_err)?;
    let mut seq_out = StringChunkedBuilder::new("sequence".into(), seqs.len());
    let mut qual_out = StringChunkedBuilder::new("quality_scores".into(), seqs.len());
    let mut validity = Vec::with_capacity(seqs.len());
    for (s, q) in seqs.iter().zip(quals.iter()) {
        let (Some(s), Some(q)) = (s, q) else {
            seq_out.append_null();
            qual_out.append_null();
            validity.push(false);
            continue;
        };
        let (a, b) = trimmer
            .span(s.as_bytes(), q.as_bytes())
            .map_err(compute_err)?;
        let (Some(s), Some(q)) = (s.get(a..b), q.get(a..b)) else {
            polars_bail!(ComputeError: "seq_trim_quality: cannot trim inside a multi-byte character");
        };
        seq_out.append_value(s);
        qual_out.append_value(q);
        validity.push(true);
    }
    let fields = [
        seq_out.finish().into_series(),
        qual_out.finish().into_series(),
    ];
    let out = StructChunked::from_series(seqs.name().clone(), seqs.len(), fields.iter())?
        .with_outer_validity(Some(Bitmap::from_iter(validity)));
    Ok(out.into_series())
}
