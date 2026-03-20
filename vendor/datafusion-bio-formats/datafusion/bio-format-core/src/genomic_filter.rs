//! Genomic filter extraction from DataFusion SQL expressions.
//!
//! Parses SQL `WHERE` clauses to extract genomic region constraints (chromosome, start, end)
//! that can be used for index-based random access in BAM/CRAM/VCF files.

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, Operator};

/// A genomic region extracted from SQL filters, in noodles-compatible 1-based coordinates.
#[derive(Debug, Clone)]
pub struct GenomicRegion {
    /// Chromosome/reference sequence name
    pub chrom: String,
    /// 1-based inclusive start position (None = from beginning of chromosome)
    pub start: Option<u64>,
    /// 1-based inclusive end position (None = to end of chromosome)
    pub end: Option<u64>,
    /// If true, this region represents only the unmapped reads for this chromosome
    /// (reads with a reference_sequence_id but no alignment position).
    /// These reads are not covered by BAI bin queries and must be read via direct seek.
    pub unmapped_tail: bool,
}

/// Result of analyzing DataFusion filter expressions for genomic region information.
#[derive(Debug, Clone)]
pub struct GenomicFilterAnalysis {
    /// Regions that can be queried via index (e.g., BAI, CRAI, TBI)
    pub regions: Vec<GenomicRegion>,
    /// Filters that are NOT genomic coordinate filters and should be applied post-read
    pub residual_filters: Vec<Expr>,
    /// All original filters — since index queries are inexact, DataFusion must re-evaluate
    pub all_filters: Vec<Expr>,
}

/// Analyze filter expressions and extract genomic regions for index queries.
///
/// Supports SQL patterns:
/// - `chrom = 'chr1'`
/// - `chrom = 'chr1' AND start >= 1000 AND end <= 2000`
/// - `chrom IN ('chr1', 'chr2')`
/// - `chrom = 'chr1' AND start BETWEEN 1000 AND 2000`
///
/// # Arguments
/// * `filters` - DataFusion filter expressions from `scan()`
/// * `coordinate_system_zero_based` - If true, input coordinates are 0-based half-open;
///   they will be converted to 1-based closed for noodles region queries
pub fn extract_genomic_regions(
    filters: &[Expr],
    coordinate_system_zero_based: bool,
) -> GenomicFilterAnalysis {
    let mut chroms: Vec<String> = Vec::new();
    let mut start_lower: Option<u64> = None;
    let mut end_upper: Option<u64> = None;
    let mut residual_filters: Vec<Expr> = Vec::new();

    for filter in filters {
        collect_genomic_constraints(
            filter,
            &mut chroms,
            &mut start_lower,
            &mut end_upper,
            &mut residual_filters,
            coordinate_system_zero_based,
        );
    }

    // Deduplicate chromosomes
    chroms.sort();
    chroms.dedup();

    let regions = if chroms.is_empty() {
        Vec::new()
    } else {
        chroms
            .into_iter()
            .map(|chrom| GenomicRegion {
                chrom,
                start: start_lower,
                end: end_upper,
                unmapped_tail: false,
            })
            .collect()
    };

    GenomicFilterAnalysis {
        regions,
        residual_filters,
        all_filters: filters.to_vec(),
    }
}

/// Build regions for a full-scan when an index is available but no genomic filters are present.
/// Creates one region per reference sequence (chromosome) for parallel partitioning.
///
/// # Arguments
/// * `reference_names` - List of chromosome/contig names from the file header
pub fn build_full_scan_regions(reference_names: &[String]) -> Vec<GenomicRegion> {
    reference_names
        .iter()
        .map(|name| GenomicRegion {
            chrom: name.clone(),
            start: None,
            end: None,
            unmapped_tail: false,
        })
        .collect()
}

/// Check if a filter expression involves genomic coordinate columns (chrom, start, end).
pub fn is_genomic_coordinate_filter(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary_expr) => {
            if matches!(binary_expr.op, Operator::And) {
                is_genomic_coordinate_filter(&binary_expr.left)
                    || is_genomic_coordinate_filter(&binary_expr.right)
            } else if let Expr::Column(col) = &*binary_expr.left {
                matches!(col.name.as_str(), "chrom" | "start" | "end")
            } else {
                false
            }
        }
        Expr::Between(between) => {
            if let Expr::Column(col) = &*between.expr {
                matches!(col.name.as_str(), "start" | "end")
            } else {
                false
            }
        }
        Expr::InList(in_list) => {
            if let Expr::Column(col) = &*in_list.expr {
                matches!(col.name.as_str(), "chrom")
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Recursively collect genomic constraints from a filter expression tree.
fn collect_genomic_constraints(
    expr: &Expr,
    chroms: &mut Vec<String>,
    start_lower: &mut Option<u64>,
    end_upper: &mut Option<u64>,
    residual_filters: &mut Vec<Expr>,
    coordinate_system_zero_based: bool,
) {
    match expr {
        Expr::BinaryExpr(binary_expr) if matches!(binary_expr.op, Operator::And) => {
            // Recurse into AND
            collect_genomic_constraints(
                &binary_expr.left,
                chroms,
                start_lower,
                end_upper,
                residual_filters,
                coordinate_system_zero_based,
            );
            collect_genomic_constraints(
                &binary_expr.right,
                chroms,
                start_lower,
                end_upper,
                residual_filters,
                coordinate_system_zero_based,
            );
        }
        Expr::BinaryExpr(binary_expr) => {
            if let Expr::Column(col) = &*binary_expr.left {
                if let Expr::Literal(scalar, _) = &*binary_expr.right {
                    match col.name.as_str() {
                        "chrom" => {
                            if binary_expr.op == Operator::Eq {
                                if let Some(s) = scalar_to_string(scalar) {
                                    chroms.push(s);
                                    return;
                                }
                            }
                            // chrom with non-eq operators goes to residual
                            residual_filters.push(expr.clone());
                        }
                        "start" => {
                            if let Some(val) = scalar_to_u64(scalar) {
                                let val_1based = if coordinate_system_zero_based {
                                    val + 1
                                } else {
                                    val
                                };
                                match binary_expr.op {
                                    Operator::Eq => {
                                        *start_lower = Some(
                                            start_lower.map_or(val_1based, |v| v.max(val_1based)),
                                        );
                                        // For equality on start, also set end if not already set more tightly
                                        // (The actual end position depends on record length, so this is approximate)
                                    }
                                    Operator::Gt => {
                                        *start_lower = Some(
                                            start_lower
                                                .map_or(val_1based + 1, |v| v.max(val_1based + 1)),
                                        );
                                    }
                                    Operator::GtEq => {
                                        *start_lower = Some(
                                            start_lower.map_or(val_1based, |v| v.max(val_1based)),
                                        );
                                    }
                                    _ => {
                                        residual_filters.push(expr.clone());
                                    }
                                }
                                return;
                            }
                            residual_filters.push(expr.clone());
                        }
                        "end" => {
                            if let Some(val) = scalar_to_u64(scalar) {
                                // End is always stored as 1-based inclusive (even in 0-based mode)
                                // because noodles alignment_end() returns 1-based inclusive
                                let val_1based = val;
                                match binary_expr.op {
                                    Operator::Eq => {
                                        *end_upper = Some(
                                            end_upper.map_or(val_1based, |v| v.min(val_1based)),
                                        );
                                    }
                                    Operator::Lt => {
                                        let upper = val_1based.saturating_sub(1);
                                        *end_upper =
                                            Some(end_upper.map_or(upper, |v| v.min(upper)));
                                    }
                                    Operator::LtEq => {
                                        *end_upper = Some(
                                            end_upper.map_or(val_1based, |v| v.min(val_1based)),
                                        );
                                    }
                                    _ => {
                                        residual_filters.push(expr.clone());
                                    }
                                }
                                return;
                            }
                            residual_filters.push(expr.clone());
                        }
                        _ => {
                            residual_filters.push(expr.clone());
                        }
                    }
                } else {
                    residual_filters.push(expr.clone());
                }
            } else {
                residual_filters.push(expr.clone());
            }
        }
        Expr::Between(between) => {
            if let Expr::Column(col) = &*between.expr {
                if col.name == "start" && !between.negated {
                    if let (Expr::Literal(low, _), Expr::Literal(high, _)) =
                        (&*between.low, &*between.high)
                    {
                        if let (Some(low_val), Some(high_val)) =
                            (scalar_to_u64(low), scalar_to_u64(high))
                        {
                            let low_1based = if coordinate_system_zero_based {
                                low_val + 1
                            } else {
                                low_val
                            };
                            let high_1based = if coordinate_system_zero_based {
                                high_val + 1
                            } else {
                                high_val
                            };
                            *start_lower =
                                Some(start_lower.map_or(low_1based, |v| v.max(low_1based)));
                            *end_upper =
                                Some(end_upper.map_or(high_1based, |v| v.min(high_1based)));
                            return;
                        }
                    }
                }
            }
            residual_filters.push(expr.clone());
        }
        Expr::InList(in_list) => {
            if let Expr::Column(col) = &*in_list.expr {
                if col.name == "chrom" && !in_list.negated {
                    let mut extracted_chroms: Vec<String> = in_list
                        .list
                        .iter()
                        .filter_map(|e| {
                            if let Expr::Literal(scalar, _) = e {
                                scalar_to_string(scalar)
                            } else {
                                None
                            }
                        })
                        .collect();
                    if !extracted_chroms.is_empty() {
                        chroms.append(&mut extracted_chroms);
                        return;
                    }
                }
            }
            residual_filters.push(expr.clone());
        }
        _ => {
            residual_filters.push(expr.clone());
        }
    }
}

/// Extract a string value from a ScalarValue.
fn scalar_to_string(scalar: &ScalarValue) -> Option<String> {
    match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Extract a u64 value from a numeric ScalarValue.
fn scalar_to_u64(scalar: &ScalarValue) -> Option<u64> {
    match scalar {
        ScalarValue::UInt32(Some(v)) => Some(*v as u64),
        ScalarValue::UInt64(Some(v)) => Some(*v),
        ScalarValue::Int32(Some(v)) if *v >= 0 => Some(*v as u64),
        ScalarValue::Int64(Some(v)) if *v >= 0 => Some(*v as u64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{Between, col, lit};

    #[test]
    fn test_extract_chrom_eq() {
        let filters = vec![col("chrom").eq(lit("chr1"))];
        let analysis = extract_genomic_regions(&filters, true);
        assert_eq!(analysis.regions.len(), 1);
        assert_eq!(analysis.regions[0].chrom, "chr1");
        assert!(analysis.regions[0].start.is_none());
        assert!(analysis.regions[0].end.is_none());
        assert!(analysis.residual_filters.is_empty());
    }

    #[test]
    fn test_extract_chrom_in_list() {
        let filters = vec![col("chrom").in_list(vec![lit("chr1"), lit("chr2")], false)];
        let analysis = extract_genomic_regions(&filters, true);
        assert_eq!(analysis.regions.len(), 2);
        assert_eq!(analysis.regions[0].chrom, "chr1");
        assert_eq!(analysis.regions[1].chrom, "chr2");
    }

    #[test]
    fn test_extract_chrom_with_range_zero_based() {
        // chrom = 'chr1' AND start >= 999 AND end <= 2000
        // In 0-based mode, start 999 becomes 1000 in 1-based
        let filters = vec![
            col("chrom").eq(lit("chr1")),
            col("start").gt_eq(lit(999u32)),
            col("end").lt_eq(lit(2000u32)),
        ];
        let analysis = extract_genomic_regions(&filters, true);
        assert_eq!(analysis.regions.len(), 1);
        assert_eq!(analysis.regions[0].chrom, "chr1");
        assert_eq!(analysis.regions[0].start, Some(1000)); // 999 + 1
        assert_eq!(analysis.regions[0].end, Some(2000));
        assert!(analysis.residual_filters.is_empty());
    }

    #[test]
    fn test_extract_chrom_with_range_one_based() {
        let filters = vec![
            col("chrom").eq(lit("chr1")),
            col("start").gt_eq(lit(1000u32)),
            col("end").lt_eq(lit(2000u32)),
        ];
        let analysis = extract_genomic_regions(&filters, false);
        assert_eq!(analysis.regions.len(), 1);
        assert_eq!(analysis.regions[0].start, Some(1000));
        assert_eq!(analysis.regions[0].end, Some(2000));
    }

    #[test]
    fn test_non_genomic_filter_becomes_residual() {
        let filters = vec![
            col("chrom").eq(lit("chr1")),
            col("mapping_quality").gt_eq(lit(30u32)),
        ];
        let analysis = extract_genomic_regions(&filters, true);
        assert_eq!(analysis.regions.len(), 1);
        assert_eq!(analysis.residual_filters.len(), 1);
    }

    #[test]
    fn test_no_genomic_filters() {
        let filters = vec![col("mapping_quality").gt_eq(lit(30u32))];
        let analysis = extract_genomic_regions(&filters, true);
        assert!(analysis.regions.is_empty());
        assert_eq!(analysis.residual_filters.len(), 1);
    }

    #[test]
    fn test_build_full_scan_regions() {
        let names = vec!["chr1".to_string(), "chr2".to_string(), "chr3".to_string()];
        let regions = build_full_scan_regions(&names);
        assert_eq!(regions.len(), 3);
        assert_eq!(regions[0].chrom, "chr1");
        assert!(regions[0].start.is_none());
    }

    #[test]
    fn test_is_genomic_coordinate_filter() {
        assert!(is_genomic_coordinate_filter(&col("chrom").eq(lit("chr1"))));
        assert!(is_genomic_coordinate_filter(
            &col("start").gt_eq(lit(1000u32))
        ));
        assert!(!is_genomic_coordinate_filter(
            &col("mapping_quality").gt_eq(lit(30u32))
        ));
    }

    #[test]
    fn test_between_start() {
        let between_expr = Expr::Between(Between {
            expr: Box::new(col("start")),
            negated: false,
            low: Box::new(lit(999u32)),
            high: Box::new(lit(1999u32)),
        });
        let analysis = extract_genomic_regions(&[between_expr], true);
        // No chrom specified, so no regions
        assert!(analysis.regions.is_empty());
        // But the start/end constraints would be set if chrom were present
    }

    #[test]
    fn test_between_with_chrom() {
        let filters = vec![
            col("chrom").eq(lit("chr1")),
            Expr::Between(Between {
                expr: Box::new(col("start")),
                negated: false,
                low: Box::new(lit(999u32)),
                high: Box::new(lit(1999u32)),
            }),
        ];
        let analysis = extract_genomic_regions(&filters, true);
        assert_eq!(analysis.regions.len(), 1);
        assert_eq!(analysis.regions[0].chrom, "chr1");
        assert_eq!(analysis.regions[0].start, Some(1000)); // 999 + 1
        assert_eq!(analysis.regions[0].end, Some(2000)); // 1999 + 1
    }
}
