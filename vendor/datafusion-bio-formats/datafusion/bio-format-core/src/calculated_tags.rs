/// Utilities for calculating SAM alignment tags that can be derived from
/// alignment data and reference sequences.
///
/// Some tags like MD (mismatch descriptor) and NM (edit distance) describe
/// differences between reads and reference sequences. CRAM files may omit these
/// tags since they can be calculated on-the-fly from the alignment features.
///
/// References:
/// - SAM specification: <https://samtools.github.io/hts-specs/SAMtags.pdf>
/// - samtools calmd: <http://www.htslib.org/doc/samtools-calmd.html>
use noodles_sam::alignment::Record;
use noodles_sam::alignment::record::Cigar;
use noodles_sam::alignment::record::Sequence;
use noodles_sam::alignment::record::cigar::op::Kind as OpKind;

/// Calculate the NM tag (edit distance) from alignment data.
///
/// NM represents the number of differences (mismatches plus inserted and
/// deleted bases) between the sequence and reference. This includes:
/// - Mismatches (substitutions)
/// - Insertions in the read
/// - Deletions from the reference
///
/// # Arguments
/// * `record` - The alignment record
/// * `reference_seq` - The reference sequence (optional, if available)
///
/// # Returns
/// * `Some(edit_distance)` if calculable
/// * `None` if insufficient data (no CIGAR, unmapped, etc.)
///
/// # Note
/// Without reference sequence data, this can only count indels from CIGAR.
/// With reference sequence, it can also count mismatches by comparing bases.
pub fn calculate_nm_tag<R: Record>(record: &R, reference_seq: Option<&[u8]>) -> Option<i32> {
    // Unmapped reads don't have edit distance
    let flags = record.flags().ok()?;
    if flags.is_unmapped() {
        return None;
    }

    let cigar = record.cigar();
    if cigar.is_empty() {
        return None;
    }

    let mut edit_distance = 0i32;
    let mut read_pos = 0usize;
    let mut ref_pos = 0usize;

    // Count insertions and deletions from CIGAR
    for op_result in cigar.as_ref().iter() {
        let op = op_result.ok()?; // Return None if CIGAR parsing fails
        let kind = op.kind();
        let len = op.len();

        match kind {
            OpKind::Match | OpKind::SequenceMatch | OpKind::SequenceMismatch => {
                // If we have reference sequence, count actual mismatches
                if let Some(ref_bases) = reference_seq {
                    let read_seq = record.sequence();
                    for i in 0..len {
                        if read_pos + i < read_seq.len() && ref_pos + i < ref_bases.len() {
                            let read_base = read_seq.get(read_pos + i);
                            let ref_base = ref_bases[ref_pos + i];

                            // Compare bases (case-insensitive for A, C, G, T)
                            if let Some(rb) = read_base {
                                let rb_upper = (char::from(rb) as u8).to_ascii_uppercase();
                                let ref_upper = ref_base.to_ascii_uppercase();
                                if rb_upper != ref_upper
                                    && is_valid_base(rb_upper)
                                    && is_valid_base(ref_upper)
                                {
                                    edit_distance += 1;
                                }
                            }
                        }
                    }
                } else if kind == OpKind::SequenceMismatch {
                    // If we only have CIGAR (no reference), count SequenceMismatch ops
                    edit_distance += len as i32;
                }

                read_pos += len;
                ref_pos += len;
            }
            OpKind::Insertion => {
                edit_distance += len as i32;
                read_pos += len;
            }
            OpKind::Deletion => {
                edit_distance += len as i32;
                ref_pos += len;
            }
            OpKind::SoftClip => {
                read_pos += len;
            }
            OpKind::HardClip | OpKind::Pad | OpKind::Skip => {
                // These don't contribute to edit distance
            }
        }
    }

    Some(edit_distance)
}

/// Calculate the MD tag (mismatch descriptor) from alignment data.
///
/// MD encodes the positions of mismatches and deletions relative to the reference.
/// Format: Numbers represent matching bases, letters show reference bases at mismatches,
/// ^BASES shows deleted reference bases.
///
/// Examples:
/// - "100" = 100 matching bases
/// - "10A5" = 10 matches, mismatch (ref=A), 5 matches
/// - "10^ACG5" = 10 matches, 3 base deletion (ACG), 5 matches
///
/// # Arguments
/// * `record` - The alignment record
/// * `reference_seq` - The reference sequence (required)
///
/// # Returns
/// * `Some(md_string)` if calculable
/// * `None` if insufficient data (no reference, no CIGAR, unmapped)
///
/// # Note
/// This requires reference sequence data and cannot be calculated from CIGAR alone.
pub fn calculate_md_tag<R: Record>(record: &R, reference_seq: &[u8]) -> Option<String> {
    // Unmapped reads don't have MD tags
    let flags = record.flags().ok()?;
    if flags.is_unmapped() {
        return None;
    }

    let cigar = record.cigar();
    if cigar.is_empty() {
        return None;
    }

    let mut md = String::new();
    let mut match_count = 0usize;
    let mut read_pos = 0usize;
    let mut ref_pos = 0usize;
    let read_seq = record.sequence();

    for op_result in cigar.as_ref().iter() {
        let op = op_result.ok()?; // Return None if CIGAR parsing fails
        let kind = op.kind();
        let len = op.len();

        match kind {
            OpKind::Match | OpKind::SequenceMatch => {
                for i in 0..len {
                    if read_pos + i >= read_seq.len() || ref_pos + i >= reference_seq.len() {
                        break;
                    }

                    let read_base = read_seq.get(read_pos + i);
                    let ref_base = reference_seq[ref_pos + i];

                    if let Some(rb) = read_base {
                        let rb_upper = (char::from(rb) as u8).to_ascii_uppercase();
                        let ref_upper = ref_base.to_ascii_uppercase();

                        if rb_upper == ref_upper
                            || !is_valid_base(rb_upper)
                            || !is_valid_base(ref_upper)
                        {
                            match_count += 1;
                        } else {
                            // Mismatch found
                            md.push_str(&match_count.to_string());
                            md.push(ref_upper as char);
                            match_count = 0;
                        }
                    }
                }
                read_pos += len;
                ref_pos += len;
            }
            OpKind::SequenceMismatch => {
                // Explicit mismatch - need to output mismatched reference bases
                for i in 0..len {
                    if ref_pos + i < reference_seq.len() {
                        md.push_str(&match_count.to_string());
                        let ref_base = reference_seq[ref_pos + i];
                        md.push(ref_base.to_ascii_uppercase() as char);
                        match_count = 0;
                    }
                }
                read_pos += len;
                ref_pos += len;
            }
            OpKind::Insertion => {
                // Insertions don't appear in MD tag (they're in the read, not reference)
                read_pos += len;
            }
            OpKind::Deletion => {
                // Deletions: output match count, then ^DELETED_BASES
                md.push_str(&match_count.to_string());
                md.push('^');
                for i in 0..len {
                    if ref_pos + i < reference_seq.len() {
                        let ref_base = reference_seq[ref_pos + i];
                        md.push(ref_base.to_ascii_uppercase() as char);
                    }
                }
                match_count = 0;
                ref_pos += len;
            }
            OpKind::SoftClip => {
                read_pos += len;
            }
            OpKind::Skip => {
                // Skipped regions (introns) advance reference position
                ref_pos += len;
            }
            OpKind::HardClip | OpKind::Pad => {
                // These don't affect MD calculation
            }
        }
    }

    // Append final match count
    md.push_str(&match_count.to_string());

    Some(md)
}

/// Check if a base is valid for mismatch counting (A, C, G, T, or U).
///
/// The NM tag counts only case-insensitive A, C, G, T (and U for RNA) as
/// potential matches according to SAM specification.
fn is_valid_base(base: u8) -> bool {
    matches!(base, b'A' | b'C' | b'G' | b'T' | b'U')
}

#[cfg(test)]
mod tests {
    use super::*;
    use noodles_core::Position;
    use noodles_sam::alignment::RecordBuf;
    use noodles_sam::alignment::record::Flags;
    use noodles_sam::alignment::record::MappingQuality;
    use noodles_sam::alignment::record::cigar::Op;
    use noodles_sam::alignment::record::cigar::op::Kind;

    fn create_test_record(ops: Vec<Op>, sequence: &str) -> RecordBuf {
        use noodles_sam::alignment::record_buf::{
            Cigar as RecordCigar, QualityScores, Sequence as RecordSequence,
        };

        let cigar = RecordCigar::from(ops);

        RecordBuf::builder()
            .set_flags(Flags::empty())
            .set_reference_sequence_id(0)
            .set_alignment_start(Position::try_from(1).unwrap())
            .set_mapping_quality(MappingQuality::try_from(60).unwrap())
            .set_cigar(cigar)
            .set_sequence(RecordSequence::from(sequence.as_bytes().to_vec()))
            .set_quality_scores(QualityScores::from(vec![b'~'; sequence.len()]))
            .build()
    }

    #[test]
    fn test_calculate_nm_perfect_match() {
        let record = create_test_record(vec![Op::new(Kind::Match, 10)], "ACGTACGTAC");
        let reference = b"ACGTACGTAC";

        let nm = calculate_nm_tag(&record, Some(reference));
        assert_eq!(nm, Some(0)); // Perfect match
    }

    #[test]
    fn test_calculate_nm_with_mismatch() {
        let record = create_test_record(vec![Op::new(Kind::Match, 10)], "ACGTACGTAC");
        let reference = b"ACGTCCGTAC"; // One mismatch at position 5

        let nm = calculate_nm_tag(&record, Some(reference));
        assert_eq!(nm, Some(1));
    }

    #[test]
    fn test_calculate_nm_with_insertion() {
        let record = create_test_record(
            vec![
                Op::new(Kind::Match, 5),
                Op::new(Kind::Insertion, 2),
                Op::new(Kind::Match, 3),
            ],
            "ACGTAXXCGT",
        );

        let nm = calculate_nm_tag(&record, None);
        assert_eq!(nm, Some(2)); // 2 inserted bases
    }

    #[test]
    fn test_calculate_nm_with_deletion() {
        let record = create_test_record(
            vec![
                Op::new(Kind::Match, 5),
                Op::new(Kind::Deletion, 2),
                Op::new(Kind::Match, 3),
            ],
            "ACGTACGT",
        );

        let nm = calculate_nm_tag(&record, None);
        assert_eq!(nm, Some(2)); // 2 deleted bases
    }

    #[test]
    fn test_calculate_md_perfect_match() {
        let record = create_test_record(vec![Op::new(Kind::Match, 10)], "ACGTACGTAC");
        let reference = b"ACGTACGTAC";

        let md = calculate_md_tag(&record, reference);
        assert_eq!(md, Some("10".to_string()));
    }

    #[test]
    fn test_calculate_md_with_mismatch() {
        let record = create_test_record(vec![Op::new(Kind::Match, 10)], "ACGTACGTAC");
        let reference = b"ACGTCCGTAC"; // C->A mismatch at position 5 (0-based 4)

        let md = calculate_md_tag(&record, reference);
        assert_eq!(md, Some("4C5".to_string())); // 4 matches, mismatch (ref=C), 5 matches
    }

    #[test]
    fn test_calculate_md_with_deletion() {
        let record = create_test_record(
            vec![
                Op::new(Kind::Match, 5),
                Op::new(Kind::Deletion, 2),
                Op::new(Kind::Match, 3),
            ],
            "ACGTACGT",
        );
        let reference = b"ACGTAXXCGT"; // XX deleted

        let md = calculate_md_tag(&record, reference);
        assert_eq!(md, Some("5^XX3".to_string()));
    }

    #[test]
    fn test_calculate_nm_unmapped() {
        use noodles_sam::alignment::record_buf::{
            Cigar as RecordCigar, QualityScores, Sequence as RecordSequence,
        };

        let cigar: RecordCigar = vec![Op::new(Kind::Match, 10)].into();
        let sequence = "ACGTACGTAC";

        let record = RecordBuf::builder()
            .set_flags(Flags::UNMAPPED)
            .set_cigar(cigar)
            .set_sequence(RecordSequence::from(sequence.as_bytes().to_vec()))
            .set_quality_scores(QualityScores::from(vec![b'~'; sequence.len()]))
            .build();

        let nm = calculate_nm_tag(&record, None);
        assert_eq!(nm, None); // Unmapped reads return None
    }
}
