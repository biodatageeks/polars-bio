//! Balanced partition assignment for indexed genomic reads.
//!
//! When an index (BAI/CRAI/TBI/CSI) is available, this module distributes genomic regions
//! across a target number of partitions using index-derived size estimates. This achieves:
//!
//! - **Balanced workloads**: Linear-scan partitioning places boundaries at byte thresholds
//! - **Controlled concurrency**: Number of partitions = `min(target_partitions, effective_regions)`
//! - **Precise region splitting**: Chromosomes are split at exact byte-budget boundaries

use crate::genomic_filter::GenomicRegion;
use log::debug;

/// Size estimate for a single genomic region, derived from index metadata.
#[derive(Debug, Clone)]
pub struct RegionSizeEstimate {
    /// The genomic region (chromosome + optional start/end).
    pub region: GenomicRegion,
    /// Estimated compressed bytes for this region from the index.
    pub estimated_bytes: u64,
    /// Length of the contig in base pairs (if known). Used for sub-region splitting.
    pub contig_length: Option<u64>,
    /// Number of unmapped reads for this reference (from BAI metadata pseudobin).
    /// Unmapped reads have a reference_sequence_id but no alignment position.
    /// They are NOT returned by ranged BAI queries and require a separate seek.
    pub unmapped_count: u64,
    /// Sorted 1-based start positions of non-empty leaf bins from the index.
    /// Used for data-aware split positioning: splits are placed proportionally
    /// to where actual data exists rather than proportionally to base pairs.
    pub nonempty_bin_positions: Vec<u64>,
    /// Base pairs per leaf bin (16384 for BAI/TBI, 0 = no bin data available).
    pub leaf_bin_span: u64,
}

/// One partition's assignment: one or more regions to be processed sequentially.
#[derive(Debug, Clone)]
pub struct PartitionAssignment {
    /// Regions assigned to this partition, processed sequentially.
    pub regions: Vec<GenomicRegion>,
    /// Sum of estimated bytes across all assigned regions.
    pub total_estimated_bytes: u64,
}

/// Distribute genomic regions across `target_partitions` using linear-scan partitioning.
///
/// # Algorithm
///
/// Walk through regions in order, maintaining a byte budget per partition.
/// When the budget is exhausted, start a new partition. If a region exceeds
/// the remaining budget AND has a known `contig_length`, split it at the
/// byte-budget boundary (proportional to base pairs). This guarantees each
/// partition receives approximately `total_bytes / target` estimated bytes.
///
/// When a chromosome has unmapped reads (from BAI metadata), an `unmapped_tail`
/// region is emitted for direct-seek reading of unmapped records.
///
/// # Edge cases
///
/// - `target_partitions <= 1` → single partition with all regions
/// - All estimates are 0 → round-robin distribution
/// - Regions without `contig_length` are treated as atomic (unsplittable)
pub fn balance_partitions(
    estimates: Vec<RegionSizeEstimate>,
    target_partitions: usize,
) -> Vec<PartitionAssignment> {
    if estimates.is_empty() {
        return Vec::new();
    }

    let target = target_partitions.max(1);

    // Single partition fast path
    if target == 1 {
        let total: u64 = estimates.iter().map(|e| e.estimated_bytes).sum();
        return vec![PartitionAssignment {
            regions: estimates.into_iter().map(|e| e.region).collect(),
            total_estimated_bytes: total,
        }];
    }

    let total_bytes: u64 = estimates.iter().map(|e| e.estimated_bytes).sum();

    // All-zero: round-robin distribution
    if total_bytes == 0 {
        let num_bins = target.min(estimates.len());
        let mut bins: Vec<PartitionAssignment> = (0..num_bins)
            .map(|_| PartitionAssignment {
                regions: Vec::new(),
                total_estimated_bytes: 0,
            })
            .collect();
        for (i, est) in estimates.into_iter().enumerate() {
            bins[i % num_bins].regions.push(est.region);
        }
        return bins;
    }

    // Cap effective target: can't create more useful partitions than total bytes
    let effective_target = target.min(total_bytes as usize);

    // Compute per-partition budgets: distribute total_bytes evenly with remainder
    // First `extra` partitions get base_budget+1, rest get base_budget
    let base_budget = total_bytes / effective_target as u64;
    let extra = (total_bytes % effective_target as u64) as usize;

    let budget_for = |idx: usize| -> u64 {
        if idx < extra {
            base_budget + 1
        } else {
            base_budget
        }
    };

    // Linear scan: walk through regions placing partition boundaries at byte thresholds
    let mut partitions: Vec<PartitionAssignment> = Vec::with_capacity(effective_target);
    partitions.push(PartitionAssignment {
        regions: Vec::new(),
        total_estimated_bytes: 0,
    });
    let mut zero_est_counts: Vec<usize> = vec![0; 1];
    let mut budget: u64 = budget_for(0);

    for est in &estimates {
        let mut remaining: u64 = est.estimated_bytes;

        // Determine the splittable range for this region (1-based inclusive coordinates)
        let (eff_start, eff_end) = match (est.region.start, est.region.end, est.contig_length) {
            (Some(s), Some(e), _) if e >= s => (s, e),
            (_, _, Some(cl)) if cl > 0 => (1u64, cl),
            _ => (0, 0), // not splittable
        };
        let can_split = eff_end > 0 && eff_end >= eff_start;
        let mut pos: u64 = eff_start; // current 1-based start position
        let mut was_split = false;

        // 0-byte regions still need to be assigned (they may have data
        // despite having 0 estimated bytes — e.g., when all contigs share
        // a single BGZF block and the compressed offset range is 0).
        // Distribute to the partition with the fewest regions to prevent
        // alt contigs, decoys, and unplaced scaffolds from piling onto
        // the last partition and creating tail latency.
        if remaining == 0 {
            let min_idx = partitions
                .iter()
                .enumerate()
                .min_by_key(|(_, p)| p.regions.len())
                .map(|(i, _)| i)
                .unwrap();
            partitions[min_idx].regions.push(est.region.clone());
            zero_est_counts[min_idx] += 1;
            continue;
        }

        while remaining > 0 {
            // If budget exhausted, start a new partition
            if budget == 0 && partitions.len() < effective_target {
                let new_idx = partitions.len();
                partitions.push(PartitionAssignment {
                    regions: Vec::new(),
                    total_estimated_bytes: 0,
                });
                zero_est_counts.push(0);
                budget = budget_for(new_idx);
            }

            let is_last = partitions.len() >= effective_target;
            // Check if we have enough base pairs remaining to split
            let remaining_bp = if can_split && pos <= eff_end {
                eff_end - pos + 1
            } else {
                0
            };
            let splittable = remaining_bp > 1; // need >=2 bp for a meaningful split

            if remaining <= budget || is_last || !splittable {
                // Take all remaining bytes: fits in budget, last partition, or can't split
                // Use end=None for the last sub-region to avoid capping at contig_length —
                // reads beyond contig_length (if any) would be lost otherwise.
                let region = if can_split && pos <= eff_end && was_split {
                    GenomicRegion {
                        chrom: est.region.chrom.clone(),
                        start: Some(pos),
                        end: None,
                        unmapped_tail: false,
                    }
                } else {
                    est.region.clone()
                };
                let p = partitions.last_mut().unwrap();
                p.regions.push(region);
                p.total_estimated_bytes += remaining;
                budget = budget.saturating_sub(remaining);
                remaining = 0;
            } else {
                // Split: take budget's worth from this chromosome
                was_split = true;

                let sub_end = if !est.nonempty_bin_positions.is_empty() && est.leaf_bin_span > 0 {
                    // Data-aware: split at the position of the k-th non-empty bin
                    let range_start = est.nonempty_bin_positions.partition_point(|&p| p < pos);
                    let range_end = est
                        .nonempty_bin_positions
                        .partition_point(|&p| p <= eff_end);
                    let bins_in_range = range_end - range_start;

                    if bins_in_range > 1 {
                        // budget <= remaining (checked above), so result <= bins_in_range (usize)
                        let bins_to_take =
                            (bins_in_range as u128 * budget as u128 / remaining as u128) as usize;
                        let bins_to_take = bins_to_take.clamp(1, bins_in_range - 1);
                        let target_idx = range_start + bins_to_take - 1;
                        let bin_start = est.nonempty_bin_positions[target_idx];
                        (bin_start + est.leaf_bin_span - 1).min(eff_end - 1)
                    } else {
                        // <=1 bin: fall back to bp-proportional
                        let bp = (remaining_bp as u128 * budget as u128 / remaining as u128) as u64;
                        pos + bp.clamp(1, remaining_bp - 1) - 1
                    }
                } else {
                    // No bin data: original bp-proportional logic
                    let bp = (remaining_bp as u128 * budget as u128 / remaining as u128) as u64;
                    pos + bp.clamp(1, remaining_bp - 1) - 1
                };

                debug!(
                    "Splitting {} at bp {} (budget={}, remaining={})",
                    est.region.chrom, sub_end, budget, remaining
                );

                let region = GenomicRegion {
                    chrom: est.region.chrom.clone(),
                    start: Some(pos),
                    end: Some(sub_end),
                    unmapped_tail: false,
                };
                let p = partitions.last_mut().unwrap();
                p.regions.push(region);
                p.total_estimated_bytes += budget;
                remaining -= budget;
                pos = sub_end + 1;

                // Start new partition
                if partitions.len() < effective_target {
                    let new_idx = partitions.len();
                    partitions.push(PartitionAssignment {
                        regions: Vec::new(),
                        total_estimated_bytes: 0,
                    });
                    zero_est_counts.push(0);
                    budget = budget_for(new_idx);
                } else {
                    budget = 0; // is_last will handle on next iteration
                }
            }
        }

        // Emit unmapped tail for any reference that has unmapped reads.
        // Indexed range queries do not guarantee coverage of these records.
        if est.unmapped_count > 0 {
            let tail = GenomicRegion {
                chrom: est.region.chrom.clone(),
                start: None,
                end: None,
                unmapped_tail: true,
            };
            let p = partitions.last_mut().unwrap();
            p.regions.push(tail);
            p.total_estimated_bytes += 1;
            budget = budget.saturating_sub(1);
        }
    }

    // Remove empty partitions
    partitions.retain(|p| !p.regions.is_empty());

    let total_zero_est: usize = zero_est_counts.iter().sum();
    debug!(
        "Balanced {} regions ({} zero-est) into {} partitions (target: {})",
        partitions.iter().map(|b| b.regions.len()).sum::<usize>(),
        total_zero_est,
        partitions.len(),
        target
    );
    for (i, bin) in partitions.iter().enumerate() {
        let zero_ct = zero_est_counts.get(i).copied().unwrap_or(0);
        debug!(
            "  Partition {}: {} regions ({} zero-est), ~{} estimated bytes",
            i,
            bin.regions.len(),
            zero_ct,
            bin.total_estimated_bytes
        );
    }

    partitions
}

#[cfg(test)]
mod tests {
    use super::*;

    fn region(chrom: &str) -> GenomicRegion {
        GenomicRegion {
            chrom: chrom.to_string(),
            start: None,
            end: None,
            unmapped_tail: false,
        }
    }

    fn estimate(chrom: &str, bytes: u64, contig_len: Option<u64>) -> RegionSizeEstimate {
        RegionSizeEstimate {
            region: region(chrom),
            estimated_bytes: bytes,
            contig_length: contig_len,
            unmapped_count: 0,
            nonempty_bin_positions: Vec::new(),
            leaf_bin_span: 0,
        }
    }

    #[test]
    fn test_empty_estimates() {
        let result = balance_partitions(vec![], 4);
        assert!(result.is_empty());
    }

    #[test]
    fn test_single_partition() {
        let estimates = vec![
            estimate("chr1", 100, None),
            estimate("chr2", 50, None),
            estimate("chrX", 30, None),
        ];
        let result = balance_partitions(estimates, 1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].regions.len(), 3);
        assert_eq!(result[0].total_estimated_bytes, 180);
    }

    #[test]
    fn test_uniform_distribution() {
        // 4 regions of equal size, target 2 partitions
        let estimates = vec![
            estimate("chr1", 100, None),
            estimate("chr2", 100, None),
            estimate("chr3", 100, None),
            estimate("chr4", 100, None),
        ];
        let result = balance_partitions(estimates, 2);
        assert_eq!(result.len(), 2);
        // Each partition should get 2 regions of 100 bytes each
        assert_eq!(result[0].total_estimated_bytes, 200);
        assert_eq!(result[1].total_estimated_bytes, 200);
    }

    #[test]
    fn test_skewed_unsplittable() {
        // chr1 is much larger than others, but none have contig_length (unsplittable)
        // Linear scan: chr1(100) exceeds budget(80), can't split → P0=100.
        // chr2+chr3 fill P1=60.
        let estimates = vec![
            estimate("chr1", 100, None),
            estimate("chr2", 50, None),
            estimate("chr3", 10, None),
        ];
        let result = balance_partitions(estimates, 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].total_estimated_bytes, 100);
        assert_eq!(result[1].total_estimated_bytes, 60);
    }

    #[test]
    fn test_skewed_splittable() {
        // Same sizes but with contig_length → chr1 can be split at budget boundary
        // total=160, target=2, budget=80 each
        // chr1(100): take 80 → P0, remaining 20 → P1
        // chr2(50): add to P1 → P1=70
        // chr3(10): add to P1 → P1=80
        let estimates = vec![
            estimate("chr1", 100, Some(249_000_000)),
            estimate("chr2", 50, None),
            estimate("chr3", 10, None),
        ];
        let result = balance_partitions(estimates, 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].total_estimated_bytes, 80);
        assert_eq!(result[1].total_estimated_bytes, 80);
    }

    #[test]
    fn test_region_splitting() {
        // One huge region that should be split
        let estimates = vec![
            estimate("chr1", 200, Some(249_000_000)),
            estimate("chr2", 10, None),
        ];
        let result = balance_partitions(estimates, 4);
        // chr1 should be split, total regions > 2
        let total_regions: usize = result.iter().map(|b| b.regions.len()).sum();
        assert!(
            total_regions > 2,
            "Expected splitting, got {total_regions} regions"
        );
        assert!(result.len() <= 4, "Should not exceed target_partitions");
    }

    #[test]
    fn test_zero_estimates_round_robin() {
        let estimates = vec![
            estimate("chr1", 0, None),
            estimate("chr2", 0, None),
            estimate("chr3", 0, None),
            estimate("chr4", 0, None),
        ];
        let result = balance_partitions(estimates, 2);
        assert_eq!(result.len(), 2);
        // Round-robin: each bin gets 2 regions
        assert_eq!(result[0].regions.len(), 2);
        assert_eq!(result[1].regions.len(), 2);
    }

    #[test]
    fn test_fewer_regions_than_partitions_unsplittable() {
        // Without contig_length, can't split → only 2 partitions
        let estimates = vec![estimate("chr1", 100, None), estimate("chr2", 50, None)];
        let result = balance_partitions(estimates, 8);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_fewer_regions_than_partitions_splittable() {
        // With contig_length, can split to fill all target partitions
        let estimates = vec![
            estimate("chr1", 100, Some(249_000_000)),
            estimate("chr2", 50, Some(243_000_000)),
        ];
        let result = balance_partitions(estimates, 8);
        assert!(
            result.len() <= 8,
            "Should not exceed target: got {}",
            result.len()
        );
        assert!(
            result.len() > 2,
            "Splittable regions should fill more than 2 partitions: got {}",
            result.len()
        );
    }

    #[test]
    fn test_single_region_unsplittable() {
        let estimates = vec![estimate("chr1", 100, None)];
        let result = balance_partitions(estimates, 4);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].regions.len(), 1);
    }

    #[test]
    fn test_partition_count_never_exceeds_target() {
        let estimates = vec![
            estimate("chr1", 100, Some(249_000_000)),
            estimate("chr2", 90, Some(243_000_000)),
            estimate("chr3", 80, Some(198_000_000)),
            estimate("chr4", 70, Some(191_000_000)),
            estimate("chr5", 60, Some(181_000_000)),
            estimate("chrX", 50, Some(155_000_000)),
        ];
        let result = balance_partitions(estimates, 4);
        assert!(
            result.len() <= 4,
            "Got {} partitions, expected <= 4",
            result.len()
        );
    }

    #[test]
    fn test_all_regions_present_after_balancing() {
        let estimates = vec![
            estimate("chr1", 100, None),
            estimate("chr2", 50, None),
            estimate("chr3", 30, None),
            estimate("chrX", 20, None),
        ];
        let result = balance_partitions(estimates, 3);
        let mut all_chroms: Vec<String> = result
            .iter()
            .flat_map(|b| b.regions.iter().map(|r| r.chrom.clone()))
            .collect();
        all_chroms.sort();
        all_chroms.dedup();
        assert_eq!(all_chroms, vec!["chr1", "chr2", "chr3", "chrX"]);
    }

    #[test]
    fn test_single_contig_splits_to_target_partitions() {
        // Single contig with known length: should split to exactly target_partitions
        let estimates = vec![estimate("chr1", 1000, Some(249_000_000))];

        for target in [2, 4, 8] {
            let result = balance_partitions(estimates.clone(), target);
            assert_eq!(
                result.len(),
                target,
                "Single contig with target={} should produce {} partitions, got {}",
                target,
                target,
                result.len()
            );
            // All sub-regions should be chr1 with a start position
            let all_regions: Vec<&GenomicRegion> =
                result.iter().flat_map(|b| b.regions.iter()).collect();
            for region in &all_regions {
                assert_eq!(region.chrom, "chr1");
                assert!(region.start.is_some(), "Sub-regions should have start");
            }
            // Intermediate sub-regions should have end; last sub-region has end=None
            for region in &all_regions[..all_regions.len() - 1] {
                assert!(
                    region.end.is_some(),
                    "Intermediate sub-regions should have end"
                );
            }
            assert!(
                all_regions.last().unwrap().end.is_none(),
                "Last sub-region should have end=None (open-ended)"
            );
        }
    }

    #[test]
    fn test_realistic_human_genome_distribution() {
        // Simulating human genome chromosome sizes (compressed bytes, proportional)
        let estimates = vec![
            estimate("chr1", 249, Some(249_000_000)),
            estimate("chr2", 243, Some(243_000_000)),
            estimate("chr3", 198, Some(198_000_000)),
            estimate("chr4", 191, Some(191_000_000)),
            estimate("chr5", 181, Some(181_000_000)),
            estimate("chr6", 171, Some(171_000_000)),
            estimate("chr7", 159, Some(159_000_000)),
            estimate("chr8", 146, Some(146_000_000)),
            estimate("chr9", 141, Some(141_000_000)),
            estimate("chr10", 136, Some(136_000_000)),
            estimate("chr11", 135, Some(135_000_000)),
            estimate("chr12", 134, Some(134_000_000)),
            estimate("chr13", 115, Some(115_000_000)),
            estimate("chr14", 107, Some(107_000_000)),
            estimate("chr15", 102, Some(102_000_000)),
            estimate("chr16", 90, Some(90_000_000)),
            estimate("chr17", 84, Some(84_000_000)),
            estimate("chr18", 80, Some(80_000_000)),
            estimate("chr19", 59, Some(59_000_000)),
            estimate("chr20", 64, Some(64_000_000)),
            estimate("chr21", 47, Some(47_000_000)),
            estimate("chr22", 51, Some(51_000_000)),
            estimate("chrX", 155, Some(155_000_000)),
            estimate("chrY", 57, Some(57_000_000)),
        ];
        let total: u64 = estimates.iter().map(|e| e.estimated_bytes).sum();
        let result = balance_partitions(estimates, 8);

        assert!(result.len() <= 8);
        assert!(!result.is_empty());

        // Total bytes should be exactly preserved (integer arithmetic)
        let result_total: u64 = result.iter().map(|b| b.total_estimated_bytes).sum();
        assert_eq!(
            result_total, total,
            "Total bytes should be exactly preserved: {result_total} vs {total}"
        );

        // Linear scan guarantees near-perfect balance
        let max_bytes = result
            .iter()
            .map(|b| b.total_estimated_bytes)
            .max()
            .unwrap();
        let min_bytes = result
            .iter()
            .map(|b| b.total_estimated_bytes)
            .min()
            .unwrap();
        // With linear scan, max/min ratio should be very close to 1
        assert!(
            max_bytes <= min_bytes * 2,
            "Partitions too imbalanced: max={max_bytes} min={min_bytes}"
        );
    }

    fn estimate_with_unmapped(
        chrom: &str,
        bytes: u64,
        contig_len: Option<u64>,
        unmapped: u64,
    ) -> RegionSizeEstimate {
        RegionSizeEstimate {
            region: region(chrom),
            estimated_bytes: bytes,
            contig_length: contig_len,
            unmapped_count: unmapped,
            nonempty_bin_positions: Vec::new(),
            leaf_bin_span: 0,
        }
    }

    #[test]
    fn test_unmapped_tail_emitted_when_unmapped_count_nonzero() {
        // chr1 is large and has unmapped reads; chr2 is small with no unmapped
        let estimates = vec![
            estimate_with_unmapped("chr1", 200, Some(249_000_000), 1000),
            estimate_with_unmapped("chr2", 10, None, 0),
        ];
        let result = balance_partitions(estimates, 4);

        // Collect all regions
        let all_regions: Vec<&GenomicRegion> =
            result.iter().flat_map(|b| b.regions.iter()).collect();

        // There should be an unmapped_tail region for chr1
        let unmapped_tails: Vec<&&GenomicRegion> =
            all_regions.iter().filter(|r| r.unmapped_tail).collect();
        assert_eq!(
            unmapped_tails.len(),
            1,
            "Expected exactly 1 unmapped_tail region"
        );
        assert_eq!(unmapped_tails[0].chrom, "chr1");
        assert!(unmapped_tails[0].start.is_none());
        assert!(unmapped_tails[0].end.is_none());

        // No unmapped_tail for chr2 (unmapped_count = 0)
        let chr2_unmapped: Vec<&&GenomicRegion> = all_regions
            .iter()
            .filter(|r| r.chrom == "chr2" && r.unmapped_tail)
            .collect();
        assert!(chr2_unmapped.is_empty());
    }

    #[test]
    fn test_unmapped_tail_emitted_even_when_no_split() {
        // chrM is small relative to ideal (total=200, target=4, ideal=50, chrM=5 < 50)
        // so it won't be split, but we should still emit an unmapped_tail when
        // unmapped_count > 0.
        let estimates = vec![
            estimate_with_unmapped("chr1", 100, Some(249_000_000), 500),
            estimate_with_unmapped("chr2", 95, Some(243_000_000), 300),
            estimate_with_unmapped("chrM", 5, Some(16_569), 100),
        ];
        let result = balance_partitions(estimates, 4);

        let all_regions: Vec<&GenomicRegion> =
            result.iter().flat_map(|b| b.regions.iter()).collect();
        let chrm_unmapped: Vec<&&GenomicRegion> = all_regions
            .iter()
            .filter(|r| r.chrom == "chrM" && r.unmapped_tail)
            .collect();
        assert_eq!(
            chrm_unmapped.len(),
            1,
            "Should emit unmapped_tail for unsplit regions when unmapped_count > 0"
        );
    }

    #[test]
    fn test_linear_scan_perfect_balance() {
        // Linear scan should produce near-perfect balance for splittable regions
        // total = 750, target = 4 → budget = 187 or 188 each
        let estimates = vec![
            estimate("chr1", 249, Some(249_000_000)),
            estimate("chr2", 243, Some(243_000_000)),
            estimate("chr3", 198, Some(198_000_000)),
            estimate("chrX", 60, Some(155_000_000)),
        ];
        let result = balance_partitions(estimates, 4);

        assert_eq!(result.len(), 4);
        let total: u64 = result.iter().map(|b| b.total_estimated_bytes).sum();
        assert_eq!(total, 750);

        // Each partition should be within ±1 of ideal (187.5)
        for (i, p) in result.iter().enumerate() {
            assert!(
                p.total_estimated_bytes >= 187 && p.total_estimated_bytes <= 189,
                "Partition {} has {} bytes, expected ~188",
                i,
                p.total_estimated_bytes
            );
        }
    }

    #[test]
    fn test_zero_byte_regions_not_dropped() {
        // When some contigs share a BGZF block, their compressed byte range is 0.
        // These regions must still be included in partition assignments.
        let estimates = vec![
            estimate("chr1", 0, None),
            estimate("chr2", 100, None),
            estimate("chrX", 0, None),
        ];
        let result = balance_partitions(estimates, 4);

        let mut all_chroms: Vec<String> = result
            .iter()
            .flat_map(|b| b.regions.iter().map(|r| r.chrom.clone()))
            .collect();
        all_chroms.sort();
        assert_eq!(
            all_chroms,
            vec!["chr1", "chr2", "chrX"],
            "All regions must be present even with 0-byte estimates"
        );
    }

    #[test]
    fn test_linear_scan_preserves_total_bytes() {
        // Verify total bytes are exactly preserved across various configurations
        for target in [2, 3, 4, 8, 16] {
            let estimates = vec![
                estimate("chr1", 249, Some(249_000_000)),
                estimate("chr2", 243, Some(243_000_000)),
                estimate("chr3", 198, Some(198_000_000)),
            ];
            let total: u64 = estimates.iter().map(|e| e.estimated_bytes).sum();
            let result = balance_partitions(estimates, target);
            let result_total: u64 = result.iter().map(|b| b.total_estimated_bytes).sum();
            assert_eq!(
                result_total, total,
                "Total bytes not preserved for target={target}: {result_total} vs {total}"
            );
        }
    }

    fn estimate_with_bins(
        chrom: &str,
        bytes: u64,
        contig_len: Option<u64>,
        bin_positions: Vec<u64>,
        leaf_bin_span: u64,
    ) -> RegionSizeEstimate {
        RegionSizeEstimate {
            region: region(chrom),
            estimated_bytes: bytes,
            contig_length: contig_len,
            unmapped_count: 0,
            nonempty_bin_positions: bin_positions,
            leaf_bin_span,
        }
    }

    #[test]
    fn test_bin_aware_split_concentrates_on_data() {
        // All data is in the first 10% of chr1 (bins at positions 1..~25M of a 249M contig).
        // With bp-proportional splitting, sub-regions would span the full 249M evenly.
        // With bin-aware splitting, sub-regions should cluster in the first ~25M.
        let span: u64 = 16384;
        // 100 bins in the first 25M (positions 1, 16385, 32769, ...)
        let bins: Vec<u64> = (0..100).map(|i| i * span + 1).collect();

        let estimates = vec![estimate_with_bins(
            "chr1",
            1000,
            Some(249_000_000),
            bins,
            span,
        )];
        let result = balance_partitions(estimates, 4);

        assert_eq!(result.len(), 4);

        // All intermediate split ends should be within the data region (< 2M)
        let all_regions: Vec<&GenomicRegion> =
            result.iter().flat_map(|b| b.regions.iter()).collect();
        for region in &all_regions[..all_regions.len() - 1] {
            if let Some(end) = region.end {
                assert!(
                    end < 2_000_000,
                    "Split end {end} should be near data (< 2M), not spread across full contig"
                );
            }
        }
    }

    #[test]
    fn test_bin_aware_split_preserves_total_bytes() {
        let span: u64 = 16384;
        let bins: Vec<u64> = (0..200).map(|i| i * span + 1).collect();

        for target in [2, 4, 8] {
            let estimates = vec![estimate_with_bins(
                "chr1",
                500,
                Some(249_000_000),
                bins.clone(),
                span,
            )];
            let total: u64 = estimates.iter().map(|e| e.estimated_bytes).sum();
            let result = balance_partitions(estimates, target);
            let result_total: u64 = result.iter().map(|b| b.total_estimated_bytes).sum();
            assert_eq!(
                result_total, total,
                "Bin-aware: total bytes not preserved for target={target}: {result_total} vs {total}"
            );
        }
    }

    #[test]
    fn test_bin_aware_fallback_when_no_bins() {
        // Empty bin positions → should behave identically to bp-proportional
        let with_bins = estimate_with_bins("chr1", 200, Some(249_000_000), Vec::new(), 0);
        let without_bins = estimate("chr1", 200, Some(249_000_000));

        let result_bins = balance_partitions(vec![with_bins], 4);
        let result_no_bins = balance_partitions(vec![without_bins], 4);

        assert_eq!(result_bins.len(), result_no_bins.len());
        for (a, b) in result_bins.iter().zip(result_no_bins.iter()) {
            assert_eq!(a.total_estimated_bytes, b.total_estimated_bytes);
            assert_eq!(a.regions.len(), b.regions.len());
            for (ra, rb) in a.regions.iter().zip(b.regions.iter()) {
                assert_eq!(ra.start, rb.start);
                assert_eq!(ra.end, rb.end);
            }
        }
    }

    #[test]
    fn test_bin_aware_sparse_wes_pattern() {
        // Simulate WES: exonic regions scattered across a large chromosome.
        // 3 clusters of bins at positions ~10M, ~50M, ~200M
        let span: u64 = 16384;
        let mut bins = Vec::new();
        // Cluster 1: 40 bins near 10M
        for i in 0..40 {
            bins.push(10_000_000 + i * span);
        }
        // Cluster 2: 30 bins near 50M
        for i in 0..30 {
            bins.push(50_000_000 + i * span);
        }
        // Cluster 3: 30 bins near 200M
        for i in 0..30 {
            bins.push(200_000_000 + i * span);
        }
        bins.sort_unstable();

        let estimates = vec![estimate_with_bins(
            "chr1",
            600,
            Some(249_000_000),
            bins,
            span,
        )];
        let result = balance_partitions(estimates, 4);

        assert_eq!(result.len(), 4);

        // Total bytes must be preserved
        let result_total: u64 = result.iter().map(|b| b.total_estimated_bytes).sum();
        assert_eq!(result_total, 600);

        // Split points should be near bin cluster boundaries, not evenly spaced
        let all_regions: Vec<&GenomicRegion> =
            result.iter().flat_map(|b| b.regions.iter()).collect();
        let split_ends: Vec<u64> = all_regions.iter().filter_map(|r| r.end).collect();

        // With 100 bins split into 4 partitions (~25 bins each):
        // splits should be within cluster regions, not at 62M, 124M, 187M (bp-proportional)
        for &end in &split_ends {
            let near_cluster = (10_000_000..=11_000_000).contains(&end)
                || (50_000_000..=51_000_000).contains(&end)
                || (200_000_000..=201_000_000).contains(&end);
            assert!(
                near_cluster,
                "Split at {end} should be near a data cluster, not in empty genomic space"
            );
        }
    }

    #[test]
    fn test_zero_estimate_regions_distributed_evenly() {
        // Simulate human genome: 24 chromosomes with data + 60 zero-estimate alt/decoy contigs.
        // With target=8, zero-estimate regions should be spread across partitions,
        // not piled onto the last one.
        let mut estimates = vec![
            estimate("chr1", 249, Some(249_000_000)),
            estimate("chr2", 243, Some(243_000_000)),
            estimate("chr3", 198, Some(198_000_000)),
            estimate("chr4", 191, Some(191_000_000)),
            estimate("chr5", 181, Some(181_000_000)),
            estimate("chr6", 171, Some(171_000_000)),
            estimate("chr7", 159, Some(159_000_000)),
            estimate("chr8", 146, Some(146_000_000)),
            estimate("chr9", 141, Some(141_000_000)),
            estimate("chr10", 136, Some(136_000_000)),
            estimate("chr11", 135, Some(135_000_000)),
            estimate("chr12", 134, Some(134_000_000)),
            estimate("chr13", 115, Some(115_000_000)),
            estimate("chr14", 107, Some(107_000_000)),
            estimate("chr15", 102, Some(102_000_000)),
            estimate("chr16", 90, Some(90_000_000)),
            estimate("chr17", 84, Some(84_000_000)),
            estimate("chr18", 80, Some(80_000_000)),
            estimate("chr19", 59, Some(59_000_000)),
            estimate("chr20", 64, Some(64_000_000)),
            estimate("chr21", 47, Some(47_000_000)),
            estimate("chr22", 51, Some(51_000_000)),
            estimate("chrX", 155, Some(155_000_000)),
            estimate("chrY", 57, Some(57_000_000)),
        ];
        // Add 60 zero-estimate alt/decoy contigs (like chrUn_*, chr*_random, HLA-*)
        for i in 0..60 {
            estimates.push(estimate(&format!("alt_contig_{i}"), 0, None));
        }

        let result = balance_partitions(estimates, 8);

        // All regions must be present
        let total_regions: usize = result.iter().map(|b| b.regions.len()).sum();
        assert!(
            total_regions >= 84,
            "Expected at least 84 regions (24 chroms + 60 alts), got {total_regions}"
        );

        // Total estimated bytes preserved
        let result_total: u64 = result.iter().map(|b| b.total_estimated_bytes).sum();
        assert_eq!(result_total, 3095); // sum of all chromosome bytes

        // Count zero-estimate regions per partition
        let zero_est_per_partition: Vec<usize> = result
            .iter()
            .map(|p| {
                p.regions
                    .iter()
                    .filter(|r| r.chrom.starts_with("alt_contig_"))
                    .count()
            })
            .collect();

        let max_zero = *zero_est_per_partition.iter().max().unwrap();
        // With 60 zero-est regions across 8 partitions, ideal is ~8 each.
        // Allow up to 15 (< 2x ideal share) to account for uneven data-region counts.
        assert!(
            max_zero <= 15,
            "Max zero-est regions on one partition = {max_zero}, expected <= 15. Distribution: {zero_est_per_partition:?}"
        );

        // No partition should have 0 zero-estimate regions (all should get some)
        let min_zero = *zero_est_per_partition.iter().min().unwrap();
        assert!(
            min_zero >= 1,
            "Every partition should get at least 1 zero-est region. Distribution: {zero_est_per_partition:?}"
        );
    }

    #[test]
    fn test_zero_estimate_not_all_on_last_partition() {
        // 3 data regions + 20 zero-estimate scaffolds, target=4.
        // Before the fix, all 20 would land on the last partition.
        let mut estimates = vec![
            estimate("chr1", 100, Some(249_000_000)),
            estimate("chr2", 80, Some(243_000_000)),
            estimate("chr3", 60, Some(198_000_000)),
        ];
        for i in 0..20 {
            estimates.push(estimate(&format!("scaffold_{i}"), 0, None));
        }

        let result = balance_partitions(estimates, 4);

        // All regions present
        let total_regions: usize = result.iter().map(|b| b.regions.len()).sum();
        assert!(
            total_regions >= 23,
            "Expected at least 23 regions, got {total_regions}"
        );

        // Total bytes preserved
        let result_total: u64 = result.iter().map(|b| b.total_estimated_bytes).sum();
        assert_eq!(result_total, 240);

        // Count scaffolds per partition
        let scaffold_per_partition: Vec<usize> = result
            .iter()
            .map(|p| {
                p.regions
                    .iter()
                    .filter(|r| r.chrom.starts_with("scaffold_"))
                    .count()
            })
            .collect();

        // No single partition should have all 20 scaffolds
        let max_scaffolds = *scaffold_per_partition.iter().max().unwrap();
        assert!(
            max_scaffolds < 20,
            "One partition got all {max_scaffolds} scaffolds — not distributed! {scaffold_per_partition:?}"
        );

        // With 20 scaffolds across 4 partitions, max should be <= 8 (generous threshold)
        assert!(
            max_scaffolds <= 8,
            "Max scaffolds on one partition = {max_scaffolds}, expected <= 8. Distribution: {scaffold_per_partition:?}"
        );
    }
}
