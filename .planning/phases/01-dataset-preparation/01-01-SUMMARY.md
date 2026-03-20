---
phase: 01-dataset-preparation
plan: 01
subsystem: testing
tags: [jupyter, encode, gencode, bam, gtf, benchmark, dataset]

# Dependency graph
requires: []
provides:
  - "Benchmark notebook with dataset download, validation, and tool installation cells"
  - "Reproducible dataset preparation for ENCODE ENCFF588KDY BAM + GENCODE v49 GTF"
affects: [01-02, 02-counting-implementation]

# Tech tracking
tech-stack:
  added: [samtools, featureCounts-2.1.1, HTSeq-count-2.1.2, hyperfine, gtime]
  patterns: [notebook-as-documentation, validation-first-downloads, env-var-data-paths]

key-files:
  created:
    - notebooks/feature_counting_benchmark.ipynb
  modified: []

key-decisions:
  - "Skipped samtools sort since ENCODE BAM is already coordinate-sorted (SO:coordinate in header)"
  - "Tool installation commands documented as markdown (not executable) to avoid env pollution"

patterns-established:
  - "POLARS_BIO_BENCHMARK_DATA env var for all external data paths"
  - "Validation-first pattern: every download followed by integrity/readability check"
  - "%%bash -s $DATA_DIR for passing Python variables to bash cells"

requirements-completed: [DATA-01, DATA-02, DATA-03]

# Metrics
duration: 2min
completed: 2026-03-20
---

# Phase 01 Plan 01: Dataset Preparation Summary

**Benchmark notebook with BAM/GTF download, chromosome naming validation, and tool install docs for ENCODE ENCFF588KDY + GENCODE v49**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-20T06:49:16Z
- **Completed:** 2026-03-20T06:50:46Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- Created 24-cell Jupyter notebook covering full dataset preparation workflow
- BAM download/verify/index cells with MD5 check, sort verification, and flagstat validation
- GTF download/decompress cells with polars-bio scan_gtf readability check
- Chromosome naming consistency validation (BAM header vs GTF seqname intersection)
- Reference tool installation documentation for featureCounts 2.1.1, HTSeq-count 2.1.2, hyperfine, gtime

## Task Commits

Each task was committed atomically:

1. **Task 1: Create benchmark notebook with data download and validation cells** - `7dc18ac` (feat)

## Files Created/Modified
- `notebooks/feature_counting_benchmark.ipynb` - Benchmark notebook with dataset preparation sections (24 cells)

## Decisions Made
- Skipped `samtools sort` step since ENCODE BAM is already coordinate-sorted (documented in notebook with header verification cell)
- Tool installation commands placed in markdown cells (not executable) to avoid environment pollution during notebook runs

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Notebook ready for data download execution (user runs cells to fetch ~3.2 GB BAM and ~1.6 GB GTF)
- Plan 01-02 can proceed to add benchmark execution sections to the same notebook
- Blockers from STATE.md still apply: CIGAR-end column and NH tag availability need resolution before Phase 2 coding

## Self-Check: PASSED

- notebooks/feature_counting_benchmark.ipynb: FOUND
- Commit 7dc18ac: FOUND

---
*Phase: 01-dataset-preparation*
*Completed: 2026-03-20*
