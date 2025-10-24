#!/bin/bash
# Compare benchmark results from baseline and PR directories
# Usage: compare_benchmark_results.sh <baseline_dir> <pr_dir> <threshold> <baseline_tag> <pr_ref>

set -e

BASELINE_DIR="$1"
PR_DIR="$2"
THRESHOLD="${3:-150}"
BASELINE_TAG="$4"
PR_REF="$5"

if [ -z "$BASELINE_DIR" ] || [ -z "$PR_DIR" ]; then
    echo "Usage: $0 <baseline_dir> <pr_dir> [threshold] [baseline_tag] [pr_ref]"
    exit 1
fi

echo "Comparing benchmarks between $BASELINE_DIR and $PR_DIR"
echo "Threshold: $THRESHOLD%"
echo "Baseline: $BASELINE_TAG"
echo "PR: $PR_REF"
echo

# Create output directories
mkdir -p comparison_results
mkdir -p comparison_reports

# Track overall status
TOTAL_REGRESSIONS=0
OPERATIONS_COMPARED=0

# Process each operation CSV using find with proper handling of spaces
while IFS= read -r -d '' BASELINE_CSV; do
    # Get operation name from filename
    # Pattern: {operation}-{config}_{testcase}.csv
    # Examples:
    #   "overlap-single-4tools_7-8.csv" -> operation="overlap"
    #   "overlap-single-4tools_7.csv" -> operation="overlap"
    #   "overlap_gnomad-sv-vcf.csv" -> operation="overlap"
    #   "count_overlaps-multi-8tools_1-2.csv" -> operation="count_overlaps"
    BASENAME=$(basename "$BASELINE_CSV")

    # Remove .csv extension and test case pattern (_anything)
    # We match the last underscore and everything after it
    STEM=$(echo "$BASENAME" | sed 's/\.csv$//' | sed 's/_[^_]*$//')

    # Extract operation name (everything before first dash, or entire name if no dash)
    if echo "$STEM" | grep -q '-'; then
        OPERATION=$(echo "$STEM" | cut -d'-' -f1)
    else
        OPERATION="$STEM"
    fi

    # Find corresponding PR CSV
    PR_CSV=$(find "$PR_DIR" -name "${OPERATION}*.csv" -type f | head -1)

    if [ -z "$PR_CSV" ] || [ ! -f "$PR_CSV" ]; then
        echo "Warning: No matching PR CSV found for $OPERATION (looking for ${OPERATION}*.csv in $PR_DIR)"
        continue
    fi

    echo "Comparing $OPERATION:"
    echo "  Baseline: $BASELINE_CSV"
    echo "  PR: $PR_CSV"

    # Run parser for this operation
    python3 benchmarks/parse_benchmark_results.py \
        "$BASELINE_CSV" \
        "$PR_CSV" \
        --threshold "$THRESHOLD" \
        --baseline-tag "$BASELINE_TAG" \
        --pr-ref "$PR_REF" \
        --output-json "comparison_results/${OPERATION}_results.json" \
        --output-comparison "comparison_results/${OPERATION}_comparison.json" \
        --output-report "comparison_reports/${OPERATION}_report.md" \
        || true

    # Extract regression count
    if [ -f "comparison_results/${OPERATION}_comparison.json" ]; then
        REGRESSIONS=$(jq -r '.summary.regressions' "comparison_results/${OPERATION}_comparison.json" 2>/dev/null || echo "0")
        TOTAL_REGRESSIONS=$((TOTAL_REGRESSIONS + REGRESSIONS))
        OPERATIONS_COMPARED=$((OPERATIONS_COMPARED + 1))
    fi

    echo
done < <(find "$BASELINE_DIR" -name "*.csv" -type f -print0)

# Check if any operations were compared
if [ $OPERATIONS_COMPARED -eq 0 ]; then
    echo "Error: No CSV files found in baseline directory or no comparisons succeeded"
    exit 1
fi

echo "========================================================================"
echo "Comparison complete:"
echo "  Operations compared: $OPERATIONS_COMPARED"
echo "  Total regressions: $TOTAL_REGRESSIONS"
echo

# Generate combined report
COMBINED_REPORT="comparison_report_combined.md"
echo "# Benchmark Comparison: $PR_REF vs $BASELINE_TAG" > "$COMBINED_REPORT"
echo "" >> "$COMBINED_REPORT"
echo "**Threshold:** $THRESHOLD%" >> "$COMBINED_REPORT"
echo "**Operations compared:** $OPERATIONS_COMPARED" >> "$COMBINED_REPORT"
echo "**Total regressions:** $TOTAL_REGRESSIONS" >> "$COMBINED_REPORT"
echo "" >> "$COMBINED_REPORT"

# Append individual operation reports
for REPORT in comparison_reports/*_report.md; do
    if [ -f "$REPORT" ]; then
        echo "" >> "$COMBINED_REPORT"
        echo "---" >> "$COMBINED_REPORT"
        echo "" >> "$COMBINED_REPORT"
        cat "$REPORT" >> "$COMBINED_REPORT"
    fi
done

echo "Combined report written to $COMBINED_REPORT"

# Create summary JSON
echo "{\"operations_compared\": $OPERATIONS_COMPARED, \"total_regressions\": $TOTAL_REGRESSIONS}" > comparison_summary.json

# Exit with error if regressions found
if [ $TOTAL_REGRESSIONS -gt 0 ]; then
    echo "⚠️  Performance regressions detected!"
    exit 0  # Don't fail the workflow, just warn
else
    echo "✓ No performance regressions detected"
    exit 0
fi
