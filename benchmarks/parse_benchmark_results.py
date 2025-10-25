#!/usr/bin/env python3
"""
Parse polars-bio-bench CSV results and convert to github-action-benchmark format.

This script:
1. Parses CSV benchmark output from polars-bio-bench
2. Calculates per-operation averages across test cases
3. Compares baseline vs PR results
4. Outputs JSON format compatible with github-action-benchmark
5. Checks if performance exceeds threshold (default 150%)
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional


def parse_csv_results(csv_path: Path) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse CSV benchmark results and group by operation.

    Supports two CSV formats:

    Format 1 (aggregated polars-bio-bench results):
    Library,Min (s),Max (s),Mean (s),Speedup
    polars_bio,0.035,0.043,0.038,2.70x

    Format 2 (detailed results):
    operation,tool,test_case,time_ms,memory_mb,...

    Returns:
        Dict mapping operation names to list of result dictionaries
    """
    results = defaultdict(list)

    try:
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            first_row = None

            # Detect format by checking column names
            fieldnames = reader.fieldnames or []
            is_aggregated_format = "Library" in fieldnames and "Mean (s)" in fieldnames

            # Extract operation name from filename for aggregated format
            operation_name = (
                csv_path.stem.split("_")[0] if is_aggregated_format else None
            )

            for row_idx, row in enumerate(reader):
                if is_aggregated_format:
                    # Format 1: Library,Min (s),Max (s),Mean (s),Speedup
                    tool = row.get("Library", "unknown")
                    mean_s_str = row.get("Mean (s)", row.get("Mean", ""))

                    try:
                        time_s = float(mean_s_str)
                        time_ms = time_s * 1000  # Convert seconds to milliseconds
                    except (ValueError, TypeError):
                        print(
                            f"Warning: Could not parse Mean time for {tool} in {csv_path}",
                            file=sys.stderr,
                        )
                        continue

                    # Use filename as operation if available, otherwise 'benchmark'
                    operation = operation_name or "benchmark"

                    results[operation].append(
                        {
                            "operation": operation,
                            "tool": tool,
                            "test_case": f"{tool}_aggregated",
                            "time_ms": time_ms,
                            "row": row,
                        }
                    )

                else:
                    # Format 2: operation,tool,test_case,time_ms,...
                    operation = row.get("operation", row.get("test_name", "unknown"))
                    tool = row.get("tool", "polars_bio")
                    test_case = row.get(
                        "test_case", row.get("dataset", f"test_{row_idx}")
                    )

                    # Try different possible column names for timing
                    time_ms = None
                    for time_col in [
                        "time_ms",
                        "execution_time",
                        "time",
                        "duration_ms",
                        "mean_ms",
                    ]:
                        if time_col in row and row[time_col]:
                            try:
                                time_ms = float(row[time_col])
                                break
                            except (ValueError, TypeError):
                                continue

                    if time_ms is None:
                        print(
                            f"Warning: Could not find timing data for {operation}/{test_case}",
                            file=sys.stderr,
                        )
                        continue

                    results[operation].append(
                        {
                            "operation": operation,
                            "tool": tool,
                            "test_case": test_case,
                            "time_ms": time_ms,
                            "row": row,
                        }
                    )

    except FileNotFoundError:
        print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error parsing CSV: {e}", file=sys.stderr)
        sys.exit(1)

    return dict(results)


def calculate_operation_averages(
    results: Dict[str, List[Dict[str, Any]]], tool_filter: str = "polars_bio"
) -> Dict[str, float]:
    """
    Calculate average time per operation across all test cases.

    For aggregated format (multiple tools), extracts only the specified tool's result.
    For detailed format (single tool, multiple test cases), averages across test cases.

    Args:
        results: Dict mapping operation names to list of results
        tool_filter: Tool name to filter for (default: 'polars_bio')

    Returns:
        Dict mapping operation names to average time in ms
    """
    averages = {}

    for operation, test_results in results.items():
        if not test_results:
            continue

        # Check if this is aggregated format (multiple tools per operation)
        tools = set(r["tool"] for r in test_results)
        if len(tools) > 1:
            # Aggregated format: filter for specific tool
            filtered_results = [r for r in test_results if r["tool"] == tool_filter]
            if filtered_results:
                # Already averaged in the source data, just take the value
                averages[operation] = filtered_results[0]["time_ms"]
        else:
            # Detailed format: average across all test cases
            times = [r["time_ms"] for r in test_results if "time_ms" in r]
            if times:
                averages[operation] = sum(times) / len(times)

    return averages


def compare_results(
    baseline_results: Dict[str, List[Dict[str, Any]]],
    pr_results: Dict[str, List[Dict[str, Any]]],
    threshold_percent: float = 150.0,
) -> Dict[str, Any]:
    """
    Compare baseline and PR results using per-operation averages.

    Args:
        baseline_results: Parsed baseline benchmark results
        pr_results: Parsed PR benchmark results
        threshold_percent: Alert threshold (e.g., 150 = 150% degradation)

    Returns:
        Dict with comparison results and alerts
    """
    baseline_avgs = calculate_operation_averages(baseline_results)
    pr_avgs = calculate_operation_averages(pr_results)

    comparison = {
        "operations": {},
        "alerts": [],
        "summary": {
            "total_operations": len(set(baseline_avgs.keys()) | set(pr_avgs.keys())),
            "regressions": 0,
            "improvements": 0,
            "stable": 0,
        },
    }

    # Compare each operation
    all_operations = set(baseline_avgs.keys()) | set(pr_avgs.keys())

    for operation in sorted(all_operations):
        baseline_avg = baseline_avgs.get(operation)
        pr_avg = pr_avgs.get(operation)

        op_result = {
            "operation": operation,
            "baseline_avg_ms": baseline_avg,
            "pr_avg_ms": pr_avg,
            "ratio": None,
            "percent_change": None,
            "status": "unknown",
        }

        if baseline_avg is None:
            op_result["status"] = "new"
            op_result["note"] = "Operation not present in baseline"
        elif pr_avg is None:
            op_result["status"] = "removed"
            op_result["note"] = "Operation not present in PR"
        else:
            ratio = pr_avg / baseline_avg
            percent_change = (ratio - 1.0) * 100

            op_result["ratio"] = ratio
            op_result["percent_change"] = percent_change

            if ratio > (threshold_percent / 100.0):
                op_result["status"] = "regression"
                comparison["summary"]["regressions"] += 1
                comparison["alerts"].append(
                    {
                        "operation": operation,
                        "baseline_avg_ms": baseline_avg,
                        "pr_avg_ms": pr_avg,
                        "ratio": ratio,
                        "percent_change": percent_change,
                        "threshold_percent": threshold_percent,
                    }
                )
            elif ratio < 0.95:  # Improvement threshold (5% faster)
                op_result["status"] = "improvement"
                comparison["summary"]["improvements"] += 1
            else:
                op_result["status"] = "stable"
                comparison["summary"]["stable"] += 1

        comparison["operations"][operation] = op_result

    return comparison


def convert_to_benchmark_action_format(
    results: Dict[str, List[Dict[str, Any]]], name: str = "polars-bio-bench"
) -> List[Dict[str, Any]]:
    """
    Convert results to github-action-benchmark JSON format.

    Uses the "custom-smaller-is-better" format where lower values are better.

    Args:
        results: Parsed benchmark results grouped by operation
        name: Benchmark suite name

    Returns:
        List of benchmark entries in github-action-benchmark format
    """
    benchmarks = []

    # Calculate per-operation averages
    averages = calculate_operation_averages(results)

    for operation, avg_time in averages.items():
        benchmarks.append(
            {"name": f"{name}/{operation}", "unit": "ms", "value": avg_time}
        )

    # Also include individual test cases for transparency
    for operation, test_results in results.items():
        for result in test_results:
            test_case = result["test_case"]
            benchmarks.append(
                {
                    "name": f"{name}/{operation}/{test_case}",
                    "unit": "ms",
                    "value": result["time_ms"],
                }
            )

    return benchmarks


def generate_comparison_report(
    comparison: Dict[str, Any],
    baseline_tag: str,
    pr_ref: str,
    output_format: str = "markdown",
) -> str:
    """
    Generate a human-readable comparison report.

    Args:
        comparison: Comparison results from compare_results()
        baseline_tag: Git tag used as baseline
        pr_ref: PR branch or commit ref
        output_format: 'markdown' or 'text'

    Returns:
        Formatted comparison report
    """
    if output_format == "markdown":
        lines = [
            f"# Benchmark Comparison: {pr_ref} vs {baseline_tag}",
            "",
            f"**Summary:** {comparison['summary']['regressions']} regressions, "
            f"{comparison['summary']['improvements']} improvements, "
            f"{comparison['summary']['stable']} stable",
            "",
        ]

        if comparison["alerts"]:
            lines.extend(
                [
                    "## ⚠️ Performance Regressions Detected",
                    "",
                    "| Operation | Baseline (ms) | PR (ms) | Change | Status |",
                    "|-----------|---------------|---------|--------|--------|",
                ]
            )

            for alert in comparison["alerts"]:
                lines.append(
                    f"| {alert['operation']} | "
                    f"{alert['baseline_avg_ms']:.2f} | "
                    f"{alert['pr_avg_ms']:.2f} | "
                    f"+{alert['percent_change']:.1f}% | "
                    f"❌ {alert['ratio']:.2f}x baseline (threshold: {alert['threshold_percent']:.0f}%) |"
                )

            lines.append("")

        lines.extend(
            [
                "## All Operations",
                "",
                "| Operation | Baseline (ms) | PR (ms) | Change | Status |",
                "|-----------|---------------|---------|--------|--------|",
            ]
        )

        for operation, result in sorted(comparison["operations"].items()):
            status_icon = {
                "regression": "❌",
                "improvement": "✅",
                "stable": "✓",
                "new": "🆕",
                "removed": "🗑️",
                "unknown": "❓",
            }.get(result["status"], "?")

            baseline_str = (
                f"{result['baseline_avg_ms']:.2f}"
                if result["baseline_avg_ms"] is not None
                else "N/A"
            )
            pr_str = (
                f"{result['pr_avg_ms']:.2f}"
                if result["pr_avg_ms"] is not None
                else "N/A"
            )

            if result["percent_change"] is not None:
                change_str = f"{result['percent_change']:+.1f}%"
            else:
                change_str = "N/A"

            lines.append(
                f"| {operation} | {baseline_str} | {pr_str} | {change_str} | "
                f"{status_icon} {result['status']} |"
            )

        return "\n".join(lines)

    else:  # text format
        lines = [
            f"Benchmark Comparison: {pr_ref} vs {baseline_tag}",
            "=" * 70,
            f"Summary: {comparison['summary']['regressions']} regressions, "
            f"{comparison['summary']['improvements']} improvements, "
            f"{comparison['summary']['stable']} stable",
            "",
        ]

        if comparison["alerts"]:
            lines.append("PERFORMANCE REGRESSIONS DETECTED:")
            for alert in comparison["alerts"]:
                lines.append(
                    f"  - {alert['operation']}: "
                    f"{alert['baseline_avg_ms']:.2f}ms -> {alert['pr_avg_ms']:.2f}ms "
                    f"({alert['percent_change']:+.1f}%, exceeds {alert['threshold_percent']:.0f}% threshold)"
                )
            lines.append("")

        return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Parse polars-bio-bench CSV results and compare against baseline"
    )
    parser.add_argument(
        "baseline_csv", type=Path, help="Path to baseline benchmark CSV results"
    )
    parser.add_argument("pr_csv", type=Path, help="Path to PR benchmark CSV results")
    parser.add_argument(
        "--threshold",
        type=float,
        default=150.0,
        help="Alert threshold percentage (default: 150.0 = 150%% degradation)",
    )
    parser.add_argument(
        "--baseline-tag",
        type=str,
        default="baseline",
        help="Baseline git tag name for reporting",
    )
    parser.add_argument(
        "--pr-ref",
        type=str,
        default="PR",
        help="PR reference (branch/commit) for reporting",
    )
    parser.add_argument(
        "--output-json", type=Path, help="Output path for github-action-benchmark JSON"
    )
    parser.add_argument(
        "--output-comparison", type=Path, help="Output path for comparison results JSON"
    )
    parser.add_argument(
        "--output-report", type=Path, help="Output path for markdown comparison report"
    )
    parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="Exit with code 1 if regressions detected",
    )

    args = parser.parse_args()

    # Parse both CSV files
    print(f"Parsing baseline results from {args.baseline_csv}")
    baseline_results = parse_csv_results(args.baseline_csv)

    print(f"Parsing PR results from {args.pr_csv}")
    pr_results = parse_csv_results(args.pr_csv)

    # Compare results
    print(f"Comparing results with {args.threshold}% threshold")
    comparison = compare_results(baseline_results, pr_results, args.threshold)

    # Output comparison JSON
    if args.output_comparison:
        with open(args.output_comparison, "w") as f:
            json.dump(comparison, f, indent=2)
        print(f"Comparison results written to {args.output_comparison}")

    # Output benchmark action format
    if args.output_json:
        pr_benchmarks = convert_to_benchmark_action_format(
            pr_results, f"polars-bio-bench/{args.pr_ref}"
        )
        with open(args.output_json, "w") as f:
            json.dump(pr_benchmarks, f, indent=2)
        print(f"Benchmark JSON written to {args.output_json}")

    # Output comparison report
    if args.output_report:
        report = generate_comparison_report(comparison, args.baseline_tag, args.pr_ref)
        with open(args.output_report, "w") as f:
            f.write(report)
        print(f"Comparison report written to {args.output_report}")
    else:
        # Print to console
        report = generate_comparison_report(comparison, args.baseline_tag, args.pr_ref)
        print("\n" + report)

    # Summary
    print(f"\n{'='*70}")
    print(
        f"Summary: {comparison['summary']['regressions']} regressions, "
        f"{comparison['summary']['improvements']} improvements, "
        f"{comparison['summary']['stable']} stable"
    )

    if comparison["alerts"]:
        print(f"\n⚠️  {len(comparison['alerts'])} performance regression(s) detected!")
        for alert in comparison["alerts"]:
            print(
                f"  - {alert['operation']}: {alert['percent_change']:+.1f}% "
                f"(threshold: {alert['threshold_percent']:.0f}%)"
            )

    # Exit with error if regressions found and --fail-on-regression is set
    if args.fail_on_regression and comparison["alerts"]:
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
