#!/usr/bin/env python3
"""
Generate bar chart comparisons for benchmark results.
Creates one figure per operation showing baseline vs PR for each tool.
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# Try to import tomllib (Python 3.11+), fall back to tomli
try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None


def extract_library_versions(benchmark_dir: Path) -> Dict[str, str]:
    """Extract library versions from benchmark repository's pyproject.toml.

    Returns empty dict if versions cannot be extracted.
    """
    versions = {}

    if not tomllib:
        print("Warning: tomllib/tomli not available, cannot extract versions")
        return versions

    pyproject_path = benchmark_dir / "pyproject.toml"

    if not pyproject_path.exists():
        print(f"Warning: {pyproject_path} not found, versions will not be displayed")
        return versions

    try:
        with open(pyproject_path, "rb") as f:
            pyproject = tomllib.load(f)
            # Check both dependencies and dev-dependencies
            dependencies = (
                pyproject.get("tool", {}).get("poetry", {}).get("dependencies", {})
            )
            dev_dependencies = (
                pyproject.get("tool", {}).get("poetry", {}).get("dev-dependencies", {})
            )
            # Merge both, preferring dev-dependencies
            all_deps = {**dependencies, **dev_dependencies}

            # Map package names to our tool names
            package_mapping = {
                "polars-bio": "polars_bio",
                "pyranges1": "pyranges1",
                "genomicranges": "genomicranges",
                "GenomicRanges": "genomicranges",  # Alternative spelling
                "bioframe": "bioframe",
            }

            for package_name, tool_name in package_mapping.items():
                if package_name in all_deps:
                    dep = all_deps[package_name]
                    if isinstance(dep, str):
                        # Remove version specifiers like ^, ~, >=, etc.
                        version = dep.lstrip("^~>=<")
                        versions[tool_name] = version
                    elif isinstance(dep, dict) and "version" in dep:
                        version = dep["version"].lstrip("^~>=<")
                        versions[tool_name] = version

        print(f"Extracted versions: {versions}")
    except Exception as e:
        print(f"Warning: Could not read versions from {pyproject_path}: {e}")

    return versions


def generate_html_charts(
    baseline_dir: Path,
    pr_dir: Path,
    output_dir: Path,
    baseline_name: str,
    pr_name: str,
    benchmark_repo_dir: Path = None,
):
    """Generate HTML with bar charts comparing baseline vs PR results."""

    # Extract library versions from benchmark repo if available
    if benchmark_repo_dir and benchmark_repo_dir.exists():
        tool_versions = extract_library_versions(benchmark_repo_dir)
    else:
        # No versions available
        tool_versions = {}
        print("Warning: Benchmark repo not provided, versions will not be displayed")

    # Read all CSV files from both directories
    baseline_csvs = list(baseline_dir.glob("*.csv"))
    pr_csvs = list(pr_dir.glob("*.csv"))

    if not baseline_csvs:
        print(f"Error: No CSV files found in {baseline_dir}")
        sys.exit(1)

    if not pr_csvs:
        print(f"Error: No CSV files found in {pr_dir}")
        sys.exit(1)

    # Parse results by operation
    operations_data = {}

    for csv_file in baseline_csvs:
        # Extract operation and test case from filename
        # Pattern: {operation}-{config}_{testcase}.csv
        # Examples:
        #   "overlap-single-4tools_7-8.csv" -> operation="overlap", test_case="7-8"
        #   "overlap-single-4tools_7.csv" -> operation="overlap", test_case="7"
        #   "overlap_gnomad-sv-vcf.csv" -> operation="overlap", test_case="gnomad-sv-vcf"
        #   "count_overlaps-multi-8tools_1-2.csv" -> operation="count_overlaps", test_case="1-2"
        stem = csv_file.stem

        # Extract test case from end (pattern: _anything except underscore)
        test_case_match = re.search(r"_([^_]+)$", stem)
        if test_case_match:
            test_case = test_case_match.group(1)
            # Remove test case from stem to get operation + config
            stem_without_testcase = stem[: test_case_match.start()]
        else:
            test_case = "default"
            stem_without_testcase = stem

        # Extract operation name (everything before first dash, or entire name if no dash)
        # This handles operations with underscores like "count_overlaps"
        if "-" in stem_without_testcase:
            operation_name = stem_without_testcase.split("-")[0]
        else:
            operation_name = stem_without_testcase

        if operation_name not in operations_data:
            operations_data[operation_name] = {"tools": {}, "test_cases": set()}

        operations_data[operation_name]["test_cases"].add(test_case)

        # Read baseline data
        # CSV format: Library,Min (s),Max (s),Mean (s),Speedup
        with open(csv_file) as f:
            lines = f.readlines()
            if len(lines) < 2:
                continue

            headers = lines[0].strip().split(",")
            for line in lines[1:]:
                parts = line.strip().split(",")
                if len(parts) < 4:
                    continue

                tool = parts[0]  # Library name (e.g., "polars_bio")
                mean_time_seconds = float(parts[3])  # Mean (s)
                mean_time_ms = mean_time_seconds * 1000  # Convert to milliseconds

                if tool not in operations_data[operation_name]["tools"]:
                    operations_data[operation_name]["tools"][tool] = {
                        "baseline": {},
                        "pr": {},
                    }

                operations_data[operation_name]["tools"][tool]["baseline"][
                    test_case
                ] = mean_time_ms

    # Read PR data
    for csv_file in pr_csvs:
        # Extract operation and test case from filename (same logic as baseline)
        stem = csv_file.stem

        # Extract test case from end (pattern: _anything except underscore)
        test_case_match = re.search(r"_([^_]+)$", stem)
        if test_case_match:
            test_case = test_case_match.group(1)
            stem_without_testcase = stem[: test_case_match.start()]
        else:
            test_case = "default"
            stem_without_testcase = stem

        # Extract operation name (everything before first dash, or entire name if no dash)
        if "-" in stem_without_testcase:
            operation_name = stem_without_testcase.split("-")[0]
        else:
            operation_name = stem_without_testcase

        if operation_name not in operations_data:
            continue

        # Read PR data
        # CSV format: Library,Min (s),Max (s),Mean (s),Speedup
        with open(csv_file) as f:
            lines = f.readlines()
            if len(lines) < 2:
                continue

            for line in lines[1:]:
                parts = line.strip().split(",")
                if len(parts) < 4:
                    continue

                tool = parts[0]  # Library name
                mean_time_seconds = float(parts[3])  # Mean (s)
                mean_time_ms = mean_time_seconds * 1000  # Convert to milliseconds

                if tool in operations_data[operation_name]["tools"]:
                    operations_data[operation_name]["tools"][tool]["pr"][
                        test_case
                    ] = mean_time_ms

    # Generate timestamp for report
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    # Generate HTML
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Benchmark Comparison: {baseline_name} vs {pr_name}</title>
    <script src="https://cdn.plot.ly/plotly-2.26.0.min.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            background-color: white;
            padding: 20px;
            margin-bottom: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            margin: 0 0 10px 0;
            color: #333;
        }}
        .subtitle {{
            color: #666;
            font-size: 14px;
        }}
        .chart-container {{
            background-color: white;
            padding: 20px;
            margin-bottom: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h2 {{
            margin-top: 0;
            color: #333;
            text-transform: capitalize;
        }}
        .legend {{
            margin: 20px 0;
            padding: 15px;
            background-color: #f9f9f9;
            border-radius: 4px;
        }}
        .legend-item {{
            display: inline-block;
            margin-right: 20px;
        }}
        .legend-color {{
            display: inline-block;
            width: 20px;
            height: 20px;
            margin-right: 5px;
            vertical-align: middle;
            border-radius: 2px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Benchmark Comparison</h1>
        <div class="subtitle">
            <strong>Baseline:</strong> {baseline_name} &nbsp;|&nbsp;
            <strong>PR:</strong> {pr_name} &nbsp;|&nbsp;
            <strong>Generated:</strong> {timestamp}
        </div>
        <div class="legend">
            <div class="legend-item">
                <span class="legend-color" style="background-color: #636EFA;"></span>
                <span>Baseline [tag: {baseline_name}]</span>
            </div>
            <div class="legend-item">
                <span class="legend-color" style="background-color: #EF553B;"></span>
                <span>PR [branch: {pr_name}]</span>
            </div>
        </div>
    </div>
"""

    # Generate one chart per operation - TOTAL RUNTIME
    for operation_name in sorted(operations_data.keys()):
        data = operations_data[operation_name]
        tools = sorted(data["tools"].keys())
        test_cases = sorted(data["test_cases"])

        html_content += f"""
    <div class="chart-container">
        <h2>{operation_name.replace('_', ' ').title()} Operation - Total Runtime</h2>
        <div id="chart-{operation_name}-total"></div>
    </div>
"""

    # Generate detailed charts per operation - PER TEST CASE
    for operation_name in sorted(operations_data.keys()):
        data = operations_data[operation_name]
        tools = sorted(data["tools"].keys())
        test_cases = sorted(data["test_cases"])

        html_content += f"""
    <div class="chart-container">
        <h2>{operation_name.replace('_', ' ').title()} Operation - Per Test Case</h2>
        <div id="chart-{operation_name}-detail"></div>
    </div>
"""

    html_content += """
    <script>
"""

    # Generate JavaScript for each TOTAL chart
    for operation_name in sorted(operations_data.keys()):
        data = operations_data[operation_name]
        tools = sorted(data["tools"].keys())

        # Prepare data for total runtime grouped bar chart
        html_content += f"""
        // Data for {operation_name} - TOTAL RUNTIME
        var data_{operation_name}_total = [
"""

        for tool in tools:
            tool_data = data["tools"][tool]
            baseline_values = []
            pr_values = []
            test_case_labels = []

            # Sum across all test cases (total runtime)
            baseline_total = sum(tool_data["baseline"].values())
            baseline_count = len(tool_data["baseline"])
            pr_total = sum(tool_data["pr"].values())
            pr_count = len(tool_data["pr"])

            # Create trace for baseline
            html_content += f"""
            {{
                x: ['{tool}'],
                y: [{baseline_total:.3f}],
                text: ['{baseline_total:.1f}'],
                textposition: 'outside',
                name: 'Baseline',
                type: 'bar',
                marker: {{color: '#636EFA'}},
                showlegend: {'true' if tool == tools[0] else 'false'},
                hovertemplate: '{tool}<br>Baseline: %{{y:.2f}} ms ({baseline_count} tests)<extra></extra>'
            }},
"""

            # Create trace for PR
            html_content += f"""
            {{
                x: ['{tool}'],
                y: [{pr_total:.3f}],
                text: ['{pr_total:.1f}'],
                textposition: 'outside',
                name: 'PR',
                type: 'bar',
                marker: {{color: '#EF553B'}},
                showlegend: {'true' if tool == tools[0] else 'false'},
                hovertemplate: '{tool}<br>PR: %{{y:.2f}} ms ({pr_count} tests)<extra></extra>'
            }},
"""

        html_content += f"""
        ];

        var layout_{operation_name}_total = {{
            barmode: 'group',
            xaxis: {{
                title: 'Tool',
                tickangle: -45
            }},
            yaxis: {{
                title: 'Total Time (ms) - Sum across all test cases',
                type: 'linear'
            }},
            hovermode: 'closest',
            height: 500,
            margin: {{
                l: 80,
                r: 50,
                b: 100,
                t: 50
            }}
        }};

        Plotly.newPlot('chart-{operation_name}-total', data_{operation_name}_total, layout_{operation_name}_total, {{responsive: true}});
"""

    # Generate JavaScript for each DETAILED chart (per test case)
    for operation_name in sorted(operations_data.keys()):
        data = operations_data[operation_name]
        tools = sorted(data["tools"].keys())
        test_cases = sorted(data["test_cases"])

        html_content += f"""
        // Data for {operation_name} - PER TEST CASE DETAIL
        var data_{operation_name}_detail = [
"""

        # Create traces for each tool (baseline solid, PR striped with same color)
        tool_colors = {
            "polars_bio": "#636EFA",
            "pyranges1": "#00CC96",
            "genomicranges": "#FFA15A",
            "bioframe": "#FF6692",
        }

        # Note: tool_versions is passed from the parent function scope

        for idx, tool in enumerate(tools):
            tool_data = data["tools"][tool]
            tool_color = tool_colors.get(tool, "#636EFA")
            tool_version = tool_versions.get(tool, "")
            tool_display = f"{tool} {tool_version}" if tool_version else tool

            # Baseline trace (solid)
            baseline_values = [tool_data["baseline"].get(tc, 0) for tc in test_cases]
            html_content += f"""
            {{
                x: {test_cases},
                y: {baseline_values},
                name: '{tool_display} (baseline)',
                type: 'bar',
                marker: {{color: '{tool_color}'}},
                legendgroup: '{tool}',
                hovertemplate: '{tool_display}<br>Test: %{{x}}<br>Baseline: %{{y:.2f}} ms<extra></extra>'
            }},
"""

            # PR trace (striped with same color)
            pr_values = [tool_data["pr"].get(tc, 0) for tc in test_cases]
            html_content += f"""
            {{
                x: {test_cases},
                y: {pr_values},
                name: '{tool_display} (PR)',
                type: 'bar',
                marker: {{color: '{tool_color}', pattern: {{shape: '/'}}}},
                legendgroup: '{tool}',
                hovertemplate: '{tool_display}<br>Test: %{{x}}<br>PR: %{{y:.2f}} ms<extra></extra>'
            }},
"""

        html_content += f"""
        ];

        var layout_{operation_name}_detail = {{
            barmode: 'group',
            xaxis: {{
                title: 'Test Case',
                type: 'category'
            }},
            yaxis: {{
                title: 'Time (ms)',
                type: 'linear'
            }},
            hovermode: 'closest',
            height: 600,
            margin: {{
                l: 80,
                r: 50,
                b: 80,
                t: 50
            }},
            legend: {{
                orientation: 'v',
                x: 1.02,
                y: 1
            }}
        }};

        Plotly.newPlot('chart-{operation_name}-detail', data_{operation_name}_detail, layout_{operation_name}_detail, {{responsive: true}});
"""

    html_content += """
    </script>
</body>
</html>
"""

    # Write HTML file
    output_file = output_dir / "benchmark_comparison.html"
    output_file.write_text(html_content)
    print(f"Generated comparison chart: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Generate benchmark comparison charts")
    parser.add_argument(
        "baseline_dir", type=Path, help="Directory with baseline CSV results"
    )
    parser.add_argument("pr_dir", type=Path, help="Directory with PR CSV results")
    parser.add_argument("output_dir", type=Path, help="Output directory for HTML chart")
    parser.add_argument(
        "--baseline-name", default="Baseline", help="Name for baseline (e.g., tag name)"
    )
    parser.add_argument(
        "--pr-name", default="PR", help="Name for PR (e.g., branch name)"
    )
    parser.add_argument(
        "--benchmark-repo",
        type=Path,
        default=None,
        help="Path to benchmark repository (for extracting library versions)",
    )

    args = parser.parse_args()

    if not args.baseline_dir.exists():
        print(f"Error: Baseline directory not found: {args.baseline_dir}")
        sys.exit(1)

    if not args.pr_dir.exists():
        print(f"Error: PR directory not found: {args.pr_dir}")
        sys.exit(1)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    generate_html_charts(
        args.baseline_dir,
        args.pr_dir,
        args.output_dir,
        args.baseline_name,
        args.pr_name,
        args.benchmark_repo,
    )


if __name__ == "__main__":
    main()
