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


def _extract_div_block(html: str, start_marker: str) -> tuple[str, str, str]:
    """Split *html* around the first DIV that matches *start_marker*.

    Returns a tuple ``(before, block, after)`` where ``block`` contains the
    complete ``<div ...>...</div>`` segment (including all nested DIVs). If the
    marker is not found an empty block is returned alongside the original html
    as the first element.
    """

    start = html.find(start_marker)
    if start == -1:
        return html, "", ""

    before = html[:start]
    i = start
    depth = 0

    while i < len(html):
        char = html[i]
        if char != "<":
            i += 1
            continue

        # Closing divs reduce the depth and potentially terminate the block
        if html.startswith("</div>", i):
            depth -= 1
            i += len("</div>")
            if depth == 0:
                block = html[start:i]
                after = html[i:]
                return before, block, after
            continue

        # Opening divs (including the marker itself) increase the depth
        if html.startswith("<div", i):
            depth += 1
            closing = html.find(">", i)
            if closing == -1:
                break
            i = closing + 1
            continue

        # Skip over other tags such as <h1>, <script>, etc.
        closing = html.find(">", i)
        if closing == -1:
            break
        i = closing + 1

    # Marker not properly closed – return original content
    return html, "", ""


def _remove_header_block(body_content: str) -> str:
    """Remove the leading header block from the per-runner HTML fragment."""

    before, header_block, after = _extract_div_block(
        body_content, '<div class="header">'
    )
    if not header_block:
        return body_content
    # Trim leading whitespace to keep markup tidy once embedded in the tab
    return (before + after).lstrip()


def _create_tabbed_html(
    runner_htmls: Dict[str, str],
    runner_labels: Dict[str, str],
    baseline_name: str,
    pr_name: str,
) -> str:
    """Create HTML with tabs for multiple runners.

    Args:
        runner_htmls: Dict mapping runner name to full HTML content
        runner_labels: Dict mapping runner name to display label
        baseline_name: Baseline version name
        pr_name: PR/target version name

    Returns:
        Complete HTML with tabbed interface
    """
    import re
    from datetime import datetime

    # Extract body content from each runner's HTML
    runner_bodies = {}
    for runner_name, html in runner_htmls.items():
        # Extract content between <body> tags
        body_match = re.search(r"<body>(.*?)</body>", html, re.DOTALL)
        if body_match:
            runner_bodies[runner_name] = body_match.group(1)
        else:
            runner_bodies[runner_name] = html

    # Generate timestamp
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    # Strip out the per-runner headers so charts stay within their tab panels
    for runner_name in list(runner_bodies.keys()):
        runner_bodies[runner_name] = _remove_header_block(runner_bodies[runner_name])

    # Start building tabbed HTML
    html = f"""<!DOCTYPE html>
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

        /* Tab styling */
        .tab-container {{
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
            overflow: hidden;
        }}
        .tab-buttons {{
            display: flex;
            border-bottom: 2px solid #e0e0e0;
            background-color: #fafafa;
        }}
        .tab-button {{
            padding: 15px 25px;
            cursor: pointer;
            border: none;
            background: none;
            font-size: 16px;
            font-weight: 500;
            color: #666;
            transition: all 0.3s ease;
            border-bottom: 3px solid transparent;
        }}
        .tab-button:hover {{
            background-color: #f0f0f0;
            color: #333;
        }}
        .tab-button.active {{
            color: #1976d2;
            border-bottom-color: #1976d2;
            background-color: white;
        }}
        .tab-content {{
            display: none;
            padding: 20px;
        }}
        .tab-content.active {{
            display: block;
        }}

        /* Chart styling */
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
            margin-right: 8px;
            vertical-align: middle;
            border-radius: 2px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Benchmark Comparison: {baseline_name} vs {pr_name}</h1>
        <div class="subtitle">Generated: {timestamp}</div>
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

    <div class="tab-container">
        <div class="tab-buttons">
"""

    # Add tab buttons
    for idx, (runner_name, label) in enumerate(sorted(runner_labels.items())):
        active_class = " active" if idx == 0 else ""
        html += f'            <button class="tab-button{active_class}" data-runner="{runner_name}" onclick="switchTab(\'{runner_name}\')">{label}</button>\n'

    html += """        </div>
"""

    # Add tab content panels
    for idx, runner_name in enumerate(sorted(runner_bodies.keys())):
        active_class = " active" if idx == 0 else ""
        body_content = runner_bodies[runner_name]

        # Make all chart IDs and data variable names unique by adding runner suffix
        # Replace chart div IDs: chart-{operation}-{type} -> chart-{operation}-{type}-{runner}
        body_content = re.sub(
            r'id="(chart-[^"]+)"', rf'id="\1-{runner_name}"', body_content
        )
        # Make data and layout variable names unique
        # Replace: var data_{operation}_{type} -> var data_{operation}_{type}_{runner}
        body_content = re.sub(
            r"\bvar (data_[a-zA-Z0-9_]+)\b",
            rf"var \1_{runner_name}",
            body_content,
        )
        # Replace: var layout_{operation}_{type} -> var layout_{operation}_{type}_{runner}
        body_content = re.sub(
            r"\bvar (layout_[a-zA-Z0-9_]+)\b",
            rf"var \1_{runner_name}",
            body_content,
        )

        # Replace Plotly.newPlot calls with lazy initialization per runner
        # Store initializer functions and invoke them when the tab becomes visible
        def replace_newplot_with_lazy(match):
            chart_id = match.group(1)
            data_var = match.group(2)
            layout_var = match.group(3)
            chart_id_with_runner = f"{chart_id}-{runner_name}"
            return (
                "        window.runnerInitializers = window.runnerInitializers || {};\n"
                f"        window.runnerInitializers['{runner_name}'] = window.runnerInitializers['{runner_name}'] || [];\n"
                f"        window.runnerInitializers['{runner_name}'].push(function() {{\n"
                f"            const target = document.getElementById('{chart_id_with_runner}');\n"
                "            if (!target) { return; }\n"
                f"            Plotly.newPlot(target, {data_var}_{runner_name}, {layout_var}_{runner_name}, {{responsive: true}});\n"
                "        });"
            )

        body_content = re.sub(
            r"Plotly\.newPlot\('(chart-[^']+)', (data_[a-zA-Z0-9_]+), (layout_[a-zA-Z0-9_]+)(?:, \{responsive: true\})?\);",
            replace_newplot_with_lazy,
            body_content,
        )

        html += f"""        <div id="tab-{runner_name}" class="tab-content{active_class}">
{body_content}
        </div>
"""

    html += """    </div>

    <script>
        // Manage lazy initialization functions per runner tab
        window.runnerInitializers = window.runnerInitializers || {};
        if (typeof window.initializedTabs === 'undefined') {
            window.initializedTabs = typeof Set !== 'undefined' ? new Set() : {};
        } else if (typeof Set !== 'undefined' && !(window.initializedTabs instanceof Set)) {
            window.initializedTabs = new Set();
        }

        function tabAlreadyInitialized(name) {
            if (window.initializedTabs instanceof Set) {
                return window.initializedTabs.has(name);
            }
            return Boolean(window.initializedTabs[name]);
        }

        function markTabInitialized(name) {
            if (window.initializedTabs instanceof Set) {
                window.initializedTabs.add(name);
            } else {
                window.initializedTabs[name] = true;
            }
        }

        function initializeTabCharts(runnerName) {
            if (tabAlreadyInitialized(runnerName)) {
                return;
            }

            const initializers = window.runnerInitializers[runnerName] || [];
            initializers.forEach(initFn => {
                try {
                    initFn();
                } catch (err) {
                    console.error('Failed to render charts for', runnerName, err);
                }
            });

            markTabInitialized(runnerName);
        }

        function switchTab(runnerName) {
            // Hide all tab contents
            document.querySelectorAll('.tab-content').forEach(content => {
                content.classList.remove('active');
            });

            // Deactivate all tab buttons
            document.querySelectorAll('.tab-button').forEach(button => {
                button.classList.remove('active');
            });

            // Show selected tab content
            const tabContent = document.getElementById('tab-' + runnerName);
            if (tabContent) {
                tabContent.classList.add('active');
            }

            // Activate selected tab button
            document.querySelectorAll('.tab-button').forEach(button => {
                if (button.dataset.runner === runnerName) {
                    button.classList.add('active');
                }
            });

            // Initialize charts for this tab (if not already done)
            setTimeout(() => {
                initializeTabCharts(runnerName);
            }, 10);
        }

        // Initialize the first tab on page load
        document.addEventListener('DOMContentLoaded', function() {
            const activeTab = document.querySelector('.tab-content.active');
            if (activeTab) {
                const runnerName = activeTab.id.replace('tab-', '');
                initializeTabCharts(runnerName);
            }
        });
    </script>
</body>
</html>
"""

    return html


def generate_multi_runner_html(
    runners: Dict[str, Dict],
    output_file: Path,
    baseline_name: str,
    pr_name: str,
    benchmark_repo: Path = None,
):
    """Generate HTML with tabs for multiple runners."""

    if len(runners) == 1:
        # Single runner - use existing logic
        runner_name = list(runners.keys())[0]
        runner_info = runners[runner_name]

        print(f"Generating chart for single runner: {runner_name}")

        # Create output directory
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Use existing generate_html_charts but output to specific file
        temp_output_dir = output_file.parent / "temp"
        temp_output_dir.mkdir(exist_ok=True)

        generate_html_charts(
            runner_info["baseline_dir"],
            runner_info["pr_dir"],
            temp_output_dir,
            baseline_name,
            pr_name,
            benchmark_repo,
        )

        # Move the generated file to the desired location
        generated_file = temp_output_dir / "benchmark_comparison.html"
        if generated_file.exists():
            generated_file.replace(output_file)
            temp_output_dir.rmdir()
            print(f"Generated chart: {output_file}")
        else:
            print(f"Error: Generated file not found at {generated_file}")

    else:
        # Multiple runners - generate tabbed interface
        print(
            f"Generating multi-runner HTML with {len(runners)} tabs: {list(runners.keys())}"
        )

        output_file.parent.mkdir(parents=True, exist_ok=True)
        temp_output_dir = output_file.parent / "temp"
        temp_output_dir.mkdir(exist_ok=True)

        # Generate charts for each runner
        runner_htmls = {}
        for runner_name in sorted(runners.keys()):
            runner_info = runners[runner_name]

            # Generate HTML for this runner
            temp_runner_dir = temp_output_dir / runner_name
            temp_runner_dir.mkdir(exist_ok=True)

            generate_html_charts(
                runner_info["baseline_dir"],
                runner_info["pr_dir"],
                temp_runner_dir,
                baseline_name,
                pr_name,
                benchmark_repo,
            )

            # Read the generated HTML
            generated_file = temp_runner_dir / "benchmark_comparison.html"
            if generated_file.exists():
                with open(generated_file) as f:
                    runner_htmls[runner_name] = f.read()
                print(f"Generated chart for {runner_name}")
            else:
                print(f"Warning: Could not generate chart for {runner_name}")

        if not runner_htmls:
            print("Error: No charts were generated")
            return

        # Get runner labels
        runner_labels = {}
        for runner_name, runner_info in runners.items():
            if "runner_info" in runner_info:
                info = runner_info["runner_info"]
                os_name = info.get("os", runner_name)
                arch = info.get("arch", "")
                if os_name == "linux":
                    runner_labels[runner_name] = "Linux AMD64"
                elif os_name == "macos":
                    runner_labels[runner_name] = "macOS ARM64"
                else:
                    runner_labels[runner_name] = f"{os_name.title()} {arch}".strip()
            else:
                runner_labels[runner_name] = runner_name.title()

        # Create tabbed HTML
        tabs_html = _create_tabbed_html(
            runner_htmls, runner_labels, baseline_name, pr_name
        )

        # Write output
        with open(output_file, "w") as f:
            f.write(tabs_html)

        # Cleanup temp directory
        import shutil

        shutil.rmtree(temp_output_dir)

        print(
            f"Generated multi-runner comparison with {len(runner_htmls)} tabs: {output_file}"
        )


def discover_runner_results(results_dir: Path) -> Dict[str, Dict]:
    """Discover runner results from the artifacts directory.

    Returns a dict mapping runner name to {baseline_dir, pr_dir, runner_info}.
    """
    runners = {}

    # Find all benchmark-results-* directories
    for artifact_dir in results_dir.glob("benchmark-results-*"):
        if not artifact_dir.is_dir():
            continue

        # Extract runner name from directory name
        runner_name = artifact_dir.name.replace("benchmark-results-", "")

        baseline_dir = artifact_dir / "baseline_results"
        pr_dir = artifact_dir / "pr_results"
        runner_info_file = artifact_dir / "runner_info.json"

        if baseline_dir.exists() and pr_dir.exists():
            runner_info = {}
            if runner_info_file.exists():
                with open(runner_info_file) as f:
                    runner_info = json.load(f)

            runners[runner_name] = {
                "baseline_dir": baseline_dir,
                "pr_dir": pr_dir,
                "runner_info": runner_info,
            }
            print(
                f"Found runner: {runner_name} ({runner_info.get('os', 'unknown')}/{runner_info.get('arch', 'unknown')})"
            )

    return runners


def main():
    parser = argparse.ArgumentParser(description="Generate benchmark comparison charts")
    parser.add_argument(
        "baseline_dir",
        nargs="?",
        type=Path,
        help="Directory with baseline CSV results (single-runner mode)",
    )
    parser.add_argument(
        "pr_dir",
        nargs="?",
        type=Path,
        help="Directory with PR CSV results (single-runner mode)",
    )
    parser.add_argument(
        "output_dir", nargs="?", type=Path, help="Output directory for HTML chart"
    )
    parser.add_argument("--multi-runner", action="store_true", help="Multi-runner mode")
    parser.add_argument(
        "--results-dir",
        type=Path,
        help="Directory containing all runner results (multi-runner mode)",
    )
    parser.add_argument(
        "--output", type=Path, help="Output HTML file path (multi-runner mode)"
    )
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

    if args.multi_runner:
        # Multi-runner mode
        if not args.results_dir or not args.output:
            print(
                "Error: --results-dir and --output are required for multi-runner mode"
            )
            sys.exit(1)

        if not args.results_dir.exists():
            print(f"Error: Results directory not found: {args.results_dir}")
            sys.exit(1)

        # Discover all runners
        runners = discover_runner_results(args.results_dir)

        if not runners:
            print("Error: No runner results found in", args.results_dir)
            sys.exit(1)

        # Generate multi-runner HTML
        generate_multi_runner_html(
            runners,
            args.output,
            args.baseline_name,
            args.pr_name,
            args.benchmark_repo,
        )

    else:
        # Single-runner mode (backward compatible)
        if not args.baseline_dir or not args.pr_dir or not args.output_dir:
            print(
                "Error: baseline_dir, pr_dir, and output_dir are required for single-runner mode"
            )
            sys.exit(1)

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
