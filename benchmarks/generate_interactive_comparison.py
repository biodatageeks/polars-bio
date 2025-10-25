#!/usr/bin/env python3
"""
Generate interactive HTML benchmark comparison report with historical data selection.
Version 3: Simplified dropdowns (no architecture), dynamic tabs, improved styling.
"""

import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


def load_index(data_dir: Path) -> Dict:
    """Load the master index of all datasets."""
    index_path = data_dir / "index.json"
    with open(index_path) as f:
        return json.load(f)


def parse_csv_results(csv_path: Path) -> Dict[str, Dict]:
    """Parse a single CSV file and return results keyed by library."""
    results = {}
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            library = row["Library"]
            results[library] = {
                "min_seconds": float(row["Min (s)"]),
                "max_seconds": float(row["Max (s)"]),
                "mean_seconds": float(row["Mean (s)"]),
                "mean_ms": float(row["Mean (s)"]) * 1000,  # Convert to ms
                "speedup": row["Speedup"],
            }
    return results


def extract_operation_info(filename: str) -> Dict[str, str]:
    """Extract operation name and test case from filename."""
    stem = filename.replace(".csv", "")

    # Extract test case from end
    test_case_match = re.search(r"_([^_]+)$", stem)
    if test_case_match:
        test_case = test_case_match.group(1)
        stem_without_testcase = stem[: test_case_match.start()]
    else:
        test_case = "default"
        stem_without_testcase = stem

    # Extract operation name
    if "-" in stem_without_testcase:
        operation_name = stem_without_testcase.split("-")[0]
    else:
        operation_name = stem_without_testcase

    return {"operation": operation_name, "test_case": test_case}


def load_dataset_results(data_dir: Path, dataset_info: Dict) -> Dict:
    """Load all benchmark results for a dataset."""
    dataset_path = data_dir / dataset_info["path"]
    results_dir = dataset_path / "results"

    if not results_dir.exists():
        return None

    # Load metadata
    metadata_path = dataset_path / "metadata.json"
    metadata = {}
    if metadata_path.exists():
        with open(metadata_path) as f:
            metadata = json.load(f)

    # Organize results by operation
    operations = {}

    for csv_file in results_dir.glob("*.csv"):
        op_info = extract_operation_info(csv_file.name)
        operation = op_info["operation"]
        test_case = op_info["test_case"]

        if operation not in operations:
            operations[operation] = {"tools": {}, "test_cases": set()}

        operations[operation]["test_cases"].add(test_case)

        try:
            csv_results = parse_csv_results(csv_file)
            for tool, tool_data in csv_results.items():
                if tool not in operations[operation]["tools"]:
                    operations[operation]["tools"][tool] = {}
                operations[operation]["tools"][tool][test_case] = tool_data["mean_ms"]
        except Exception as e:
            print(f"Error parsing {csv_file}: {e}")

    # Convert sets to sorted lists
    for operation in operations.values():
        operation["test_cases"] = sorted(operation["test_cases"])

    return {
        "id": dataset_info["id"],
        "label": dataset_info["label"],
        "ref": dataset_info["ref"],
        "runner": dataset_info["runner"],
        "runner_label": dataset_info["runner_label"],
        "metadata": metadata,
        "operations": operations,
    }


def generate_html_report(data_dir: Path, output_path: Path):
    """Generate interactive HTML report."""

    # Load index and all datasets
    index = load_index(data_dir)

    # Load data for each dataset with unique keys
    # For branches with multiple commits, include commit SHA in the key
    all_datasets = {}
    for dataset_info in index["datasets"]:
        dataset_data = load_dataset_results(data_dir, dataset_info)
        if dataset_data:
            # Create unique dataset key
            if dataset_info["ref_type"] == "branch":
                commit_sha = dataset_info.get("commit_sha", "unknown")
                dataset_key = f"{dataset_info['id']}@{commit_sha}"
            else:
                dataset_key = dataset_info["id"]

            all_datasets[dataset_key] = dataset_data

    # Group datasets by ref (tags) or by commit SHA (branches)
    refs_by_type = {"tag": {}, "branch": {}}
    for dataset_info in index["datasets"]:
        ref = dataset_info["ref"]
        ref_type = dataset_info["ref_type"]
        runner = dataset_info["runner"]

        # For branches, use commit SHA as unique key; for tags, use ref name
        if ref_type == "branch":
            commit_sha = dataset_info.get("commit_sha", "unknown")
            # Use commit SHA as key to differentiate multiple commits
            unique_key = f"{ref}@{commit_sha}"
            # Create unique dataset key matching what we used above
            dataset_key = f"{dataset_info['id']}@{commit_sha}"
        else:
            unique_key = ref
            dataset_key = dataset_info["id"]

        if unique_key not in refs_by_type[ref_type]:
            refs_by_type[ref_type][unique_key] = {
                "label": dataset_info["label"],
                "ref": ref,
                "ref_type": ref_type,
                "commit_sha": dataset_info.get("commit_sha"),
                "is_latest_tag": dataset_info.get("is_latest_tag", False),
                "runners": {},
            }

        # Store the unique dataset key (not the original non-unique ID)
        refs_by_type[ref_type][unique_key]["runners"][runner] = dataset_key

    # Generate HTML
    html = generate_html_template(index, all_datasets, refs_by_type)

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    output_path.write_text(html)
    print(f"Generated interactive report: {output_path}")


def generate_html_template(index: Dict, datasets: Dict, refs_by_type: Dict) -> str:
    """Generate the HTML template."""

    # Embed all data as JSON
    embedded_data = {"index": index, "datasets": datasets, "refs_by_type": refs_by_type}

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Benchmark Comparison - Interactive</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            padding: 20px;
            background-color: #f5f5f5;
        }}

        /* Selection Panel Styles */
        .selection-panel {{
            background-color: white;
            padding: 25px;
            margin-bottom: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}

        .selection-panel h2 {{
            margin: 0 0 15px 0;
            color: #333;
            font-size: 18px;
            font-weight: 600;
        }}

        .selection-row {{
            display: flex;
            align-items: center;
            gap: 15px;
            margin-bottom: 15px;
        }}

        .selection-row label {{
            font-weight: 600;
            min-width: 80px;
            color: #495057;
        }}

        .selection-row select {{
            flex: 1;
            padding: 10px 15px;
            border: 1px solid #ced4da;
            border-radius: 4px;
            font-size: 14px;
            background: white;
            cursor: pointer;
        }}

        .selection-row select:focus {{
            outline: none;
            border-color: #007bff;
            box-shadow: 0 0 0 3px rgba(0,123,255,0.25);
        }}

        .vs-label {{
            font-weight: 700;
            color: #6c757d;
            font-size: 18px;
            padding: 0 10px;
        }}

        .button-group {{
            display: flex;
            gap: 10px;
            margin-top: 15px;
        }}

        button {{
            padding: 10px 20px;
            border: none;
            border-radius: 4px;
            font-size: 14px;
            cursor: pointer;
            font-weight: 500;
        }}

        .btn-primary {{
            background: #007bff;
            color: white;
        }}

        .btn-primary:hover {{
            background: #0056b3;
        }}

        .btn-secondary {{
            background: #6c757d;
            color: white;
        }}

        .btn-secondary:hover {{
            background: #545b62;
        }}

        /* Header Styles */
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

        /* Runner Tabs - More Visible */
        .runner-tabs-wrapper {{
            background-color: white;
            padding: 15px 20px 0 20px;
            margin-bottom: 0;
            border-radius: 8px 8px 0 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}

        .runner-tabs {{
            display: flex;
            gap: 10px;
            border-bottom: 2px solid #e9ecef;
        }}

        .runner-tab {{
            padding: 12px 24px;
            background: #f8f9fa;
            border: 1px solid #dee2e6;
            border-bottom: none;
            border-radius: 6px 6px 0 0;
            cursor: pointer;
            font-size: 14px;
            font-weight: 600;
            color: #495057;
            transition: all 0.2s;
            margin-bottom: -2px;
        }}

        .runner-tab:hover {{
            background: #e9ecef;
        }}

        .runner-tab.active {{
            background: white;
            color: #007bff;
            border-color: #007bff;
            border-bottom-color: white;
        }}

        .runner-content {{
            display: none;
        }}

        .runner-content.active {{
            display: block;
        }}

        /* Chart Container Styles */
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

        .loading {{
            text-align: center;
            padding: 40px;
            color: #6c757d;
        }}

        .error {{
            background: #f8d7da;
            border: 1px solid #f5c6cb;
            color: #721c24;
            padding: 15px;
            border-radius: 4px;
            margin: 20px 0;
        }}

        optgroup {{
            font-weight: 600;
        }}
    </style>
</head>
<body>
    <div class="selection-panel">
        <h2>📊 Select Datasets to Compare</h2>

        <div class="selection-row">
            <label for="baseline-select">Baseline:</label>
            <select id="baseline-select">
                <option value="">Loading...</option>
            </select>
        </div>

        <div class="selection-row">
            <span class="vs-label">vs</span>
        </div>

        <div class="selection-row">
            <label for="target-select">Target:</label>
            <select id="target-select">
                <option value="">Loading...</option>
            </select>
        </div>

        <div class="button-group">
            <button class="btn-primary" onclick="app.loadComparison()">Compare</button>
            <button class="btn-secondary" onclick="app.resetToDefault()">Reset to Default</button>
        </div>
    </div>

    <div id="runner-tabs-container"></div>
    <div id="charts-container"></div>

    <script>
        // Embedded data
        const DATA = {json.dumps(embedded_data, indent=2)};

        // Tool color scheme
        const TOOL_COLORS = {{
            "polars_bio": "#636EFA",
            "pyranges1": "#00CC96",
            "genomicranges": "#FFA15A",
            "bioframe": "#FF6692"
        }};

        // Application state
        const app = {{
            currentBaseline: null,  // ref name
            currentTarget: null,    // ref name
            currentRunner: null,
            availableRunners: [],

            init() {{
                this.populateDropdowns();
                this.setDefaults();
                this.loadComparison();
            }},

            populateDropdowns() {{
                const baselineSelect = document.getElementById('baseline-select');
                const targetSelect = document.getElementById('target-select');

                baselineSelect.innerHTML = '';
                targetSelect.innerHTML = '';

                // Tags
                const tags = Object.values(DATA.refs_by_type.tag);
                if (tags.length > 0) {{
                    const tagGroup = document.createElement('optgroup');
                    tagGroup.label = 'Tags';
                    tags.forEach(ref => {{
                        const option = document.createElement('option');
                        option.value = ref.ref;
                        option.textContent = ref.label + (ref.is_latest_tag ? ' ⭐ Latest' : '');
                        tagGroup.appendChild(option);
                    }});
                    baselineSelect.appendChild(tagGroup.cloneNode(true));
                    targetSelect.appendChild(tagGroup.cloneNode(true));
                }}

                // Branches (each commit gets a separate entry)
                const branches = Object.entries(DATA.refs_by_type.branch).map(([key, data]) => {{
                    return {{ key: key, ...data }};
                }});
                if (branches.length > 0) {{
                    const branchGroup = document.createElement('optgroup');
                    branchGroup.label = 'Branches/Commits';
                    branches.forEach(ref => {{
                        const option = document.createElement('option');
                        option.value = ref.key;  // Use unique key (ref@sha)
                        option.textContent = ref.label;  // Display with commit SHA
                        branchGroup.appendChild(option);
                    }});
                    baselineSelect.appendChild(branchGroup.cloneNode(true));
                    targetSelect.appendChild(branchGroup.cloneNode(true));
                }}
            }},

            setDefaults() {{
                // Find latest tag
                const latestTagEntry = Object.entries(DATA.refs_by_type.tag).find(([key, ref]) => ref.is_latest_tag);
                // Find first branch (most recent commit)
                const firstBranchEntry = Object.entries(DATA.refs_by_type.branch)[0];
                const targetEntry = firstBranchEntry || Object.entries(DATA.refs_by_type.tag)[0];

                if (latestTagEntry) {{
                    const [tagKey, tagData] = latestTagEntry;
                    document.getElementById('baseline-select').value = tagKey;
                    this.currentBaseline = tagKey;
                }}

                if (targetEntry) {{
                    const [targetKey, targetData] = targetEntry;
                    document.getElementById('target-select').value = targetKey;
                    this.currentTarget = targetKey;
                }}
            }},

            resetToDefault() {{
                this.setDefaults();
                this.loadComparison();
            }},

            getRefData(refKey) {{
                // Find ref in tags or branches using unique key
                return DATA.refs_by_type.tag[refKey] || DATA.refs_by_type.branch[refKey];
            }},

            loadComparison() {{
                const baselineRef = document.getElementById('baseline-select').value;
                const targetRef = document.getElementById('target-select').value;

                if (!baselineRef || !targetRef) {{
                    alert('Please select both baseline and target datasets');
                    return;
                }}

                if (baselineRef === targetRef) {{
                    alert('Please select different datasets for comparison');
                    return;
                }}

                this.currentBaseline = baselineRef;
                this.currentTarget = targetRef;

                const baselineRefData = this.getRefData(baselineRef);
                const targetRefData = this.getRefData(targetRef);

                if (!baselineRefData || !targetRefData) {{
                    document.getElementById('charts-container').innerHTML =
                        '<div class="error">Error: Could not load dataset data</div>';
                    return;
                }}

                // Find common runners
                const baselineRunners = Object.keys(baselineRefData.runners);
                const targetRunners = Object.keys(targetRefData.runners);
                const commonRunners = baselineRunners.filter(r => targetRunners.includes(r));

                if (commonRunners.length === 0) {{
                    document.getElementById('charts-container').innerHTML =
                        '<div class="error">Error: No common runners between selected datasets</div>';
                    return;
                }}

                this.availableRunners = commonRunners;

                // Setup runner tabs
                this.setupRunnerTabs();

                // Generate charts for first runner
                this.currentRunner = commonRunners[0];
                this.generateCharts();
            }},

            setupRunnerTabs() {{
                const tabsContainer = document.getElementById('runner-tabs-container');

                if (this.availableRunners.length === 1) {{
                    // Single runner - show simple label
                    const runner = this.availableRunners[0];
                    const baselineRefData = this.getRefData(this.currentBaseline);
                    const datasetId = baselineRefData.runners[runner];
                    const dataset = DATA.datasets[datasetId];

                    tabsContainer.innerHTML = `
                        <div class="runner-tabs-wrapper">
                            <div class="runner-tabs">
                                <div class="runner-tab active">
                                    ${{dataset.runner_label}}
                                </div>
                            </div>
                        </div>
                    `;
                }} else {{
                    // Multiple runners - show clickable tabs
                    const tabs = this.availableRunners.map((runner, idx) => {{
                        const baselineRefData = this.getRefData(this.currentBaseline);
                        const datasetId = baselineRefData.runners[runner];
                        const dataset = DATA.datasets[datasetId];
                        const active = idx === 0 ? 'active' : '';

                        return `<button class="runner-tab ${{active}}" onclick="app.switchRunner('${{runner}}')">
                            ${{dataset.runner_label}}
                        </button>`;
                    }}).join('');

                    tabsContainer.innerHTML = `
                        <div class="runner-tabs-wrapper">
                            <div class="runner-tabs">
                                ${{tabs}}
                            </div>
                        </div>
                    `;
                }}
            }},

            switchRunner(runner) {{
                this.currentRunner = runner;

                // Update active tab
                document.querySelectorAll('.runner-tab').forEach(tab => {{
                    tab.classList.remove('active');
                }});
                event.target.classList.add('active');

                // Regenerate charts
                this.generateCharts();
            }},

            generateCharts() {{
                const container = document.getElementById('charts-container');
                const timestamp = new Date().toISOString().replace('T', ' ').substring(0, 19) + ' UTC';

                // Get datasets for current runner
                const baselineRefData = this.getRefData(this.currentBaseline);
                const targetRefData = this.getRefData(this.currentTarget);

                const baselineDatasetId = baselineRefData.runners[this.currentRunner];
                const targetDatasetId = targetRefData.runners[this.currentRunner];

                const baseline = DATA.datasets[baselineDatasetId];
                const target = DATA.datasets[targetDatasetId];

                // Generate header
                let html = `
                    <div class="header">
                        <h1>Benchmark Comparison</h1>
                        <div class="subtitle">
                            <strong>Baseline:</strong> ${{baseline.label}} &nbsp;|&nbsp;
                            <strong>Target:</strong> ${{target.label}} &nbsp;|&nbsp;
                            <strong>Generated:</strong> ${{timestamp}}
                        </div>
                        <div class="legend">
                            <div class="legend-item">
                                <span class="legend-color" style="background-color: #636EFA;"></span>
                                <span>Baseline [tag: ${{baseline.label}}]</span>
                            </div>
                            <div class="legend-item">
                                <span class="legend-color" style="background-color: #EF553B;"></span>
                                <span>Target [branch: ${{target.label}}]</span>
                            </div>
                        </div>
                    </div>
                `;

                // Get all unique operations
                const operations = new Set([
                    ...Object.keys(baseline.operations),
                    ...Object.keys(target.operations)
                ]);

                if (operations.size === 0) {{
                    container.innerHTML = '<div class="error">No benchmark data available</div>';
                    return;
                }}

                // Generate chart containers
                const sortedOps = Array.from(operations).sort();
                sortedOps.forEach(opName => {{
                    const opTitle = opName.replace('_', ' ').charAt(0).toUpperCase() +
                                   opName.replace('_', ' ').slice(1);

                    html += `
                        <div class="chart-container">
                            <h2>${{opTitle}} Operation - Total Runtime</h2>
                            <div id="chart-${{opName}}-total"></div>
                        </div>
                        <div class="chart-container">
                            <h2>${{opTitle}} Operation - Per Test Case</h2>
                            <div id="chart-${{opName}}-detail"></div>
                        </div>
                    `;
                }});

                container.innerHTML = html;

                // Render all charts
                sortedOps.forEach(opName => {{
                    this.renderTotalChart(opName, baseline, target);
                    this.renderDetailChart(opName, baseline, target);
                }});
            }},

            renderTotalChart(opName, baseline, target) {{
                const baselineOp = baseline.operations[opName];
                const targetOp = target.operations[opName];

                if (!baselineOp || !targetOp) {{
                    return;
                }}

                const tools = new Set([
                    ...Object.keys(baselineOp.tools),
                    ...Object.keys(targetOp.tools)
                ]);

                const traces = [];

                Array.from(tools).sort().forEach((tool, idx) => {{
                    const baselineData = baselineOp.tools[tool] || {{}};
                    const targetData = targetOp.tools[tool] || {{}};

                    const baselineTotal = Object.values(baselineData).reduce((a, b) => a + b, 0);
                    const targetTotal = Object.values(targetData).reduce((a, b) => a + b, 0);
                    const baselineCount = Object.keys(baselineData).length;
                    const targetCount = Object.keys(targetData).length;

                    traces.push({{
                        x: [tool],
                        y: [baselineTotal],
                        text: [baselineTotal.toFixed(1)],
                        textposition: 'outside',
                        name: 'Baseline',
                        type: 'bar',
                        marker: {{ color: '#636EFA' }},
                        showlegend: idx === 0,
                        hovertemplate: `${{tool}}<br>Baseline: %{{y:.2f}} ms (${{baselineCount}} tests)<extra></extra>`
                    }});

                    traces.push({{
                        x: [tool],
                        y: [targetTotal],
                        text: [targetTotal.toFixed(1)],
                        textposition: 'outside',
                        name: 'Target',
                        type: 'bar',
                        marker: {{ color: '#EF553B' }},
                        showlegend: idx === 0,
                        hovertemplate: `${{tool}}<br>Target: %{{y:.2f}} ms (${{targetCount}} tests)<extra></extra>`
                    }});
                }});

                const layout = {{
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

                Plotly.newPlot(`chart-${{opName}}-total`, traces, layout, {{ responsive: true }});
            }},

            renderDetailChart(opName, baseline, target) {{
                const baselineOp = baseline.operations[opName];
                const targetOp = target.operations[opName];

                if (!baselineOp || !targetOp) {{
                    return;
                }}

                const testCases = new Set([
                    ...baselineOp.test_cases,
                    ...targetOp.test_cases
                ]);
                const sortedTestCases = Array.from(testCases).sort();

                const tools = new Set([
                    ...Object.keys(baselineOp.tools),
                    ...Object.keys(targetOp.tools)
                ]);

                const traces = [];

                Array.from(tools).sort().forEach(tool => {{
                    const toolColor = TOOL_COLORS[tool] || '#636EFA';
                    const baselineData = baselineOp.tools[tool] || {{}};
                    const targetData = targetOp.tools[tool] || {{}};

                    const baselineValues = sortedTestCases.map(tc => baselineData[tc] || 0);
                    traces.push({{
                        x: sortedTestCases,
                        y: baselineValues,
                        name: `${{tool}} (baseline)`,
                        type: 'bar',
                        marker: {{ color: toolColor }},
                        legendgroup: tool,
                        hovertemplate: `${{tool}}<br>Test: %{{x}}<br>Baseline: %{{y:.2f}} ms<extra></extra>`
                    }});

                    const targetValues = sortedTestCases.map(tc => targetData[tc] || 0);
                    traces.push({{
                        x: sortedTestCases,
                        y: targetValues,
                        name: `${{tool}} (target)`,
                        type: 'bar',
                        marker: {{
                            color: toolColor,
                            pattern: {{ shape: '/' }}
                        }},
                        legendgroup: tool,
                        hovertemplate: `${{tool}}<br>Test: %{{x}}<br>Target: %{{y:.2f}} ms<extra></extra>`
                    }});
                }});

                const layout = {{
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

                Plotly.newPlot(`chart-${{opName}}-detail`, traces, layout, {{ responsive: true }});
            }}
        }};

        // Initialize app
        document.addEventListener('DOMContentLoaded', () => {{
            app.init();
        }});
    </script>
</body>
</html>
"""

    return html


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Generate interactive benchmark comparison report"
    )
    parser.add_argument(
        "--data-dir", type=Path, required=True, help="Path to benchmark-data directory"
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="Output HTML file path"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    args = parser.parse_args()

    if args.verbose:
        print(f"Loading data from: {args.data_dir}")

    try:
        generate_html_report(args.data_dir, args.output)
        print(f"\n✅ Interactive report generated successfully!")
        if args.verbose:
            print(f"📂 Output: {args.output}")
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback

            traceback.print_exc()
        sys.exit(1)
