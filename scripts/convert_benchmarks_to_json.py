#!/usr/bin/env python3
"""
Convert benchmark CSV results to JSON format required by github-action-benchmark.

This script reads CSV benchmark results from the benchmarks/results/
directory and converts them to JSON format for github-action-benchmark.
"""

import json
import csv
import sys
from pathlib import Path
from typing import Dict, List, Any
import statistics


def read_csv_results(csv_file: Path) -> List[Dict[str, Any]]:
    """Read benchmark results from CSV file"""
    results = []
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                results.append(row)
    except Exception as e:
        print(f"Error reading {csv_file}: {e}", file=sys.stderr)
        
    return results


def calculate_speedup(polars_bio_time: float, baseline_time: float) -> float:
    """Calculate speedup ratio (higher is better)"""
    if polars_bio_time <= 0:
        return 0.0
    return baseline_time / polars_bio_time


def process_benchmark_file(csv_file: Path,
                           test_name: str) -> List[Dict[str, Any]]:
    """Process a single benchmark CSV file and extract metrics"""
    results = read_csv_results(csv_file)
    
    if not results:
        return []
    
    # Group results by library and test configuration
    grouped_results = {}

    for row in results:
        try:
            library = row.get('library', '')
            test_type = row.get('test_type', '')
            proj_pushdown = row.get('projection_pushdown', 'False')
            pred_pushdown = row.get('predicate_pushdown', 'False')
            total_time = float(row.get('total_time', 0))
            threads = row.get('threads', '1')
            
            key = f"{library}_{test_type}_{proj_pushdown}_{pred_pushdown}_{threads}"

            if key not in grouped_results:
                grouped_results[key] = {
                    'library': library,
                    'test_type': test_type,
                    'projection_pushdown': proj_pushdown,
                    'predicate_pushdown': pred_pushdown,
                    'threads': threads,
                    'times': []
                }

            grouped_results[key]['times'].append(total_time)

        except (ValueError, KeyError) as e:
            print(f"Error processing row in {csv_file}: {e}", file=sys.stderr)
            continue

    # Calculate aggregated metrics
    benchmark_data = []

    pandas_baseline = None

    # First pass: find baselines
    for key, data in grouped_results.items():
        if data['library'] == 'pandas' and data['test_type'] == 'read_only':
            avg_time = statistics.mean(data['times'])
            pandas_baseline = avg_time

    # Second pass: create benchmark entries
    for key, data in grouped_results.items():
        if not data['times']:
            continue

        avg_time = statistics.mean(data['times'])
        min_time = min(data['times'])
        max_time = max(data['times'])
        std_dev = statistics.stdev(data['times']) if len(data['times']) > 1 else 0

        # Create test name
        test_config = f"{data['library']}_{data['test_type']}"
        if data['projection_pushdown'] == 'True':
            test_config += "_proj"
        if data['predicate_pushdown'] == 'True':
            test_config += "_pred"
        if data['threads'] != '1':
            test_config += f"_t{data['threads']}"

        full_test_name = f"{test_name}_{test_config}"

        # Calculate throughput (operations per second) - higher is better
        throughput = 1.0 / avg_time if avg_time > 0 else 0

        benchmark_entry = {
            "name": full_test_name,
            "unit": "ops/sec",
            "value": throughput,
            "extra": {
                "avg_time_seconds": avg_time,
                "min_time_seconds": min_time,
                "max_time_seconds": max_time,
                "std_dev_seconds": std_dev,
                "library": data['library'],
                "test_type": data['test_type'],
                "optimization_config": {
                    "projection_pushdown": data['projection_pushdown'],
                    "predicate_pushdown": data['predicate_pushdown'],
                    "threads": data['threads']
                }
            }
        }

        # Add speedup information if baselines are available
        if data['library'] == 'polars-bio' and pandas_baseline:
            speedup = calculate_speedup(avg_time, pandas_baseline)
            benchmark_entry["extra"]["speedup_vs_pandas"] = speedup

        benchmark_data.append(benchmark_entry)

    return benchmark_data


def main():
    """Main function to convert all benchmark CSVs to JSON format"""
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    benchmarks_dir = project_root / "benchmarks"
    results_dir = benchmarks_dir / "results"

    if not results_dir.exists():
        print(f"Results directory not found: {results_dir}", file=sys.stderr)
        sys.exit(1)

    # Find all CSV files in results directory
    csv_files = list(results_dir.glob("*.csv"))

    if not csv_files:
        print("No CSV benchmark files found", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(csv_files)} benchmark CSV files")

    all_benchmarks = []

    # Process each CSV file
    for csv_file in csv_files:
        test_name = csv_file.stem  # filename without extension
        print(f"Processing {test_name}...")

        benchmark_data = process_benchmark_file(csv_file, test_name)
        all_benchmarks.extend(benchmark_data)
        print(f"  -> Extracted {len(benchmark_data)} benchmark entries")

    # Create output JSON for github-action-benchmark
    output_data = all_benchmarks

    # Write JSON output
    output_file = benchmarks_dir / "benchmark_results.json"

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2)

        count = len(all_benchmarks)
        print(f"\n✅ Successfully converted {count} benchmarks to {output_file}")
        print(f"JSON file size: {output_file.stat().st_size} bytes")

        # Print summary
        libraries = set()
        test_types = set()
        for bench in all_benchmarks:
            libraries.add(bench["extra"]["library"])
            test_types.add(bench["extra"]["test_type"])

        print("\nSummary:")
        print(f"  Libraries: {', '.join(sorted(libraries))}")
        print(f"  Test types: {', '.join(sorted(test_types))}")

    except Exception as e:
        print(f"Error writing output file: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
