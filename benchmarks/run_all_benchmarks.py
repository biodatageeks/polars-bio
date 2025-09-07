#!/usr/bin/env python3
"""
Master script to run all benchmark tests and generate visualizations.
"""

import os
import subprocess
import sys
import time
from pathlib import Path


def run_benchmark(script_name: str, description: str):
    """Run a benchmark script and report results"""
    print(f"\n{'='*60}")
    print(f"Running {description}")
    print(f"Script: {script_name}")
    print(f"{'='*60}")

    start_time = time.time()
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            cwd=Path(__file__).parent,
            capture_output=False,  # Let output stream through
            check=True,
        )
        elapsed = time.time() - start_time
        print(f"✅ {description} completed in {elapsed:.1f}s")
        return True
    except subprocess.CalledProcessError as e:
        elapsed = time.time() - start_time
        print(f"❌ {description} failed after {elapsed:.1f}s")
        print(f"Error: {e}")
        return False
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ {description} failed after {elapsed:.1f}s")
        print(f"Unexpected error: {e}")
        return False


def check_requirements():
    """Check if GFF file exists"""
    gff_file = "/tmp/gencode.v49.annotation.gff3.bgz"
    if not Path(gff_file).exists():
        print(f"❌ Required GFF file not found: {gff_file}")
        print("Please download the GENCODE GFF file first:")
        print(
            "wget -O /tmp/gencode.v49.annotation.gff3.bgz https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_49/gencode.v49.annotation.gff3.gz"
        )
        return False

    file_size = Path(gff_file).stat().st_size / (1024 * 1024)  # MB
    print(f"✅ GFF file found: {gff_file} ({file_size:.1f} MB)")
    return True


def main():
    """Run all benchmarks in sequence"""
    print("🚀 Starting polars-bio benchmark suite...")

    # Check requirements
    if not check_requirements():
        sys.exit(1)

    # Create results directory
    Path("results").mkdir(exist_ok=True)

    # Define benchmarks to run
    benchmarks = [
        ("01_general_performance.py", "General Performance Comparison"),
        ("02_memory_profiling.py", "Memory Usage Profiling"),
        ("03_thread_scalability.py", "Thread Scalability Testing"),
        ("04_projection_pruning.py", "Projection Pruning Performance"),
        ("05_predicate_pushdown.py", "Predicate Pushdown Performance"),
        ("06_combined_optimizations.py", "Combined Optimizations Testing"),
    ]

    # Track results
    results = {}
    total_start_time = time.time()

    # Run each benchmark
    for script, description in benchmarks:
        success = run_benchmark(script, description)
        results[description] = success

        if not success:
            print(f"⚠️  {description} failed, but continuing with other benchmarks...")

        # Small delay between benchmarks
        time.sleep(2)

    # Generate visualizations
    print(f"\n{'='*60}")
    print("Generating visualizations...")
    print(f"{'='*60}")

    viz_success = run_benchmark("visualize_results.py", "Visualization Generation")
    results["Visualization Generation"] = viz_success

    # Summary
    total_elapsed = time.time() - total_start_time
    print(f"\n{'='*60}")
    print("🏁 BENCHMARK SUITE COMPLETE")
    print(f"{'='*60}")
    print(f"Total runtime: {total_elapsed/60:.1f} minutes")
    print("\\nResults Summary:")

    successful = 0
    for description, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"  {description}: {status}")
        if success:
            successful += 1

    print(f"\\n{successful}/{len(results)} benchmarks completed successfully")

    # Show output files
    if Path("results").exists():
        result_files = list(Path("results").glob("*"))
        if result_files:
            print(f"\\nGenerated files ({len(result_files)}):")
            for file_path in sorted(result_files):
                size_kb = file_path.stat().st_size / 1024
                print(f"  📄 {file_path.name} ({size_kb:.1f} KB)")

    if successful == len(results):
        print("\\n🎉 All benchmarks completed successfully!")
        print("Check the results/ directory for CSV files and visualizations.")
    else:
        print(f"\\n⚠️  {len(results) - successful} benchmarks failed.")
        print("Check the console output above for error details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
