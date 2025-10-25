# polars-bio Performance Benchmarking

This directory contains tools and documentation for performance benchmarking of polars-bio using the [polars-bio-bench](https://github.com/biodatageeks/polars-bio-bench) framework.

## Overview

The benchmarking system compares polars-bio performance against competing libraries (pyranges1, genomicranges, bioframe) across key genomic operations:

- **overlap**: Find overlapping genomic intervals
- **nearest**: Find nearest genomic intervals
- **count_overlaps**: Count overlaps between interval sets

## GitHub Actions Workflow

### Triggering Benchmarks

Benchmarks run via **manual workflow dispatch only**. They are not triggered automatically on PRs or commits to conserve resources and give developers control.

#### Via GitHub UI

1. Go to the **Actions** tab in the repository
2. Select **Performance Benchmarks** workflow
3. Click **Run workflow**
4. Configure parameters (optional):
   - **Alert threshold**: Performance degradation threshold (default: 150%)
   - **Baseline tag**: Git tag to compare against (default: latest tag)
   - **Target branch**: Branch to benchmark (default: current branch)
5. Click **Run workflow**

#### Via GitHub CLI

```bash
# Run with default parameters (150% threshold, latest tag as baseline)
gh workflow run benchmark.yml

# Run with custom threshold
gh workflow run benchmark.yml -f alert_threshold=120

# Run with specific baseline tag
gh workflow run benchmark.yml -f baseline_tag=0.17.0

# Run on specific branch
gh workflow run benchmark.yml -f target_branch=feature/my-optimization

# Combine multiple parameters
gh workflow run benchmark.yml \
  -f alert_threshold=150 \
  -f baseline_tag=0.18.0 \
  -f target_branch=main
```

### Workflow Parameters

| Parameter | Description | Default | Valid Range |
|-----------|-------------|---------|-------------|
| `alert_threshold` | Performance degradation threshold (%) | 150 | 100-1000 |
| `baseline_tag` | Git tag to use as baseline | Latest tag | Any valid git tag |
| `target_branch` | Branch/commit to benchmark | Current branch | Any valid git ref |

### How It Works

1. **Baseline Execution**
   - Identifies latest git tag (or uses specified tag)
   - Checks out baseline code at that tag
   - Installs baseline version of polars-bio
   - Runs `benchmark_single_thread-4tools-pull-request.yaml` configuration
   - Saves baseline results

2. **Target Execution**
   - Checks out target branch/commit
   - Installs target version of polars-bio
   - Runs same benchmark configuration
   - Saves target results

3. **Comparison & Analysis**
   - Parses both CSV result files
   - **Calculates per-operation averages** across test cases
   - Compares averages against threshold
   - Generates comparison report
   - Stores results in gh-pages branch at `/dev/bench/`

4. **Reporting**
   - Posts comparison table as workflow summary
   - Posts PR comment (if triggered from PR)
   - Uploads results as workflow artifacts
   - Updates benchmark visualization on GitHub Pages

### Per-Operation Averaging

The benchmark system uses **per-operation averaging** to provide stable, meaningful comparisons:

- Each operation (overlap, nearest, count_overlaps) runs multiple test cases
- Individual test case times can have high variance
- We calculate the **average execution time per operation** across all test cases
- Threshold comparison uses these **operation averages**, not individual test cases

**Example:**

```
overlap operation with 6 test cases:
  test_case_1: 10ms
  test_case_2: 12ms
  test_case_3: 11ms
  test_case_4: 13ms
  test_case_5: 9ms
  test_case_6: 11ms

  Average: 11ms

If baseline average was 7ms:
  Ratio = 11 / 7 = 1.57 (157%)

If threshold is 150%:
  157% > 150% → REGRESSION ALERT
```

### Baseline Comparison

The system compares against **tagged releases** rather than commit-to-commit:

**Why tagged releases?**
- Tagged releases represent stable, validated baselines
- More meaningful than comparing against volatile main branch
- Aligns with how users experience performance (via releases)
- Reduces false positives from intermediate development work

**Baseline selection:**
1. Default: Uses latest git tag (e.g., `0.18.0`)
2. Override: Specify `baseline_tag` parameter for custom baseline
3. Fallback: Errors if no tags exist (requires at least one release tag)

### Alert Threshold

The **default threshold is 150%** (1.5x degradation triggers alert).

**Why 150%?**
- Catches moderate to severe regressions
- Accounts for expected variance on GitHub Actions runners
- Per-operation averaging reduces noise vs individual test cases
- Consistent Linux environment (ubuntu-latest) reduces variance
- Can be adjusted per run based on change type

**Adjusting the threshold:**

```bash
# More sensitive (120% = 1.2x degradation)
gh workflow run benchmark.yml -f alert_threshold=120

# Less sensitive (200% = 2x degradation)
gh workflow run benchmark.yml -f alert_threshold=200
```

**When to adjust:**
- Performance-critical changes: Use lower threshold (120-130%)
- Experimental features: Use higher threshold (180-200%)
- Major refactoring: Consider higher threshold initially

## Runner Environment

Benchmarks run exclusively on **ubuntu-latest** Linux runners:

**Why Linux only?**
- Consistent environment reduces performance variance
- Most polars-bio users deploy on Linux servers
- Simplifies infrastructure (no cross-platform complexity)
- Predictable CPU, memory, and I/O characteristics

**Runner specifications:**
- OS: Ubuntu (latest LTS)
- CPU: 2-core x86_64
- RAM: 7 GB
- Disk: SSD

**Expected variance:** ±5-10% due to shared runner infrastructure

## Benchmark Configuration

The workflow uses `conf/benchmark_single_thread-4tools-pull-request.yaml` from polars-bio-bench:

- **Operations**: overlap, nearest, count_overlaps
- **Tools**: polars_bio, pyranges1, genomicranges, bioframe
- **Dataset**: databio (representative genomic intervals)
- **Repetitions**: 3x per test case
- **Parallelism**: Disabled (single-thread for reproducibility)

**Estimated runtime:** 20-25 minutes (including baseline)

## Interpreting Results

### Comparison Report

The workflow generates a markdown comparison table:

```markdown
## Benchmark Comparison: feature-branch vs 0.18.0

**Summary:** 1 regressions, 2 improvements, 0 stable

## ⚠️ Performance Regressions Detected

| Operation | Baseline (ms) | PR (ms) | Change | Status |
|-----------|---------------|---------|--------|--------|
| overlap   | 45.20         | 75.30   | +66.6% | ❌ Exceeds 150% threshold |

## All Operations

| Operation       | Baseline (ms) | PR (ms) | Change  | Status       |
|-----------------|---------------|---------|---------|--------------|
| overlap         | 45.20         | 75.30   | +66.6%  | ❌ regression |
| nearest         | 32.10         | 28.50   | -11.2%  | ✅ improvement |
| count_overlaps  | 18.40         | 15.20   | -17.4%  | ✅ improvement |
```

### Status Icons

- ❌ **regression**: Exceeds alert threshold
- ✅ **improvement**: >5% faster than baseline
- ✓ **stable**: Within acceptable range
- 🆕 **new**: Operation not in baseline
- 🗑️ **removed**: Operation not in target

### Viewing Historical Results

Benchmark results are stored on GitHub Pages at:

https://biodatageeks.org/polars-bio/dev/bench/

This page shows:
- Time-series charts for each operation
- Performance trends over time
- Comparison across different tools
- Interactive filtering and zoom

## Local Testing

You can run the benchmark parser locally to test changes:

```bash
# Parse and compare results
python benchmarks/parse_benchmark_results.py \
  baseline_results.csv \
  pr_results.csv \
  --threshold 150 \
  --baseline-tag v0.18.0 \
  --pr-ref my-feature

# Output options
python benchmarks/parse_benchmark_results.py \
  baseline_results.csv \
  pr_results.csv \
  --output-json results.json \
  --output-comparison comparison.json \
  --output-report report.md \
  --fail-on-regression  # Exit 1 if regressions found
```

## Troubleshooting

### No benchmarks running

**Symptom:** Workflow completes but no benchmark results

**Solutions:**
1. Check that polars-bio-bench repository was cloned successfully
2. Verify benchmark configuration file exists
3. Check workflow logs for errors in benchmark execution
4. Ensure baseline tag can be built and installed

### High variance between runs

**Symptom:** Same code shows different performance on different runs

**Solutions:**
1. GitHub Actions runners have shared infrastructure, expect ±5-10% variance
2. Consider increasing alert threshold (e.g., 180-200%)
3. Per-operation averaging already reduces variance; if still high, check for I/O-bound operations
4. Run multiple benchmark executions and compare trends

### Baseline tag not found

**Symptom:** Error: "Tag 'X' does not exist"

**Solutions:**
1. Ensure you've specified a valid git tag name
2. Use `git tag --sort=-creatordate` to list available tags
3. If no tags exist, create one: `git tag v0.1.0 && git push origin v0.1.0`

### Parser errors

**Symptom:** Error parsing CSV benchmark output

**Solutions:**
1. Check that polars-bio-bench produces valid CSV output
2. Verify CSV has required columns: `operation`, `tool`, `test_case`, timing column
3. Check for empty or malformed CSV files
4. Update parser if polars-bio-bench CSV format has changed

### Workflow fails on baseline installation

**Symptom:** Cannot install baseline version from tag

**Solutions:**
1. Ensure tagged version can be built (old tags may have dependency issues)
2. Consider using a more recent tag as baseline
3. Check that baseline tag has all required files
4. Verify Rust compilation succeeds for that version

## GitHub Pages Structure

The gh-pages branch has the following structure:

```
gh-pages/
├── index.html              # Documentation site (managed by mkdocs)
├── api/                    # API documentation
├── blog/                   # Blog posts
├── dev/
│   └── bench/              # Benchmark results (managed by workflow)
│       ├── data.js         # Benchmark history
│       ├── index.html      # Benchmark visualization
│       └── ...             # Other benchmark files
└── ...                     # Other documentation files
```

**Important:** The benchmark workflow only modifies `/dev/bench/` directory. Documentation remains unchanged.

## FAQ

### Q: Why manual trigger only?

**A:** Benchmarks take 20-25 minutes to run. Manual triggering:
- Gives developers control over when to benchmark
- Reduces GitHub Actions runner usage
- Prevents blocking PRs on benchmark completion
- Allows selective benchmarking of performance-sensitive changes

### Q: Why compare against tags instead of main?

**A:** Tags represent stable release baselines:
- Main branch changes frequently, causing noisy comparisons
- Users care about performance relative to releases, not intermediate commits
- Tagged baselines provide stable reference points
- Reduces false positives from development churn

### Q: Why 150% threshold?

**A:**
- Catches moderate regressions (1.5x slowdown)
- Accounts for runner variance (±5-10%)
- Per-operation averaging smooths outliers
- Lower than previous 200% to catch more issues
- Can be adjusted per run based on needs

### Q: Can I run benchmarks on multiple platforms?

**A:** No, the workflow runs Linux only by design:
- Reduces complexity and cost
- Most deployments are Linux
- Cross-platform benchmarking adds variance without proportional value
- Use local benchmarking for platform-specific testing

### Q: How do I benchmark unreleased code?

**A:** Use the `target_branch` parameter:

```bash
# Benchmark feature branch against latest release
gh workflow run benchmark.yml -f target_branch=feature/my-optimization

# Benchmark specific commit
gh workflow run benchmark.yml -f target_branch=abc1234
```

### Q: What if there are no git tags?

**A:** The workflow requires at least one tag. Create one:

```bash
git tag v0.1.0
git push origin v0.1.0
```

### Q: Can I cache baseline results?

**A:** Currently no, but this is planned. Each run executes both baseline and target benchmarks to ensure consistent environment.

## Contributing

To improve the benchmarking system:

1. **Parser improvements**: Edit `benchmarks/parse_benchmark_results.py`
2. **Workflow changes**: Edit `.github/workflows/benchmark.yml`
3. **Documentation updates**: Edit this file
4. **Test changes locally** before committing
5. **Run manual benchmark** to verify changes work end-to-end

## References

- [polars-bio-bench repository](https://github.com/biodatageeks/polars-bio-bench)
- [github-action-benchmark](https://github.com/benchmark-action/github-action-benchmark)
- [GitHub Actions documentation](https://docs.github.com/en/actions)
- [Benchmark results visualization](https://biodatageeks.org/polars-bio/dev/bench/)
