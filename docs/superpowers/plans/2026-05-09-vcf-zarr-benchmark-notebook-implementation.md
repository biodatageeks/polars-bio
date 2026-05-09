# VCF Zarr Benchmark Notebook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a self-contained notebook that benchmarks large-scale VCF Zarr reads and analytical queries across polars-bio, sgkit, and zarr-python.

**Architecture:** Implement a notebook-only benchmark harness in `notebooks/vcf_zarr_benchmark.ipynb`. The notebook owns dataset profile metadata, gated data preparation, tool adapters, benchmark timing, validation, summary tables, and plots. It treats sgkit and zarr-python as optional adapters and records skipped scenarios instead of aborting when a tool cannot support a scenario.

**Tech Stack:** Jupyter notebook JSON, Python 3, polars-bio, Polars, pandas, optional sgkit/xarray/Dask, optional zarr-python, optional matplotlib/seaborn, optional bio2zarr/vcf2zarr CLI.

---

## File Structure

- Create `notebooks/vcf_zarr_benchmark.ipynb`: the complete executable benchmark notebook.
- Modify no library code in `polars_bio/` or `src/`.
- Use existing fixture `tests/data/io/vcf_zarr/multi_chrom.vcz` for smoke execution.

## Task 1: Notebook Skeleton And Configuration

**Files:**
- Create: `notebooks/vcf_zarr_benchmark.ipynb`

- [x] **Step 1: Create the notebook with title, usage, and configuration cells**

  Generate a notebook with markdown explaining:

  - benchmark purpose,
  - required/optional dependencies,
  - environment variables:
    - `VCZ_BENCH_PROFILE`
    - `VCZ_BENCH_DATA_DIR`
    - `VCZ_BENCH_ENABLE_DOWNLOAD`
    - `VCZ_BENCH_ENABLE_CONVERT`
    - `VCZ_BENCH_FORCE_REBUILD`
    - `VCZ_BENCH_TARGET_PARTITIONS`
    - `VCZ_BENCH_REPETITIONS`
    - `VCZ_BENCH_WARMUPS`
    - `VCZ_BENCH_SAMPLE_COUNT`
    - `VCZ_BENCH_SAMPLES`
    - `VCZ_BENCH_CUSTOM_VCF`
    - `VCZ_BENCH_CUSTOM_VCZ`
    - `VCZ_BENCH_REGION`

  Add code defining:

  ```python
  from __future__ import annotations

  import importlib
  import os
  import platform
  import shutil
  import subprocess
  import sys
  import time
  from dataclasses import dataclass
  from pathlib import Path
  from typing import Any, Callable

  import pandas as pd
  import polars as pl
  import polars_bio as pb
  ```

- [x] **Step 2: Add environment parsing helpers**

  Include helpers:

  ```python
  def env_flag(name: str, default: bool = False) -> bool:
      value = os.environ.get(name)
      if value is None:
          return default
      return value.strip().lower() in {"1", "true", "yes", "on"}


  def env_int(name: str, default: int) -> int:
      value = os.environ.get(name)
      return default if value in (None, "") else int(value)


  def env_csv(name: str, default: str) -> list[str]:
      value = os.environ.get(name, default)
      return [part.strip() for part in value.split(",") if part.strip()]
  ```

- [x] **Step 3: Verify notebook imports**

  Run:

  ```bash
  .venv/bin/python - <<'PY'
  import pandas, polars, polars_bio
  print("imports ok")
  PY
  ```

  Expected: prints `imports ok`.

## Task 2: Dataset Profiles And Gated Data Prep

**Files:**
- Modify: `notebooks/vcf_zarr_benchmark.ipynb`

- [x] **Step 1: Add dataset profile metadata**

  Add `DATASET_PROFILES` with at least these profiles:

  ```python
  FTP_BASE = "https://ftp.1000genomes.ebi.ac.uk/vol1/ftp"

  DATASET_PROFILES = {
      "phase3_chr22": {
          "description": "1000 Genomes Phase 3 chr22 genotype VCF",
          "vcf_urls": [
              f"{FTP_BASE}/release/20130502/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz",
          ],
          "index_urls": [
              f"{FTP_BASE}/release/20130502/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz.tbi",
          ],
          "default_region": "22:20000000-25000000",
          "notes": "Small fallback profile.",
      },
      "30x_chr22": {
          "description": "1000 Genomes 30x high-coverage chr22 phased panel",
          "vcf_urls": [
              f"{FTP_BASE}/data_collections/1000G_2504_high_coverage/working/20220422_3202_phased_SNV_INDEL_SV/1kGP_high_coverage_Illumina.chr22.filtered.SNV_INDEL_SV_phased_panel.vcf.gz",
          ],
          "index_urls": [
              f"{FTP_BASE}/data_collections/1000G_2504_high_coverage/working/20220422_3202_phased_SNV_INDEL_SV/1kGP_high_coverage_Illumina.chr22.filtered.SNV_INDEL_SV_phased_panel.vcf.gz.tbi",
          ],
          "default_region": "chr22:20000000-25000000",
          "notes": "Recommended default profile.",
      },
      "phase3_wgs_sites": {
          "description": "1000 Genomes Phase 3 WGS sites-only VCF",
          "vcf_urls": [
              f"{FTP_BASE}/release/20130502/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5c.20130502.sites.vcf.gz",
          ],
          "index_urls": [
              f"{FTP_BASE}/release/20130502/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5c.20130502.sites.vcf.gz.tbi",
          ],
          "default_region": "22:20000000-25000000",
          "notes": "Large variant-only profile.",
      },
  }
  ```

  Then generate `phase3_wgs_autosomes` and `30x_wgs_autosomes` from chr1-22 URL templates, and add `legacy_wes_exome` plus `custom`.

- [x] **Step 2: Add profile resolution**

  Build `CONFIG` with resolved paths:

  ```python
  PROFILE_NAME = os.environ.get("VCZ_BENCH_PROFILE", "30x_chr22")
  DATA_DIR = Path(os.environ.get("VCZ_BENCH_DATA_DIR", Path.cwd() / "tmp" / "vcz-bench-data")).expanduser()
  PROFILE = DATASET_PROFILES[PROFILE_NAME]
  PROFILE_DIR = DATA_DIR / PROFILE_NAME
  VCZ_PATH = Path(os.environ["VCZ_BENCH_CUSTOM_VCZ"]).expanduser() if os.environ.get("VCZ_BENCH_CUSTOM_VCZ") else PROFILE_DIR / f"{PROFILE_NAME}.vcz"
  ```

- [x] **Step 3: Add gated download and conversion functions**

  Include functions:

  ```python
  def download_file(url: str, output: Path, force: bool = False) -> None:
      if output.exists() and not force:
          print(f"reusing {output}")
          return
      output.parent.mkdir(parents=True, exist_ok=True)
      subprocess.run(["curl", "-L", "--fail", "--continue-at", "-", "--output", str(output), url], check=True)


  def run_vcf2zarr(vcf_paths: list[Path], output_vcz: Path, force: bool = False) -> None:
      if output_vcz.exists() and not force:
          print(f"reusing {output_vcz}")
          return
      exe = shutil.which("vcf2zarr")
      cmd_prefix = [exe] if exe else [sys.executable, "-m", "bio2zarr", "vcf2zarr"]
      icf_path = output_vcz.with_suffix(".icf")
      if force:
          shutil.rmtree(output_vcz, ignore_errors=True)
          shutil.rmtree(icf_path, ignore_errors=True)
      subprocess.run([*cmd_prefix, "explode", *map(str, vcf_paths), str(icf_path)], check=True)
      subprocess.run([*cmd_prefix, "encode", str(icf_path), str(output_vcz)], check=True)
  ```

- [x] **Step 4: Add gate enforcement cells**

  The cell must print remediation instead of silently downloading:

  ```python
  if missing_vcf_paths and not ENABLE_DOWNLOAD:
      raise RuntimeError("Missing VCF inputs. Set VCZ_BENCH_ENABLE_DOWNLOAD=1 or provide VCZ_BENCH_CUSTOM_VCF.")

  if not VCZ_PATH.exists() and not ENABLE_CONVERT:
      raise RuntimeError("Missing VCZ output. Set VCZ_BENCH_ENABLE_CONVERT=1 or provide VCZ_BENCH_CUSTOM_VCZ.")
  ```

## Task 3: Metadata And Optional Tool Adapters

**Files:**
- Modify: `notebooks/vcf_zarr_benchmark.ipynb`

- [x] **Step 1: Add optional import helpers**

  Include:

  ```python
  def optional_import(module_name: str):
      try:
          return importlib.import_module(module_name)
      except Exception as exc:
          print(f"optional dependency {module_name!r} unavailable: {exc}")
          return None
  ```

- [x] **Step 2: Add VCF Zarr metadata inspection**

  Use zarr-python if available:

  ```python
  zarr = optional_import("zarr")

  def open_zarr_group(path: Path):
      if zarr is None:
          raise RuntimeError("zarr-python is not installed")
      return zarr.open_group(str(path), mode="r")
  ```

  Add helpers to read arrays, decode bytes, infer variant/sample counts, choose INFO/FORMAT fields, and choose a default region.

- [x] **Step 3: Add sgkit opener**

  Try `sgkit.load_dataset`, then `xarray.open_zarr`, and raise a `SkipBenchmark` if neither works.

## Task 4: Benchmark Harness And Scenarios

**Files:**
- Modify: `notebooks/vcf_zarr_benchmark.ipynb`

- [x] **Step 1: Add result dataclass and skip exception**

  ```python
  class SkipBenchmark(Exception):
      pass


  @dataclass
  class ResultRow:
      tool: str
      scenario: str
      profile: str
      partition: str
      repetition: int
      status: str
      elapsed_s: float | None
      validation: str
      error: str
      notes: str
  ```

- [x] **Step 2: Add timing helper**

  ```python
  def measure(tool: str, scenario: str, partition: str, fn: Callable[[], Any], validate: Callable[[Any], str], notes: str = "") -> list[ResultRow]:
      rows = []
      try:
          for _ in range(WARMUPS):
              validate(fn())
          for rep in range(REPETITIONS):
              started = time.perf_counter()
              output = fn()
              elapsed = time.perf_counter() - started
              rows.append(ResultRow(tool, scenario, PROFILE_NAME, partition, rep, "ok", elapsed, validate(output), "", notes))
      except SkipBenchmark as exc:
          rows.append(ResultRow(tool, scenario, PROFILE_NAME, partition, -1, "skipped", None, "", str(exc), notes))
      except Exception as exc:
          rows.append(ResultRow(tool, scenario, PROFILE_NAME, partition, -1, "failed", None, "", repr(exc), notes))
      return rows
  ```

- [x] **Step 3: Add polars-bio scenarios**

  Implement functions for:

  - open metadata,
  - full core scan,
  - `chrom,start` projection,
  - INFO projection,
  - range filter,
  - variant lookup,
  - sample FORMAT query.

- [x] **Step 4: Add zarr-python scenarios**

  Implement raw-array equivalents for the same scenarios where arrays exist. Raise `SkipBenchmark` for missing arrays.

- [x] **Step 5: Add sgkit scenarios**

  Implement xarray/sgkit equivalents where variables exist. Raise `SkipBenchmark` for unsupported variables or incompatible datasets.

## Task 5: Tables, Plots, And Smoke Execution

**Files:**
- Modify: `notebooks/vcf_zarr_benchmark.ipynb`

- [x] **Step 1: Add result table creation**

  Build:

  ```python
  raw_results_pd = pd.DataFrame([row.__dict__ for row in rows])
  raw_results_pl = pl.from_pandas(raw_results_pd)
  summary_pd = (
      raw_results_pd[raw_results_pd["status"] == "ok"]
      .groupby(["tool", "scenario", "partition"], dropna=False)
      .agg(
          median_s=("elapsed_s", "median"),
          min_s=("elapsed_s", "min"),
          max_s=("elapsed_s", "max"),
          runs=("elapsed_s", "count"),
      )
      .reset_index()
  )
  ```

- [x] **Step 2: Add plots**

  Use matplotlib/seaborn if available:

  - median runtime by scenario/tool,
  - polars-bio speedup by `target_partitions`.

  If plotting dependencies are unavailable, print a message and keep the tables.

- [x] **Step 3: Smoke execute notebook against repo fixture**

  Run:

  ```bash
  VCZ_BENCH_PROFILE=custom \
  VCZ_BENCH_CUSTOM_VCZ=tests/data/io/vcf_zarr/multi_chrom.vcz \
  VCZ_BENCH_TARGET_PARTITIONS=1 \
  VCZ_BENCH_REPETITIONS=1 \
  VCZ_BENCH_WARMUPS=0 \
  .venv/bin/jupyter nbconvert --to notebook --execute notebooks/vcf_zarr_benchmark.ipynb --output /tmp/vcf_zarr_benchmark.executed.ipynb
  ```

  Expected: notebook executes; sgkit/zarr scenarios may be skipped if optional dependencies are missing, but polars-bio scenarios run.

- [x] **Step 4: Validate notebook JSON and diff**

  Run:

  ```bash
  .venv/bin/python -m json.tool notebooks/vcf_zarr_benchmark.ipynb >/tmp/vcf_zarr_benchmark.json
  git diff --check -- notebooks/vcf_zarr_benchmark.ipynb
  ```

  Expected: both commands exit 0.

- [x] **Step 5: Commit**

  ```bash
  git add notebooks/vcf_zarr_benchmark.ipynb
  git commit -m "Add VCF Zarr benchmark notebook"
  ```
