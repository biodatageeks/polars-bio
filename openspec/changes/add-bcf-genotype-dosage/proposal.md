# Change: Add typed BCF genotype dosage output

## Why

Converting cohort-scale BCF `GT` values to VCF strings and parsing them again in
Polars is much slower than decoding the native integer series directly.

## What Changes

- Add an explicit `genotype_output="dosage"` option to VCF read/scan APIs.
- Keep string output as the compatibility default.
- Use the upstream streaming typed BCF sink and expose nullable `Int8` dosage.
- Verify VCF-compatible BCF behavior across the eager, lazy, SQL, schema,
  projection, predicate, coordinate, INFO, and FORMAT surfaces.
- Document CSI-backed range pushdown and parallel BCF partition processing.
- Benchmark equivalent one-thread output against snputils, including peak RSS.

## Impact

- Affected specs: `vcf`
- Affected code: `polars_bio/io.py`, `polars_bio/sql.py`, `src/lib.rs`,
  `src/option.rs`, `src/scan.rs`, VCF/BCF tests and reading documentation
