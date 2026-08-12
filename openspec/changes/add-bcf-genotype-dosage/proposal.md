# Change: Add typed BCF genotype dosage output

## Why

Converting cohort-scale BCF `GT` values to VCF strings and parsing them again in
Polars is much slower than decoding the native integer series directly.

## What Changes

- Add an explicit `genotype_output="dosage"` option to VCF read/scan APIs.
- Keep string output as the compatibility default.
- Use the upstream streaming typed BCF sink and expose nullable `Int8` dosage.
- Benchmark equivalent one-thread output against snputils, including peak RSS.

## Impact

- Affected specs: `vcf`
- Affected code: `polars_bio/io.py`, `src/option.rs`, `src/scan.rs`, VCF tests
