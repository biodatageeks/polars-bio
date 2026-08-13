# Change: Add typed BCF genotype dosage output

## Why

Converting cohort-scale BCF `GT` values to VCF strings and parsing them again in
Polars is much slower than decoding the native integer series directly.

## What Changes

- Add dedicated `read_bcf`, `scan_bcf`, `describe_bcf`, and `register_bcf` APIs;
  expose an explicit `genotype_output="dosage"` option where GT data is read.
- Keep string output as the BCF compatibility default.
- **BREAKING** Restrict `read_vcf`, `scan_vcf`, `describe_vcf`, and
  `register_vcf` to text VCF; remove `genotype_output` and deprecated
  `genotype_encoding_raw` from the VCF read/scan methods.
- Do not expose `genotype_encoding_raw` from the new BCF APIs; that option
  remains available only on the VCF Zarr APIs where it still controls output.
- Use the upstream streaming typed BCF sink and expose nullable `Int8` dosage.
- Verify VCF-compatible BCF behavior across the dedicated eager/lazy, SQL,
  schema, projection, predicate, coordinate, INFO, and FORMAT surfaces.
- Document CSI-backed range pushdown and parallel BCF partition processing.
- Benchmark equivalent one-thread output against snputils, including peak RSS.

## Impact

- Affected specs: `vcf`
- Affected code: `polars_bio/io.py`, package exports, benchmark call sites,
  VCF/BCF tests, and reading documentation
