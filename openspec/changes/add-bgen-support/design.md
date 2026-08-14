# Design: BGEN genotype input

## Context

BGEN carries one probability distribution per sample per variant, not a call.
Every other variant format polars-bio reads carries calls, so the integration
has to decide how much of BGEN's shape to preserve and how much to flatten.

The provider (`datafusion-bio-format-bgen`) is a new crate rather than an
extension of an existing one, which is the first time polars-bio has taken on a
new upstream provider crate for a format with no existing logical schema to
reuse.

## Goals / Non-Goals

- Goals: read BGEN 1.2/1.3 without conversion; preserve probability semantics
  losslessly; make cohort-scale scans usable; keep the API shaped like the other
  formats.
- Non-Goals: writing BGEN; presenting BGEN as VCF; hard-calling genotypes.

## Decisions

### Alleles carry no reference/alternate semantics

BGEN stores an ordered allele list and does not define which allele is the
reference. Presenting `ref`/`alt` columns would require inventing that
distinction, and the choice would be wrong for any file whose producer ordered
alleles differently. `alleles` therefore keeps the encoded order, and dosage is
defined explicitly as the expected copy count of `alleles[1]` rather than "the
ALT dosage".

Alternative considered: infer reference from an external FASTA. Rejected as it
makes reading depend on a second file and a genome build.

### Two genotype outputs rather than one

`probability` preserves every state and is the lossless default. `dosage`
collapses each distribution to one number, which is what most association work
wants and is far cheaper to materialize.

Dosage rejects multiallelic variants instead of collapsing non-reference alleles
together, because a single dosage cannot represent more than two alleles without
choosing which to count. This mirrors the BCF dosage decision already taken in
`add-bcf-genotype-dosage`.

### Ploidy is emitted, not assumed

BGEN permits per-sample ploidy. `genotypes.PLOIDY` is emitted alongside the
values so a caller can interpret a variable-ploidy file correctly. The cost is
extra output volume in dosage mode, which is visible in the benchmark's peak
RSS; the alternative — assuming diploid — would silently corrupt non-diploid
files.

### Description reports the emitted schema, not a field dictionary

`describe_vcf` and `describe_bcf` return an INFO/FORMAT dictionary. BGEN has no
such header, so `describe_bgen` returns one row per emitted column plus the
file-level properties the provider records (layout, index provenance, whether
sample names were generated). This keeps the verb meaningful without inventing a
dictionary the format does not have.

## Risks / Trade-offs

- **Row order.** A scan with more than one partition may emit rows out of source
  order, because DataFusion coalesces partitions as their batches become ready.
  Content is unaffected and this matches the merged BCF provider, but it is a
  behaviour users must know about; it is documented in `features/reading.md`.
- **Probability materialization cost.** The nested Arrow list crosses Polars
  before reaching NumPy, and that conversion does not parallelize. The benchmark
  records polars-bio as slower than snputils for the probability workload; the
  dosage workload is faster. Improving this means avoiding the Polars round-trip
  for nested output, which is out of scope here.
- **Error handling divergence.** The BGEN registration arm returns its errors
  while the other arms panic. Rather than leave BGEN inconsistent, the shared
  `register_table` now restores a previous registration when construction fails,
  so the divergence is confined to BGEN returning `ValueError` where other
  formats still raise `PanicException`.

## Migration Plan

Additive: new functions and a new `InputFormat` variant. No existing API changes
behaviour, and `ReadOptions` gains an appended optional field.

## Open Questions

- Should the remaining format arms also return errors instead of panicking?
  Doing so is mechanical but touches every format, so it is left to a follow-up.
