# Change: Add a dense BGEN genotype matrix reader

## Why

Association testing, PCA and relatedness consume the whole-cohort genotype
matrix, not a DataFrame. `scan_bgen` can produce the values, but getting a dense
array out of it costs a consolidation the comparison readers never perform: the
scan builds Arrow record batches and the caller concatenates them into one
contiguous buffer. On chromosome 22 of 1000 Genomes that is a serial pass over
10 GB.

That pass does not parallelise. Measured stage by stage, the scan itself scaled
5.6x from one partition to eight while the consolidation scaled 1.5x and grew to
36% of the read, holding end-to-end scaling at 4.15x against a decoder that
divides 6.3x.

This mirrors `add-pgen-genotype-matrix`, which made the same argument for PGEN
and reached the same answer.

## What Changes

- Add `read_bgen_matrix`, returning a dense `float32` NumPy matrix rather than a
  DataFrame, with the variant positions and sample names that label its axes.
- Decode straight into the destination array: the provider is given the caller's
  buffer and writes each variant's dosages at their final address, so no
  consolidation stage exists.
- Dosage only. BGEN probabilities are variable width and have no single dense
  shape, so the matrix reader takes no output mode at all rather than accepting
  one and rejecting it. The probability states are read through
  `scan_bgen(genotype_output="probability")`.
- Add `genotype_fields` to `scan_bgen` and `read_bgen`, selecting which children
  of the `genotypes` struct to emit. `PLOIDY` is a byte per genotype and a NumPy
  view of a result keeps the whole Arrow struct alive, so a dosage-only caller
  can decline it.

## Impact

- New public API: `read_bgen_matrix`, and a new `genotype_fields` argument on two
  existing functions. Both are additive; `genotype_fields=None` preserves the
  previous behaviour exactly.
- Requires `datafusion-bio-formats` at `a5d5fe5` or later
  ([#234](https://github.com/biodatageeks/datafusion-bio-formats/pull/234),
  [#235](https://github.com/biodatageeks/datafusion-bio-formats/pull/235),
  [#236](https://github.com/biodatageeks/datafusion-bio-formats/pull/236),
  [#237](https://github.com/biodatageeks/datafusion-bio-formats/pull/237)).
- Measured on chr22, 993,881 x 2,548 = 2,532,408,788 dosages: 11.826 s at one
  thread against the `bgen` package's 15.415 s and snputils' 21.737 s; 2.083 s
  at eight partitions, scaling 5.68x. Bit-identical to the `bgen` reference
  across every cell.
