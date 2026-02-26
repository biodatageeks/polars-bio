# Implementation Tasks

## 1. Dependency alignment
- [x] 1.1 Update `datafusion-bio-format-*` git revisions to `f3fd02e25d4ee5b9bf2ca0a575ee991c1fadccb1`.
- [x] 1.2 Refresh `Cargo.lock` for updated VCF dependency graph.

## 2. Python I/O behavior alignment
- [x] 2.1 Update VCF read/scan docs to describe nested multisample `genotypes`.
- [x] 2.2 Align `info_fields=None` path with upstream default semantics.
- [x] 2.3 Update VCF metadata extraction to read sample names from `bio.vcf.samples`.

## 3. Tests and fixtures
- [x] 3.1 Add `tests/data/io/vcf/multisample.vcf.gz` fixture.
- [x] 3.2 Update multisample FORMAT tests to validate nested `genotypes`.
- [x] 3.3 Update multisample write roundtrip tests to validate nested `genotypes`.

## 4. Validation
- [x] 4.1 Run targeted VCF test suite and confirm pass.
- [x] 4.2 Run `openspec validate update-vcf-multisample-genotypes-schema --strict`.
