# Phase 1: Dataset Preparation - Context

**Gathered:** 2026-03-20
**Status:** Ready for planning

<domain>
## Phase Boundary

Download, validate, and index the benchmark dataset (ENCODE ENCSR329MHM BAM + GENCODE v49 GTF). Install featureCounts 2.1.1 and HTSeq-count 2.1.2 in a benchmark environment. Verify chromosome naming consistency and data integrity. No counting or benchmarking happens in this phase.

</domain>

<decisions>
## Implementation Decisions

### Data storage location
- Benchmark data lives at an external path configured via `POLARS_BIO_BENCHMARK_DATA` environment variable
- Default/documented path: `/Users/mwiewior/research/data/polars-bio/rnaseq`
- Flat directory layout — BAM, BAI, GTF, and tool outputs all in one directory
- No subdirectory organization needed at this scale

### Data acquisition
- Document download commands in the notebook, no dedicated setup script
- ENCODE BAM downloaded via HTTPS from ENCODE portal (accession ENCFF588KDY for Rep1)
- GENCODE v49 comprehensive GTF from gencodegenes.org
- samtools sort + index after download

### Environment isolation
- Use `uv` for Python environment management (HTSeq-count 2.1.2 is a Python package)
- Use `conda/mamba` for subread (featureCounts 2.1.1) since it's a C binary not on PyPI
- Single conda env for featureCounts; HTSeq installed via uv/pip in the polars-bio dev env or a separate uv env
- hyperfine and gtime installed system-wide (brew or apt)
- Document all install commands inline in the notebook — no environment.yml file

### Validation checks
- samtools flagstat to verify ~36M total reads in BAM
- Spot-check chromosome names from BAM header and GTF first column match (both chr-prefixed)
- pb.scan_gtf() on downloaded GTF to verify polars-bio can read it
- Version string checks for featureCounts and htseq-count

### Claude's Discretion
- Exact download URLs (derive from ENCODE accession + GENCODE release page)
- samtools sort/index command details
- Whether to verify BAM MD5 checksums from ENCODE portal

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Dataset sources
- `.planning/research/STACK.md` — ENCODE accession ENCSR329MHM, GENCODE v49 URLs, featureCounts/HTSeq versions, conda install commands
- `.planning/research/PITFALLS.md` — Chromosome naming mismatch prevention (Pitfall 3), MAPQ threshold documentation (Pitfall 5)

### Project context
- `.planning/PROJECT.md` — Core value, constraints, benchmark scope
- `.planning/REQUIREMENTS.md` — DATA-01, DATA-02, DATA-03 requirements

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `pb.scan_gtf()` with `attr_fields=["gene_id"]`: already tested in `tests/test_io_gtf.py` — use to validate GTF readability
- `pb.scan_bam()`: tested in `tests/test_io_bam.py` — use to verify BAM schema and chromosome names
- Existing test data in `tests/data/io/bam/` and `tests/data/io/gtf/` — small fixtures, not usable for benchmark but confirm I/O pipeline works

### Established Patterns
- polars-bio uses Poetry for Python deps, maturin for Rust build — benchmark deps should not pollute this
- Test data lives in `tests/data/io/{format}/` — benchmark data is external (too large for repo)

### Integration Points
- The notebook will import polars_bio and call `pb.scan_bam()` / `pb.scan_gtf()` for validation
- featureCounts and HTSeq-count are invoked as subprocesses, not imported

</code_context>

<specifics>
## Specific Ideas

- External data path at `$POLARS_BIO_BENCHMARK_DATA` keeps the repo clean and supports different machines
- User prefers inline documentation over automation scripts at this stage

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 01-dataset-preparation*
*Context gathered: 2026-03-20*
