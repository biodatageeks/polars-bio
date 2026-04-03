# Phase 1: Dataset Preparation - Research

**Researched:** 2026-03-20
**Domain:** Benchmark dataset acquisition, validation, and tool environment setup
**Confidence:** HIGH

## Summary

Phase 1 prepares all data and tools needed for downstream benchmark phases. The work involves downloading an ENCODE BAM file (ENCFF588KDY, ~3.2 GB) and GENCODE v49 comprehensive GTF, validating them (read counts, chromosome naming consistency, polars-bio readability), and installing featureCounts 2.1.1 and HTSeq-count 2.1.2 in appropriate environments.

The ENCODE BAM is already coordinate-sorted (ENCODE STAR pipeline outputs `Aligned.sortedByCoord.out.bam`), so only BAI indexing is needed -- not a full `samtools sort`. The GENCODE v49 GTF uses chr-prefixed contigs matching ENCODE GRCh38 BAMs, so no chromosome name normalization should be required, but a spot-check is mandatory. HTSeq 2.1.2 (released Feb 2026 on PyPI) is the correct version per user decisions, superseding the 2.0.5 version listed in STACK.md.

**Primary recommendation:** Document all commands inline in the benchmark notebook. Validate data integrity before moving to Phase 2 -- zero-count results from chr-naming mismatch are silent and catastrophic.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Benchmark data lives at an external path configured via `POLARS_BIO_BENCHMARK_DATA` environment variable
- Default/documented path: `/Users/mwiewior/research/data/polars-bio/rnaseq`
- Flat directory layout -- BAM, BAI, GTF, and tool outputs all in one directory
- No subdirectory organization needed at this scale
- Document download commands in the notebook, no dedicated setup script
- ENCODE BAM downloaded via HTTPS from ENCODE portal (accession ENCFF588KDY for Rep1)
- GENCODE v49 comprehensive GTF from gencodegenes.org
- samtools sort + index after download
- Use `uv` for Python environment management (HTSeq-count 2.1.2 is a Python package)
- Use `conda/mamba` for subread (featureCounts 2.1.1) since it's a C binary not on PyPI
- Single conda env for featureCounts; HTSeq installed via uv/pip in the polars-bio dev env or a separate uv env
- hyperfine and gtime installed system-wide (brew or apt)
- Document all install commands inline in the notebook -- no environment.yml file
- samtools flagstat to verify ~36M total reads in BAM
- Spot-check chromosome names from BAM header and GTF first column match (both chr-prefixed)
- pb.scan_gtf() on downloaded GTF to verify polars-bio can read it
- Version string checks for featureCounts and htseq-count

### Claude's Discretion
- Exact download URLs (derive from ENCODE accession + GENCODE release page)
- samtools sort/index command details
- Whether to verify BAM MD5 checksums from ENCODE portal

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| DATA-01 | Download and index ENCODE ENCSR329MHM BAM (HepG2, SE76, ~36M reads, GRCh38) | ENCODE download URL verified (ENCFF588KDY.bam, 3.21 GB, md5: 90779101e28e5b425be8e51310314b89). BAM is already coordinate-sorted from ENCODE pipeline; only `samtools index` needed. |
| DATA-02 | Download Gencode v49 comprehensive GTF (GRCh38.p14) | GTF URL verified: `https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_49/gencode.v49.annotation.gtf.gz`. Use CHR version (reference chromosomes only). |
| DATA-03 | Verify chromosome naming consistency between BAM and GTF (chr-prefixed) | Both ENCODE GRCh38 BAMs and GENCODE v49 CHR GTF use chr-prefixed contigs. Spot-check by comparing `samtools view -H` SQ lines against GTF seqname column. |
</phase_requirements>

## Standard Stack

### Core
| Tool | Version | Purpose | Why Standard |
|------|---------|---------|--------------|
| samtools | 1.21+ | BAM indexing, flagstat validation, header inspection | Universal BAM manipulation tool; already expected in any bioinformatics environment |
| featureCounts (subread) | 2.1.1 | Reference counting tool (speed target) | Bioconda package `subread=2.1.1`; de-facto standard for bulk RNA-seq |
| HTSeq-count | 2.1.2 | Reference counting tool (correctness cross-check) | PyPI `HTSeq==2.1.2` (released Feb 2026); latest stable release |
| wget/curl | system | File downloads | Standard HTTP download tools |

### Supporting
| Tool | Version | Purpose | When to Use |
|------|---------|---------|-------------|
| gunzip | system | Decompress GTF.gz | After GTF download |
| md5/md5sum | system | Verify BAM integrity against ENCODE checksum | Optional but recommended for DATA-01 |

### Note on HTSeq Version
STACK.md references HTSeq 2.0.5 but CONTEXT.md specifies 2.1.2. HTSeq 2.1.2 was released on Feb 4, 2026 on PyPI. Use **2.1.2** per the user's decision. The REQUIREMENTS.md also specifies 2.1.2. Install via `uv pip install HTSeq==2.1.2` or `pip install HTSeq==2.1.2`.

**Installation:**
```bash
# featureCounts via conda/mamba
conda install -c bioconda subread=2.1.1
# OR
mamba install -c bioconda subread=2.1.1

# HTSeq via uv
uv pip install HTSeq==2.1.2

# samtools (if not already available)
conda install -c bioconda samtools
```

## Architecture Patterns

### Data Directory Layout
```
$POLARS_BIO_BENCHMARK_DATA/          # e.g., /Users/mwiewior/research/data/polars-bio/rnaseq
    ENCFF588KDY.bam                  # downloaded, already sorted
    ENCFF588KDY.bam.bai              # created by samtools index
    gencode.v49.annotation.gtf       # decompressed from .gtf.gz
```

### Notebook Documentation Pattern
All setup commands documented as markdown cells in the benchmark notebook with executable shell cells (`!command` or `%%bash`). No standalone scripts. Pattern:

1. Markdown cell explaining what and why
2. Code cell with the command
3. Code cell verifying the result

### Validation-First Pattern
Every download is followed immediately by a validation step before proceeding:
- BAM download -> md5sum check -> samtools flagstat -> samtools view -H (chr check)
- GTF download -> gunzip -> pb.scan_gtf() -> check seqname column
- Tool install -> version string check

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| BAM sorting | Custom sort logic | `samtools sort` | samtools is the standard; but ENCODE BAM is already sorted so just verify |
| BAM indexing | Manual BAI creation | `samtools index` | Produces standard BAI index recognized by all tools |
| Checksum verification | Custom hash code | `md5` (macOS) / `md5sum` (Linux) | Standard system tool |
| Chromosome name extraction from BAM | Parse BAM binary | `samtools view -H \| grep @SQ` | Standard header inspection |

## Common Pitfalls

### Pitfall 1: Re-sorting an Already-Sorted BAM
**What goes wrong:** ENCODE STAR pipeline BAMs are already coordinate-sorted. Running `samtools sort` on an already-sorted file wastes time (~15-30 min for 3.2 GB) and disk space (temporary files).
**Why it happens:** The CONTEXT.md says "samtools sort + index after download" but ENCODE BAMs are pre-sorted.
**How to avoid:** Check the BAM header for `SO:coordinate` tag (`samtools view -H ENCFF588KDY.bam | head -1`). If present, skip sort and proceed directly to `samtools index`. Document this finding in the notebook.
**Warning signs:** `@HD VN:1.4 SO:coordinate` in the first line of the BAM header confirms it's already sorted.

### Pitfall 2: Chromosome Naming Mismatch (Silent Zero Counts)
**What goes wrong:** If BAM uses `chr1` and GTF uses `1`, all overlaps produce zero counts. No error is thrown.
**Why it happens:** Different genome annotation sources use different naming conventions.
**How to avoid:** Both ENCODE GRCh38 BAMs and GENCODE v49 CHR annotation use `chr` prefix. Verify by spot-checking: extract unique contig names from BAM header and compare with unique values in GTF column 1. Do this check programmatically in the notebook.
**Warning signs:** Zero overlap counts; `samtools idxstats` shows reads on `chr1` but GTF has `1`.

### Pitfall 3: Using Wrong GENCODE GTF Variant
**What goes wrong:** GENCODE provides multiple GTF files: CHR (reference chromosomes only), ALL (with patches/haplotypes), PRI (primary assembly). Using ALL or PRI adds extra contigs that may not match the BAM reference.
**How to avoid:** Use the CHR variant: `gencode.v49.annotation.gtf.gz` (not `chr_patch_hapl_scaff` or `primary_assembly`). This matches what production RNA-seq pipelines use.

### Pitfall 4: Forgetting to Decompress GTF
**What goes wrong:** The GENCODE GTF downloads as `.gtf.gz`. polars-bio's `scan_gtf()` may or may not handle gzipped GTF transparently.
**How to avoid:** Always decompress with `gunzip gencode.v49.annotation.gtf.gz` before use. The uncompressed file is ~1.6 GB.

### Pitfall 5: HTSeq Build Dependencies
**What goes wrong:** HTSeq requires compilation of C extensions (Cython). On macOS, this can fail without proper build tools.
**How to avoid:** Ensure Xcode command-line tools are installed (`xcode-select --install`). Alternatively, conda/mamba provides pre-built wheels: `mamba install -c bioconda htseq=2.1.2` if pip installation fails.

## Code Examples

### Verify ENCODE BAM is already sorted
```bash
# Check sort order in BAM header
samtools view -H ENCFF588KDY.bam | head -1
# Expected output: @HD	VN:1.4	SO:coordinate
```

### Download and prepare BAM (DATA-01)
```bash
export POLARS_BIO_BENCHMARK_DATA="/Users/mwiewior/research/data/polars-bio/rnaseq"
mkdir -p "$POLARS_BIO_BENCHMARK_DATA"
cd "$POLARS_BIO_BENCHMARK_DATA"

# Download BAM (~3.2 GB)
wget -O ENCFF588KDY.bam "https://www.encodeproject.org/files/ENCFF588KDY/@@download/ENCFF588KDY.bam"

# Verify MD5 (optional but recommended)
# Expected: 90779101e28e5b425be8e51310314b89
md5 ENCFF588KDY.bam        # macOS
# md5sum ENCFF588KDY.bam   # Linux

# Verify sort order (expect SO:coordinate -- skip samtools sort if present)
samtools view -H ENCFF588KDY.bam | head -1

# Index (creates .bai file)
samtools index ENCFF588KDY.bam

# Validate read count (~36M total)
samtools flagstat ENCFF588KDY.bam
```

### Download and prepare GTF (DATA-02)
```bash
cd "$POLARS_BIO_BENCHMARK_DATA"

# Download GENCODE v49 comprehensive GTF
wget https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_49/gencode.v49.annotation.gtf.gz

# Decompress
gunzip gencode.v49.annotation.gtf.gz
```

### Verify chromosome naming consistency (DATA-03)
```python
import polars_bio as pb

# Check BAM chromosome names
bam_contigs = (
    pb.scan_bam("$POLARS_BIO_BENCHMARK_DATA/ENCFF588KDY.bam")
    .select("contig")
    .unique()
    .collect()
)
print("BAM contigs sample:", bam_contigs.head(5))

# Check GTF chromosome names
gtf_contigs = (
    pb.scan_gtf("$POLARS_BIO_BENCHMARK_DATA/gencode.v49.annotation.gtf")
    .select("seqname")
    .unique()
    .collect()
)
print("GTF seqnames sample:", gtf_contigs.head(5))

# Verify overlap (both should use chr prefix)
bam_set = set(bam_contigs["contig"].to_list())
gtf_set = set(gtf_contigs["seqname"].to_list())
common = bam_set & gtf_set
print(f"Common contigs: {len(common)}")
assert len(common) > 0, "FATAL: No common chromosome names between BAM and GTF!"
assert any(c.startswith("chr") for c in common), "Expected chr-prefixed contigs"
```

### Verify tool installations
```bash
# featureCounts version
featureCounts -v
# Expected output contains: featureCounts v2.1.1

# HTSeq-count version
htseq-count --version
# Expected: 2.1.2
```

### Verify GTF readability with polars-bio
```python
import polars_bio as pb

gtf = pb.scan_gtf(
    "$POLARS_BIO_BENCHMARK_DATA/gencode.v49.annotation.gtf",
    attr_fields=["gene_id"]
)
# Quick validation
exons = gtf.filter(pb.col("feature") == "exon").collect()
print(f"Total exon rows: {len(exons)}")  # Expected: ~1.4M
print(f"Unique gene_ids: {exons['gene_id'].n_unique()}")  # Expected: ~60k
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| HTSeq 2.0.x | HTSeq 2.1.2 | Feb 2026 | New release; user explicitly chose this version |
| Manual BAM sort after ENCODE download | Verify pre-sorted, index only | Always (ENCODE uses STAR --outSAMtype BAM SortedByCoordinate) | Saves 15-30 min per BAM |

## Open Questions

1. **Does `pb.scan_gtf()` handle uncompressed 1.6 GB GTF efficiently?**
   - What we know: scan_gtf works on small test fixtures in the test suite
   - What's unclear: Performance on the full GENCODE v49 (~1.4M exon rows)
   - Recommendation: Test during validation; if slow, note for Phase 2 optimization

2. **Is the ENCODE BAM truly coordinate-sorted?**
   - What we know: ENCODE STAR pipeline uses `--outSAMtype BAM SortedByCoordinate`; HIGH confidence
   - What's unclear: Whether any post-processing step might have re-ordered it
   - Recommendation: Check `@HD SO:coordinate` header tag; if absent, run `samtools sort`

3. **HTSeq 2.1.2 compatibility with current Python version**
   - What we know: HTSeq 2.0.x supported Python 3.8-3.12; 2.1.2 is new
   - What's unclear: Python 3.13 compatibility
   - Recommendation: Install in the polars-bio dev env or a separate uv env; test `htseq-count --version`

## Sources

### Primary (HIGH confidence)
- [ENCODE ENCFF588KDY file page](https://www.encodeproject.org/files/ENCFF588KDY/) -- file size 3.21 GB, md5: 90779101e28e5b425be8e51310314b89, GRCh38
- [GENCODE Human Release 49](https://www.gencodegenes.org/human/) -- GTF URLs verified, CHR/ALL/PRI variants documented
- [HTSeq PyPI page](https://pypi.org/project/HTSeq/) -- version 2.1.2 released Feb 4, 2026
- [Bioconda subread package](https://bioconda.github.io/recipes/subread/README.html) -- subread 2.1.1 available

### Secondary (MEDIUM confidence)
- [ENCODE STAR pipeline](https://github.com/ENCODE-DCC/long-rna-seq-pipeline/blob/master/DAC/STAR_RSEM.sh) -- confirms `--outSAMtype BAM SortedByCoordinate` output

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all tools verified on official sources with exact versions
- Architecture: HIGH -- flat directory layout is a locked user decision
- Pitfalls: HIGH -- chromosome naming and sort-order issues verified against official ENCODE/GENCODE documentation

**Research date:** 2026-03-20
**Valid until:** 2026-04-20 (stable domain; ENCODE accessions and GENCODE releases are permanent)
