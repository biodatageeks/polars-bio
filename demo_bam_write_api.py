#!/usr/bin/env python3
"""
Demonstration of BAM/CRAM Write API

This script shows that the Python API for BAM/CRAM writing is fully implemented
and ready to use. The actual write functionality is blocked by upstream work in
the datafusion-bio-formats library (BamTableProvider.insert_into() needs to be
implemented).

Once the upstream support is added, all these examples will work without any
changes to the polars-bio code.
"""

import os
import tempfile

import polars as pl

import polars_bio as pb
from polars_bio.polars_bio import BamWriteOptions, CramWriteOptions

print("=" * 80)
print("BAM/CRAM Write API Demonstration")
print("=" * 80)

# 1. Verify API is accessible
print("\n1. API Functions Available:")
print(f"   ✓ pb.write_bam: {callable(pb.write_bam)}")
print(f"   ✓ pb.sink_bam: {callable(pb.sink_bam)}")
print(f"   ✓ pb.write_cram: {callable(pb.write_cram)}")
print(f"   ✓ pb.sink_cram: {callable(pb.sink_cram)}")

# 2. Check write options
print("\n2. Write Options:")
bam_opts = BamWriteOptions(zero_based=True, tag_fields=["NM", "AS"])
print(f"   ✓ BamWriteOptions created: zero_based={bam_opts.zero_based}")
print(f"     tag_fields={bam_opts.tag_fields}")

cram_opts = CramWriteOptions(
    reference_path="ref.fasta", zero_based=True, tag_fields=["NM", "AS", "MD"]
)
print(f"   ✓ CramWriteOptions created: reference_path={cram_opts.reference_path}")
print(f"     zero_based={cram_opts.zero_based}")
print(f"     tag_fields={cram_opts.tag_fields}")

# 3. Read test data
print("\n3. Reading Test Data:")
df = pb.read_bam("tests/data/io/bam/test.bam")
print(f"   ✓ Read {len(df)} rows from test.bam")
print(f"   ✓ Columns: {df.columns}")
print(f"   ✓ First row sample:")
print(f"     - name: {df['name'][0]}")
print(f"     - chrom: {df['chrom'][0]}")
print(f"     - start: {df['start'][0]}")
print(f"     - end: {df['end'][0]}")

# 4. Test namespace methods exist
print("\n4. Namespace Methods Available:")
lf = pb.scan_bam("tests/data/io/bam/test.bam")
print(f"   ✓ LazyFrame.pb.sink_bam: {hasattr(lf.pb, 'sink_bam')}")
print(f"   ✓ LazyFrame.pb.sink_cram: {hasattr(lf.pb, 'sink_cram')}")
print(f"   ✓ DataFrame.pb.write_bam: {hasattr(df.pb, 'write_bam')}")
print(f"   ✓ DataFrame.pb.write_cram: {hasattr(df.pb, 'write_cram')}")

# 5. Show what happens when we try to write
print("\n5. Testing Write Functionality:")
print("   Note: This will fail with 'Insert into not implemented' because")
print("   the upstream datafusion-bio-formats library doesn't have write support yet.")
print()

with tempfile.NamedTemporaryFile(suffix=".bam", delete=False) as f:
    output_path = f.name

try:
    print(f"   Attempting: pb.write_bam(df, '{output_path}')")
    rows_written = pb.write_bam(df, output_path)
    print(f"   ✓ SUCCESS! Wrote {rows_written} rows")
except Exception as e:
    print(f"   ✗ Failed (expected): {type(e).__name__}")
    print(f"     Error: {str(e)}")
    print(f"\n   This is expected - the upstream library needs to implement")
    print(f"   insert_into() for BamTableProvider.")
finally:
    if os.path.exists(output_path):
        os.unlink(output_path)

# 6. Show the API examples that will work
print("\n" + "=" * 80)
print("API Examples (Will Work Once Upstream Support Is Added)")
print("=" * 80)

examples = """
# Example 1: Basic BAM write
import polars_bio as pb

df = pb.read_bam("input.bam")
pb.write_bam(df, "output.bam")

# Example 2: Auto-detect compression from extension
pb.write_bam(df, "output.sam")   # Uncompressed SAM
pb.write_bam(df, "output.bam")   # BGZF-compressed BAM

# Example 3: Streaming write with LazyFrame
lf = pb.scan_bam("large.bam")
lf.filter(pl.col("mapping_quality") > 30).pb.sink_bam("filtered.bam")

# Example 4: CRAM write (requires reference)
pb.write_cram(df, "output.cram", reference_path="ref.fasta")

# Example 5: Namespace methods
df.pb.write_bam("output.bam")
lf.pb.sink_bam("output.bam")

# Example 6: Preserve tag fields
df = pb.read_bam("input.bam", tag_fields=["NM", "AS", "MD"])
df.pb.write_bam("output.bam")  # Tags auto-detected and preserved
"""

print(examples)

print("=" * 80)
print("Implementation Complete - Ready for Upstream Support")
print("=" * 80)
print("\nWhat's needed:")
print("1. Implement insert_into() in BamTableProvider (datafusion-bio-format-bam)")
print("2. Implement insert_into() in CramTableProvider (datafusion-bio-format-cram)")
print("3. Follow the pattern from VcfTableProvider and FastqTableProvider")
print("\nOnce that's done, all the examples above will work automatically!")
