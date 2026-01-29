"""
Tests for LazyFrame streaming fix (Issue #71).

This test suite verifies that scan_vcf() and other scan operations work in true
streaming fashion without materializing the entire file in memory.

Background:
-----------
Prior to this fix, scan_vcf() would:
1. Register a streaming table provider (good!)
2. Immediately execute `SELECT *` to materialize ALL data (bad!)
3. Extract metadata from materialized DataFrame
4. Create a "LazyFrame" from already-materialized data (fake lazy!)

This caused OOM errors on large VCF files.

The Fix:
--------
1. Added `py_get_table_schema()` Rust function to extract schema without reading data
2. Updated `_read_file()` to use schema extraction instead of materialization
3. Modified `_lazy_scan()` to query tables on-demand when `.collect()` is called

Result: True streaming - data is only read when actually needed!
"""

import tracemalloc

import polars as pl

import polars_bio as pb


class TestLazyStreamingFix:
    """Tests verifying that scan operations don't materialize data prematurely."""

    def test_scan_vcf_minimal_memory_usage(self):
        """Verify scan_vcf uses minimal memory (no materialization)."""
        tracemalloc.start()
        snapshot1 = tracemalloc.take_snapshot()

        # Scan VCF - should only register table and extract schema
        lf = pb.scan_vcf("tests/data/io/vcf/vep.vcf.gz")

        snapshot2 = tracemalloc.take_snapshot()
        mem_diff = sum(
            stat.size_diff for stat in snapshot2.compare_to(snapshot1, "lineno")
        )
        mem_mb = mem_diff / (1024 * 1024)

        tracemalloc.stop()

        # Memory usage should be minimal (< 5MB for metadata/schema)
        # This is the KEY test - if this fails, we're materializing the file!
        assert mem_mb < 5, f"scan_vcf used {mem_mb:.2f} MB - likely materializing data"

    def test_scan_vcf_returns_lazyframe(self):
        """Verify scan_vcf returns a LazyFrame (not DataFrame)."""
        lf = pb.scan_vcf("tests/data/io/vcf/vep.vcf.gz")

        # Should be a LazyFrame
        assert isinstance(lf, pl.LazyFrame)

        # Should have the PYTHON SCAN in the plan (our IO source)
        plan = lf.explain()
        assert "PYTHON SCAN" in plan

    def test_scan_vcf_collect_works(self):
        """Verify collect() actually streams and retrieves data."""
        lf = pb.scan_vcf("tests/data/io/vcf/vep.vcf.gz")

        # Collect should work and return data
        df = lf.head(5).collect()

        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0
        assert "chrom" in df.columns
        assert "start" in df.columns

    def test_scan_vcf_with_filter_streams(self):
        """Verify filtering works with streaming."""
        lf = pb.scan_vcf("tests/data/io/vcf/vep.vcf.gz")

        # Apply filter and collect
        df = lf.filter(pl.col("start") > 26960000).collect()

        assert len(df) > 0
        assert all(df["start"] > 26960000)

    def test_scan_vcf_metadata_preserved(self):
        """Verify metadata extraction still works without materialization."""
        from polars_bio._metadata import get_metadata

        lf = pb.scan_vcf("tests/data/io/vcf/vep.vcf.gz")
        meta = get_metadata(lf)

        # Metadata should be present
        assert meta["format"] == "vcf"
        assert meta["path"] == "tests/data/io/vcf/vep.vcf.gz"

        # Header metadata should exist
        assert "header" in meta
        header = meta["header"]
        assert "version" in header
        assert header["version"] == "VCFv4.2"

    def test_multiple_scans_dont_accumulate_memory(self):
        """Verify multiple scans don't accumulate memory (proving laziness)."""
        tracemalloc.start()
        snapshot1 = tracemalloc.take_snapshot()

        # Scan the same file 10 times
        lazyframes = []
        for _ in range(10):
            lf = pb.scan_vcf("tests/data/io/vcf/vep.vcf.gz")
            lazyframes.append(lf)

        snapshot2 = tracemalloc.take_snapshot()
        mem_diff = sum(
            stat.size_diff for stat in snapshot2.compare_to(snapshot1, "lineno")
        )
        mem_mb = mem_diff / (1024 * 1024)

        tracemalloc.stop()

        # 10 scans should still use minimal memory (< 10MB)
        # If materializing, this would use 10x the file size!
        assert mem_mb < 10, f"10 scans used {mem_mb:.2f} MB - likely materializing"

    def test_schema_extraction_without_materialization(self):
        """Test py_get_table_schema extracts schema without reading data."""
        from polars_bio.context import ctx
        from polars_bio.polars_bio import (
            InputFormat,
            py_get_table_schema,
            py_register_table,
        )

        # Register table (no data read)
        table = py_register_table(
            ctx, "tests/data/io/vcf/multisample.vcf", None, InputFormat.Vcf, None
        )

        # Get schema (should not read data)
        tracemalloc.start()
        snapshot1 = tracemalloc.take_snapshot()

        schema = py_get_table_schema(ctx, table.name)

        snapshot2 = tracemalloc.take_snapshot()
        mem_diff = sum(
            stat.size_diff for stat in snapshot2.compare_to(snapshot1, "lineno")
        )
        mem_mb = mem_diff / (1024 * 1024)

        tracemalloc.stop()

        # Schema extraction should use minimal memory
        assert mem_mb < 1, f"Schema extraction used {mem_mb:.2f} MB"

        # Schema should be valid
        assert len(schema) > 0
        assert "chrom" in [field.name for field in schema]

    def test_scan_with_sql_still_works(self):
        """Verify SQL path (register + sql) still works after the fix."""
        # Register table
        pb.register_vcf("tests/data/io/vcf/vep.vcf.gz", "test_sql_vcf")

        # Execute SQL query
        lf = pb.sql("SELECT chrom, start, ref, alt FROM test_sql_vcf")

        # Should return LazyFrame
        assert isinstance(lf, pl.LazyFrame)

        # Collect should work
        df = lf.collect()
        assert len(df) > 0  # File has 2 rows
        assert list(df.columns) == ["chrom", "start", "ref", "alt"]


class TestOtherFormatsStreaming:
    """Verify the fix works for other formats too (FASTQ, GFF, etc.)."""

    def test_scan_fastq_minimal_memory(self):
        """Verify scan_fastq is also lazy."""
        tracemalloc.start()
        snapshot1 = tracemalloc.take_snapshot()

        lf = pb.scan_fastq("tests/data/io/fastq/test.fastq.gz")

        snapshot2 = tracemalloc.take_snapshot()
        mem_diff = sum(
            stat.size_diff for stat in snapshot2.compare_to(snapshot1, "lineno")
        )
        mem_mb = mem_diff / (1024 * 1024)

        tracemalloc.stop()

        assert mem_mb < 5, f"scan_fastq used {mem_mb:.2f} MB - likely materializing"

    def test_scan_gff_minimal_memory(self):
        """Verify scan_gff is also lazy."""
        tracemalloc.start()
        snapshot1 = tracemalloc.take_snapshot()

        lf = pb.scan_gff("tests/data/io/gff/Homo_sapiens.GRCh38.111.gff3.gz")

        snapshot2 = tracemalloc.take_snapshot()
        mem_diff = sum(
            stat.size_diff for stat in snapshot2.compare_to(snapshot1, "lineno")
        )
        mem_mb = mem_diff / (1024 * 1024)

        tracemalloc.stop()

        assert mem_mb < 5, f"scan_gff used {mem_mb:.2f} MB - likely materializing"
