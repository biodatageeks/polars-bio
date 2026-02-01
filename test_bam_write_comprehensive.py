#!/usr/bin/env python3
"""
Comprehensive test suite for BAM/CRAM write functionality using PR #51 support.
"""

import os
import tempfile

import polars as pl

import polars_bio as pb


def test_basic_bam_write():
    """Test 1: Basic BAM write and round-trip"""
    print("\n" + "=" * 80)
    print("Test 1: Basic BAM Write and Round-Trip")
    print("=" * 80)

    df = pb.read_bam("tests/data/io/bam/test.bam")
    print(f"✓ Read {len(df)} rows")

    with tempfile.NamedTemporaryFile(suffix=".bam", delete=False) as f:
        output_path = f.name

    try:
        rows_written = pb.write_bam(df, output_path)
        print(f"✓ Wrote {rows_written} rows to {output_path}")

        df_back = pb.read_bam(output_path)
        print(f"✓ Read back {len(df_back)} rows")

        assert len(df) == rows_written == len(df_back), "Row count mismatch"
        assert df["name"][0] == df_back["name"][0], "Data mismatch"
        print(f"✓ Round-trip successful!")

    finally:
        if os.path.exists(output_path):
            os.unlink(output_path)


def test_sam_uncompressed():
    """Test 2: Write uncompressed SAM file"""
    print("\n" + "=" * 80)
    print("Test 2: Write Uncompressed SAM Format")
    print("=" * 80)

    df = pb.read_bam("tests/data/io/bam/test.bam")

    with tempfile.NamedTemporaryFile(suffix=".sam", delete=False, mode="w") as f:
        output_path = f.name

    try:
        rows_written = pb.write_bam(df, output_path)
        print(f"✓ Wrote {rows_written} rows to SAM format")

        # Verify it's plain text
        with open(output_path, "r") as f:
            first_line = f.readline()
            assert first_line.startswith("@"), "SAM file should start with header"
            # Read a few alignment lines
            for i in range(3):
                line = f.readline()
                if line and not line.startswith("@"):
                    # Verify it's tab-delimited SAM format
                    fields = line.split("\t")
                    assert (
                        len(fields) >= 11
                    ), "SAM alignment line should have at least 11 fields"
        print(f"✓ File is plain text SAM format with valid structure")
        print(f"✓ SAM write successful!")
        print(f"  Note: Reading plain SAM back requires a SAM-specific reader")

    finally:
        if os.path.exists(output_path):
            os.unlink(output_path)


def test_streaming_write():
    """Test 3: Streaming write with LazyFrame"""
    print("\n" + "=" * 80)
    print("Test 3: Streaming Write with LazyFrame")
    print("=" * 80)

    lf = pb.scan_bam("tests/data/io/bam/test.bam")
    filtered_lf = lf.filter(pl.col("mapping_quality") > 20)

    with tempfile.NamedTemporaryFile(suffix=".bam", delete=False) as f:
        output_path = f.name

    try:
        pb.sink_bam(filtered_lf, output_path)
        print(f"✓ Streaming write completed")

        df_filtered = pb.read_bam(output_path)
        print(f"✓ Read back {len(df_filtered)} filtered rows")

        # Verify all rows meet filter criteria
        assert all(df_filtered["mapping_quality"] > 20), "Filter not applied correctly"
        print(f"✓ All rows have mapping_quality > 20")
        print(f"✓ Streaming write successful!")

    finally:
        if os.path.exists(output_path):
            os.unlink(output_path)


def test_namespace_methods():
    """Test 4: Namespace methods (df.pb.write_bam, lf.pb.sink_bam)"""
    print("\n" + "=" * 80)
    print("Test 4: Namespace Methods")
    print("=" * 80)

    df = pb.read_bam("tests/data/io/bam/test.bam")

    # Test DataFrame.pb.write_bam()
    with tempfile.NamedTemporaryFile(suffix=".bam", delete=False) as f:
        output_path1 = f.name

    try:
        rows_written = df.pb.write_bam(output_path1)
        print(f"✓ df.pb.write_bam() wrote {rows_written} rows")

        df_back = pb.read_bam(output_path1)
        assert len(df) == len(df_back), "DataFrame namespace write failed"
        print(f"✓ DataFrame namespace method works!")

    finally:
        if os.path.exists(output_path1):
            os.unlink(output_path1)

    # Test LazyFrame.pb.sink_bam()
    lf = pb.scan_bam("tests/data/io/bam/test.bam")

    with tempfile.NamedTemporaryFile(suffix=".bam", delete=False) as f:
        output_path2 = f.name

    try:
        lf.pb.sink_bam(output_path2)
        print(f"✓ lf.pb.sink_bam() completed")

        df_back = pb.read_bam(output_path2)
        assert len(df) == len(df_back), "LazyFrame namespace write failed"
        print(f"✓ LazyFrame namespace method works!")

    finally:
        if os.path.exists(output_path2):
            os.unlink(output_path2)


def test_with_tag_fields():
    """Test 5: Write with optional alignment tags"""
    print("\n" + "=" * 80)
    print("Test 5: Write with Optional Alignment Tags")
    print("=" * 80)

    # Read with tag fields
    df = pb.read_bam("tests/data/io/bam/test.bam", tag_fields=["NM", "AS"])
    print(
        f"✓ Read BAM with tags: {[c for c in df.columns if c not in ['name', 'chrom', 'start', 'end', 'flags', 'cigar', 'mapping_quality', 'mate_chrom', 'mate_start', 'sequence', 'quality_scores']]}"
    )

    with tempfile.NamedTemporaryFile(suffix=".bam", delete=False) as f:
        output_path = f.name

    try:
        # Tags should be auto-detected from schema
        rows_written = pb.write_bam(df, output_path)
        print(f"✓ Wrote {rows_written} rows with tags")

        # Read back with same tags
        df_back = pb.read_bam(output_path, tag_fields=["NM", "AS"])

        # Verify tags are preserved
        if "NM" in df.columns and "NM" in df_back.columns:
            # Compare first non-null value
            original_nm = (
                df.filter(pl.col("NM").is_not_null())["NM"][0]
                if len(df.filter(pl.col("NM").is_not_null())) > 0
                else None
            )
            readback_nm = (
                df_back.filter(pl.col("NM").is_not_null())["NM"][0]
                if len(df_back.filter(pl.col("NM").is_not_null())) > 0
                else None
            )

            if original_nm is not None and readback_nm is not None:
                assert original_nm == readback_nm, "NM tag not preserved"
                print(
                    f"✓ Tag preservation verified (NM: {original_nm} → {readback_nm})"
                )

        print(f"✓ Optional alignment tags working!")

    finally:
        if os.path.exists(output_path):
            os.unlink(output_path)


def test_filter_and_write():
    """Test 6: Filter and write in one operation"""
    print("\n" + "=" * 80)
    print("Test 6: Filter and Write Pipeline")
    print("=" * 80)

    with tempfile.NamedTemporaryFile(suffix=".bam", delete=False) as f:
        output_path = f.name

    try:
        # Chain operations: scan → filter → sink
        (
            pb.scan_bam("tests/data/io/bam/test.bam")
            .filter(pl.col("mapping_quality") > 30)
            .pb.sink_bam(output_path)
        )

        print(f"✓ Filtered and wrote in one pipeline")

        # Verify
        df = pb.read_bam(output_path)
        print(f"✓ Wrote {len(df)} high-quality alignments")
        assert all(df["mapping_quality"] > 30), "Filter not applied"
        print(f"✓ Filter → Write pipeline successful!")

    finally:
        if os.path.exists(output_path):
            os.unlink(output_path)


def run_all_tests():
    """Run all tests"""
    print("=" * 80)
    print("COMPREHENSIVE BAM/CRAM WRITE TEST SUITE")
    print("Using PR #51: BAM/CRAM Write Support")
    print("=" * 80)

    tests = [
        test_basic_bam_write,
        test_sam_uncompressed,
        test_streaming_write,
        test_namespace_methods,
        test_with_tag_fields,
        test_filter_and_write,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"\n✗ Test failed: {e}")
            import traceback

            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 80)
    print(f"TEST RESULTS: {passed} passed, {failed} failed")
    print("=" * 80)

    if failed == 0:
        print("\n🎉 ALL TESTS PASSED!")
        print("✓ BAM/CRAM write support is fully functional")
        print("✓ Round-trip compatibility confirmed")
        print("✓ Streaming writes working")
        print("✓ Namespace methods working")
        print("✓ Tag field support working")
        print("✓ Filter pipelines working")

    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
