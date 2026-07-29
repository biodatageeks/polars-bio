"""Regression tests for CRAM files whose bases series uses Huffman coding (#429).

Reading such a file used to abort with ``ComputeError: ... task panicked with
message "not yet implemented"``, raised from ``noodles``'s ``Byte::decode_take``,
whose Huffman arm was unimplemented for the bulk decode path.

``huffman_byte_encoding.cram`` is written by samtools in ``no_ref`` mode, so it
needs no external reference. It holds 500 poly-N reads: 150 mapped to chr1, 150
to chr2 and 200 unmapped. The constant sequence makes htslib encode the bases
series as Huffman over a single-symbol alphabet, which is the shape seen in
practice in the poly-N unmapped tail of a WGS file.

Two properties of the fixture are load-bearing, so please keep them if you ever
regenerate it:

1. The unmapped reads are required. The bases series is read only for unmapped
   records; mapped ones rebuild their sequence from features and never reach the
   Huffman path.
2. The fixture ships **without** a ``.crai``, which keeps these tests on the
   sequential read path. ``TestCRAMIndexedUnmappedReads`` below covers the
   indexed path over the same data.
"""

import shutil

import polars as pl
import pysam
from _expected import DATA_DIR

import polars_bio as pb

HUFFMAN_CRAM = f"{DATA_DIR}/io/cram/huffman_byte_encoding.cram"

TOTAL_READS = 500
MAPPED_READS = 300
UNMAPPED_READS = 200
MAPPED_PER_CHROM = 150
READ_LENGTH = 60


class TestCRAMHuffmanByteEncoding:
    def test_scan_reads_all_records(self):
        """A full scan decodes the Huffman block instead of panicking."""
        df = pb.scan_cram(HUFFMAN_CRAM).collect()
        assert df.height == TOTAL_READS

    def test_read_matches_pysam(self):
        """Decoded sequences agree with an independent reader."""
        df = pb.read_cram(HUFFMAN_CRAM)

        with pysam.AlignmentFile(HUFFMAN_CRAM, "rc", check_sq=False) as fh:
            expected = [record.query_sequence for record in fh]

        assert df.height == len(expected)
        assert df["sequence"].to_list() == expected
        assert set(df["sequence"].unique().to_list()) == {"N" * READ_LENGTH}

    def test_mapped_reads_keep_their_reference(self):
        """Decoding the Huffman block leaves mapped records undisturbed."""
        df = pb.scan_cram(HUFFMAN_CRAM).collect()

        for chrom in ("chr1", "chr2"):
            assert df.filter(pl.col("chrom") == chrom).height == MAPPED_PER_CHROM

    def test_depth_on_huffman_cram(self):
        """`pb.depth` was the entry point in the original report (#429)."""
        result = pb.depth(HUFFMAN_CRAM, dense_mode="force", per_base=True).collect()

        assert result.height > 0
        assert set(result["contig"].unique().to_list()) <= {"chr1", "chr2"}


class TestCRAMIndexedUnmappedReads:
    """A scan returns the same records with or without a CRAI.

    Region queries cover only placed reads, so an indexed full scan used to omit
    the unplaced, unmapped tail — silently, and without an error. A CRAI does
    describe the unmapped slice (reference sequence ID -1), so the reader now
    seeks to it. Documented under "Unmapped reads and indexed scans" in
    docs/features/reading.md; these tests pin the documented row counts.
    """

    def test_indexed_and_unindexed_scans_agree(self, tmp_path):
        unindexed = tmp_path / "unindexed.cram"
        shutil.copy(HUFFMAN_CRAM, unindexed)

        indexed = tmp_path / "indexed.cram"
        shutil.copy(HUFFMAN_CRAM, indexed)
        pysam.index(str(indexed))
        assert (tmp_path / "indexed.cram.crai").exists()

        assert pb.scan_cram(str(unindexed)).collect().height == TOTAL_READS
        assert pb.scan_cram(str(indexed)).collect().height == TOTAL_READS

    def test_indexed_scan_exposes_unmapped_reads_with_null_chrom(self, tmp_path):
        cram = tmp_path / "indexed.cram"
        shutil.copy(HUFFMAN_CRAM, cram)
        pysam.index(str(cram))

        df = pb.scan_cram(str(cram)).collect()

        assert df.filter(pl.col("chrom").is_null()).height == UNMAPPED_READS
        assert df.filter(pl.col("chrom").is_not_null()).height == MAPPED_READS

    def test_indexed_scan_does_not_duplicate_records(self, tmp_path):
        cram = tmp_path / "indexed.cram"
        shutil.copy(HUFFMAN_CRAM, cram)
        pysam.index(str(cram))

        df = pb.scan_cram(str(cram)).collect()

        assert df["name"].n_unique() == TOTAL_READS

    def test_indexed_region_query_excludes_unmapped_reads(self, tmp_path):
        cram = tmp_path / "indexed.cram"
        shutil.copy(HUFFMAN_CRAM, cram)
        pysam.index(str(cram))

        result = pb.scan_cram(str(cram)).filter(pl.col("chrom") == "chr1").collect()

        assert result.height == MAPPED_PER_CHROM
