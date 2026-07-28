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
2. The fixture ships **without** a ``.crai`` on purpose. An indexed CRAM scan
   skips unmapped reads, so adding an index next to this file would stop these
   tests from covering the bug.
"""

import shutil

import polars as pl
import pysam
from _expected import DATA_DIR

import polars_bio as pb

HUFFMAN_CRAM = f"{DATA_DIR}/io/cram/huffman_byte_encoding.cram"

TOTAL_READS = 500
MAPPED_READS = 300
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


class TestCRAMIndexUnmappedReads:
    """A CRAI cannot address the unmapped tail, so an indexed scan omits it.

    Documented under "CRAM index (CRAI) limitations" in docs/features/reading.md.
    This test pins the behaviour so the documented row counts stay true.
    """

    def test_indexed_scan_omits_unmapped_reads(self, tmp_path):
        cram = tmp_path / "indexed.cram"
        shutil.copy(HUFFMAN_CRAM, cram)
        pysam.index(str(cram))
        assert (tmp_path / "indexed.cram.crai").exists()

        assert pb.scan_cram(str(cram)).collect().height == MAPPED_READS

    def test_unindexed_scan_returns_unmapped_reads(self, tmp_path):
        cram = tmp_path / "unindexed.cram"
        shutil.copy(HUFFMAN_CRAM, cram)

        assert pb.scan_cram(str(cram)).collect().height == TOTAL_READS
