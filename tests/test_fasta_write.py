"""Tests for FASTA write functionality."""

from pathlib import Path

import polars as pl
import pytest

import polars_bio as pb

TEST_DIR = Path(__file__).parent
DATA_DIR = TEST_DIR / "data"


class TestFastaWriteBasic:
    """Basic FASTA write tests."""

    def test_write_fasta_uncompressed(self, tmp_path):
        """Test writing uncompressed FASTA."""
        input_path = f"{DATA_DIR}/io/fasta/example.fasta"
        output_path = tmp_path / "output.fasta"

        df = pb.read_fasta(input_path)
        row_count = pb.write_fasta(df, str(output_path))

        assert row_count == len(df)
        assert output_path.exists()

        # Verify we can read it back
        df2 = pb.read_fasta(str(output_path))
        assert len(df2) == len(df)

    def test_write_fasta_gz(self, tmp_path):
        """Test writing gzip-compressed FASTA."""
        input_path = f"{DATA_DIR}/io/fasta/example.fasta"
        output_path = tmp_path / "output.fasta.gz"

        df = pb.read_fasta(input_path)
        row_count = pb.write_fasta(df, str(output_path))

        assert row_count == len(df)
        assert output_path.exists()

        # Verify we can read it back
        df2 = pb.read_fasta(str(output_path))
        assert len(df2) == len(df)

    def test_write_auto_compression_detection(self, tmp_path):
        """Test that compression is auto-detected from extension."""
        input_path = f"{DATA_DIR}/io/fasta/example.fasta"

        # Uncompressed
        output_fa = tmp_path / "test.fasta"
        df = pb.read_fasta(input_path)
        pb.write_fasta(df, str(output_fa))

        # Read first bytes to check it's not compressed
        with open(output_fa, "rb") as f:
            first_bytes = f.read(2)
        assert first_bytes != b"\x1f\x8b", "File should not be gzip compressed"

        # Compressed
        output_gz = tmp_path / "test.fasta.gz"
        pb.write_fasta(df, str(output_gz))

        # Read first bytes to check it is compressed
        with open(output_gz, "rb") as f:
            first_bytes = f.read(2)
        assert first_bytes == b"\x1f\x8b", "File should be gzip compressed"


class TestFastaRoundTrip:
    """Round-trip tests for FASTA read-write-read."""

    def test_roundtrip_basic(self, tmp_path):
        """Basic round-trip: read -> write -> read."""
        input_path = f"{DATA_DIR}/io/fasta/example.fasta"
        output_path = tmp_path / "roundtrip.fasta"

        df1 = pb.read_fasta(input_path)
        pb.write_fasta(df1, str(output_path))
        df2 = pb.read_fasta(str(output_path))

        assert df1.shape == df2.shape
        assert set(df1.columns) == set(df2.columns)

    def test_sequence_preserved(self, tmp_path):
        """Verify DNA sequences are preserved exactly."""
        input_path = f"{DATA_DIR}/io/fasta/example.fasta"
        output_path = tmp_path / "sequence_test.fasta"

        df1 = pb.read_fasta(input_path)
        pb.write_fasta(df1, str(output_path))
        df2 = pb.read_fasta(str(output_path))

        assert df1["sequence"].to_list() == df2["sequence"].to_list()

    def test_names_preserved(self, tmp_path):
        """Verify sequence names are preserved exactly."""
        input_path = f"{DATA_DIR}/io/fasta/example.fasta"
        output_path = tmp_path / "names_test.fasta"

        df1 = pb.read_fasta(input_path)
        pb.write_fasta(df1, str(output_path))
        df2 = pb.read_fasta(str(output_path))

        assert df1["name"].to_list() == df2["name"].to_list()

    def test_descriptions_preserved(self, tmp_path):
        """Verify descriptions are preserved (including None/empty)."""
        input_path = f"{DATA_DIR}/io/fasta/example.fasta"
        output_path = tmp_path / "desc_test.fasta"

        df1 = pb.read_fasta(input_path)
        pb.write_fasta(df1, str(output_path))
        df2 = pb.read_fasta(str(output_path))

        assert df1["description"].to_list() == df2["description"].to_list()

    def test_compressed_roundtrip(self, tmp_path):
        """Test round-trip with gzip compression."""
        input_path = f"{DATA_DIR}/io/fasta/example.fasta"
        output_path = tmp_path / "roundtrip.fasta.gz"

        df1 = pb.read_fasta(input_path)
        pb.write_fasta(df1, str(output_path))
        df2 = pb.read_fasta(str(output_path))

        assert df1.shape == df2.shape
        assert df1["sequence"].to_list() == df2["sequence"].to_list()
        assert df1["name"].to_list() == df2["name"].to_list()

    def test_bgz_compressed_roundtrip(self, tmp_path):
        """Test round-trip with BGZF compression."""
        input_path = f"{DATA_DIR}/io/fasta/example.fasta"
        output_path = tmp_path / "roundtrip.fasta.bgz"

        df1 = pb.read_fasta(input_path)
        pb.write_fasta(df1, str(output_path))
        df2 = pb.read_fasta(str(output_path))

        assert df1.shape == df2.shape
        assert df1["name"].to_list() == df2["name"].to_list()
        assert df1["sequence"].to_list() == df2["sequence"].to_list()
        assert df1["description"].to_list() == df2["description"].to_list()


class TestFastaSink:
    """Tests for sink_fasta streaming write."""

    def test_sink_fasta_lazy(self, tmp_path):
        """Test sink_fasta with LazyFrame."""
        input_path = f"{DATA_DIR}/io/fasta/example.fasta"
        output_path = tmp_path / "sink_output.fasta"

        lf = pb.scan_fasta(input_path)
        pb.sink_fasta(lf, str(output_path))

        assert output_path.exists()
        df = pb.read_fasta(str(output_path))
        assert len(df) == 10  # example.fasta has 10 records

    def test_sink_fasta_with_limit(self, tmp_path):
        """Test sink_fasta with limited LazyFrame."""
        input_path = f"{DATA_DIR}/io/fasta/example.fasta"
        output_path = tmp_path / "sink_limited.fasta"

        lf = pb.scan_fasta(input_path).limit(3)
        pb.sink_fasta(lf, str(output_path))

        assert output_path.exists()
        df = pb.read_fasta(str(output_path))
        assert len(df) == 3

    def test_sink_fasta_compressed(self, tmp_path):
        """Test sink_fasta with gzip compression."""
        input_path = f"{DATA_DIR}/io/fasta/example.fasta"
        output_path = tmp_path / "sink_output.fasta.gz"

        lf = pb.scan_fasta(input_path)
        pb.sink_fasta(lf, str(output_path))

        assert output_path.exists()
        df = pb.read_fasta(str(output_path))
        assert len(df) == 10


class TestFastaFromPolarsDataFrame:
    """Tests for writing FASTA from constructed DataFrames."""

    def test_write_from_polars_df(self, tmp_path):
        """Test writing a Polars DataFrame constructed in memory."""
        output_path = tmp_path / "from_df.fasta"

        df = pl.DataFrame(
            {
                "name": ["seq1", "seq2", "seq3"],
                "description": ["first sequence", "second sequence", "third sequence"],
                "sequence": ["ATCG", "GATTACA", "NNNNNNNNNN"],
            }
        )

        row_count = pb.write_fasta(df, str(output_path))
        assert row_count == 3

        df2 = pb.read_fasta(str(output_path))
        assert len(df2) == 3
        assert df2["sequence"].to_list() == ["ATCG", "GATTACA", "NNNNNNNNNN"]

    def test_write_long_sequences(self, tmp_path):
        """Test writing sequences longer than 80 characters (FASTA line width)."""
        output_path = tmp_path / "long_seq.fasta"

        long_seq = "ATCG" * 100  # 400 chars
        df = pl.DataFrame(
            {
                "name": ["long_read"],
                "description": ["a very long sequence"],
                "sequence": [long_seq],
            }
        )

        row_count = pb.write_fasta(df, str(output_path))
        assert row_count == 1

        df2 = pb.read_fasta(str(output_path))
        assert df2["sequence"].to_list() == [long_seq]

    def test_polars_namespace_write(self, tmp_path):
        """Test writing via the df.pb.write_fasta() namespace."""
        input_path = f"{DATA_DIR}/io/fasta/example.fasta"
        output_path = tmp_path / "namespace_output.fasta"

        df = pb.read_fasta(input_path)
        row_count = df.pb.write_fasta(str(output_path))

        assert row_count == len(df)
        df2 = pb.read_fasta(str(output_path))
        assert len(df2) == len(df)

    def test_polars_namespace_sink(self, tmp_path):
        """Test streaming write via the lf.pb.sink_fasta() namespace."""
        input_path = f"{DATA_DIR}/io/fasta/example.fasta"
        output_path = tmp_path / "namespace_sink.fasta"

        lf = pb.scan_fasta(input_path)
        lf.pb.sink_fasta(str(output_path))

        assert output_path.exists()
        df = pb.read_fasta(str(output_path))
        assert len(df) == 10
