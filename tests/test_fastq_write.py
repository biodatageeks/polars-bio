"""Tests for FASTQ write functionality."""

from pathlib import Path

import polars as pl
import pytest

import polars_bio as pb

TEST_DIR = Path(__file__).parent
DATA_DIR = TEST_DIR / "data"


class TestFastqWriteBasic:
    """Basic FASTQ write tests."""

    def test_write_fastq_uncompressed(self, tmp_path):
        """Test writing uncompressed FASTQ."""
        input_path = f"{DATA_DIR}/io/fastq/example.fastq"
        output_path = tmp_path / "output.fastq"

        df = pb.read_fastq(input_path)
        row_count = pb.write_fastq(df, str(output_path))

        assert row_count == len(df)
        assert output_path.exists()

        # Verify we can read it back
        df2 = pb.read_fastq(str(output_path))
        assert len(df2) == len(df)

    def test_write_fastq_gz(self, tmp_path):
        """Test writing gzip-compressed FASTQ."""
        input_path = f"{DATA_DIR}/io/fastq/example.fastq"
        output_path = tmp_path / "output.fastq.gz"

        df = pb.read_fastq(input_path)
        row_count = pb.write_fastq(df, str(output_path))

        assert row_count == len(df)
        assert output_path.exists()

        # Verify we can read it back
        df2 = pb.read_fastq(str(output_path))
        assert len(df2) == len(df)

    def test_write_fastq_documented_column_order(self, tmp_path):
        """Test writing FASTQ data with columns ordered as documented."""
        output_path = tmp_path / "documented_order.fastq"
        sequence = "ACGT" * 37
        quality_scores = "I" * len(sequence)
        df = pl.DataFrame(
            {
                "name": ["read1"],
                "sequence": [sequence],
                "quality_scores": [quality_scores],
            }
        ).with_columns(pl.lit(None).alias("description"))

        row_count = pb.write_fastq(df, str(output_path))

        assert row_count == 1
        df2 = pb.read_fastq(str(output_path))
        assert df2["sequence"].to_list() == [sequence]
        assert df2["quality_scores"].to_list() == [quality_scores]

    def test_write_fastq_without_description(self, tmp_path):
        """Test writing FASTQ data without the optional description column."""
        output_path = tmp_path / "without_description.fastq"
        sequence = "ACGT"
        quality_scores = "IIII"
        df = pl.DataFrame(
            {
                "name": ["read1"],
                "sequence": [sequence],
                "quality_scores": [quality_scores],
            }
        )

        row_count = pb.write_fastq(df, str(output_path))

        assert row_count == 1
        df2 = pb.read_fastq(str(output_path))
        assert df2["description"].to_list() == [None]
        assert df2["sequence"].to_list() == [sequence]
        assert df2["quality_scores"].to_list() == [quality_scores]

    def test_write_fastq_shuffled_columns(self, tmp_path):
        """Test writing FASTQ data when columns are not in writer order."""
        output_path = tmp_path / "shuffled.fastq"
        sequence = "ACGT"
        quality_scores = "IIII"
        df = pl.DataFrame(
            {
                "quality_scores": [quality_scores],
                "sequence": [sequence],
                "name": ["read1"],
            }
        )

        row_count = pb.write_fastq(df, str(output_path))

        assert row_count == 1
        df2 = pb.read_fastq(str(output_path))
        assert df2["name"].to_list() == ["read1"]
        assert df2["description"].to_list() == [None]
        assert df2["sequence"].to_list() == [sequence]
        assert df2["quality_scores"].to_list() == [quality_scores]

    def test_write_auto_compression_detection(self, tmp_path):
        """Test that compression is auto-detected from extension."""
        input_path = f"{DATA_DIR}/io/fastq/example.fastq"

        # Uncompressed
        output_fq = tmp_path / "test.fastq"
        df = pb.read_fastq(input_path)
        pb.write_fastq(df, str(output_fq))

        # Read first bytes to check it's not compressed
        with open(output_fq, "rb") as f:
            first_bytes = f.read(2)
        assert first_bytes != b"\x1f\x8b", "File should not be gzip compressed"

        # Compressed
        output_gz = tmp_path / "test.fastq.gz"
        pb.write_fastq(df, str(output_gz))

        # Read first bytes to check it is compressed
        with open(output_gz, "rb") as f:
            first_bytes = f.read(2)
        assert first_bytes == b"\x1f\x8b", "File should be gzip compressed"


class TestFastqRoundTrip:
    """Round-trip tests for FASTQ read-write-read."""

    def test_roundtrip_basic(self, tmp_path):
        """Basic round-trip: read -> write -> read."""
        input_path = f"{DATA_DIR}/io/fastq/example.fastq"
        output_path = tmp_path / "roundtrip.fastq"

        df1 = pb.read_fastq(input_path)
        pb.write_fastq(df1, str(output_path))
        df2 = pb.read_fastq(str(output_path))

        assert df1.shape == df2.shape
        assert set(df1.columns) == set(df2.columns)

    def test_sequence_preserved(self, tmp_path):
        """Verify DNA sequences are preserved exactly."""
        input_path = f"{DATA_DIR}/io/fastq/example.fastq"
        output_path = tmp_path / "sequence_test.fastq"

        df1 = pb.read_fastq(input_path)
        pb.write_fastq(df1, str(output_path))
        df2 = pb.read_fastq(str(output_path))

        assert df1["sequence"].to_list() == df2["sequence"].to_list()

    def test_quality_scores_preserved(self, tmp_path):
        """Verify quality scores are preserved exactly."""
        input_path = f"{DATA_DIR}/io/fastq/example.fastq"
        output_path = tmp_path / "quality_test.fastq"

        df1 = pb.read_fastq(input_path)
        pb.write_fastq(df1, str(output_path))
        df2 = pb.read_fastq(str(output_path))

        assert df1["quality_scores"].to_list() == df2["quality_scores"].to_list()

    def test_names_preserved(self, tmp_path):
        """Verify read names are preserved exactly."""
        input_path = f"{DATA_DIR}/io/fastq/example.fastq"
        output_path = tmp_path / "names_test.fastq"

        df1 = pb.read_fastq(input_path)
        pb.write_fastq(df1, str(output_path))
        df2 = pb.read_fastq(str(output_path))

        assert df1["name"].to_list() == df2["name"].to_list()

    def test_compressed_roundtrip(self, tmp_path):
        """Test round-trip with gzip compression."""
        input_path = f"{DATA_DIR}/io/fastq/example.fastq"
        output_path = tmp_path / "roundtrip.fastq.gz"

        df1 = pb.read_fastq(input_path)
        pb.write_fastq(df1, str(output_path))
        df2 = pb.read_fastq(str(output_path))

        assert df1.shape == df2.shape
        assert df1["sequence"].to_list() == df2["sequence"].to_list()
        assert df1["quality_scores"].to_list() == df2["quality_scores"].to_list()

    def test_bgz_compressed_roundtrip(self, tmp_path):
        """Test round-trip with BGZF compression."""
        input_path = f"{DATA_DIR}/io/fastq/example.fastq"
        output_path = tmp_path / "roundtrip.fastq.bgz"

        df1 = pb.read_fastq(input_path)
        pb.write_fastq(df1, str(output_path))
        df2 = pb.read_fastq(str(output_path))

        assert df1.shape == df2.shape
        assert df1["name"].to_list() == df2["name"].to_list()
        assert df1["sequence"].to_list() == df2["sequence"].to_list()
        assert df1["quality_scores"].to_list() == df2["quality_scores"].to_list()


class TestFastqSink:
    """Tests for sink_fastq streaming write."""

    def test_sink_fastq_lazy(self, tmp_path):
        """Test sink_fastq with LazyFrame."""
        input_path = f"{DATA_DIR}/io/fastq/example.fastq"
        output_path = tmp_path / "sink_output.fastq"

        lf = pb.scan_fastq(input_path)
        pb.sink_fastq(lf, str(output_path))

        assert output_path.exists()
        df = pb.read_fastq(str(output_path))
        assert len(df) == 200  # example.fastq has 200 records

    def test_sink_fastq_with_limit(self, tmp_path):
        """Test sink_fastq with limited LazyFrame."""
        input_path = f"{DATA_DIR}/io/fastq/example.fastq"
        output_path = tmp_path / "sink_limited.fastq"

        lf = pb.scan_fastq(input_path).limit(10)
        pb.sink_fastq(lf, str(output_path))

        assert output_path.exists()
        df = pb.read_fastq(str(output_path))
        assert len(df) == 10

    def test_sink_fastq_documented_column_order(self, tmp_path):
        """Test streaming FASTQ data with columns ordered as documented."""
        output_path = tmp_path / "sink_documented_order.fastq"
        sequence = "ACGT" * 37
        quality_scores = "I" * len(sequence)
        df = pl.DataFrame(
            {
                "name": ["read1"],
                "sequence": [sequence],
                "quality_scores": [quality_scores],
            }
        ).with_columns(pl.lit(None).alias("description"))

        pb.sink_fastq(df.lazy(), str(output_path))

        df2 = pb.read_fastq(str(output_path))
        assert df2["sequence"].to_list() == [sequence]
        assert df2["quality_scores"].to_list() == [quality_scores]

    def test_sink_fastq_from_parquet_shuffled_columns(self, tmp_path):
        """Test streaming FASTQ data from parquet with columns in source order."""
        parquet_path = tmp_path / "shuffled.parquet"
        output_path = tmp_path / "sink_shuffled.fastq"
        sequence = "ACGT"
        quality_scores = "IIII"
        df = pl.DataFrame(
            {
                "quality_scores": [quality_scores],
                "sequence": [sequence],
                "name": ["read1"],
            }
        )
        df.write_parquet(parquet_path)

        pb.sink_fastq(pl.scan_parquet(parquet_path), str(output_path))

        df2 = pb.read_fastq(str(output_path))
        assert df2["name"].to_list() == ["read1"]
        assert df2["description"].to_list() == [None]
        assert df2["sequence"].to_list() == [sequence]
        assert df2["quality_scores"].to_list() == [quality_scores]


class TestFastqFromCompressed:
    """Tests for writing from compressed input sources."""

    def test_read_bgz_write_uncompressed(self, tmp_path):
        """Test reading BGZF and writing uncompressed."""
        input_path = f"{DATA_DIR}/io/fastq/example.fastq.bgz"
        output_path = tmp_path / "from_bgz.fastq"

        df = pb.read_fastq(input_path)
        pb.write_fastq(df, str(output_path))

        df2 = pb.read_fastq(str(output_path))
        assert df["sequence"].to_list() == df2["sequence"].to_list()

    def test_read_gz_write_uncompressed(self, tmp_path):
        """Test reading GZIP and writing uncompressed."""
        input_path = f"{DATA_DIR}/io/fastq/example.fastq.gz"
        output_path = tmp_path / "from_gz.fastq"

        df = pb.read_fastq(input_path)
        pb.write_fastq(df, str(output_path))

        df2 = pb.read_fastq(str(output_path))
        assert df["sequence"].to_list() == df2["sequence"].to_list()
