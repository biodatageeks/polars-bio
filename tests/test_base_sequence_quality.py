from pathlib import Path
import polars as pl
import pandas as pd

import pytest

import polars_bio as pb


class TestBaseSequenceQuality:
    def record_header(self, len: int):
        return f"@test\n{'N'*len}\n+\n"

    def simple_test_data(self):
        data = self.record_header(1) + chr(5 + 33) + "\n"
        result = pl.DataFrame(
            {
                "pos": [0],
                "avg": [5.0],
                "q1": [5.0],
                "median": [5.0],
                "q3": [5.0],
                "lower": [5.0],
                "upper": [5.0],
            }
        )

        return data, result

    @pytest.mark.usefixtures("tmp_path")
    def test_result_from_empty_fastq_should_be_empty_df_polars(self, tmp_path: Path):
        fastq_file = tmp_path / "test.fastq"
        fastq_file.write_text("", encoding="ascii")

        result = pb.base_sequence_quality(fastq_file)
        assert isinstance(result, pl.DataFrame)
        assert pb.base_sequence_quality(fastq_file).is_empty()

    @pytest.mark.usefixtures("tmp_path")
    def test_result_from_empty_fastq_should_be_empty_df_pandas(self, tmp_path: Path):
        fastq_file = tmp_path / "test.fastq"
        fastq_file.write_text("", encoding="ascii")

        result = pb.base_sequence_quality(fastq_file, output_type="pandas.DataFrame")
        assert isinstance(result, pd.DataFrame)
        assert pb.base_sequence_quality(fastq_file).is_empty()

    @pytest.mark.usefixtures("tmp_path")
    def test_one_record_one_length(self, tmp_path: Path):
        data, expected_result = self.simple_test_data()
        fastq_file = tmp_path / "test.fastq"
        fastq_file.write_text(data, encoding="ascii")

        result = pb.base_sequence_quality(fastq_file)
        assert result.equals(expected_result)

    @pytest.mark.usefixtures("tmp_path")
    def test_one_record_one_length_lazyframe(self, tmp_path: Path):
        data, expected_result = self.simple_test_data()
        fastq_file = tmp_path / "test.fastq"
        fastq_file.write_text(data, encoding="ascii")

        lf = pb.read_fastq(str(fastq_file))
        result = pb.base_sequence_quality(lf)

        assert result.equals(expected_result)

    @pytest.mark.usefixtures("tmp_path")
    def test_one_record_one_length_polars(self, tmp_path: Path):
        data, expected_result = self.simple_test_data()
        fastq_file = tmp_path / "test.fastq"
        fastq_file.write_text(data, encoding="ascii")

        lf = pb.read_fastq(str(fastq_file))
        polars_df = lf.collect()
        result = pb.base_sequence_quality(polars_df)

        assert result.equals(expected_result)

    @pytest.mark.usefixtures("tmp_path")
    def test_one_record_one_length_pandas(self, tmp_path: Path):
        data, expected_result = self.simple_test_data()
        fastq_file = tmp_path / "test.fastq"
        fastq_file.write_text(data, encoding="ascii")

        lf = pb.read_fastq(str(fastq_file))
        pandas_df = lf.collect().to_pandas()
        result = pb.base_sequence_quality(pandas_df)

        assert result.equals(expected_result)

    @pytest.mark.usefixtures("tmp_path")
    def test_one_record_two_length(self, tmp_path: Path):
        fastq_file = tmp_path / "test.fastq"
        fastq_file.write_text(
            self.record_header(1) + chr(5 + 33) + chr(6 + 33) + "\n",
            encoding="ascii",
        )

        result = pb.base_sequence_quality(fastq_file).sort(by="pos")
        assert result.equals(
            pl.DataFrame(
                {
                    "pos": [0, 1],
                    "avg": [5.0, 6.0],
                    "q1": [5.0, 6.0],
                    "median": [5.0, 6.0],
                    "q3": [5.0, 6.0],
                    "lower": [5.0, 6.0],
                    "upper": [5.0, 6.0],
                }
            )
        )

    @pytest.mark.usefixtures("tmp_path")
    def test_two_record_one_length(self, tmp_path: Path):
        fastq_file = tmp_path / "test.fastq"

        file_content = ""
        for read in [0, 2]:
            file_content += self.record_header(2) + chr(read + 33) + "\n"

        fastq_file.write_text(file_content, encoding="ascii")

        result = pb.base_sequence_quality(fastq_file)
        assert result.equals(
            pl.DataFrame(
                {
                    "pos": [0],
                    "avg": [1.0],
                    "q1": [0.5],
                    "median": [1.0],
                    "q3": [1.5],
                    "lower": [-1.0],
                    "upper": [3.0],
                }
            )
        )

    @pytest.mark.usefixtures("tmp_path")
    def test_three_record_one_length(self, tmp_path: Path):
        fastq_file = tmp_path / "test.fastq"

        file_content = ""
        for read in [0, 2, 4]:
            file_content += self.record_header(3) + chr(read + 33) + "\n"

        fastq_file.write_text(file_content, encoding="ascii")

        result = pb.base_sequence_quality(fastq_file)
        assert result.equals(
            pl.DataFrame(
                {
                    "pos": [0],
                    "avg": [2.0],
                    "q1": [1.0],
                    "median": [2.0],
                    "q3": [3.0],
                    "lower": [-2.0],
                    "upper": [6.0],
                }
            )
        )

    @pytest.mark.usefixtures("tmp_path")
    def test_four_record_one_length(self, tmp_path: Path):
        fastq_file = tmp_path / "test.fastq"

        file_content = ""
        for read in [0, 2, 4, 9]:
            file_content += self.record_header(3) + chr(read + 33) + "\n"

        fastq_file.write_text(file_content, encoding="ascii")

        result = pb.base_sequence_quality(fastq_file)
        assert result.equals(
            pl.DataFrame(
                {
                    "pos": [0],
                    "avg": [3.75],
                    "q1": [1.5],
                    "median": [3.0],
                    "q3": [5.25],
                    "lower": [-4.125],
                    "upper": [10.875],
                }
            )
        )

    @pytest.mark.usefixtures("tmp_path")
    def test_four_record_two_length(self, tmp_path: Path):
        fastq_file = tmp_path / "test.fastq"

        file_content = ""
        for read1, read2 in zip([0, 2, 4, 9], [1, 3, 5, 10]):
            file_content += (
                self.record_header(3) + chr(read1 + 33) + chr(read2 + 33) + "\n"
            )

        fastq_file.write_text(file_content, encoding="ascii")

        result = pb.base_sequence_quality(fastq_file).sort(by="pos")
        assert result.equals(
            pl.DataFrame(
                {
                    "pos": [0, 1],
                    "avg": [3.75, 4.75],
                    "q1": [1.5, 2.5],
                    "median": [3.0, 4.0],
                    "q3": [5.25, 6.25],
                    "lower": [-4.125, -3.125],
                    "upper": [10.875, 11.875],
                }
            )
        )
