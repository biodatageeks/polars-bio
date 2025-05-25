import polars as pl
import polars_bio as pb
import pytest
from _expected import DATA_DIR

@pytest.fixture
def test_fastq_file():
	return f"{DATA_DIR}/base_quality/example.fastq"

@pytest.fixture
def fastq_df(test_fastq_file):
	return pb.read_fastq(test_fastq_file)

def test_base_sequence_quality_udaf(fastq_df):
	# from fastqc report.html hydrated data field
	expected_first = [0.0, 30.135, 26.5, 31.0, 33.0, 34.0, 38.5]
	res_df = pb.quality_udaf(fastq_df)
	result = res_df.collect()[0][0][0][0].as_py()
	assert expected_first == result
# def test_base_sequence_quality_basic(fastq_df):
# 	"""Test basic functionality of base sequence quality calculation"""
# 	# Calculate quality metrics
# 	res_df = pb.base_sequence_quality(fastq_df)
# 	quality_df = res_df.collect()[0]
# 	# Check output schema
# 	expected_columns = [
# 		"pos", "average", "upper", "q3", "median", "q1", "lower"
# 	]
# 	assert all(col in quality_df.columns for col in expected_columns)
# 
# 	# Check that we have the correct number of positions
# 	# First sequence is 35 bases, second is 11 bases
# 	assert quality_df["pos"].max() == 34  # 0-based indexing
# 
# 	# Check that quality scores are within expected ranges
# 	assert quality_df["lower"].min() >= 0  # Phred+33 minimum
# 	assert quality_df["upper"].max() <= 40  # Phred+33 typical maximum
# 
# 	# Check that statistics are properly ordered
# 	assert (quality_df["lower"] <= quality_df["q1"]).all()
# 	assert (quality_df["q1"] <= quality_df["median"]).all()
# 	assert (quality_df["median"] <= quality_df["q3"]).all()
# 	assert (quality_df["q3"] <= quality_df["upper"]).all()
#
# def test_base_sequence_quality_values(fastq_df):
# 	"""Test specific quality score calculations"""
# 	quality_df = pb.base_sequence_quality(fastq_df)
#
# 	# Test first position (should be '!' in first read, 'I' in second read)
# 	first_pos = quality_df.filter(pl.col("pos") == 0)
# 	assert first_pos["lower"].item() == 0  # '!' = 33 - 33 = 0
# 	assert first_pos["upper"].item() == 40  # 'I' = 73 - 33 = 40
#
# 	# Test a position with consistent quality (position 5 in second read)
# 	# All 'I' characters should give same quality score
# 	pos_5 = quality_df.filter(pl.col("pos") == 5)
# 	assert pos_5["lower"].item() == pos_5["upper"].item()
# 	assert pos_5["q1"].item() == pos_5["q3"].item()
# 	assert pos_5["median"].item() == pos_5["average"].item()
#
# def test_base_sequence_quality_empty_input():
# 	"""Test behavior with empty input"""
# 	empty_df = pl.DataFrame({
# 		"name": [],
# 		"sequence": [],
# 		"quality_scores": []
# 	})
#
# 	with pytest.raises(Exception):
# 		pb.base_sequence_quality(empty_df)
#
# def test_base_sequence_quality_missing_columns():
# 	"""Test behavior with missing required columns"""
# 	invalid_df = pl.DataFrame({
# 		"name": ["SEQ1"],
# 		"sequence": ["ACGT"]
# 	})
#
# 	with pytest.raises(Exception):
# 		pb.base_sequence_quality(invalid_df)
#
# def test_base_sequence_quality_invalid_scores(fastq_df):
# 	"""Test behavior with invalid quality scores"""
# 	# Modify the quality scores to include invalid characters
# 	modified_df = fastq_df.with_columns(
# 		pl.col("quality_scores").str.replace("!", "@")  # '@' is invalid in Phred+33
# 	)
#
# 	with pytest.raises(Exception):
# 		pb.base_sequence_quality(modified_df)
#
# def test_base_sequence_quality_parallel_processing(fastq_df):
# 	"""Test that the operation works with parallel processing"""
# 	# Create a larger dataset by duplicating the test data
# 	large_df = pl.concat([fastq_df] * 100)
#
# 	# Calculate quality metrics
# 	quality_df = pb.base_sequence_quality(large_df)
#
# 	# Verify results are the same as with single copy
# 	single_quality_df = pb.base_sequence_quality(fastq_df)
# 	assert quality_df.equals(single_quality_df)
#
# def test_base_sequence_quality_memory_efficiency(fastq_df):
# 	"""Test that the operation is memory efficient"""
# 	import psutil
# 	import os
#
# 	process = psutil.Process(os.getpid())
# 	initial_memory = process.memory_info().rss
#
# 	# Create a large dataset
# 	large_df = pl.concat([fastq_df] * 1000)
#
# 	# Calculate quality metrics
# 	quality_df = pb.base_sequence_quality(large_df)
#
# 	# Check memory usage
# 	final_memory = process.memory_info().rss
# 	memory_increase = final_memory - initial_memory
#
# 	# Memory increase should be reasonable (less than 1GB)
# 	assert memory_increase < 1024 * 1024 * 1024
#
# def test_base_sequence_quality_visualization(fastq_df):
# 	"""Test the visualization functionality"""
# 	from polars_bio.base_quality_viz import plot_base_quality
#
# 	# Calculate quality metrics
# 	quality_df = pb.base_sequence_quality(fastq_df)
#
# 	# Create visualization
# 	fig = plot_base_quality(quality_df)
#
# 	# Check that the figure has the expected components
# 	assert len(fig.data) == 4  # Box plot + Q1/Q3 lines + median line + average line
# 	assert fig.layout.title.text == "Phred score"
# 	assert fig.layout.xaxis.title.text == "Phred score"
# 	assert fig.layout.yaxis.title.text == "Position in read (bp)"