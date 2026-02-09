from pandas.testing import assert_frame_equal

import polars_bio as pb


def test_read_fastq_parallel():
    """
    Compare the results of reading a FASTQ file with 1 partition vs multiple partitions.
    Parallel reads are now automatic when target_partitions > 1 and a GZI index is present.
    """
    file_path = "tests/data/io/fastq/sample_parallel.fastq.bgz"

    # 1. Get the baseline DataFrame by reading with a single partition.
    pb.set_option("datafusion.execution.target_partitions", "1")
    expected_df = pb.read_fastq(file_path).to_pandas()

    # 2. Test with different partition counts (parallel reads kick in automatically).
    for i in [1, 2, 3, 4]:
        pb.set_option("datafusion.execution.target_partitions", str(i))

        result_df = pb.read_fastq(file_path).to_pandas()

        # 3. Compare the results.
        # We sort by name to ensure the order is consistent, as parallel execution
        # does not guarantee row order.
        expected_sorted = expected_df.sort_values("name").reset_index(drop=True)
        result_sorted = result_df.sort_values("name").reset_index(drop=True)

        assert_frame_equal(result_sorted, expected_sorted, check_like=True)
