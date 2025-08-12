from pandas.testing import assert_frame_equal

import polars_bio as pb


def test_read_fastq_parallel():
    """
    Compare the results of reading a FASTQ file with parallel=False and parallel=True
    for different numbers of target partitions.
    """
    file_path = "tests/data/io/fastq/sample_parallel.fastq.bgz"

    # 1. Get the baseline correct DataFrame by reading without parallelism.
    # Use a fresh context to ensure no leftover settings interfere.
    pb.set_option("datafusion.execution.target_partitions", "1")
    expected_df = pb.read_fastq(file_path, parallel=False).collect().to_pandas()

    # 2. Test parallel reading with different partition counts.
    for i in [1, 2, 3, 4]:
        # Use a fresh context for each run to ensure the setting is applied correctly.
        pb.set_option("datafusion.execution.target_partitions", str(i))

        # Read with parallelism enabled
        result_df = pb.read_fastq(file_path, parallel=True).collect().to_pandas()

        # 3. Compare the results.
        # We sort by name to ensure the order is consistent, as parallel execution
        # does not guarantee row order.
        expected_sorted = expected_df.sort_values("name").reset_index(drop=True)
        result_sorted = result_df.sort_values("name").reset_index(drop=True)

        assert_frame_equal(result_sorted, expected_sorted, check_like=True)
