import polars_bio as pb


def test_set_option_accepts_numeric_values():
    key = "datafusion.execution.target_partitions"
    original = pb.get_option(key)
    try:
        pb.set_option(key, 2)
        assert pb.get_option(key) == "2"
    finally:
        if original is not None:
            pb.set_option(key, original)
