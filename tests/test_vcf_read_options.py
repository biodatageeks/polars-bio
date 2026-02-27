from polars_bio.polars_bio import PyObjectStorageOptions, VcfReadOptions


def test_vcf_read_options_positional_args_backward_compatible():
    object_storage_options = PyObjectStorageOptions(
        allow_anonymous=False,
        enable_request_payer=False,
        compression_type="auto",
        chunk_size=64,
    )

    # Keep historical positional order:
    # (info_fields, format_fields, object_storage_options, zero_based)
    opts = VcfReadOptions(None, None, object_storage_options, False)

    assert opts.samples is None
    assert opts.zero_based is False


def test_vcf_read_options_samples_still_supported():
    opts = VcfReadOptions(samples=["HG002"])

    assert opts.samples == ["HG002"]
