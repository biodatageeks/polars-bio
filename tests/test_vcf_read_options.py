import pytest

import polars_bio as pb
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
    assert opts.genotype_output == "string"


def test_vcf_read_options_samples_still_supported():
    opts = VcfReadOptions(samples=["HG002"])

    assert opts.samples == ["HG002"]


def test_vcf_read_options_accepts_explicit_dosage_output():
    opts = VcfReadOptions(genotype_output="dosage")

    assert opts.genotype_output == "dosage"


def test_scan_bcf_rejects_unknown_genotype_output_before_opening_source():
    with pytest.raises(ValueError, match="genotype_output must be either"):
        pb.scan_bcf("unused.bcf", genotype_output="alleles")
