import os
from pathlib import Path

import polars as pl
import pytest

import polars_bio as pb

# Define a temporary directory for test artifacts
TMP_DIR = Path("/tmp/polars_bio_it_ensembl_bgz")


@pytest.fixture(scope="module", autouse=True)
def setup_teardown():
    """Set up the temporary directory before tests and clean it up after."""
    TMP_DIR.mkdir(exist_ok=True)
    yield
    for f in TMP_DIR.glob("*"):
        try:
            f.unlink()
        except OSError as e:
            print(f"Error while deleting file {f}: {e}")
    try:
        TMP_DIR.rmdir()
    except OSError as e:
        print(f"Error while deleting directory {TMP_DIR}: {e}")


class TestEnsemblVCFIntegrationWithBgzip:
    VCF_URLS = [
        "https://ftp.ensembl.org/pub/current_variation/vcf/homo_sapiens/homo_sapiens_phenotype_associated.vcf.gz",
        "https://ftp.ensembl.org/pub/current_variation/vcf/homo_sapiens/homo_sapiens-chr15.vcf.gz",
    ]

    @pytest.mark.parametrize("url", VCF_URLS)
    def test_download_read_write_vcf_with_bgzip(self, url):
        """
        Tests the full pipeline of downloading a VCF, describing it, reading it with
        all info fields, and writing it to Parquet, specifying bgzip compression.
        """
        filename = url.split("/")[-1]
        local_vcf_path = TMP_DIR / filename
        local_parquet_path = TMP_DIR / f"{filename}.parquet"

        # 1. Download the file
        result = os.system(f"curl -L --fail -o {local_vcf_path} {url}")
        assert result == 0, f"curl command failed with exit code {result}"
        assert local_vcf_path.exists(), f"Failed to download {url}"

        # 2. Describe VCF to get all INFO fields
        info_df = pb.describe_vcf(str(local_vcf_path), compression_type="bgz")
        assert isinstance(info_df, pl.DataFrame)
        info_fields = info_df.get_column("name").to_list()
        assert info_fields, f"No INFO fields found in {local_vcf_path}"

        # 3. Read VCF with all info fields
        df = pb.read_vcf(
            str(local_vcf_path), info_fields=info_fields, compression_type="bgz"
        ).collect()

        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0, "VCF file was read as empty"

        # Check that info fields were parsed as columns (parser lowercases them)
        for field in info_fields:
            assert (
                field.lower() in df.columns
            ), f"Expected info field '{field.lower()}' not in DataFrame"

        # 4. Save to Parquet
        df.write_parquet(local_parquet_path)
        assert local_parquet_path.exists(), "Failed to write Parquet file"
