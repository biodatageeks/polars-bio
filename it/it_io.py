import os

import pytest

import polars_bio as pb


class TestIOVCFInfo:
    vcf_priv = "s3://polarsbio/vep.vcf.bgz"
    vcf_pub = "s3://polarsbiopublic/vep.vcf.bgz"
    vcf_aws_pub = "s3://gnomad-public-us-east-1/release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr21.vcf.bgz"

    def test_count_priv(self):
        vcf_infos_mixed_cases = (
            pb.read_vcf(self.vcf_priv, thread_num=1, allow_anonymous=False)
            .limit(1)
            .collect()
        )
        assert len(vcf_infos_mixed_cases) == 1

    @pytest.mark.xfail(strict=True)
    def test_count_minio_pub_no_anonymous(self):
        os.unsetenv("AWS_ACCESS_KEY_ID")
        os.unsetenv("AWS_SECRET_ACCESS_KEY")
        vcf_infos_mixed_cases = (
            pb.read_vcf(
                self.vcf_pub, thread_num=1, allow_anonymous=False, max_retries=0
            )
            .limit(1)
            .collect()
        )
        assert len(vcf_infos_mixed_cases) == 1

    def test_count_minio_pub_anonymous(self):
        os.unsetenv("AWS_ACCESS_KEY_ID")
        os.unsetenv("AWS_SECRET_ACCESS_KEY")
        vcf_infos_mixed_cases = (
            pb.read_vcf(self.vcf_pub, thread_num=1, allow_anonymous=True)
            .limit(1)
            .collect()
        )
        assert len(vcf_infos_mixed_cases) == 1

    def test_count_aws_pub_anonymous(self):
        os.unsetenv("AWS_ACCESS_KEY_ID")
        os.unsetenv("AWS_SECRET_ACCESS_KEY")
        os.unsetenv("AWS_ENDPOINT_URL")
        os.environ["AWS_REGION"] = "us-east-1"
        vcf_infos_mixed_cases = (
            pb.read_vcf(self.vcf_aws_pub, thread_num=1, allow_anonymous=True)
            .limit(1)
            .collect()
        )
        assert len(vcf_infos_mixed_cases) == 1
