import os

import pytest

import polars_bio as pb


class TestIOVCFAZBLOB:
    vcf_big = "http://127.0.0.1:10000/devstoreaccount1/polarsbio/vep.vcf.bgz"
    vcf_infos_mixed_cases = (
        pb.read_vcf(vcf_big, info_fields=[], thread_num=1, allow_anonymous=False)
        .limit(1)
        .collect()
    )

    def test_count(self):
        assert len(self.vcf_infos_mixed_cases) == 1


class TestIOVCFS3:
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


class TestIOVCFGCS:
    vcf_big = "gs://gcp-public-data--gnomad/release/2.1.1/liftover_grch38/vcf/genomes/gnomad.genomes.r2.1.1.sites.liftover_grch38.vcf.bgz"
    vcf_infos_mixed_cases = (
        pb.read_vcf(
            vcf_big, info_fields=["AF", "vep"], thread_num=1, allow_anonymous=True
        )
        .limit(1)
        .collect()
    )

    def test_count(self):
        assert len(self.vcf_infos_mixed_cases) == 1


class TestVCFViewsOperations:
    def test_view(self):
        vcf_big = "gs://gcp-public-data--gnomad/release/2.1.1/liftover_grch38/vcf/genomes/gnomad.genomes.r2.1.1.sites.liftover_grch38.vcf.bgz"
        pb.register_vcf(
            vcf_big,
            "gnomad_big",
            info_fields=["AF", "vep"],
            thread_num=1,
            allow_anonymous=True,
        )
        pb.register_view(
            "v_gnomad_big",
            "SELECT chrom, start, end, split_part(vep, '|', 3) AS impact from gnomad_big where array_element(af,1)=0 and split_part(vep, '|', 3) in ('HIGH', 'MODERATE') limit 10",
        )
        vcf_sv = "gs://gcp-public-data--gnomad/release/4.1/genome_sv/gnomad.v4.1.sv.sites.vcf.gz"
        pb.register_vcf(
            vcf_sv,
            "gnomad_sv",
            thread_num=1,
            info_fields=["SVTYPE", "SVLEN"],
            allow_anonymous=True,
        )
        pb.register_view(
            "v_gnomad_sv", "SELECT chrom, start, end FROM gnomad_sv limit 100"
        )
        assert len(pb.sql("SELECT * FROM v_gnomad_big").collect()) == 10
        assert len(pb.nearest("v_gnomad_sv", "v_gnomad_big").collect()) == 100
        assert len(pb.overlap("v_gnomad_sv", "v_gnomad_big").collect()) == 43


class TestIOVCFGCSStream:
    df_gcs_bgz = (
        pb.read_vcf(
            "gs://gcp-public-data--gnomad/release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr21.vcf.bgz",
            allow_anonymous=True,
        )
        .limit(3)
        .collect()
    )
    df_gcs_none = (
        pb.read_vcf(
            "gs://genomics-public-data/platinum-genomes/vcf/NA12878_S1.genome.vcf",
            allow_anonymous=True,
        )
        .limit(5)
        .collect()
    )

    def test_count(self):
        assert len(self.df_gcs_bgz) == 3
        assert len(self.df_gcs_none) == 5


class TestIOVCFAuth:

    def test_count_auth(self):
        df_gcs_auth = (
            pb.read_vcf(
                "gs://polars-bio-it/vep.vcf.bgz", allow_anonymous=True, max_retries=1
            )
            .limit(1)
            .collect()
        )
        assert len(df_gcs_auth) == 1

    @pytest.mark.xfail(strict=True)
    def test_count_anonymous(self):
        os.unsetenv("GOOGLE_APPLICATION_CREDENTIALS")
        df_gcs_anonymous = (
            pb.read_vcf(
                "gs://polars-bio-it/vep.vcf.bgz", allow_anonymous=True, max_retries=1
            )
            .limit(1)
            .collect()
        )

        assert len(df_gcs_anonymous) == 1
