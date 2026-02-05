from _expected import DATA_DIR

import polars_bio as pb


class TestIOVCF:
    df_bgz = pb.read_vcf(f"{DATA_DIR}/io/vcf/vep.vcf.bgz")
    df_gz = pb.read_vcf(f"{DATA_DIR}/io/vcf/vep.vcf.gz")
    df_none = pb.read_vcf(f"{DATA_DIR}/io/vcf/vep.vcf")
    df_bgz_wrong_extension = pb.read_vcf(
        f"{DATA_DIR}/io/vcf/wrong_extension.vcf.bgz", compression_type="gz"
    )
    df_gz_wrong_extension = pb.read_vcf(
        f"{DATA_DIR}/io/vcf/wrong_extension.vcf.gz", compression_type="bgz"
    )

    def test_count(self):
        assert len(self.df_none) == 2
        assert len(self.df_gz) == 2
        assert len(self.df_bgz) == 2

    def test_compression_override(self):
        assert len(self.df_bgz_wrong_extension) == 2
        assert len(self.df_gz_wrong_extension) == 2

    def test_fields(self):
        assert self.df_bgz["chrom"][0] == "21" and self.df_none["chrom"][0] == "21"
        # 1-based coordinates by default
        assert (
            self.df_bgz["start"][1] == 26965148 and self.df_none["start"][1] == 26965148
        )
        assert self.df_bgz["ref"][0] == "G" and self.df_none["ref"][0] == "G"

    def test_sql_projection_pushdown(self):
        """Test SQL queries work with projection pushdown without specifying info_fields."""
        file_path = f"{DATA_DIR}/io/vcf/vep.vcf.bgz"

        # Register VCF table without info_fields parameter
        pb.register_vcf(file_path, "test_vcf_projection")

        # Test 1: Static columns only
        static_result = pb.sql(
            "SELECT chrom, start, ref, alt FROM test_vcf_projection"
        ).collect()
        assert len(static_result) == 2
        assert list(static_result.columns) == ["chrom", "start", "ref", "alt"]
        assert static_result["chrom"][0] == "21"

        # Test 2: Mixed query with potential INFO fields (should work automatically)
        # Note: We don't know which INFO fields exist in the test VCF, so we'll test count
        count_result = pb.sql(
            "SELECT COUNT(*) as total FROM test_vcf_projection"
        ).collect()
        assert count_result["total"][0] == 2

        # Test 3: Chromosome aggregation
        chr_result = pb.sql(
            "SELECT chrom, COUNT(*) as count FROM test_vcf_projection GROUP BY chrom"
        ).collect()
        assert len(chr_result) >= 1
        assert chr_result["count"][0] == 2  # All variants are on chr21

        # Test 4: INFO field access (should work automatically with updated registration)
        # First find the actual CSQ column name (case-insensitive)
        all_columns_result = pb.sql(
            "SELECT * FROM test_vcf_projection LIMIT 1"
        ).collect()
        csq_col = next(
            (col for col in all_columns_result.columns if col.lower() == "csq"), None
        )

        if csq_col:
            info_result = pb.sql(
                f'SELECT chrom, start, "{csq_col}" FROM test_vcf_projection LIMIT 1'
            ).collect()
            assert len(info_result) == 1
            assert list(info_result.columns) == ["chrom", "start", csq_col]
            assert info_result[csq_col][0] is not None  # CSQ field should have data
