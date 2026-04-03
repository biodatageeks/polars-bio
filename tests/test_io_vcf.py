from _expected import DATA_DIR

import polars_bio as pb


class TestIOVCF:
    """Tests for VCF read functionality."""

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


class TestVCFInfoFormatCollision:
    """Tests for single-sample VCF with INFO/FORMAT column name collision (issue #350)."""

    vcf_path = f"{DATA_DIR}/io/vcf/single_sample_collision.vcf"

    def test_schema_has_fmt_prefix(self):
        """FORMAT DP is renamed to fmt_DP when INFO DP exists."""
        df = pb.read_vcf(self.vcf_path)
        assert "DP" in df.columns, "INFO DP should be present as 'DP'"
        assert "fmt_DP" in df.columns, "FORMAT DP should be renamed to 'fmt_DP'"
        assert "GT" in df.columns, "Non-colliding FORMAT fields keep original names"
        assert "GQ" in df.columns

    def test_info_and_format_values_correct(self):
        """INFO DP and FORMAT fmt_DP have distinct, correct values."""
        df = pb.read_vcf(self.vcf_path)
        # INFO DP = 50, 60
        assert df["DP"][0] == 50
        assert df["DP"][1] == 60
        # FORMAT DP (renamed to fmt_DP) = 20, 30
        assert df["fmt_DP"][0] == 20
        assert df["fmt_DP"][1] == 30

    def test_non_colliding_format_values(self):
        """GT and GQ (no collision) have correct values."""
        df = pb.read_vcf(self.vcf_path)
        assert df["GT"][0] == "0/1"
        assert df["GT"][1] == "1/1"
        assert df["GQ"][0] == 99
        assert df["GQ"][1] == 95

    def test_scan_vcf_collision(self):
        """scan_vcf also handles the collision correctly."""
        lf = pb.scan_vcf(self.vcf_path)
        df = lf.collect()
        assert "DP" in df.columns
        assert "fmt_DP" in df.columns
        assert df["DP"][0] == 50
        assert df["fmt_DP"][0] == 20

    def test_sql_query_collision(self):
        """SQL queries can reference both DP and fmt_DP."""
        pb.register_vcf(self.vcf_path, "collision_vcf")
        result = pb.sql(
            'SELECT "DP", "fmt_DP", "GT" FROM collision_vcf ORDER BY start'
        ).collect()
        assert len(result) == 2
        assert result["DP"][0] == 50
        assert result["fmt_DP"][0] == 20

    def test_write_roundtrip_collision(self, tmp_path):
        """Write and read back preserves both INFO and FORMAT DP."""
        df = pb.read_vcf(self.vcf_path)
        out_path = str(tmp_path / "collision_roundtrip.vcf")
        rows_written = pb.write_vcf(df, out_path)
        assert rows_written == 2

        df_back = pb.read_vcf(out_path)
        assert len(df_back) == 2
        assert "DP" in df_back.columns
        assert "fmt_DP" in df_back.columns
        assert df_back["DP"][0] == 50
        assert df_back["fmt_DP"][0] == 20

    def test_multisample_collision_unaffected(self):
        """Multi-sample VCF with same-named INFO/FORMAT fields is not affected."""
        df = pb.read_vcf(f"{DATA_DIR}/io/vcf/multisample.vcf")
        # Multi-sample: FORMAT fields are nested in genotypes, no fmt_ prefix
        assert "fmt_DP" not in df.columns
        # INFO AF should be present at top level
        assert "AF" in df.columns


class TestVCFWrite:
    """Tests for VCF write functionality."""

    def test_write_vcf_roundtrip(self, tmp_path):
        """VCF -> VCF roundtrip: read, write, read back."""
        df = pb.read_vcf(f"{DATA_DIR}/io/vcf/vep.vcf")
        out_path = str(tmp_path / "roundtrip.vcf")

        rows_written = pb.write_vcf(df, out_path)
        assert rows_written == 2

        df_back = pb.read_vcf(out_path)
        assert len(df_back) == 2
        assert df_back["chrom"][0] == df["chrom"][0]
        assert df_back["start"][0] == df["start"][0]
        assert df_back["ref"][0] == df["ref"][0]

    def test_sink_vcf_roundtrip(self, tmp_path):
        """Streaming VCF write roundtrip: scan, sink, read back."""
        lf = pb.scan_vcf(f"{DATA_DIR}/io/vcf/vep.vcf")
        out_path = str(tmp_path / "sink.vcf")

        pb.sink_vcf(lf, out_path)

        df_back = pb.read_vcf(out_path)
        assert len(df_back) == 2

    def test_write_vcf_bgz(self, tmp_path):
        """Write VCF with BGZF compression."""
        df = pb.read_vcf(f"{DATA_DIR}/io/vcf/vep.vcf")
        out_path = str(tmp_path / "output.vcf.bgz")

        rows_written = pb.write_vcf(df, out_path)
        assert rows_written == 2

        df_back = pb.read_vcf(out_path)
        assert len(df_back) == 2

    def test_write_vcf_gz(self, tmp_path):
        """Write VCF with GZIP compression."""
        df = pb.read_vcf(f"{DATA_DIR}/io/vcf/vep.vcf")
        out_path = str(tmp_path / "output.vcf.gz")

        rows_written = pb.write_vcf(df, out_path)
        assert rows_written == 2

        df_back = pb.read_vcf(out_path)
        assert len(df_back) == 2

    def test_sink_vcf_bgz(self, tmp_path):
        """Streaming write VCF with BGZF compression."""
        lf = pb.scan_vcf(f"{DATA_DIR}/io/vcf/vep.vcf")
        out_path = str(tmp_path / "sink.vcf.bgz")

        pb.sink_vcf(lf, out_path)

        df_back = pb.read_vcf(out_path)
        assert len(df_back) == 2
