import shutil
from pathlib import Path

from _expected import DATA_DIR

import polars_bio as pb


class TestIOGFF:
    df_bgz = pb.read_gff(f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3.bgz")
    df_gz = pb.read_gff(f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3.gz")
    df_none = pb.read_gff(f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3")
    df_bgz_wrong_extension = pb.read_gff(
        f"{DATA_DIR}/io/gff/wrong_extension.gff3.gz", compression_type="bgz"
    )

    def test_count(self):
        assert len(self.df_none) == 3
        assert len(self.df_gz) == 3
        assert len(self.df_bgz) == 3

    def test_compression_override(self):
        assert len(self.df_bgz_wrong_extension) == 3

    def test_fields(self):
        assert self.df_bgz["chrom"][0] == "chr1" and self.df_none["chrom"][0] == "chr1"
        # 1-based coordinates by default
        assert self.df_bgz["start"][1] == 11869 and self.df_none["start"][1] == 11869
        assert self.df_bgz["type"][2] == "exon" and self.df_none["type"][2] == "exon"
        assert self.df_bgz["attributes"][0][0] == {
            "tag": "ID",
            "value": "ENSG00000223972.5",
        }

    def test_register_table(self):
        pb.register_gff(
            f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3.bgz", "test_gff3"
        )
        # Use count(chrom) instead of count(*) due to DataFusion table provider issue
        count = pb.sql("select count(*) as cnt from test_gff3").collect()
        assert count["cnt"][0] == 3

    def test_register_gff_unnest(self):
        pb.register_gff(
            f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3.bgz", "test_gff3_unnest"
        )
        # Use count(chrom) instead of count(*) due to DataFusion table provider issue
        # Note: Without attr_fields, attributes remain as array - test that table registration works
        count = pb.sql(
            "select count(chrom) as cnt from test_gff3_unnest where chrom = 'chr1'"
        ).collect()
        assert count["cnt"][0] == 3

        # Test that attributes column is available (as array)
        attrs = pb.sql("select attributes from test_gff3_unnest limit 1").collect()
        assert len(attrs["attributes"][0]) > 0  # Should have attribute data

    def test_consistent_attribute_flattening(self):
        """Test that attribute field flattening works consistently for both projection modes."""
        file_path = f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3.bgz"

        # Test case 1: projection_pushdown=True (optimized path)
        result_pushdown = (
            pb.scan_gff(file_path, projection_pushdown=True)
            .select(["chrom", "start", "gene_id"])
            .collect()
        )

        # Test case 2: projection_pushdown=False (attribute extraction path)
        result_no_pushdown = (
            pb.scan_gff(file_path, projection_pushdown=False)
            .select(["chrom", "start", "gene_id"])
            .collect()
        )

        # Both should work and return identical results
        assert result_pushdown.shape == result_no_pushdown.shape
        assert result_pushdown.columns == result_no_pushdown.columns
        assert list(result_pushdown.columns) == ["chrom", "start", "gene_id"]

        # Both should have the same gene_id values
        assert result_pushdown["gene_id"][0] == result_no_pushdown["gene_id"][0]
        assert (
            result_pushdown["gene_id"][0] == "ENSG00000223972.5"
        )  # Expected value from test data

        # Test with multiple attribute fields
        multi_result_pushdown = (
            pb.scan_gff(file_path, projection_pushdown=True)
            .select(["gene_id", "gene_type"])
            .collect()
        )

        multi_result_no_pushdown = (
            pb.scan_gff(file_path, projection_pushdown=False)
            .select(["gene_id", "gene_type"])
            .collect()
        )

        assert multi_result_pushdown.shape == multi_result_no_pushdown.shape
        assert multi_result_pushdown.columns == multi_result_no_pushdown.columns
        assert (
            multi_result_pushdown["gene_id"][0]
            == multi_result_no_pushdown["gene_id"][0]
        )

    def test_sql_projection_pushdown(self):
        """Test SQL queries work with projection pushdown without specifying attr_fields."""
        file_path = f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3.bgz"

        # Register GFF table without attr_fields parameter
        pb.register_gff(file_path, "test_gff_projection")

        # Test 1: Static columns only (this should work)
        static_result = pb.sql(
            "SELECT chrom, start, `end`, type FROM test_gff_projection"
        ).collect()
        assert len(static_result) == 3
        assert list(static_result.columns) == ["chrom", "start", "end", "type"]
        assert static_result["chrom"][0] == "chr1"

        # Test 2: Query nested attributes structure (available by default)
        attr_result = pb.sql(
            "SELECT chrom, start, attributes FROM test_gff_projection LIMIT 1"
        ).collect()
        assert len(attr_result) == 1
        assert list(attr_result.columns) == ["chrom", "start", "attributes"]
        assert len(attr_result["attributes"][0]) > 0  # Should have attribute data

        # Test 3: Count query
        count_result = pb.sql(
            "SELECT COUNT(*) as total FROM test_gff_projection"
        ).collect()
        assert count_result["total"][0] == 3

        # Test 4: Feature type aggregation
        type_result = pb.sql(
            "SELECT type, COUNT(*) as count FROM test_gff_projection GROUP BY type"
        ).collect()
        assert len(type_result) >= 1

        # Test 5: Verify attributes contain expected fields (like gene_id)
        # Note: GFF SQL doesn't auto-flatten attributes like scan_gff does, but we can verify structure
        attrs_result = pb.sql(
            "SELECT attributes FROM test_gff_projection LIMIT 1"
        ).collect()
        attrs_list = attrs_result["attributes"][0]
        assert len(attrs_list) > 0
        # Check that gene_id exists in attributes by looking at tag names
        tag_names = [attr["tag"] for attr in attrs_list if "tag" in attr]
        assert "gene_id" in tag_names  # Should contain gene_id attribute

    def test_scan_gff_numeric_prefixed_filename_projection(self, tmp_path):
        """Regression: numeric-leading inferred table names must work in SQL-backed projection paths."""
        src = Path(f"{DATA_DIR}/io/gff/multi_chrom.gff3.gz")
        dst = tmp_path / "1_multi_chrom.gff3.gz"
        shutil.copy2(src, dst)

        src_tbi = Path(f"{src}.tbi")
        if src_tbi.exists():
            shutil.copy2(src_tbi, Path(f"{dst}.tbi"))

        result = (
            pb.scan_gff(str(dst)).select(["chrom", "attributes"]).limit(1).collect()
        )
        assert len(result) == 1
        assert result.columns == ["chrom", "attributes"]
