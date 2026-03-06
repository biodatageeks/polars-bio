import polars as pl
from _expected import DATA_DIR

import polars_bio as pb

GTF_PATH = f"{DATA_DIR}/io/gtf/test.gtf"


class TestIOGTF:
    df = pb.read_gtf(GTF_PATH)

    def test_count(self):
        assert len(self.df) == 23

    def test_fields(self):
        assert self.df["chrom"][0] == "chr12"
        assert self.df["source"][0] == "HAVANA"
        assert self.df["type"][0] == "transcript"
        assert self.df["type"][1] == "exon"

    def test_attributes_nested(self):
        """Default attributes as List<Struct> with tag/value."""
        attrs = self.df["attributes"][0].to_list()
        assert isinstance(attrs, list)
        tags = [a["tag"] for a in attrs]
        assert "gene_id" in tags
        assert "gene_name" in tags

    def test_attribute_flattening(self):
        """attr_fields extracts specific attributes as columns."""
        df = pb.read_gtf(GTF_PATH, attr_fields=["gene_id", "gene_name"])
        assert "gene_id" in df.columns
        assert "gene_name" in df.columns
        assert df["gene_id"][0] == "ENSG00000111640.16"
        assert df["gene_name"][0] == "GAPDH"

    def test_consistent_attribute_flattening(self):
        """projection_pushdown=True vs False produce same results."""
        result_pushdown = (
            pb.scan_gtf(GTF_PATH, projection_pushdown=True)
            .select(["chrom", "start", "gene_id"])
            .collect()
        )
        result_no_pushdown = (
            pb.scan_gtf(GTF_PATH, projection_pushdown=False)
            .select(["chrom", "start", "gene_id"])
            .collect()
        )
        assert result_pushdown.shape == result_no_pushdown.shape
        assert result_pushdown.columns == result_no_pushdown.columns
        assert result_pushdown["gene_id"][0] == result_no_pushdown["gene_id"][0]
        assert result_pushdown["gene_id"][0] == "ENSG00000111640.16"

    def test_register_table(self):
        pb.register_gtf(GTF_PATH, "test_gtf")
        count = pb.sql("SELECT count(*) as cnt FROM test_gtf").collect()
        assert count["cnt"][0] == 23

    def test_sql_projection_pushdown(self):
        """SQL queries work with static columns, attributes, COUNT."""
        pb.register_gtf(GTF_PATH, "test_gtf_proj")

        # Static columns
        static_result = pb.sql(
            'SELECT chrom, start, "end", type FROM test_gtf_proj'
        ).collect()
        assert len(static_result) == 23
        assert static_result["chrom"][0] == "chr12"

        # Count
        count_result = pb.sql("SELECT COUNT(*) as total FROM test_gtf_proj").collect()
        assert count_result["total"][0] == 23

    def test_scan_select_static(self):
        result = pb.scan_gtf(GTF_PATH).select(["chrom", "start", "end"]).collect()
        assert list(result.columns) == ["chrom", "start", "end"]
        assert len(result) == 23

    def test_scan_select_attributes(self):
        result = pb.scan_gtf(GTF_PATH).select(["gene_id", "gene_name"]).collect()
        assert list(result.columns) == ["gene_id", "gene_name"]
        assert result["gene_id"][0] == "ENSG00000111640.16"
        assert result["gene_name"][0] == "GAPDH"

    def test_filter_by_type(self):
        result = pb.scan_gtf(GTF_PATH).filter(pl.col("type") == "exon").collect()
        assert len(result) == 9

    def test_filter_by_chrom(self):
        result = pb.scan_gtf(GTF_PATH).filter(pl.col("chrom") == "chr12").collect()
        assert len(result) == 23

    def test_predicate_pushdown(self):
        """Predicate pushdown with type/chrom filters."""
        result = (
            pb.scan_gtf(GTF_PATH, predicate_pushdown=True)
            .filter(pl.col("type") == "CDS")
            .collect()
        )
        assert len(result) == 8
        assert result["type"].to_list() == ["CDS"] * 8

    def test_coordinate_system_default(self):
        """1-based coords by default."""
        df = pb.read_gtf(GTF_PATH)
        # First record: chr12 6534012 6538371 (1-based from GTF file)
        assert df["start"][0] == 6534012
        assert df["end"][0] == 6538371

    def test_coordinate_system_zero_based(self):
        """use_zero_based=True shifts start by -1."""
        df = pb.read_gtf(GTF_PATH, use_zero_based=True)
        # Zero-based: start should be 6534011 (original 6534012 - 1)
        assert df["start"][0] == 6534011
        assert df["end"][0] == 6538371

    def test_scan_limit(self):
        result = pb.scan_gtf(GTF_PATH).limit(5).collect()
        assert len(result) == 5

    def test_quoted_attribute_values(self):
        """gene_id \"ENSG00000111640.16\" parsed correctly."""
        df = pb.read_gtf(GTF_PATH, attr_fields=["gene_id"])
        assert df["gene_id"][0] == "ENSG00000111640.16"

    def test_unquoted_attribute_values(self):
        """level 2 parsed correctly (unquoted integer-like value)."""
        df = pb.read_gtf(GTF_PATH, attr_fields=["level"])
        assert df["level"][0] == "2"

    def test_duplicate_tag_attributes(self):
        """Multiple 'tag' entries handled."""
        df = pb.read_gtf(GTF_PATH)
        attrs = df["attributes"][0].to_list()
        tag_values = [a["value"] for a in attrs if a["tag"] == "tag"]
        # The test data has multiple tag entries: "basic", "TAGENE", "appris_principal_3", "CCDS"
        assert len(tag_values) >= 3
        assert "basic" in tag_values
