import bioframe as bf
import pandas as pd
import polars as pl
import polars.testing as pl_testing
import pytest
from _expected import (
    DATA_DIR,
    PD_DF_OVERLAP,
    PD_OVERLAP_DF1,
    PD_OVERLAP_DF2,
    PL_DF1,
    PL_DF2,
    PL_DF_OVERLAP,
)

import polars_bio as pb


class TestMemoryCombinations:
    def test_frames(self):
        for df1 in [PD_OVERLAP_DF1, PL_DF1, PL_DF1.lazy()]:
            for df2 in [PD_OVERLAP_DF2, PL_DF2, PL_DF2.lazy()]:
                for output_type in [
                    "pandas.DataFrame",
                    "polars.DataFrame",
                    "polars.LazyFrame",
                ]:
                    result = pb.overlap(
                        df1,
                        df2,
                        cols1=("contig", "pos_start", "pos_end"),
                        cols2=("contig", "pos_start", "pos_end"),
                        output_type=output_type,
                        use_zero_based=False,
                    )
                    if output_type == "polars.LazyFrame":
                        result = result.collect()
                    if output_type == "pandas.DataFrame":
                        result = result.sort_values(
                            by=list(result.columns)
                        ).reset_index(drop=True)
                        pd.testing.assert_frame_equal(result, PD_DF_OVERLAP)
                    else:
                        result = result.sort(by=result.columns)
                        assert PL_DF_OVERLAP.equals(result)


class TestIOBAM:
    df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")

    def test_count(self):
        assert len(self.df) == 2333

    def test_fields(self):
        assert self.df["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert self.df["flags"][3] == 1123
        assert self.df["cigar"][4] == "101M"
        assert (
            self.df["sequence"][4]
            == "TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACC"
        )
        assert (
            self.df["quality_scores"][4]
            == "CCDACCDCDABBDCDABBDCDABBDCDABBDCD?BBCCDABBCCDABBACDA?BDCAABBDBDA.=?><;CBB2@:;??:D>?5BAC??=DC;=5=?8:76"
        )

    def test_register(self):
        pb.register_bam(f"{DATA_DIR}/io/bam/test.bam", "test_bam")
        count = pb.sql("select count(*) as cnt from test_bam").collect()
        assert count["cnt"][0] == 2333

        projection = pb.sql("select name, flags from test_bam").collect()
        assert projection["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert projection["flags"][3] == 1123


class TestIOCRAM:
    # Test with embedded reference (default)
    df = pb.read_cram(f"{DATA_DIR}/io/cram/test.cram")

    def test_count(self):
        assert len(self.df) == 2333

    def test_fields(self):
        assert self.df["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert self.df["flags"][3] == 1123
        assert self.df["cigar"][4] == "101M"
        assert (
            self.df["sequence"][4]
            == "TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACC"
        )
        assert (
            self.df["quality_scores"][4]
            == "CCDACCDCDABBDCDABBDCDABBDCDABBDCD?BBCCDABBCCDABBACDA?BDCAABBDBDA.=?><;CBB2@:;??:D>?5BAC??=DC;=5=?8:76"
        )

    def test_register(self):
        pb.register_cram(f"{DATA_DIR}/io/cram/test.cram", "test_cram")
        count = pb.sql("select count(*) as cnt from test_cram").collect()
        assert count["cnt"][0] == 2333

        projection = pb.sql("select name, flags from test_cram").collect()
        assert projection["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert projection["flags"][3] == 1123

    def test_scan_cram(self):
        """Test lazy scanning with scan_cram"""
        lf = pb.scan_cram(f"{DATA_DIR}/io/cram/test.cram")
        df = lf.select(["name", "chrom", "start"]).collect()
        assert len(df) == 2333
        assert "name" in df.columns
        assert "chrom" in df.columns
        assert "start" in df.columns

    def test_external_reference(self):
        """Test CRAM reading with external FASTA reference"""
        # Test with external reference from chr20 subset
        df = (
            pb.scan_cram(
                f"{DATA_DIR}/io/cram/external_ref/test_chr20.cram",
                reference_path=f"{DATA_DIR}/io/cram/external_ref/chr20.fa",
            )
            .limit(10)
            .collect()
        )

        assert len(df) == 10
        assert "name" in df.columns
        assert "chrom" in df.columns
        assert "sequence" in df.columns

        # Verify reads are from chr20
        assert all(df["chrom"] == "chr20")

        # Verify first read details
        assert df["name"][0] == "SRR622461.74266137"
        assert df["start"][0] == 59993
        assert df["mapping_quality"][0] == 29


class TestIOBED:
    df = pb.read_table(f"{DATA_DIR}/io/bed/test.bed", schema="bed12")

    def test_count(self):
        assert len(self.df) == 3

    def test_fields(self):
        assert self.df["chrom"][2] == "chrX"
        assert self.df["strand"][1] == "-"
        assert self.df["end"][2] == 8000


class TestFasta:
    fasta_path = f"{DATA_DIR}/io/fasta/test.fasta"

    def test_count(self):
        df = pb.read_fasta(self.fasta_path)
        assert len(df) == 2

    def test_read_fasta(self):

        df = pb.read_fasta(self.fasta_path)
        print("Actual DataFrame:")
        print(df)
        print("Actual Schema:")
        print(df.schema)

        expected_df = pl.DataFrame(
            {
                "name": ["seq1", "seq2"],
                "description": ["First sequence", "Second sequence"],
                "sequence": ["ACTG", "GATTACA"],
            }
        )

        pl_testing.assert_frame_equal(df, expected_df)


class TestIOTable:
    file = f"{DATA_DIR}/io/bed/ENCFF001XKR.bed.gz"

    def test_bed9(self):
        df_1 = pb.read_table(self.file, schema="bed9").to_pandas()
        df_1 = df_1.sort_values(by=list(df_1.columns)).reset_index(drop=True)
        df_2 = bf.read_table(self.file, schema="bed9")
        df_2 = df_2.sort_values(by=list(df_2.columns)).reset_index(drop=True)
        pd.testing.assert_frame_equal(df_1, df_2)


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


class TestFastq:
    def test_count(self):
        assert (
            pb.scan_fastq(f"{DATA_DIR}/io/fastq/example.fastq.bgz")
            .count()
            .collect()["name"][0]
            == 200
        )
        assert (
            pb.scan_fastq(f"{DATA_DIR}/io/fastq/example.fastq.gz")
            .count()
            .collect()["name"][0]
            == 200
        )
        assert (
            pb.scan_fastq(f"{DATA_DIR}/io/fastq/example.fastq")
            .count()
            .collect()["name"][0]
            == 200
        )

    def test_compression_override(self):
        assert (
            pb.scan_fastq(
                f"{DATA_DIR}/io/fastq/wrong_extension.fastq.gz", compression_type="bgz"
            )
            .count()
            .collect()["name"][0]
            == 200
        )

    def test_fields(self):
        sequences = pb.read_fastq(f"{DATA_DIR}/io/fastq/example.fastq.bgz").limit(5)
        assert sequences["name"][1] == "SRR9130495.2"
        assert (
            sequences["quality_scores"][2]
            == "@@@DDDFFHHHFHBHIIGJIJIIJIIIEHGIGIJJIIGGIIIJIIJIJIIIIIHIJJIIJJIGHGIJJIGGHC=#-#-5?EBEFFFDEEEFEAEDBCCCDC"
        )
        assert (
            sequences["sequence"][3]
            == "GGGAGGCGCCCCGACCGGCCAGGGCGTGAGCCCCAGCCCCAGCGCCATCCTGGAGCGGCGCGACGTGAAGCCAGATGAGGACCTGGCGGGCAAGGCTGGCG"
        )


class TestParallelFastq:
    @pytest.mark.parametrize("partitions", [1, 2, 3, 4])
    def test_read_parallel_fastq(self, partitions):
        pb.set_option("datafusion.execution.target_partitions", str(partitions))
        df = pb.read_fastq(
            f"{DATA_DIR}/io/fastq/sample_parallel.fastq.bgz", parallel=True
        )
        assert len(df) == 2000

    def test_read_parallel_fastq_with_limit(self):
        lf = pb.scan_fastq(
            f"{DATA_DIR}/io/fastq/sample_parallel.fastq.bgz", parallel=True
        ).limit(10)
        print(lf.explain())
        df = lf.collect()
        assert len(df) == 10


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


class TestBED:
    df_bgz = pb.read_bed(f"{DATA_DIR}/io/bed/chr16_fragile_site.bed.bgz")
    df_none = pb.read_bed(f"{DATA_DIR}/io/bed/chr16_fragile_site.bed")

    def test_count(self):
        assert len(self.df_none) == 5
        assert len(self.df_bgz) == 5

    def test_fields(self):
        assert (
            self.df_bgz["chrom"][0] == "chr16" and self.df_none["chrom"][0] == "chr16"
        )
        assert (
            self.df_bgz["start"][1] == 66700001 and self.df_none["start"][1] == 66700001
        )  # example of 1-based for start
        assert (
            self.df_bgz["name"][0] == "FRA16A" and self.df_none["name"][4] == "FRA16E"
        )

    def test_register_table(self):
        pb.register_bed(f"{DATA_DIR}/io/bed/chr16_fragile_site.bed.bgz", "test_bed")
        count = pb.sql("select count(*) as cnt from test_bed").collect()
        assert count["cnt"][0] == 5

        projection = pb.sql("select chrom, start, `end`, name from test_bed").collect()
        assert projection["chrom"][0] == "chr16"
        assert projection["start"][1] == 66700001  # example of 1-based for start
        assert projection["end"][2] == 63934965
        assert projection["name"][4] == "FRA16E"
