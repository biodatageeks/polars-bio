from pathlib import Path

import polars as pl
import pytest
from _expected import DATA_DIR

import polars_bio as pb

VEP_CACHE_DIR = DATA_DIR / "io" / "vep_cache"
VARIATION_CACHE = str(VEP_CACHE_DIR / "variation_non_tabix")
VCF_PATH = str(DATA_DIR / "io" / "vcf" / "vep.vcf.bgz")
ANNOTATION_COLUMNS = ["variation_name", "clin_sig"]


class TestAnnotations:
    def test_annotate_variants_returns_lazyframe(self):
        lf = pb.annotations.annotate_variants(
            VCF_PATH,
            VARIATION_CACHE,
            cache_format="native",
            columns=ANNOTATION_COLUMNS,
        )

        assert isinstance(lf, pl.LazyFrame)
        df = lf.collect()
        assert {"chrom", "start", "ref", "alt", "variation_name", "clin_sig"}.issubset(
            set(df.columns)
        )

    @pytest.mark.parametrize(
        "match_mode",
        ["exact", "exact_or_colocated_ids", "exact_or_vep_existing"],
    )
    def test_annotate_variants_supports_all_match_modes(self, match_mode):
        df = pb.annotations.annotate_variants(
            VCF_PATH,
            VARIATION_CACHE,
            cache_format="native",
            columns=ANNOTATION_COLUMNS,
            match_mode=match_mode,
            output_type="polars.DataFrame",
        )

        assert {"variation_name", "clin_sig"}.issubset(set(df.columns))

    def test_annotate_variants_empty_columns_uses_default_projection(self):
        df = pb.annotations.annotate_variants(
            VCF_PATH,
            VARIATION_CACHE,
            cache_format="native",
            columns=[],
            match_mode="exact_or_colocated_ids",
            output_type="polars.DataFrame",
        )

        assert "variation_name" in df.columns

    def test_annotate_variants_preserves_chr_prefixed_input(self, tmp_path):
        input_vcf = tmp_path / "chr_prefix.vcf"
        input_vcf.write_text(
            "##fileformat=VCFv4.2\n"
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
            "chr2\t123456789\t.\tA\tG\t.\t.\t.\n",
            encoding="utf-8",
        )

        df = pb.annotations.annotate_variants(
            str(input_vcf),
            VARIATION_CACHE,
            cache_format="native",
            columns=["variation_name"],
            output_type="polars.DataFrame",
        )

        assert df.height == 1
        assert df["chrom"][0] == "chr2"

    def test_annotate_variants_invalid_match_mode_raises(self):
        with pytest.raises(ValueError, match="Invalid match_mode"):
            pb.annotations.annotate_variants(
                VCF_PATH,
                VARIATION_CACHE,
                cache_format="native",
                match_mode="invalid_mode",
            )

    def test_annotate_variants_invalid_cache_format_raises(self):
        with pytest.raises(ValueError, match="Invalid cache_format"):
            pb.annotations.annotate_variants(
                VCF_PATH,
                VARIATION_CACHE,
                cache_format="bad_format",
            )

    def test_create_vep_cache_parquet_and_reuse_for_annotation(self, tmp_path):
        parquet_path = tmp_path / "variation.parquet"
        created_path = pb.annotations.create_vep_cache(
            VARIATION_CACHE,
            str(parquet_path),
            output_format="parquet",
        )

        assert created_path == str(parquet_path)
        assert parquet_path.exists()

        df = pb.annotations.annotate_variants(
            VCF_PATH,
            str(parquet_path),
            cache_format="parquet",
            columns=ANNOTATION_COLUMNS,
            output_type="polars.DataFrame",
        )
        assert {"variation_name", "clin_sig"}.issubset(set(df.columns))

    def test_create_vep_cache_fjall_and_reuse_for_annotation(self, tmp_path):
        fjall_path = tmp_path / "variation_fjall"
        created_path = pb.annotations.create_vep_cache(
            VARIATION_CACHE,
            str(fjall_path),
            output_format="fjall",
        )

        assert Path(created_path).exists()

        df = pb.annotations.annotate_variants(
            VCF_PATH,
            str(fjall_path),
            cache_format="fjall",
            columns=ANNOTATION_COLUMNS,
            output_type="polars.DataFrame",
        )
        assert {"variation_name", "clin_sig"}.issubset(set(df.columns))

    def test_create_vep_cache_invalid_output_format_raises(self):
        with pytest.raises(ValueError, match="Invalid output_format"):
            pb.annotations.create_vep_cache(
                VARIATION_CACHE,
                "/tmp/not_used",
                output_format="json",
            )
