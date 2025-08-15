import polars as pl
import polars.testing as pl_testing

import polars_bio as pb


def test_vcf_all_fields_parsing_with_info_fields():
    vcf_path = "tests/data/io/vcf/ensembl.vcf"
    info_fields = [
        "dbSNP_156",
        "TSA",
        "E_Freq",
        "E_Phenotype_or_Disease",
        "E_ExAC",
        "E_TOPMed",
        "E_gnomAD",
        "CLIN_uncertain_significance",
        "AA",
    ]
    df = pb.read_vcf(vcf_path, info_fields=info_fields).collect()

    # Expected data
    expected_df = pl.DataFrame(
        {
            "chrom": ["21", "21"],
            "start": [33248751, 5025532],
            "end": [33248751, 5025532],
            "id": ["rs549962048", "rs1879593094"],
            "ref": ["A", "G"],
            "alt": ["C|G", "C"],
            "qual": [None, None],
            "filter": ["", ""],
            "dbsnp_156": [True, True],
            "tsa": ["SNV", "SNV"],
            "e_freq": [True, True],
            "e_phenotype_or_disease": [True, False],
            "e_exac": [True, False],
            "e_topmed": [True, False],
            "e_gnomad": [True, False],
            "clin_uncertain_significance": [False, False],
            "aa": ["A", "G"],
        },
        schema={
            "chrom": pl.Utf8,
            "start": pl.UInt32,
            "end": pl.UInt32,
            "id": pl.Utf8,
            "ref": pl.Utf8,
            "alt": pl.Utf8,
            "qual": pl.Float64,
            "filter": pl.Utf8,
            "dbsnp_156": pl.Boolean,
            "tsa": pl.Utf8,
            "e_freq": pl.Boolean,
            "e_phenotype_or_disease": pl.Boolean,
            "e_exac": pl.Boolean,
            "e_topmed": pl.Boolean,
            "e_gnomad": pl.Boolean,
            "clin_uncertain_significance": pl.Boolean,
            "aa": pl.Utf8,
        },
    )

    for col in expected_df.columns:
        pl_testing.assert_series_equal(df[col], expected_df[col], check_dtype=True)
