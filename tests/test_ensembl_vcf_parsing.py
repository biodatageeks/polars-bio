import polars as pl
import polars.testing as pl_testing

import polars_bio as pb


def test_vcf_ensembl_1_parsing():
    vcf_path = "tests/data/io/vcf/ensembl.vcf"
    # Column names are normalized to lowercase with underscores
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
    # Use .select() with static columns + info fields instead of info_fields parameter
    static_columns = ["chrom", "start", "end", "id", "ref", "alt", "qual", "filter"]
    all_columns = static_columns + info_fields
    df = pb.read_vcf(vcf_path).select(all_columns)

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
            "dbSNP_156": [True, True],
            "TSA": ["SNV", "SNV"],
            "E_Freq": [True, True],
            "E_Phenotype_or_Disease": [True, False],
            "E_ExAC": [True, False],
            "E_TOPMed": [True, False],
            "E_gnomAD": [True, False],
            "CLIN_uncertain_significance": [False, False],
            "AA": ["A", "G"],
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
            "dbSNP_156": pl.Boolean,
            "TSA": pl.Utf8,
            "E_Freq": pl.Boolean,
            "E_Phenotype_or_Disease": pl.Boolean,
            "E_ExAC": pl.Boolean,
            "E_TOPMed": pl.Boolean,
            "E_gnomAD": pl.Boolean,
            "CLIN_uncertain_significance": pl.Boolean,
            "AA": pl.Utf8,
        },
    )

    for col in expected_df.columns:
        pl_testing.assert_series_equal(df[col], expected_df[col], check_dtypes=True)


def test_vcf_ensembl_2_parsing():
    vcf_path = "tests/data/io/vcf/ensembl-2.vcf"
    # Column names are normalized to lowercase with underscores
    info_fields = [
        "COSMIC_100",
        "dbSNP_156",
        "HGMD-PUBLIC_20204",
        "ClinVar_202409",
        "TSA",
        "E_Cited",
        "E_Multiple_observations",
        "E_Freq",
        "E_TOPMed",
        "E_Hapmap",
        "E_Phenotype_or_Disease",
        "E_ESP",
        "E_gnomAD",
        "E_1000G",
        "E_ExAC",
        "CLIN_risk_factor",
        "CLIN_protective",
        "CLIN_confers_sensitivity",
        "CLIN_other",
        "CLIN_drug_response",
        "CLIN_uncertain_significance",
        "CLIN_benign",
        "CLIN_likely_pathogenic",
        "CLIN_pathogenic",
        "CLIN_likely_benign",
        "CLIN_histocompatibility",
        "CLIN_not_provided",
        "CLIN_association",
        "MA",
        "MAF",
        "MAC",
        "AA",
    ]
    # Use .select() with static columns + info fields instead of info_fields parameter
    static_columns = ["chrom", "start", "end", "id", "ref", "alt", "qual", "filter"]
    all_columns = static_columns + info_fields
    df = pb.read_vcf(vcf_path).select(all_columns)

    expected_df = pl.DataFrame(
        {
            "chrom": ["1"],
            "start": [2491309],
            "end": [2491309],
            "id": ["rs368445617"],
            "ref": ["T"],
            "alt": ["A|C"],
            "qual": [None],
            "filter": [""],
            "COSMIC_100": [False],
            "dbSNP_156": [True],
            "HGMD-PUBLIC_20204": [False],
            "ClinVar_202409": [False],
            "TSA": ["SNV"],
            "E_Cited": [False],
            "E_Multiple_observations": [False],
            "E_Freq": [True],
            "E_TOPMed": [True],
            "E_Hapmap": [False],
            "E_Phenotype_or_Disease": [True],
            "E_ESP": [True],
            "E_gnomAD": [True],
            "E_1000G": [False],
            "E_ExAC": [True],
            "CLIN_risk_factor": [False],
            "CLIN_protective": [False],
            "CLIN_confers_sensitivity": [False],
            "CLIN_other": [False],
            "CLIN_drug_response": [False],
            "CLIN_uncertain_significance": [True],
            "CLIN_benign": [False],
            "CLIN_likely_pathogenic": [False],
            "CLIN_pathogenic": [False],
            "CLIN_likely_benign": [False],
            "CLIN_histocompatibility": [False],
            "CLIN_not_provided": [False],
            "CLIN_association": [False],
            "MA": [None],
            "MAF": [None],
            "MAC": [None],
            "AA": ["T"],
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
            "COSMIC_100": pl.Boolean,
            "dbSNP_156": pl.Boolean,
            "HGMD-PUBLIC_20204": pl.Boolean,
            "ClinVar_202409": pl.Boolean,
            "TSA": pl.Utf8,
            "E_Cited": pl.Boolean,
            "E_Multiple_observations": pl.Boolean,
            "E_Freq": pl.Boolean,
            "E_TOPMed": pl.Boolean,
            "E_Hapmap": pl.Boolean,
            "E_Phenotype_or_Disease": pl.Boolean,
            "E_ESP": pl.Boolean,
            "E_gnomAD": pl.Boolean,
            "E_1000G": pl.Boolean,
            "E_ExAC": pl.Boolean,
            "CLIN_risk_factor": pl.Boolean,
            "CLIN_protective": pl.Boolean,
            "CLIN_confers_sensitivity": pl.Boolean,
            "CLIN_other": pl.Boolean,
            "CLIN_drug_response": pl.Boolean,
            "CLIN_uncertain_significance": pl.Boolean,
            "CLIN_benign": pl.Boolean,
            "CLIN_likely_pathogenic": pl.Boolean,
            "CLIN_pathogenic": pl.Boolean,
            "CLIN_likely_benign": pl.Boolean,
            "CLIN_histocompatibility": pl.Boolean,
            "CLIN_not_provided": pl.Boolean,
            "CLIN_association": pl.Boolean,
            "MA": pl.Utf8,
            "MAF": pl.Float32,
            "MAC": pl.Int32,
            "AA": pl.Utf8,
        },
    )

    for col in expected_df.columns:
        pl_testing.assert_series_equal(df[col], expected_df[col], check_dtypes=True)
