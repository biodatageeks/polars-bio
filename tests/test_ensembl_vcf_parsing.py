import polars as pl
import polars.testing as pl_testing

import polars_bio as pb


def test_vcf_ensembl_1_parsing():
    vcf_path = "tests/data/io/vcf/ensembl.vcf"
    # Get all available columns first to find the actual column names
    full_df = pb.read_vcf(vcf_path)
    all_columns = set(full_df.columns)
    static_columns = {"chrom", "start", "end", "id", "ref", "alt", "qual", "filter"}
    info_columns = all_columns - static_columns

    # Map expected field names to actual column names (case-insensitive)
    expected_info_fields = [
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

    # Find actual column names by case-insensitive matching
    actual_info_fields = []
    column_mapping = {}
    for expected in expected_info_fields:
        actual = next(
            (col for col in info_columns if col.lower() == expected.lower()), None
        )
        if actual:
            actual_info_fields.append(actual)
            column_mapping[expected] = actual

    # Use .select() with static columns + actual info fields
    static_columns_list = [
        "chrom",
        "start",
        "end",
        "id",
        "ref",
        "alt",
        "qual",
        "filter",
    ]
    all_columns_to_select = static_columns_list + actual_info_fields
    df = full_df.select(all_columns_to_select)

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

    # Compare using actual column names, but use expected data structure
    for expected_col in expected_df.columns:
        if expected_col in [
            "chrom",
            "start",
            "end",
            "id",
            "ref",
            "alt",
            "qual",
            "filter",
        ]:
            # Static columns should match exactly
            pl_testing.assert_series_equal(
                df[expected_col], expected_df[expected_col], check_dtypes=True
            )
        else:
            # For INFO fields, find the actual column name
            actual_col = column_mapping.get(expected_col)
            if actual_col:
                # Rename the actual column to expected name for comparison
                actual_series = df[actual_col].alias(expected_col)
                pl_testing.assert_series_equal(
                    actual_series, expected_df[expected_col], check_dtypes=True
                )


def test_vcf_ensembl_2_parsing():
    vcf_path = "tests/data/io/vcf/ensembl-2.vcf"
    # Get all available columns first to find the actual column names
    full_df = pb.read_vcf(vcf_path)
    all_columns = set(full_df.columns)
    static_columns = {"chrom", "start", "end", "id", "ref", "alt", "qual", "filter"}
    info_columns = all_columns - static_columns

    # Map expected field names to actual column names (case-insensitive)
    expected_info_fields = [
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

    # Find actual column names by case-insensitive matching
    actual_info_fields = []
    column_mapping = {}
    for expected in expected_info_fields:
        actual = next(
            (col for col in info_columns if col.lower() == expected.lower()), None
        )
        if actual:
            actual_info_fields.append(actual)
            column_mapping[expected] = actual

    # Use .select() with static columns + actual info fields
    static_columns_list = [
        "chrom",
        "start",
        "end",
        "id",
        "ref",
        "alt",
        "qual",
        "filter",
    ]
    all_columns_to_select = static_columns_list + actual_info_fields
    df = full_df.select(all_columns_to_select)

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

    # Compare using actual column names, but use expected data structure
    for expected_col in expected_df.columns:
        if expected_col in [
            "chrom",
            "start",
            "end",
            "id",
            "ref",
            "alt",
            "qual",
            "filter",
        ]:
            # Static columns should match exactly
            pl_testing.assert_series_equal(
                df[expected_col], expected_df[expected_col], check_dtypes=True
            )
        else:
            # For INFO fields, find the actual column name
            actual_col = column_mapping.get(expected_col)
            if actual_col:
                # Rename the actual column to expected name for comparison
                actual_series = df[actual_col].alias(expected_col)
                pl_testing.assert_series_equal(
                    actual_series, expected_df[expected_col], check_dtypes=True
                )
