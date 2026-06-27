import polars as pl
from hypothesis import given, settings
from hypothesis import strategies as st

import polars_bio as pb

GTF_FIXTURE = (
    "#!genome-build GRCm38.p6\n"
    "1\thavana\tgene\t3073253\t3074322\t.\t+\t.\t"
    'gene_id "G1"; gene_biotype "TEC";\n'
    "1\thavana\ttranscript\t3073253\t3074322\t.\t+\t.\t"
    'gene_id "G1"; transcript_id "T1"; gene_biotype "TEC";\n'
    "1\tensembl\ttranscript\t3102016\t3102125\t.\t+\t.\t"
    'gene_id "G2"; transcript_id "T2"; gene_biotype "snRNA";\n'
)
ATTRS = ["gene_id", "gene_biotype", "transcript_id"]


_STR_VALUES = ["transcript", "gene", "pseudogene", "snRNA", "TEC"]
_INT_VALUES = [1, 3073253, 3100000, 9999999]


def _predicate(kind, str_value, int_value):
    """Build a type-valid predicate for the given kind."""
    if kind == "type_eq":
        return pl.col("type") == str_value
    if kind == "type_in":
        return pl.col("type").is_in(["transcript", "exon"])
    if kind == "start_ge":
        return pl.col("start") >= int_value
    if kind == "biotype_contains":
        return pl.col("gene_biotype").str.contains(str_value)
    if kind == "combined":
        return (pl.col("type") == "transcript") & pl.col("gene_biotype").str.contains(
            str_value
        )
    raise AssertionError(kind)


@settings(max_examples=120, deadline=None)
@given(
    kind=st.sampled_from(
        ["type_eq", "type_in", "start_ge", "biotype_contains", "combined"]
    ),
    str_value=st.sampled_from(_STR_VALUES),
    int_value=st.sampled_from(_INT_VALUES),
    cols=st.sampled_from(
        [
            ["transcript_id"],
            ["type", "gene_biotype"],
            ["chrom", "start", "transcript_id"],
        ]
    ),
)
def test_lazy_equals_eager(tmp_path_factory, kind, str_value, int_value, cols):
    gtf = tmp_path_factory.mktemp("eq") / "f.gtf"
    gtf.write_text(GTF_FIXTURE)
    pred = _predicate(kind, str_value, int_value)
    scan = pb.scan_gtf(str(gtf), attr_fields=ATTRS)

    lazy = scan.filter(pred).select(cols).collect().sort(by=cols)
    eager = scan.collect().filter(pred).select(cols).sort(by=cols)
    assert lazy.equals(
        eager
    ), f"kind={kind} str={str_value} int={int_value} cols={cols}"
