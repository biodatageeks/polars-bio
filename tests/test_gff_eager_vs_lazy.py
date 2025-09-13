import polars as pl

import polars_bio as pb
from tests._expected import DATA_DIR


def test_gff_lazy_vs_eager_projection_pushdown_parallel():
    path = f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3.bgz"

    # Lazy with projection pushdown and parallel read
    lf = pb.scan_gff(
        path, attr_fields=["ID"], projection_pushdown=True, parallel=True
    ).select(["chrom", "start", "end", "type", "source", "ID"])

    # Intentionally keep a second reference (no-op, mirrors the request)
    lf2 = lf
    out_lazy = lf2.collect()

    # Eager read (parallel), then select the same columns
    df_eager_full = pb.read_gff(path, attr_fields=["ID"], parallel=True)
    out_eager = df_eager_full.select(["chrom", "start", "end", "type", "source", "ID"])

    # Sort for stable comparison (avoid incidental ordering differences)
    sort_cols = ["chrom", "start", "end", "type", "source", "ID"]
    out_lazy_sorted = out_lazy.sort(by=sort_cols)
    out_eager_sorted = out_eager.sort(by=sort_cols)

    assert out_lazy_sorted.equals(out_eager_sorted)


def test_gff_attr_fields_lazy_vs_eager():
    """Test that attr_fields parameter produces identical results for lazy and eager evaluation."""
    path = f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3.bgz"
    columns = ["chrom", "start", "end", "type", "ID"]

    # Lazy evaluation with attr_fields parameter
    lazy_result = pb.scan_gff(path, attr_fields=["ID"]).select(columns).collect()

    # Eager evaluation with attr_fields parameter
    eager_result = pb.read_gff(path, attr_fields=["ID"]).select(columns)

    # Sort for stable comparison
    lazy_sorted = lazy_result.sort(by=columns)
    eager_sorted = eager_result.sort(by=columns)

    # Verify they have the same schema
    assert lazy_sorted.schema == eager_sorted.schema

    # Verify they have the same data
    assert lazy_sorted.equals(eager_sorted)

    # Verify ID column is directly accessible (not nested)
    assert "ID" in lazy_sorted.columns
    assert "ID" in eager_sorted.columns
    assert lazy_sorted["ID"].dtype == pl.String
    assert eager_sorted["ID"].dtype == pl.String


def test_gff_attr_fields_multiple_attributes():
    """Test that multiple attributes can be extracted correctly in both lazy and eager modes."""
    path = f"{DATA_DIR}/io/gff/gencode.v38.annotation.gff3.bgz"
    attr_fields = ["ID", "gene_name", "gene_type"]
    columns = ["chrom", "start", "end", "type"] + attr_fields

    # Lazy evaluation
    lazy_result = pb.scan_gff(path, attr_fields=attr_fields).select(columns).collect()

    # Eager evaluation
    eager_result = pb.read_gff(path, attr_fields=attr_fields).select(columns)

    # Sort for stable comparison
    lazy_sorted = lazy_result.sort(by=columns)
    eager_sorted = eager_result.sort(by=columns)

    # Verify results match
    assert lazy_sorted.schema == eager_sorted.schema
    assert lazy_sorted.equals(eager_sorted)

    # Verify all requested attribute columns are present
    for attr in attr_fields:
        assert attr in lazy_sorted.columns
        assert attr in eager_sorted.columns
        assert lazy_sorted[attr].dtype == pl.String
        assert eager_sorted[attr].dtype == pl.String
