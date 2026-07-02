# SQL processing

**On this page:** [Accessing registered tables](#accessing-registered-tables) · [Reusable views](#reusable-views)

polars-bio provides a SQL-like API for bioinformatic data querying or manipulation.
Check [SQL reference](https://datafusion.apache.org/user-guide/sql/index.html) for more details.

```python
import polars_bio as pb
pb.register_vcf("gs://gcp-public-data--gnomad/release/4.1/genome_sv/gnomad.v4.1.sv.sites.vcf.gz", "gnomad_sv", info_fields=["SVTYPE", "SVLEN"])
pb.sql("SELECT * FROM gnomad_sv WHERE SVTYPE = 'DEL' AND SVLEN > 1000").limit(3).collect()
```

Local VCF Zarr stores can be registered with the same SQL context:

```python
import polars_bio as pb

pb.register_vcf_zarr(
    "cohort.vcz",
    "cohort",
    info_fields=["DP"],
    format_fields=["GT"],
    samples=["HG001"],
)
pb.sql('SELECT chrom, start, "DP", genotypes FROM cohort LIMIT 5').collect()
```

## Accessing registered tables

You can access registered tables programmatically using the `ctx.table()` method, which returns a DataFusion DataFrame:

```python
import polars_bio as pb
from polars_bio.context import ctx

# Register a file as a table
pb.register_vcf("variants.vcf", name="my_variants")

# Get the table as a DataFusion DataFrame
df = ctx.table("my_variants")

# Access the Arrow schema (includes coordinate system metadata)
schema = df.schema()
print(schema.metadata)  # {b'bio.coordinate_system_zero_based': b'false'}

# Execute queries on the DataFrame
result = df.filter(df["chrom"] == "chr1").collect()
```

!!! tip
    The `ctx.table()` method is useful for:

    1. Accessing Arrow schema metadata (including coordinate system information)
    2. Using the DataFusion DataFrame API directly
    3. Integrating with other DataFusion-based tools

## Reusable views

You can use the [view](../api/sql.md#polars_bio.data_processing.register_view) mechanism to create a virtual table from a DataFrame that captures preprocessing steps and reuse it across multiple queries.
To avoid materializing the intermediate results in memory, run your processing in [streaming](https://docs.pola.rs/user-guide/concepts/streaming/) mode.
