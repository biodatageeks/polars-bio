# API reference

The polars-bio API reference is organized like the [Features](../features/index.md) guide:

- **[Reading files](reading.md)**: eager (`read_*`), lazy (`scan_*`), and describe (`describe_*`) access to bioinformatic formats from **local** and **[cloud](../features/cloud.md#cloud-storage)** storage.
- **[Genomic operations](operations.md)**: interval (range) operations — *overlap*, *nearest*, *coverage*, *merge*, *cluster*, *complement*, *subtract* — and *pileup* per-base depth.
- **[Writing files](writing.md)**: eager (`write_*`) and streaming (`sink_*`) output to bioinformatic formats.
- **[SQL interface](sql.md)**: register datasets as tables and query them with the **SQL** interface powered by [Apache DataFusion](https://datafusion.apache.org/user-guide/sql/index.html).
- **[Auxiliary](auxiliary.md)**: DataFrame metadata, session options, and error types.

There are 2 ways of using polars-bio API:

* using `polars_bio` module

!!! example

       ```python
       import polars_bio as pb
       pb.read_fastq("gs://genomics-public-data/platinum-genomes/fastq/ERR194146.fastq.gz").limit(1).collect()
       ```

* directly on a Polars LazyFrame under a registered `pb` [namespace](https://docs.pola.rs/api/python/stable/reference/api/polars.api.register_lazyframe_namespace.html#polars.api.register_lazyframe_namespace)

!!! example

       ```plaintext
        >>> type(df)
        <class 'polars.lazyframe.frame.LazyFrame'>

       ```
       ```python
          import polars_bio as pb
          df.pb.sort().limit(5).collect()
       ```



!!! tip
    1. Not all are available in both ways.
    2. You can of course use both ways in the same script.
