polars-bio API is grouped into the following categories:

- **[File I/O](#polars_bio.data_input)**: Reading files in various biological formats from **local** and **[cloud](/polars-bio/features/#cloud-storage)** storage.
- **[Data Processing](#polars_bio.data_processing)**: Exposing end user to the rich **SQL** programming interface powered by [Apache Datafusion](https://datafusion.apache.org/user-guide/sql/index.html) for operations, such as sorting, filtering and other transformations on input bioinformatic datasets registered as tables. You can easily query and process file formats such as *VCF*, *GFF*, *BAM*, *FASTQ* using SQL syntax.
- **[Interval Operations](#polars_bio.range_operations)**: Functions for performing common interval operations, such as *overlap*, *nearest*, *coverage*.

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

::: polars_bio
    handler: python
    options:
        docstring_section_style: table