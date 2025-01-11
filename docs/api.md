
## API structure

There are 2 ways of using polars-bio API:

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

 * using `polars_bio` module

!!! example

       ```python
          import polars_bio as pb
          df = pb.read_table("https://www.encodeproject.org/files/ENCFF001XKR/@@download/ENCFF001XKR.bed.gz",schema="bed9")
       ```

!!! tip
    1. Not all are available in both ways.
    2. You can of course use both ways in the same script.

::: polars_bio
    handler: python
    options:
        docstring_section_style: table