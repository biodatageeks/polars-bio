"""Custom exceptions for polars-bio."""


class CoordinateSystemMismatchError(Exception):
    """Raised when two DataFrames have different coordinate systems.

    This error occurs when attempting range operations (overlap, nearest, etc.)
    on DataFrames where one uses 0-based coordinates and the other uses 1-based
    coordinates.

    Example:
        ```python
        df1 = pb.scan_vcf("file1.vcf", one_based=False)  # 0-based
        df2 = pb.scan_vcf("file2.vcf", one_based=True)   # 1-based
        pb.overlap(df1, df2)  # Raises CoordinateSystemMismatchError
        ```
    """

    pass


class MissingCoordinateSystemError(Exception):
    """Raised when a DataFrame lacks coordinate system metadata.

    Range operations require coordinate system metadata to determine the
    correct interval semantics. This error is raised when:

    - A Polars LazyFrame/DataFrame lacks polars-config-meta metadata
    - A Pandas DataFrame lacks df.attrs["coordinate_system_zero_based"]
    - A file path registers a table without Arrow schema metadata

    For Polars DataFrames, use polars-bio I/O functions (scan_*, read_*) which
    automatically set the metadata.

    For Pandas DataFrames, set the attribute before passing to range operations:
        ```python
        df.attrs["coordinate_system_zero_based"] = True  # 0-based coords
        ```

    Example:
        ```python
        import pandas as pd
        import polars_bio as pb

        pdf = pd.read_csv("intervals.bed", sep="\t", names=["chrom", "start", "end"])
        pb.overlap(pdf, pdf)  # Raises MissingCoordinateSystemError

        # Fix: set the coordinate system metadata
        pdf.attrs["coordinate_system_zero_based"] = True
        pb.overlap(pdf, pdf)  # Works correctly
        ```
    """

    pass
