DEFAULT_INTERVAL_COLUMNS = ["chrom", "start", "end"]
DEFAULT_BATCH_SIZE = 8192

# DataFusion configuration option for coordinate system
POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED = "datafusion.bio.coordinate_system_zero_based"

# DataFusion configuration option for coordinate system metadata check
# When "false" (default), falls back to POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED and emits a warning
# When "true", MissingCoordinateSystemError is raised if metadata is missing
POLARS_BIO_COORDINATE_SYSTEM_CHECK = "datafusion.bio.coordinate_system_check"

# Session option for the processed-rows progress bar printed while a LazyFrame
# returned by polars-bio is collected
# When "true" (default), the bar is shown; when "false", it is not created
# Only polars-bio's own bars are affected, and the option can be changed at any time
POLARS_BIO_PROGRESS_BAR = "datafusion.bio.progress_bar"
