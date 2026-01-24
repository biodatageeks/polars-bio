DEFAULT_INTERVAL_COLUMNS = ["chrom", "start", "end"]
DEFAULT_BATCH_SIZE = 8192

# DataFusion configuration option for coordinate system
POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED = "datafusion.bio.coordinate_system_zero_based"

# DataFusion configuration option for coordinate system metadata check
# When "true" (default), MissingCoordinateSystemError is raised if metadata is missing
# When "false", falls back to POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED and emits a warning
POLARS_BIO_COORDINATE_SYSTEM_CHECK = "datafusion.bio.coordinate_system_check"
