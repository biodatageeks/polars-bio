import polars as pl
import polars_bio as pb

FASTQ_PATH = "./data/quality_control/example.fastq"
PARQUET_PATH = "./data/quality_control/example.parquet"
TARGET_PATH = "./data/quality_control/target.csv"

class TestBaseSequenceContent:
    def __init__(self):
        """Initialize the test class"""
        self.target_df: pl.DataFrame = pl.read_csv(TARGET_PATH)

    def test_with_fastq_path(self):
        """Test base_sequence_content with FASTQ file path as input"""

        print(f"\nRunning test with FASTQ file path input: {FASTQ_PATH}")
        result = pb.base_sequence_content(FASTQ_PATH)
        
        self._assert_equal(result)
        print("✓ FASTQ path input test PASSED")

    def test_with_parquet_path(self):
        """Test base_sequence_content with PARQUET file path as input"""

        print(f"\nRunning test with PARQUET file path input: {PARQUET_PATH}")
        result = pb.base_sequence_content(PARQUET_PATH)

        self._assert_equal(result)
        print("✓ PARQUET path input test PASSED")

    def test_with_lazy_frame(self):
        """Test base_sequence_content with LazyFrame as input"""

        print("\nRunning test with LazyFrame input")
        lazy_df = pb.read_fastq(FASTQ_PATH)
        
        result = pb.base_sequence_content(lazy_df)

        self._assert_equal(result)
        print("✓ LazyFrame input test PASSED")

    def test_with_polars_df(self):
        """Test base_sequence_content with polars DataFrame as input"""
        
        print("\nRunning test with pandas DataFrame input")
        polars_df = pb.read_fastq(FASTQ_PATH).collect()
        
        result = pb.base_sequence_content(polars_df)

        self._assert_equal(result)
        print("✓ pandas DataFrame input test PASSED")
    
    def test_with_pandas_df(self):
        """Test base_sequence_content with pandas DataFrame as input"""
        
        print("\nRunning test with polars DataFrame input")
        fastq_df = pb.read_fastq(FASTQ_PATH).collect().to_pandas()
        
        result = pb.base_sequence_content(fastq_df)

        self._assert_equal(result)
        print("✓ polars DataFrame input test PASSED")

    def _assert_equal(self, df: pl.DataFrame):
        """Helper function that assert result DataFrame and target DataFrame"""
        assert self.target_df.shape == df.shape, f"Shape mismatch! Expected {self.target_df.shape}, got {df.shape}"
        assert self.target_df.equals(df), "Data content mismatch between result and target dataframes"

    def run_all_tests(self):
        """Run all tests in this class"""
        self.test_with_fastq_path()
        self.test_with_parquet_path()
        self.test_with_lazy_frame()
        self.test_with_polars_df()
        self.test_with_pandas_df()
        print("\n✓✓ All tests completed successfully!")
