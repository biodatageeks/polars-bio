import polars as pl
import polars.testing as pl_testing
from _expected import DATA_DIR

import polars_bio as pb


class TestFasta:
    fasta_path = f"{DATA_DIR}/io/fasta/test.fasta"

    def test_count(self):
        df = pb.read_fasta(self.fasta_path)
        assert len(df) == 2

    def test_read_fasta(self):

        df = pb.read_fasta(self.fasta_path)
        print("Actual DataFrame:")
        print(df)
        print("Actual Schema:")
        print(df.schema)

        expected_df = pl.DataFrame(
            {
                "name": ["seq1", "seq2"],
                "description": ["First sequence", "Second sequence"],
                "sequence": ["ACTG", "GATTACA"],
            }
        )

        pl_testing.assert_frame_equal(df, expected_df)
