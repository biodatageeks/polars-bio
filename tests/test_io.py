import pandas as pd
import polars as pl
from _expected import (
    DATA_DIR,
    PD_DF_OVERLAP,
    PD_OVERLAP_DF1,
    PD_OVERLAP_DF2,
    PL_DF1,
    PL_DF2,
    PL_DF_OVERLAP,
)

import polars_bio as pb

# Set coordinate system metadata on test DataFrames (1-based)
PD_OVERLAP_DF1.attrs["coordinate_system_zero_based"] = False
PD_OVERLAP_DF2.attrs["coordinate_system_zero_based"] = False
PL_DF1.config_meta.set(coordinate_system_zero_based=False)
PL_DF2.config_meta.set(coordinate_system_zero_based=False)


def _lazy_with_metadata(df: pl.DataFrame) -> pl.LazyFrame:
    """Create a LazyFrame with coordinate system metadata."""
    lf = df.lazy()
    lf.config_meta.set(coordinate_system_zero_based=False)
    return lf


class TestMemoryCombinations:
    def test_frames(self):
        for df1 in [PD_OVERLAP_DF1, PL_DF1, _lazy_with_metadata(PL_DF1)]:
            for df2 in [PD_OVERLAP_DF2, PL_DF2, _lazy_with_metadata(PL_DF2)]:
                for output_type in [
                    "pandas.DataFrame",
                    "polars.DataFrame",
                    "polars.LazyFrame",
                ]:
                    result = pb.overlap(
                        df1,
                        df2,
                        cols1=("contig", "pos_start", "pos_end"),
                        cols2=("contig", "pos_start", "pos_end"),
                        output_type=output_type,
                    )
                    if output_type == "polars.LazyFrame":
                        result = result.collect()
                    if output_type == "pandas.DataFrame":
                        result = result.sort_values(
                            by=list(result.columns)
                        ).reset_index(drop=True)
                        pd.testing.assert_frame_equal(result, PD_DF_OVERLAP)
                    else:
                        result = result.sort(by=result.columns)
                        assert PL_DF_OVERLAP.equals(result)
