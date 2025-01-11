import bioframe as bf
from _expected import DATA_DIR

import polars_bio as pb


class TestPolarsExt:
    file = f"{DATA_DIR}/io/bed/ENCFF001XKR.bed.gz"

    def test_sort_bedframe(self):
        df_1_unsorted = (
            pb.read_table(self.file, schema="bed9").collect().sample(1000, shuffle=True)
        )
        df_2 = df_1_unsorted.to_pandas()
        df_2 = bf.sort_bedframe(df_2)
        df_1 = df_1_unsorted.lazy().pb.sort_bedframe().collect().to_pandas()
        assert df_1.equals(df_2)
        assert not df_1_unsorted.to_pandas().equals(df_2)
