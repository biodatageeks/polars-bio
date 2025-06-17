import logging

from _expected import BIO_PD_DF1, BIO_PD_DF2

import polars_bio as pb

pb.ctx.set_option("datafusion.execution.parquet.schema_force_view_types", "true", False)
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


class TestZeroBasedWarning:

    def test_zero_based_warning(self, caplog):
        # capture WARNING+ on your module’s logger
        caplog.set_level("WARN")

        # run the code under test
        result = pb.overlap(
            BIO_PD_DF1,
            BIO_PD_DF2,
            cols1=("contig", "pos_start", "pos_end"),
            cols2=("contig", "pos_start", "pos_end"),
            output_type="pandas.DataFrame",
            suffixes=("_1", "_3"),
            use_zero_based=True,
        )

        # caplog.records is a list of LogRecord
        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelno == logging.WARNING
        assert "0-based coordinate system was selected" in record.getMessage()

    # check no warning when default
    def test_no_zero_based_warning(self, caplog):
        # capture WARNING+ on your module’s logger
        caplog.set_level("WARN")

        # run the code under test
        result = pb.overlap(
            BIO_PD_DF1,
            BIO_PD_DF2,
            cols1=("contig", "pos_start", "pos_end"),
            cols2=("contig", "pos_start", "pos_end"),
            output_type="pandas.DataFrame",
            suffixes=("_1", "_3"),
            use_zero_based=False,
        )

        # check no text in warnings
        assert "0-based coordinate system was selected" not in caplog.text
        # check no warning
