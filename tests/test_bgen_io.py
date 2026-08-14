"""BGEN read, scan, register, and describe tests."""

import numpy as np
import polars as pl
import pytest
from _expected import DATA_DIR

import polars_bio as pb
from polars_bio.context import ctx

BGEN_DIR = DATA_DIR / "io" / "bgen"
BGEN_PATH = BGEN_DIR / "multisample.bgen"
TARGET_PARTITIONS = "datafusion.execution.target_partitions"

# The fixture is `multisample.vcf` exported by `plink2 --export bgen-1.3
# bits=8`: three unphased diploid biallelic variants for three samples, so every
# probability is exactly 0 or 1. plink2 writes the alternate allele first, so
# `alleles[1]` is the VCF reference allele and dosage counts that allele.
EXPECTED_SAMPLES = ["NA12878", "NA12879", "NA12880"]
EXPECTED_POSITIONS = [10000, 20000, 30000]
EXPECTED_RSIDS = ["rs1", "rs2", "rs3"]
EXPECTED_ALLELES = [["G", "A"], ["T", "C"], ["A", "G"]]
EXPECTED_DOSAGE = np.array(
    [[1.0, 0.0, 2.0], [2.0, 1.0, 0.0], [0.0, 2.0, 1.0]], dtype=np.float32
)
EXPECTED_PROBABILITY_WIDTH = 3


def _dosage_matrix(frame: pl.DataFrame) -> np.ndarray:
    column = frame.select("genotypes").to_arrow().column("genotypes").combine_chunks()
    struct = column.chunk(0) if hasattr(column, "chunk") else column
    values = struct.field("DS")
    flat = values.flatten().to_numpy(zero_copy_only=False)
    return np.ascontiguousarray(flat, dtype=np.float32).reshape(len(values), -1)


@pytest.fixture(autouse=True)
def single_partition():
    pb.set_option(TARGET_PARTITIONS, "1")
    yield


class TestBgenRead:
    def test_read_bgen_returns_variant_metadata(self):
        frame = pb.read_bgen(str(BGEN_PATH)).sort("start")
        assert frame.height == 3
        assert frame["start"].to_list() == EXPECTED_POSITIONS
        assert frame["rsid"].to_list() == EXPECTED_RSIDS
        assert [list(alleles) for alleles in frame["alleles"].to_list()] == (
            EXPECTED_ALLELES
        )

    def test_default_output_is_genotype_probabilities(self):
        schema = pb.scan_bgen(str(BGEN_PATH)).collect_schema()
        assert [field.name for field in schema["genotypes"].fields] == ["GP", "PLOIDY"]

    def test_dosage_output_replaces_the_probability_field(self):
        schema = pb.scan_bgen(str(BGEN_PATH), genotype_output="dosage").collect_schema()
        assert [field.name for field in schema["genotypes"].fields] == ["DS", "PLOIDY"]

    def test_dosage_output_counts_the_second_allele(self):
        frame = pb.read_bgen(str(BGEN_PATH), genotype_output="dosage").sort("start")
        np.testing.assert_allclose(
            _dosage_matrix(frame), EXPECTED_DOSAGE, rtol=0, atol=1 / 255
        )

    def test_probabilities_sum_to_one_per_sample(self):
        frame = pb.read_bgen(str(BGEN_PATH)).sort("start")
        column = (
            frame.select("genotypes").to_arrow().column("genotypes").combine_chunks()
        )
        struct = column.chunk(0) if hasattr(column, "chunk") else column
        per_sample = struct.field("GP").values
        widths = np.diff(np.asarray(per_sample.offsets, dtype=np.int64))
        states = np.asarray(
            per_sample.values.to_numpy(zero_copy_only=False), dtype=np.float32
        )
        assert widths.min() == widths.max() == EXPECTED_PROBABILITY_WIDTH
        totals = states.reshape(-1, EXPECTED_PROBABILITY_WIDTH).sum(axis=1)
        np.testing.assert_allclose(totals, 1.0, rtol=0, atol=1e-5)

    def test_sample_selection_reorders_and_subsets(self):
        frame = pb.read_bgen(
            str(BGEN_PATH),
            genotype_output="dosage",
            samples=["NA12880", "NA12878"],
        ).sort("start")
        np.testing.assert_allclose(
            _dosage_matrix(frame),
            EXPECTED_DOSAGE[:, [2, 0]],
            rtol=0,
            atol=1 / 255,
        )

    def test_sample_names_are_exposed_as_metadata(self):
        scan = pb.scan_bgen(str(BGEN_PATH))
        metadata = pb.get_metadata(scan)["header"]
        assert metadata["sample_names"] == EXPECTED_SAMPLES
        assert metadata["layout"] == "2"
        assert metadata["genotype_output"] == "probability"

    def test_projection_of_metadata_columns_only(self):
        frame = pb.scan_bgen(str(BGEN_PATH)).select("chrom", "rsid").collect()
        assert frame.columns == ["chrom", "rsid"]
        assert frame.height == 3

    def test_predicate_selects_a_single_variant(self):
        frame = (
            pb.scan_bgen(str(BGEN_PATH))
            .filter(pl.col("rsid") == "rs2")
            .select("start", "rsid")
            .collect()
        )
        assert frame["rsid"].to_list() == ["rs2"]
        assert frame["start"].to_list() == [20000]


class TestBgenPartitions:
    @pytest.mark.parametrize("partitions", ["1", "2", "4"])
    def test_content_is_independent_of_partition_count(self, partitions):
        pb.set_option(TARGET_PARTITIONS, partitions)
        frame = pb.read_bgen(str(BGEN_PATH), genotype_output="dosage").sort("start")
        assert frame["start"].to_list() == EXPECTED_POSITIONS
        np.testing.assert_allclose(
            _dosage_matrix(frame), EXPECTED_DOSAGE, rtol=0, atol=1 / 255
        )


class TestBgenRegister:
    def test_register_bgen_creates_a_queryable_table(self):
        pb.register_bgen(str(BGEN_PATH), "bgen_table", genotype_output="dosage")
        try:
            frame = pb.sql(
                "SELECT rsid, start FROM bgen_table ORDER BY start"
            ).collect()
            assert frame["rsid"].to_list() == EXPECTED_RSIDS
        finally:
            ctx.deregister_table("bgen_table")

    def test_register_bgen_rejects_an_unknown_genotype_output(self):
        with pytest.raises(ValueError, match="genotype_output"):
            pb.register_bgen(str(BGEN_PATH), "bgen_bad", genotype_output="calls")


class TestBgenDescribe:
    def test_describe_bgen_reports_schema_and_layout(self):
        described = pb.describe_bgen(str(BGEN_PATH))
        assert "genotypes" in described["name"].to_list()
        assert described["layout"].unique().to_list() == ["2"]
        assert described["index"].unique().to_list() == ["transient"]
        assert described["sample_names_synthetic"].unique().to_list() == ["false"]


    def test_describe_does_not_disturb_a_registered_table(self):
        pb.register_bgen(str(BGEN_PATH), "multisample", genotype_output="dosage")
        try:
            before = pb.sql("SELECT * FROM multisample").collect_schema()["genotypes"]
            pb.describe_bgen(str(BGEN_PATH))
            after = pb.sql("SELECT * FROM multisample").collect_schema()["genotypes"]
            assert before == after
        finally:
            ctx.deregister_table("multisample")

    def test_describe_leaves_no_table_behind(self):
        # The temporary describe table must be gone, so a query naming it fails.
        pb.describe_bgen(str(BGEN_PATH))
        leftovers = [
            name
            for name in pb.sql("SHOW TABLES").collect()["table_name"].to_list()
            if name.startswith("_pb_bgen_describe_")
        ]
        assert leftovers == []


class TestBgenValidation:
    @pytest.mark.parametrize(
        "call",
        [
            lambda path: pb.read_bgen(path),
            lambda path: pb.scan_bgen(path),
            lambda path: pb.describe_bgen(path),
            lambda path: pb.register_bgen(path, "unused"),
        ],
    )
    def test_non_bgen_paths_are_rejected(self, call):
        with pytest.raises(ValueError, match=r"\.bgen"):
            call(str(DATA_DIR / "io" / "vcf" / "multisample.vcf"))

    def test_unknown_genotype_output_is_rejected(self):
        with pytest.raises(ValueError, match="genotype_output"):
            pb.scan_bgen(str(BGEN_PATH), genotype_output="probabilities")

    def test_missing_sample_is_reported(self):
        # The BGEN registration arm returns its error rather than panicking, so
        # this must be a ValueError and not a PanicException.
        with pytest.raises(ValueError, match="NA00000"):
            pb.read_bgen(str(BGEN_PATH), samples=["NA00000"])
