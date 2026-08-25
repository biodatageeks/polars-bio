"""BGEN read, scan, register, and describe tests."""

import inspect

import numpy as np
import polars as pl
import pytest
from _expected import DATA_DIR

import polars_bio as pb
from polars_bio.context import ctx

BGEN_DIR = DATA_DIR / "io" / "bgen"
BGEN_PATH = BGEN_DIR / "multisample.bgen"
# `multisample.vcf` with two calls replaced by `./.`, exported the same way.
# multisample.bgen has no missing call, so the sentinel had nothing to write.
MISSING_CALLS_PATH = BGEN_DIR / "missing_calls.bgen"
EXPECTED_MISSING_DOSAGE = np.array(
    [[1.0, np.nan, 2.0], [np.nan, 1.0, 0.0]], dtype=np.float32
)
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


def _genotype_field(schema, name: str) -> pl.DataType:
    """Look the genotype struct field up by name rather than by position."""
    fields = {field.name: field.dtype for field in schema["genotypes"].fields}
    return fields[name]


def _dosage_matrix(frame: pl.DataFrame) -> np.ndarray:
    column = frame.select("genotypes").to_arrow().column("genotypes").combine_chunks()
    struct = column.chunk(0) if hasattr(column, "chunk") else column
    values = struct.field("DS")
    flat = values.flatten().to_numpy(zero_copy_only=False)
    return np.ascontiguousarray(flat, dtype=np.float32).reshape(len(values), -1)


def _ploidy_matrix(frame: pl.DataFrame) -> np.ndarray:
    column = frame.select("genotypes").to_arrow().column("genotypes").combine_chunks()
    struct = column.chunk(0) if hasattr(column, "chunk") else column
    values = struct.field("PLOIDY")
    flat = values.flatten().to_numpy(zero_copy_only=False)
    return np.ascontiguousarray(flat, dtype=np.uint8).reshape(len(values), -1)


@pytest.fixture(autouse=True)
def single_partition():
    """Pin partitions for these tests without leaking the setting.

    The option is process-global, so leaving it at 1 would silently make every
    later test module run single-partition.
    """
    previous = pb.get_option(TARGET_PARTITIONS)
    pb.set_option(TARGET_PARTITIONS, "1")
    try:
        yield
    finally:
        pb.set_option(TARGET_PARTITIONS, previous)


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

    def test_failed_registration_keeps_the_existing_table(self):
        pb.register_bgen(str(BGEN_PATH), "kept", genotype_output="dosage")
        try:
            with pytest.raises(ValueError, match="NA00000"):
                pb.register_bgen(str(BGEN_PATH), "kept", samples=["NA00000"])
            # The failed replacement must not have destroyed the good table.
            frame = pb.sql("SELECT rsid FROM kept ORDER BY start").collect()
            assert frame["rsid"].to_list() == EXPECTED_RSIDS
        finally:
            ctx.deregister_table("kept")

    def test_register_bgen_rejects_an_unknown_genotype_output(self):
        with pytest.raises(ValueError, match="genotype_output"):
            pb.register_bgen(str(BGEN_PATH), "bgen_bad", genotype_output="calls")

    def test_register_bgen_emits_every_child_by_default(self):
        # The control for the projection test below: without it, that test
        # passes trivially if `genotype_fields` is ignored and the struct only
        # ever had `DS` to begin with.
        pb.register_bgen(str(BGEN_PATH), "bgen_all", genotype_output="dosage")
        try:
            schema = pb.sql("SELECT genotypes FROM bgen_all").collect_schema()
            assert [f.name for f in schema["genotypes"].fields] == ["DS", "PLOIDY"]
        finally:
            ctx.deregister_table("bgen_all")

    def test_register_bgen_can_decline_the_ploidy_child(self):
        pb.register_bgen(
            str(BGEN_PATH),
            "bgen_ds_only",
            genotype_output="dosage",
            genotype_fields=["DS"],
        )
        try:
            schema = pb.sql("SELECT genotypes FROM bgen_ds_only").collect_schema()
            assert [f.name for f in schema["genotypes"].fields] == ["DS"]
        finally:
            ctx.deregister_table("bgen_ds_only")


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

    def test_describe_bgen_uses_an_explicit_index(self):
        # `describe_bgen` reports whether an index was used, so it has to be
        # able to reach one stored away from the file. A path that does not
        # exist proves the argument is opened rather than dropped.
        with pytest.raises(ValueError, match=r"(?i)bgi"):
            pb.describe_bgen(str(BGEN_PATH), bgi_path=str(BGEN_DIR / "absent.bgi"))

    def test_describe_leaves_no_table_behind(self):
        # The temporary describe table must be gone, so a query naming it fails.
        pb.describe_bgen(str(BGEN_PATH))
        leftovers = [
            name
            for name in pb.sql("SHOW TABLES").collect()["table_name"].to_list()
            if name.startswith("_pb_bgen_describe_")
        ]
        assert leftovers == []


class TestBgenProbabilityLayout:
    """The fixed layout drops per-sample offsets but needs one shared width."""

    def test_default_layout_is_nested(self):
        schema = pb.scan_bgen(str(BGEN_PATH)).collect_schema()
        assert _genotype_field(schema, "GP") == pl.List(pl.List(pl.Float32))

    def test_fixed_layout_declares_the_width_in_the_schema(self):
        schema = pb.scan_bgen(
            str(BGEN_PATH), probability_layout="fixed"
        ).collect_schema()
        assert _genotype_field(schema, "GP") == pl.List(
            pl.Array(pl.Float32, EXPECTED_PROBABILITY_WIDTH)
        )

    def test_fixed_layout_returns_the_same_probabilities(self):
        def states(layout):
            frame = pb.read_bgen(str(BGEN_PATH), probability_layout=layout).sort(
                "start"
            )
            column = (
                frame.select("genotypes")
                .to_arrow()
                .column("genotypes")
                .combine_chunks()
            )
            struct = column.chunk(0) if hasattr(column, "chunk") else column
            per_sample = struct.field("GP").values
            return np.asarray(
                per_sample.values.to_numpy(zero_copy_only=False), dtype=np.float32
            )

        np.testing.assert_array_equal(states("nested"), states("fixed"))

    def test_fixed_layout_preserves_the_sample_count_per_variant(self):
        # A fixed-width slot exists for every sample, so the per-variant list
        # still has one entry per sample rather than only the called ones.
        frame = pb.read_bgen(str(BGEN_PATH), probability_layout="fixed").sort("start")
        column = (
            frame.select("genotypes").to_arrow().column("genotypes").combine_chunks()
        )
        struct = column.chunk(0) if hasattr(column, "chunk") else column
        first_variant = struct.field("GP")[0]
        assert len(first_variant) == len(EXPECTED_SAMPLES)
        assert all(sample.as_py() is not None for sample in first_variant)

    # A file that mixes state counts is rejected by the provider; the fixture
    # here is uniformly three states, so that path is covered by
    # `fixed_probability_layout_rejects_a_mixed_width_file` in
    # datafusion-bio-format-bgen rather than duplicated with a second fixture.

    def test_unknown_layout_is_rejected(self):
        with pytest.raises(ValueError, match="probability_layout"):
            pb.scan_bgen(str(BGEN_PATH), probability_layout="packed")

    def test_layout_is_ignored_for_dosage(self):
        frame = pb.read_bgen(
            str(BGEN_PATH), genotype_output="dosage", probability_layout="fixed"
        ).sort("start")
        np.testing.assert_allclose(
            _dosage_matrix(frame), EXPECTED_DOSAGE, rtol=0, atol=1 / 255
        )


class TestBgenGenotypeFields:
    """`PLOIDY` is a byte per genotype and a NumPy view of a result keeps the
    whole struct alive, so a caller that only wants dosages must be able to
    decline it."""

    def test_default_emits_every_child(self):
        schema = pb.scan_bgen(str(BGEN_PATH), genotype_output="dosage").collect_schema()
        assert [field.name for field in schema["genotypes"].fields] == ["DS", "PLOIDY"]

    def test_value_child_only_drops_ploidy(self):
        schema = pb.scan_bgen(
            str(BGEN_PATH), genotype_output="dosage", genotype_fields=["DS"]
        ).collect_schema()
        assert [field.name for field in schema["genotypes"].fields] == ["DS"]

    def test_dropping_ploidy_leaves_the_dosages_untouched(self):
        both = pb.read_bgen(str(BGEN_PATH), genotype_output="dosage").sort("start")
        ds_only = pb.read_bgen(
            str(BGEN_PATH), genotype_output="dosage", genotype_fields=["DS"]
        ).sort("start")
        # Establish the projection actually happened before comparing values:
        # without this the value comparison passes trivially when the option is
        # ignored, which is the failure mode this test exists to catch.
        assert [f.name for f in both.schema["genotypes"].fields] == ["DS", "PLOIDY"]
        assert [f.name for f in ds_only.schema["genotypes"].fields] == ["DS"]
        # Bit patterns, not approximate equality: the projection must not change
        # a single emitted value.
        np.testing.assert_array_equal(
            _dosage_matrix(ds_only).view(np.uint32),
            _dosage_matrix(both).view(np.uint32),
        )
        np.testing.assert_allclose(
            _dosage_matrix(ds_only), EXPECTED_DOSAGE, rtol=0, atol=1 / 255
        )

    def test_ploidy_is_emitted_alongside_the_value_child(self):
        frame = pb.read_bgen(
            str(BGEN_PATH), genotype_output="dosage", genotype_fields=["DS", "PLOIDY"]
        ).sort("start")
        # The fixture is diploid throughout.
        np.testing.assert_array_equal(
            _ploidy_matrix(frame),
            np.full(
                (len(EXPECTED_POSITIONS), len(EXPECTED_SAMPLES)), 2, dtype=np.uint8
            ),
        )

    def test_ploidy_without_the_value_child_is_rejected(self):
        # Serving it would mean decoding every genotype for an array that is
        # then discarded; the provider refuses it instead.
        with pytest.raises(ValueError, match="DS"):
            pb.read_bgen(
                str(BGEN_PATH), genotype_output="dosage", genotype_fields=["PLOIDY"]
            )

    def test_children_follow_the_requested_order(self):
        schema = pb.scan_bgen(
            str(BGEN_PATH), genotype_output="dosage", genotype_fields=["PLOIDY", "DS"]
        ).collect_schema()
        assert [field.name for field in schema["genotypes"].fields] == ["PLOIDY", "DS"]

    def test_probability_mode_names_its_own_value_child(self):
        schema = pb.scan_bgen(str(BGEN_PATH), genotype_fields=["GP"]).collect_schema()
        assert [field.name for field in schema["genotypes"].fields] == ["GP"]

    def test_the_other_modes_value_child_is_rejected(self):
        # `DS` does not exist in probability mode, and asking for it must fail
        # rather than silently produce a `GP` column under another name.
        with pytest.raises(ValueError, match="DS"):
            pb.read_bgen(str(BGEN_PATH), genotype_fields=["DS"])

    def test_an_empty_selection_is_rejected(self):
        # Asking for no children at all is a mistake, not a projection. The
        # provider refuses it too; catching it here means the file is never
        # opened, and it was the one rejection path with no test.
        with pytest.raises(ValueError, match="at least one"):
            pb.read_bgen(str(BGEN_PATH), genotype_output="dosage", genotype_fields=[])

    def test_a_misspelled_child_is_rejected_before_the_file_is_opened(self):
        # A path that does not exist would raise if the name check did not come
        # first, so this pins the ordering rather than just the message.
        with pytest.raises(ValueError, match="NOPE"):
            pb.read_bgen("/nonexistent/cohort.bgen", genotype_fields=["NOPE"])

    def test_the_mode_rule_stays_with_the_provider(self):
        # `GP` is a real field name, so the Python check passes it through; only
        # the provider knows it is wrong for dosage output. That split is
        # deliberate — the mode rule has one home.
        with pytest.raises(ValueError, match="GP"):
            pb.read_bgen(
                str(BGEN_PATH), genotype_output="dosage", genotype_fields=["GP"]
            )

    def test_register_bgen_rejects_an_empty_selection(self):
        with pytest.raises(ValueError, match="at least one"):
            pb.register_bgen(str(BGEN_PATH), "bgen_empty", genotype_fields=[])

    def test_unknown_child_is_rejected(self):
        with pytest.raises(ValueError, match="NOPE"):
            pb.read_bgen(
                str(BGEN_PATH), genotype_output="dosage", genotype_fields=["NOPE"]
            )


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


class TestBgenMatrix:
    """`read_bgen_matrix` exists to avoid the consolidation `scan_bgen`'s
    consumer does, not to decode differently."""

    def test_matrix_matches_the_dataframe_path(self):
        frame = pb.read_bgen(
            str(BGEN_PATH), genotype_output="dosage", genotype_fields=["DS"]
        ).sort("start")
        expected = _dosage_matrix(frame)
        matrix = pb.read_bgen_matrix(str(BGEN_PATH))
        order = np.argsort(matrix.positions, kind="stable")
        # Bit patterns: the matrix path must be faster, not different.
        np.testing.assert_array_equal(
            matrix.values[order].view(np.uint32), expected.view(np.uint32)
        )

    def test_matrix_reports_its_axes(self):
        matrix = pb.read_bgen_matrix(str(BGEN_PATH))
        assert matrix.values.shape == (len(EXPECTED_POSITIONS), len(EXPECTED_SAMPLES))
        assert matrix.values.dtype == np.float32
        assert matrix.values.flags.c_contiguous
        assert list(matrix.sample_names) == EXPECTED_SAMPLES
        assert sorted(matrix.positions.tolist()) == EXPECTED_POSITIONS

    def test_thread_count_does_not_change_the_values(self):
        base = pb.read_bgen_matrix(str(BGEN_PATH), threads=1)
        for threads in (2, 4):
            other = pb.read_bgen_matrix(str(BGEN_PATH), threads=threads)
            np.testing.assert_array_equal(
                other.values.view(np.uint32), base.values.view(np.uint32)
            )

    def test_sample_selection_reorders_the_columns(self):
        matrix = pb.read_bgen_matrix(str(BGEN_PATH), samples=["NA12880", "NA12878"])
        assert list(matrix.sample_names) == ["NA12880", "NA12878"]
        assert matrix.values.shape[1] == 2

    def test_non_bgen_path_is_rejected(self):
        with pytest.raises(ValueError, match=r"\.bgen"):
            pb.read_bgen_matrix(str(DATA_DIR / "io" / "vcf" / "multisample.vcf"))

    def test_a_missing_call_gets_the_sentinel(self):
        matrix = pb.read_bgen_matrix(str(MISSING_CALLS_PATH))
        np.testing.assert_array_equal(matrix.values, EXPECTED_MISSING_DOSAGE)

    def test_the_sentinel_is_configurable(self):
        matrix = pb.read_bgen_matrix(str(MISSING_CALLS_PATH), missing=-1.0)
        expected = np.where(
            np.isnan(EXPECTED_MISSING_DOSAGE), -1.0, EXPECTED_MISSING_DOSAGE
        )
        np.testing.assert_array_equal(matrix.values, expected)

    def test_missing_calls_agree_with_the_dataframe_path(self):
        # The matrix writes a sentinel where the scan writes a null, so the two
        # have to be compared through that correspondence rather than directly.
        matrix = pb.read_bgen_matrix(str(MISSING_CALLS_PATH))
        frame = pb.read_bgen(str(MISSING_CALLS_PATH), genotype_output="dosage").sort(
            "start"
        )
        for row, genotypes in enumerate(frame["genotypes"].to_list()):
            for column, dosage in enumerate(genotypes["DS"]):
                observed = matrix.values[row][column]
                if dosage is None:
                    assert np.isnan(observed), f"row {row} column {column}"
                else:
                    assert observed == dosage, f"row {row} column {column}"

    def test_the_destination_is_checked_before_it_is_decoded_into(self):
        # `BgenMatrixReader` is importable and used to take the destination as
        # an integer address, so the check at that boundary is what stands
        # between a wrong array and a corrupted heap.
        from polars_bio.polars_bio import (
            BgenMatrixReader,
            BgenReadOptions,
            PyObjectStorageOptions,
        )

        options = BgenReadOptions(
            object_storage_options=PyObjectStorageOptions(
                allow_anonymous=True,
                enable_request_payer=False,
                chunk_size=8,
                concurrent_fetches=1,
                max_retries=1,
                timeout=10,
                compression_type="auto",
            ),
            genotype_output="dosage",
            genotype_fields=["DS"],
            zero_based=False,
        )
        reader = BgenMatrixReader(str(BGEN_PATH), options)
        variants, samples = reader.shape()
        with pytest.raises(TypeError, match="numpy.ndarray"):
            reader.read_into(12345, 1, float("nan"))
        with pytest.raises(ValueError, match="dtype"):
            reader.read_into(np.empty((variants, samples), dtype=np.int8), 1, 0.0)
        frozen = np.zeros((variants, samples), dtype=np.float32)
        frozen.flags.writeable = False
        with pytest.raises(ValueError, match="writable"):
            reader.read_into(frozen, 1, float("nan"))
        misaligned = np.ndarray(
            (variants, samples),
            dtype=np.float32,
            buffer=bytearray(variants * samples * 4 + 1),
            offset=1,
        )
        with pytest.raises(ValueError, match="aligned"):
            reader.read_into(misaligned, 1, float("nan"))
        with pytest.raises(TypeError, match="numpy.ndarray"):
            reader.read_into(
                type("Sub", (np.ndarray,), {})((variants, samples), dtype=np.float32),
                1,
                float("nan"),
            )
        # And the accepting case, without which the refusals prove nothing.
        values = np.empty((variants, samples), dtype=np.float32)
        reader.read_into(values, 1, float("nan"))
        np.testing.assert_array_equal(values, EXPECTED_DOSAGE)

    def test_the_matrix_path_offers_no_output_mode(self):
        # Probabilities are variable width and have no dense shape, so the
        # matrix reader declines to offer the choice rather than accepting one
        # and failing at open. Pinned because the spec now says the argument is
        # absent, and an absent argument is only observable as its absence.
        assert (
            "genotype_output" not in inspect.signature(pb.read_bgen_matrix).parameters
        )
        with pytest.raises(TypeError):
            pb.read_bgen_matrix(str(BGEN_PATH), genotype_output="probability")

    def test_matrix_positions_follow_the_coordinate_system(self):
        # The matrix labels its rows, and those labels must be the `start` the
        # scan emits under whichever coordinate system is in force. Returning
        # the raw one-based BGEN position instead puts every row one base later
        # than the DataFrame path, which no assertion about values would catch —
        # and which passes unnoticed under the one-based default, where the two
        # values coincide.
        for zero_based in (False, True):
            matrix = pb.read_bgen_matrix(str(BGEN_PATH), use_zero_based=zero_based)
            frame = pb.read_bgen(
                str(BGEN_PATH),
                genotype_output="dosage",
                genotype_fields=["DS"],
                use_zero_based=zero_based,
            ).sort("start")
            assert (
                sorted(matrix.positions.tolist()) == frame["start"].to_list()
            ), f"zero_based={zero_based}"
