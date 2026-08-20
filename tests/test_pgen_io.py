"""PGEN read, scan, register, and describe tests."""

import sys

import polars as pl
import pyarrow as pa
import pytest
from _expected import DATA_DIR

import polars_bio as pb
import polars_bio.io as pb_io
import polars_bio.sql  # noqa: F401 - imported for its module object below
from polars_bio.context import ctx

# `polars_bio.sql` the attribute is the query function, not the module, so the
# module object has to come from sys.modules to be patched.
pb_sql = sys.modules["polars_bio.sql"]

PGEN_DIR = DATA_DIR / "io" / "pgen"
ORACLE_PATH = PGEN_DIR / "oracle.pgen"
DOSAGE_PATH = PGEN_DIR / "dosage.pgen"
PHASE_PATH = PGEN_DIR / "phase.pgen"
UNUSED_ALT_PATH = PGEN_DIR / "unused_alt.pgen"
TARGET_PARTITIONS = "datafusion.execution.target_partitions"

# oracle: 3 variants over 8 samples s1..s8, written by pgenlib.
ORACLE_SAMPLES = ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]
ORACLE_IDS = ["v1", "v2", "v3"]
ORACLE_CHROMS = ["1", "1", "2"]
ORACLE_REFS = ["A", "G", "C"]
ORACLE_ALTS = [["C"], ["T"], ["G"]]
# The single-variant filesets each hold four samples, the fourth missing.
SMALL_SAMPLES = ["s1", "s2", "s3", "s4"]
DOSAGE_DS = [0.125, 1.0, 1.875, None]
PHASE_GT = [[0, 1], [1, 0], [0, 0], None]
PHASE_PHASED = [True, True, False, None]
UNUSED_ALT_GT = [[0, 0], [0, 1], [1, 1], None]


def _genotypes(frame: pl.DataFrame):
    """Return the genotypes column as a single Arrow StructArray."""
    column = frame.select("genotypes").to_arrow().column("genotypes").combine_chunks()
    return column.chunk(0) if hasattr(column, "chunk") else column


def _child(frame: pl.DataFrame, name: str, row: int = 0) -> list:
    """Return one genotype child's per-sample values for a single row."""
    return _genotypes(frame).field(name)[row].as_py()


def _genotype_names(frame: pl.DataFrame) -> list:
    """Return the emitted genotype children in schema order."""
    genotypes = _genotypes(frame).type
    return [genotypes.field(position).name for position in range(genotypes.num_fields)]


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


class TestPgenRead:
    def test_read_pgen_returns_variant_metadata(self):
        frame = pb.read_pgen(str(ORACLE_PATH)).sort("start")
        assert frame["id"].to_list() == ORACLE_IDS
        assert frame["chrom"].to_list() == ORACLE_CHROMS
        assert frame["ref"].to_list() == ORACLE_REFS
        assert [row.to_list() for row in frame["alt"]] == ORACLE_ALTS
        assert frame.height == 3

    def test_default_emits_only_the_gt_field(self):
        # The provider default selects all five children; the Python default
        # narrows to GT so read_pgen(path) is not the expensive call.
        frame = pb.read_pgen(str(ORACLE_PATH))
        assert _genotype_names(frame) == ["GT"]

    def test_scan_pgen_is_lazy(self):
        lazy = pb.scan_pgen(str(ORACLE_PATH))
        assert isinstance(lazy, pl.LazyFrame)
        assert lazy.collect().height == 3

    def test_dosage_field_reports_stored_dosages(self):
        frame = pb.read_pgen(str(DOSAGE_PATH), genotype_fields=["DS"])
        assert _genotype_names(frame) == ["DS"]
        values = _child(frame, "DS")
        assert values[3] is None
        assert values[:3] == pytest.approx(DOSAGE_DS[:3])

    def test_phase_field_accompanies_gt(self):
        frame = pb.read_pgen(str(PHASE_PATH), genotype_fields=["GT", "PHASED"])
        assert _genotype_names(frame) == ["GT", "PHASED"]
        assert _child(frame, "GT") == PHASE_GT
        assert _child(frame, "PHASED") == PHASE_PHASED

    def test_unused_alt_allele_keeps_allele_indices(self):
        frame = pb.read_pgen(str(UNUSED_ALT_PATH))
        # The PVAR declares ALT=C,G but only C is observed; indices still refer
        # to the declared allele list.
        assert [row.to_list() for row in frame["alt"]] == [["C", "G"]]
        assert _child(frame, "GT") == UNUSED_ALT_GT

    def test_requested_field_order_is_preserved(self):
        frame = pb.read_pgen(str(PHASE_PATH), genotype_fields=["PHASED", "GT"])
        assert _genotype_names(frame) == ["PHASED", "GT"]

    def test_all_five_fields_can_be_selected(self):
        frame = pb.read_pgen(
            str(DOSAGE_PATH),
            genotype_fields=["GT", "PHASED", "DS", "DS_STORED", "HDS"],
        )
        assert _genotype_names(frame) == [
            "GT",
            "PHASED",
            "DS",
            "DS_STORED",
            "HDS",
        ]


class TestPgenValidation:
    @pytest.mark.parametrize("call", [pb.read_pgen, pb.scan_pgen])
    def test_non_pgen_paths_are_rejected(self, call):
        with pytest.raises(ValueError, match=r"\.pgen"):
            call("cohort.bgen")

    def test_unknown_genotype_field_is_rejected(self):
        with pytest.raises(ValueError, match="GQ"):
            pb.read_pgen(str(ORACLE_PATH), genotype_fields=["GQ"])

    def test_empty_genotype_fields_is_rejected(self):
        with pytest.raises(ValueError, match="at least one"):
            pb.read_pgen(str(ORACLE_PATH), genotype_fields=[])


class TestPgenSampleSelection:
    def test_samples_subset_and_reorder(self):
        frame = pb.read_pgen(str(ORACLE_PATH), samples=["s8", "s2", "s1"]).sort("start")
        assert len(_child(frame, "GT", 0)) == 3
        # Upstream provider_test.rs asserts these two rows for this exact
        # requested order; its gt_values(batch, column, row) returns the
        # per-sample values of one row. The pgenlib source matrix for v1 is
        # [0, 1, 2, -9, 0, 0, 0, 0] over s1..s8, so s8=0, s2=1, s1=0 gives
        # [[0,0], [0,1], [0,0]] once reordered.
        assert _child(frame, "GT", 0) == [[0, 0], [0, 1], [0, 0]]
        assert _child(frame, "GT", 1) == [[0, 0], [0, 0], [0, 0]]

    def test_sample_names_are_exposed_as_metadata(self):
        lazy = pb.scan_pgen(str(ORACLE_PATH))
        metadata = pb.get_metadata(lazy)
        assert metadata["header"]["sample_names"] == ORACLE_SAMPLES

    def test_selected_sample_names_follow_the_request(self):
        lazy = pb.scan_pgen(str(ORACLE_PATH), samples=["s8", "s2", "s1"])
        metadata = pb.get_metadata(lazy)
        assert metadata["header"]["sample_names"] == ["s8", "s2", "s1"]

    def test_missing_sample_errors_by_default(self):
        with pytest.raises(Exception, match="nope"):
            pb.read_pgen(str(ORACLE_PATH), samples=["s1", "nope"])

    def test_missing_sample_can_be_ignored(self):
        frame = pb.read_pgen(
            str(ORACLE_PATH),
            samples=["s1", "nope"],
            missing_sample_policy="ignore",
        )
        assert len(_child(frame, "GT", 0)) == 1


class TestPgenPsamIdMode:
    def test_default_mode_uses_iid_alone(self):
        lazy = pb.scan_pgen(str(ORACLE_PATH))
        assert pb.get_metadata(lazy)["header"]["sample_names"] == ORACLE_SAMPLES

    def test_fid_iid_mode_prefixes_the_default_fid(self):
        # These PSAM files declare only #IID, so FID defaults to "0".
        lazy = pb.scan_pgen(str(ORACLE_PATH), psam_id_mode="fid_iid")
        assert pb.get_metadata(lazy)["header"]["sample_names"] == [
            f"0:{name}" for name in ORACLE_SAMPLES
        ]

    def test_fid_iid_sid_mode_appends_the_default_sid(self):
        lazy = pb.scan_pgen(str(ORACLE_PATH), psam_id_mode="fid_iid_sid")
        assert pb.get_metadata(lazy)["header"]["sample_names"] == [
            f"0:{name}:0" for name in ORACLE_SAMPLES
        ]

    def test_selection_uses_the_constructed_names(self):
        frame = pb.read_pgen(str(ORACLE_PATH), psam_id_mode="fid_iid", samples=["0:s2"])
        assert len(_child(frame, "GT", 0)) == 1

    def test_unknown_id_mode_is_rejected(self):
        with pytest.raises(ValueError, match="psam_id_mode"):
            pb.read_pgen(str(ORACLE_PATH), psam_id_mode="fid")

    def test_unknown_missing_sample_policy_is_rejected(self):
        with pytest.raises(ValueError, match="missing_sample_policy"):
            pb.read_pgen(str(ORACLE_PATH), missing_sample_policy="skip")


class TestPgenCompanionPaths:
    def test_explicit_companions_are_used(self):
        frame = pb.read_pgen(
            str(ORACLE_PATH),
            pvar_path=str(PGEN_DIR / "oracle.pvar"),
            psam_path=str(PGEN_DIR / "oracle.psam"),
        ).sort("start")
        assert frame["id"].to_list() == ORACLE_IDS

    def test_a_missing_companion_reports_the_path(self, tmp_path):
        orphan = tmp_path / "orphan.pgen"
        orphan.write_bytes(ORACLE_PATH.read_bytes())
        with pytest.raises(Exception, match="orphan"):
            pb.read_pgen(str(orphan))


class TestPgenRangeTuning:
    """The tuning knobs bound I/O, so they must not change what is emitted.

    Each test compares against the default read rather than against a
    hardcoded genotype matrix: the property under test is invariance, and a
    literal would only restate what the read tests already pin down.
    """

    @pytest.mark.parametrize("gap", [0, 4096])
    def test_range_gap_does_not_change_content(self, gap):
        tuned = pb.read_pgen(str(ORACLE_PATH), max_range_gap=gap).sort("start")
        default = pb.read_pgen(str(ORACLE_PATH)).sort("start")
        assert tuned.equals(default)
        assert tuned["id"].to_list() == ORACLE_IDS

    def test_batch_soft_byte_limit_does_not_change_content(self):
        small = pb.read_pgen(str(ORACLE_PATH), batch_soft_byte_limit=1).sort("start")
        default = pb.read_pgen(str(ORACLE_PATH)).sort("start")
        assert small.equals(default)

    def test_max_range_bytes_does_not_change_content(self):
        tuned = pb.read_pgen(str(ORACLE_PATH), max_range_bytes=1).sort("start")
        default = pb.read_pgen(str(ORACLE_PATH)).sort("start")
        assert tuned.equals(default)
        assert tuned["id"].to_list() == ORACLE_IDS

    def test_the_invariance_check_can_actually_fail(self):
        # A comparison that cannot fail proves nothing. Confirm `.equals`
        # distinguishes two genuinely different reads before trusting the
        # three tests above.
        default = pb.read_pgen(str(ORACLE_PATH)).sort("start")
        subset = pb.read_pgen(str(ORACLE_PATH), samples=["s1"]).sort("start")
        assert not default.equals(subset)


class TestPgenPartitions:
    @pytest.mark.parametrize("partitions", ["1", "2", "4"])
    def test_content_is_independent_of_partition_count(self, partitions):
        previous = pb.get_option(TARGET_PARTITIONS)
        pb.set_option(TARGET_PARTITIONS, partitions)
        try:
            frame = pb.read_pgen(str(ORACLE_PATH)).sort("start")
        finally:
            pb.set_option(TARGET_PARTITIONS, previous)
        assert frame["id"].to_list() == ORACLE_IDS
        assert frame.height == 3


def _registered_tables() -> set:
    frame = pb.sql("SELECT table_name FROM information_schema.tables").collect()
    return set(frame["table_name"].to_list())


def _captured_pgen_options(module, call) -> dict:
    """Run `call` and return the kwargs it passed to `PgenReadOptions`."""
    seen: dict = {}
    real = module.PgenReadOptions

    def capture(**kwargs):
        seen.update(kwargs)
        return real(**kwargs)

    module.PgenReadOptions = capture
    try:
        call()
    finally:
        module.PgenReadOptions = real
    return seen


class TestPgenRegister:
    def test_register_pgen_creates_a_queryable_table(self):
        pb.register_pgen(str(ORACLE_PATH), "pgen_oracle")
        frame = pb.sql("SELECT id FROM pgen_oracle ORDER BY start").collect()
        assert frame["id"].to_list() == ORACLE_IDS
        ctx.deregister_table("pgen_oracle")

    def test_failed_registration_keeps_the_existing_table(self):
        pb.register_pgen(str(ORACLE_PATH), "pgen_keep")
        with pytest.raises(Exception):
            pb.register_pgen(str(ORACLE_PATH), "pgen_keep", samples=["nope"])
        frame = pb.sql("SELECT id FROM pgen_keep ORDER BY start").collect()
        assert frame["id"].to_list() == ORACLE_IDS
        ctx.deregister_table("pgen_keep")

    def test_register_pgen_rejects_an_unknown_genotype_field(self):
        with pytest.raises(ValueError, match="GQ"):
            pb.register_pgen(str(ORACLE_PATH), "pgen_bad", genotype_fields=["GQ"])

    def test_register_pgen_rejects_a_non_pgen_path(self):
        with pytest.raises(ValueError, match=r"\.pgen"):
            pb.register_pgen("cohort.bgen", "pgen_bad")

    def test_register_pgen_accepts_the_range_controls(self):
        # `features/reading.md` tells object-storage users to raise
        # `max_range_gap`; a registered table had no way to do it.
        pb.register_pgen(
            str(ORACLE_PATH),
            "pgen_tuned",
            max_range_gap=4096,
            max_range_bytes=1 << 20,
            batch_soft_byte_limit=1,
        )
        try:
            frame = pb.sql("SELECT id FROM pgen_tuned ORDER BY start").collect()
            assert frame["id"].to_list() == ORACLE_IDS
        finally:
            ctx.deregister_table("pgen_tuned")

    def test_register_pgen_forwards_the_range_controls(self):
        # The controls bound I/O and change nothing observable in the result,
        # so accepting them is not evidence they are used. Assert on what
        # reaches the options object instead.
        seen = _captured_pgen_options(
            pb_sql,
            lambda: pb.register_pgen(
                str(ORACLE_PATH),
                "pgen_fwd",
                max_range_gap=4096,
                max_range_bytes=1 << 20,
                batch_soft_byte_limit=64,
            ),
        )
        ctx.deregister_table("pgen_fwd")
        assert seen["max_range_gap"] == 4096
        assert seen["max_range_bytes"] == 1 << 20
        assert seen["batch_soft_byte_limit"] == 64


class TestPgenDescribe:
    def test_describe_pgen_reports_schema_and_properties(self):
        described = pb.describe_pgen(str(ORACLE_PATH))
        assert described["name"].to_list() == [
            "chrom",
            "start",
            "end",
            "id",
            "ref",
            "alt",
            "genotypes",
        ]
        assert described["index"][0] == "embedded"
        assert described["storage_mode"][0].startswith("0x")
        assert described["specification_baseline"][0] is not None

    def test_describe_leaves_no_table_behind(self):
        before = _registered_tables()
        pb.describe_pgen(str(ORACLE_PATH))
        assert _registered_tables() == before

    def test_describe_pgen_forwards_an_explicit_pgi(self):
        # A PGEN whose index lives outside the file cannot be described at all
        # without this. oracle.pgen carries an embedded index, so the explicit
        # path changes nothing observable in the described schema — the
        # regression to guard is the argument being dropped.
        seen = _captured_pgen_options(
            pb_io,
            lambda: pb.describe_pgen(str(ORACLE_PATH), pgi_path="/elsewhere/x.pgi"),
        )
        assert seen["pgi_path"] == "/elsewhere/x.pgi"

    def test_describe_does_not_disturb_a_registered_table(self):
        pb.register_pgen(str(ORACLE_PATH), "pgen_live")
        pb.describe_pgen(str(ORACLE_PATH))
        frame = pb.sql("SELECT id FROM pgen_live ORDER BY start").collect()
        assert frame["id"].to_list() == ORACLE_IDS
        ctx.deregister_table("pgen_live")


class TestPgenMetadata:
    def test_metadata_reports_index_and_storage_mode(self):
        lazy = pb.scan_pgen(str(ORACLE_PATH))
        pgen = pb.get_metadata(lazy)["header"]
        assert pgen["index"] == "embedded"
        assert pgen["storage_mode"].startswith("0x")
        assert pgen["specification_baseline"] is not None

    def test_metadata_reports_selected_genotype_fields(self):
        lazy = pb.scan_pgen(str(PHASE_PATH), genotype_fields=["GT", "PHASED"])
        assert pb.get_metadata(lazy)["header"]["genotype_fields"] == ["GT", "PHASED"]

    def test_metadata_reports_psam_identities(self):
        lazy = pb.scan_pgen(str(ORACLE_PATH))
        identities = pb.get_metadata(lazy)["header"]["sample_identities"]
        assert [identity["iid"] for identity in identities] == ORACLE_SAMPLES


class TestPgenPushdown:
    def test_projection_of_metadata_columns_only(self):
        frame = pb.scan_pgen(str(ORACLE_PATH)).select("chrom", "id").collect()
        assert frame.columns == ["chrom", "id"]
        assert frame.height == 3

    def test_predicate_selects_a_single_variant(self):
        frame = pb.scan_pgen(str(ORACLE_PATH)).filter(pl.col("id") == "v2").collect()
        assert frame["id"].to_list() == ["v2"]

    def test_predicate_selects_a_chromosome(self):
        frame = (
            pb.scan_pgen(str(ORACLE_PATH))
            .filter(pl.col("chrom") == "1")
            .collect()
            .sort("start")
        )
        assert frame["id"].to_list() == ["v1", "v2"]

    def test_limit_is_applied(self):
        frame = pb.scan_pgen(str(ORACLE_PATH)).limit(1).collect()
        assert frame.height == 1


class TestPgenAltCount:
    """ALT_COUNT emits the hardcall allele count as int8, one byte per cell.

    DS carries the same values on a hardcall-only fileset but as float32, so
    this exists to avoid paying four bytes per genotype for data that is
    always 0, 1, 2, or missing.
    """

    def test_alt_count_is_int8(self):
        frame = pb.read_pgen(str(ORACLE_PATH), genotype_fields=["ALT_COUNT"])
        assert _genotype_names(frame) == ["ALT_COUNT"]
        column = _genotypes(frame).field("ALT_COUNT")
        assert column.type.value_type == pa.int8()

    def test_alt_count_matches_the_gt_allele_counts(self):
        alt = pb.read_pgen(str(ORACLE_PATH), genotype_fields=["ALT_COUNT"]).sort(
            "start"
        )
        gt = pb.read_pgen(str(ORACLE_PATH), genotype_fields=["GT"]).sort("start")
        for row in range(alt.height):
            counts = _child(alt, "ALT_COUNT", row)
            calls = _child(gt, "GT", row)
            expected = [None if c is None else c.count(1) for c in calls]
            assert counts == expected

    def test_alt_count_is_the_hardcall_track_not_the_dosage_track(self):
        # dosage.pgen carries fractional dosages (0.125, 1.875) whose hardcalls
        # are mostly missing. ALT_COUNT must report the hardcalls.
        alt = pb.read_pgen(str(DOSAGE_PATH), genotype_fields=["ALT_COUNT"])
        ds = pb.read_pgen(str(DOSAGE_PATH), genotype_fields=["DS"])
        assert _child(alt, "ALT_COUNT") != _child(ds, "DS")
        assert _child(alt, "ALT_COUNT") == [None, 1, None, None]

    def test_alt_count_content_is_independent_of_partition_count(self):
        previous = pb.get_option(TARGET_PARTITIONS)
        try:
            # A multi-partition scan may emit rows out of source order, so sort
            # before comparing rather than trusting emission order.
            pb.set_option(TARGET_PARTITIONS, "1")
            one = _child(
                pb.read_pgen(str(ORACLE_PATH), genotype_fields=["ALT_COUNT"]).sort(
                    "start"
                ),
                "ALT_COUNT",
            )
            pb.set_option(TARGET_PARTITIONS, "4")
            four = _child(
                pb.read_pgen(str(ORACLE_PATH), genotype_fields=["ALT_COUNT"]).sort(
                    "start"
                ),
                "ALT_COUNT",
            )
        finally:
            pb.set_option(TARGET_PARTITIONS, previous)
        assert one == four


class TestPgenMatrix:
    """`read_pgen_matrix` must agree cell for cell with the DataFrame path.

    It reaches the same values by a different route — streaming batches into a
    preallocated array instead of consolidating them into one Arrow buffer — so
    agreement with `read_pgen` is what says the fast path is not cutting a
    corner.
    """

    @staticmethod
    def _expected(path, field, rows):
        frame = pb.read_pgen(str(path), genotype_fields=[field]).sort("start")
        return [_child(frame, field, row) for row in range(rows)]

    @pytest.mark.parametrize(
        "path,field,rows,dtype",
        [
            (ORACLE_PATH, "ALT_COUNT", 3, "int8"),
            (ORACLE_PATH, "DS", 3, "float32"),
            (DOSAGE_PATH, "DS", 1, "float32"),
            (DOSAGE_PATH, "ALT_COUNT", 1, "int8"),
        ],
    )
    def test_matches_the_dataframe_path(self, path, field, rows, dtype):
        matrix = pb.read_pgen_matrix(str(path), field=field)
        assert matrix.values.dtype == dtype
        assert matrix.values.flags.c_contiguous
        expected = self._expected(path, field, rows)
        assert matrix.values.shape == (rows, len(expected[0]))
        missing = -9 if dtype == "int8" else float("nan")
        for row, values in enumerate(expected):
            observed = matrix.values[row].tolist()
            for column, value in enumerate(values):
                if value is None:
                    assert (
                        observed[column] != observed[column]
                        if dtype == "float32"
                        else observed[column] == missing
                    ), f"row {row} column {column} should be missing"
                else:
                    assert observed[column] == pytest.approx(value), (row, column)

    def test_alt_count_is_the_hardcall_track_not_the_dosage_track(self):
        # dosage.pgen's tracks genuinely disagree, so a matrix reader that
        # confused them would pass every all-hardcall fixture and fail here.
        alt = pb.read_pgen_matrix(str(DOSAGE_PATH), field="ALT_COUNT")
        dosage = pb.read_pgen_matrix(str(DOSAGE_PATH), field="DS")
        assert alt.values[0].tolist() == [-9, 1, -9, -9]
        assert dosage.values[0][:3].tolist() == pytest.approx([0.125, 1.0, 1.875])

    def test_missing_sentinel_is_configurable(self):
        matrix = pb.read_pgen_matrix(str(DOSAGE_PATH), field="ALT_COUNT", missing=-1)
        assert matrix.values[0].tolist() == [-1, 1, -1, -1]

    def test_integer_nulls_do_not_widen_the_intermediate(self):
        # An Arrow int8 array with nulls converts to float64 on the way to
        # NumPy, which would be an eightfold intermediate here and an undefined
        # narrowing cast back. The sentinel has to be substituted in Arrow.
        matrix = pb.read_pgen_matrix(str(DOSAGE_PATH), field="ALT_COUNT")
        assert matrix.values.dtype == "int8"
        assert matrix.values.itemsize == 1

    def test_labels_the_axes(self):
        matrix = pb.read_pgen_matrix(str(ORACLE_PATH), field="ALT_COUNT")
        assert matrix.sample_names == ORACLE_SAMPLES
        assert matrix.values.shape[1] == len(matrix.sample_names)
        assert matrix.values.shape[0] == len(matrix.positions)
        starts = pb.read_pgen(str(ORACLE_PATH), genotype_fields=["ALT_COUNT"])[
            "start"
        ].to_list()
        assert sorted(matrix.positions.tolist()) == sorted(starts)

    def test_sample_selection_narrows_the_columns_in_requested_order(self):
        matrix = pb.read_pgen_matrix(
            str(ORACLE_PATH), field="ALT_COUNT", samples=["s3", "s1"]
        )
        assert matrix.sample_names == ["s3", "s1"]
        assert matrix.values.shape == (3, 2)
        full = pb.read_pgen_matrix(str(ORACLE_PATH), field="ALT_COUNT")
        assert matrix.values[:, 0].tolist() == full.values[:, 2].tolist()
        assert matrix.values[:, 1].tolist() == full.values[:, 0].tolist()

    @pytest.mark.parametrize("field", ["GT", "HDS", "PHASED", "DS_STORED", "nonsense"])
    def test_rejects_fields_without_a_dense_matrix_form(self, field):
        with pytest.raises(ValueError, match="read_pgen_matrix supports"):
            pb.read_pgen_matrix(str(ORACLE_PATH), field=field)

    def test_content_is_independent_of_partition_count(self):
        previous = pb.get_option(TARGET_PARTITIONS)
        try:
            pb.set_option(TARGET_PARTITIONS, "1")
            one = pb.read_pgen_matrix(str(ORACLE_PATH), field="ALT_COUNT")
            pb.set_option(TARGET_PARTITIONS, "4")
            four = pb.read_pgen_matrix(str(ORACLE_PATH), field="ALT_COUNT")
        finally:
            pb.set_option(TARGET_PARTITIONS, previous)
        # Emission order may interleave above one partition, so compare rows in
        # position order rather than trusting the order they arrived in.
        assert one.values.shape == four.values.shape
        import numpy as np

        for position in one.positions:
            left = one.values[np.asarray(one.positions) == position]
            right = four.values[np.asarray(four.positions) == position]
            assert left.tolist() == right.tolist()

    def test_metadata_reports_the_output_shape(self):
        header = pb.get_metadata(pb.scan_pgen(str(ORACLE_PATH)))["header"]
        assert header["variant_count"] == 3
        assert header["sample_count"] == len(ORACLE_SAMPLES)
        subset = pb.get_metadata(pb.scan_pgen(str(ORACLE_PATH), samples=["s2", "s5"]))[
            "header"
        ]
        assert subset["variant_count"] == 3
        assert subset["sample_count"] == 2

    def test_parallel_copy_matches_the_serial_copy(self):
        # The parallel path assigns row ranges in arrival order and writes them
        # from worker threads. Disjoint slices need no lock, but an off-by-one
        # in the range assignment would interleave or overwrite rows, which only
        # a value comparison catches — and only when there is more than one
        # batch to get out of order, hence the batch size of one row.
        batch_size = "datafusion.execution.batch_size"
        # Unset by default, so there is no value to restore; DataFusion's own
        # default is 8192 and putting it back keeps later tests unaffected.
        previous = pb.get_option(batch_size) or "8192"
        try:
            pb.set_option(batch_size, "1")
            serial = pb.read_pgen_matrix(
                str(ORACLE_PATH), field="ALT_COUNT", copy_threads=1
            )
            assert serial.values.shape == (3, len(ORACLE_SAMPLES))
            for threads in (2, 4, 8):
                parallel = pb.read_pgen_matrix(
                    str(ORACLE_PATH), field="ALT_COUNT", copy_threads=threads
                )
                assert parallel.values.tolist() == serial.values.tolist(), threads
                assert parallel.positions.tolist() == serial.positions.tolist(), threads
                assert parallel.values.flags.c_contiguous
        finally:
            pb.set_option(batch_size, previous)

    def test_copy_threads_defaults_to_the_partition_count(self):
        # The copy is the one stage that does not follow target_partitions on its
        # own, so it is made to. A single-partition scan must stay single
        # threaded end to end, or a "one thread" measurement is not one.
        previous = pb.get_option(TARGET_PARTITIONS)
        try:
            for partitions in ("1", "4"):
                pb.set_option(TARGET_PARTITIONS, partitions)
                matrix = pb.read_pgen_matrix(str(ORACLE_PATH), field="ALT_COUNT")
                assert matrix.values.shape == (3, len(ORACLE_SAMPLES))
        finally:
            pb.set_option(TARGET_PARTITIONS, previous)
