"""PGEN read, scan, register, and describe tests."""

import polars as pl
import pytest
from _expected import DATA_DIR

import polars_bio as pb
from polars_bio.context import ctx

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
