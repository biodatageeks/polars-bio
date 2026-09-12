import json
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import polars_bio as pb

DATA = Path(__file__).parent / "data/structure"


def test_standalone_codec_and_residue_parity():
    atoms = pb.read_foldcomp(DATA / "1ubq.fcz").sort("atom_index")
    gold = json.loads((DATA / "1ubq.fcz.json").read_text())
    assert len(atoms) == len(gold["atoms"]) == 602
    for row, want in zip(atoms.iter_rows(named=True), gold["atoms"]):
        assert row["atom_name"] == want["atom_name"]
        assert [row[c] for c in ["x", "y", "z"]] == pytest.approx(
            want["position"], abs=1e-4, rel=0
        )
    assert atoms["occupancy"].null_count() == 602
    residues = pb.read_foldcomp(DATA / "1ubq.fcz", level="residue").sort(
        "residue_index"
    )
    names = [
        "phi_deg",
        "psi_deg",
        "omega_deg",
        "angle_n_ca_c_deg",
        "angle_ca_c_n_deg",
        "angle_c_n_ca_deg",
    ]
    assert len(residues) == 76
    for row, expected in zip(residues.iter_rows(named=True), gold["residues"]):
        for name, value in zip(names, expected["angles"]):
            if value is None:
                assert row[name] is None
            else:
                assert abs((row[name] - value + 180) % 360 - 180) < 0.01


def test_selectors_empty_missing_duplicates_sql_and_repeated_collect():
    path = DATA / "example_db"
    lazy = pb.scan_foldcomp(
        path, ids=["d1asha_", "d1it2a_", "d1asha_"], level="residue"
    )
    first = lazy.collect().sort("entry_index", "residue_index")
    assert first["entry_key"].unique().sort().to_list() == [0, 7]
    assert_frame_equal(first, lazy.collect().sort("entry_index", "residue_index"))
    numeric = pb.read_foldcomp(path, entry_keys=[7, 0], level="residue").sort(
        "entry_index", "residue_index"
    )
    assert_frame_equal(first, numeric)
    for opts in [{"ids": []}, {"entry_keys": []}]:
        empty = pb.scan_foldcomp(path, level="residue", **opts)
        assert empty.collect_schema() == lazy.collect_schema()
        assert empty.collect().is_empty()
        assert empty.select(pl.len()).collect().item() == 0
    with pytest.raises(ValueError, match="missing"):
        pb.scan_foldcomp(path, ids=["missing"])
    with pytest.raises(ValueError, match="mutually exclusive"):
        pb.scan_foldcomp(path, ids=[], entry_keys=[])
    pb.register_foldcomp("foldcomp_455", path, entry_keys=[0], level="residue")
    sql = pb.sql(
        "SELECT auth_seq_id, phi_deg FROM foldcomp_455 WHERE residue_index > 0 LIMIT 3"
    ).collect()
    expected = (
        first.filter((pl.col("entry_key") == 0) & (pl.col("residue_index") > 0))
        .select(sql.columns)
        .head(3)
    )
    assert_frame_equal(sql, expected)


def test_selected_payload_corruption_and_lookup_identity(tmp_path):
    import shutil

    path = tmp_path / "db"
    for suffix in ["", ".index", ".lookup", ".dbtype"]:
        shutil.copy2(DATA / ("example_db" + suffix), str(path) + suffix)
    with path.open("r+b") as payload:
        payload.seek(2307)  # Corrupt key 1; key 0 remains decodable.
        payload.write(b"BAD!")
    lookup = Path(str(path) + ".lookup")
    lookup.write_text(lookup.read_text().replace("d1asha_", "alias_455"))
    selected = pb.read_foldcomp(path, ids=["alias_455"], level="residue")
    assert selected["entry_key"].unique().to_list() == [0]
    assert selected["entry_name"].unique().to_list() == ["alias_455"]
    assert selected["entry_id"].unique().to_list() != ["alias_455"]
    assert pb.read_foldcomp(path, ids=[]).is_empty()
    with pytest.raises(Exception, match="key Some\\(1\\)"):
        pb.read_foldcomp(path)
    lookup.unlink()
    assert pb.read_foldcomp(path, entry_keys=[0]).height > 0
    with pytest.raises(ValueError, match="lookup"):
        pb.scan_foldcomp(path, ids=["alias_455"])
