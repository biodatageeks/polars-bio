"""End-to-end structural query parity against the native fixture export."""

import gzip
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import polars_bio as pb

DATA = Path(__file__).parent / "data/structure"
ORDER = ["source_index", "entry_index", "atom_index"]


@pytest.fixture(params=["pdb", "mmcif"])
def scan(request):
    return getattr(pb, f"scan_{request.param}"), DATA / (
        "1ubq.pdb" if request.param == "pdb" else "1ubq.cif"
    )


def test_fixture_manifest():
    manifest = json.loads((DATA / "manifest.json").read_text())
    for name, expected in (manifest["inputs"] | manifest["outputs"]).items():
        assert hashlib.sha256((DATA / name).read_bytes()).hexdigest() == expected


def test_atom_schema_values_and_repeated_collect(scan):
    reader, path = scan
    lazy = reader(path)
    assert isinstance(lazy, pl.LazyFrame)
    assert lazy.collect_schema()["x"] == pl.Float64
    assert lazy.collect_schema()["auth_seq_id"] == pl.String
    first = lazy.collect().sort(ORDER)
    assert len(first) == 660
    assert_frame_equal(first, lazy.collect().sort(ORDER))
    expected = json.loads((DATA / "1ubq.atoms.json").read_text())
    for row, wanted in zip(first.iter_rows(named=True), expected):
        for key in [
            "model_id",
            "chain_id",
            "auth_seq_id",
            "insertion_code",
            "residue_name",
            "atom_name",
            "alt_id",
        ]:
            assert row[key] == wanted[key]
        assert [row[c] for c in ["x", "y", "z"]] == pytest.approx(
            wanted["position"], abs=1e-9
        )


def test_residue_goldens_and_filtered_neighbor_context(scan):
    reader, path = scan
    lazy = reader(path, level="residue")
    result = lazy.collect().sort("residue_index")
    assert len(result) == 76
    names = [
        "phi_deg",
        "psi_deg",
        "omega_deg",
        "angle_n_ca_c_deg",
        "angle_ca_c_n_deg",
        "angle_c_n_ca_deg",
    ]
    gold = json.loads((DATA / "1ubq.residues.json").read_text())
    for row, expected in zip(result.iter_rows(named=True), gold):
        assert row["auth_seq_id"] == expected["auth_seq_id"]
        for name, value in zip(names, expected["angles"]):
            if value is None:
                assert row[name] is None
            else:
                assert abs((row[name] - value + 180) % 360 - 180) < 1e-6
    query = lambda frame: (
        frame.filter(pl.col("auth_seq_id").is_in(["2", "30", "75"]))
        .select("auth_seq_id", "phi_deg", "psi_deg")
        .head(2)
    )
    assert_frame_equal(query(lazy).collect(), query(result))
    assert lazy.select(pl.len()).collect().item() == 76
    assert lazy.select(pl.lit(42)).collect().to_series().to_list() == [42]
    assert lazy.head(0).collect().is_empty()


def test_mixed_collection_gzip_sql_and_threads(tmp_path):
    gz = tmp_path / "one.pdb.gz"
    gz.write_bytes(gzip.compress((DATA / "1ubq.pdb").read_bytes()))
    sources = [gz, DATA / "1ubq.cif", gz]
    lazy = pb.scan_structures(sources, level="residue")
    expected = lazy.collect().sort("source_index", "residue_index")
    assert len(expected) == 228
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda _: lazy.collect().sort("source_index", "residue_index"), range(2)
            )
        )
    for result in results:
        assert_frame_equal(result, expected)
    pb.register_structure("structure_455", sources, level="residue")
    sql = pb.sql(
        "SELECT source_index, auth_seq_id, phi_deg FROM structure_455 WHERE auth_seq_id = '2' ORDER BY source_index"
    ).collect()
    assert_frame_equal(
        sql, expected.filter(pl.col("auth_seq_id") == "2").select(sql.columns)
    )
    assert pb.scan_pdb(str(tmp_path / "*.gz")).select(pl.len()).collect().item() == 660


def test_metadata_and_coordinate_configuration(scan):
    reader, path = scan
    old = pb.get_option("bio.coordinate_system_zero_based")
    try:
        pb.set_option("bio.coordinate_system_zero_based", "true")
        lazy = reader(path)
        meta = pb.get_metadata(lazy)
        assert meta["coordinate_system_zero_based"] is None
        assert meta["header"]["bio.structure.coordinate_unit"] == "angstrom"
        assert_frame_equal(lazy.collect(), reader(path).collect())
    finally:
        if old is not None:
            pb.set_option("bio.coordinate_system_zero_based", old)


def test_validation_and_lazy_errors(tmp_path):
    with pytest.raises(ValueError, match="empty"):
        pb.scan_structures([])
    with pytest.raises(ValueError, match="level"):
        pb.scan_pdb(DATA / "1ubq.pdb", level="backbone")
    with pytest.raises(ValueError, match="residue level"):
        pb.scan_pdb(DATA / "1ubq.pdb", level="residue", altloc="all")
    lazy = pb.scan_pdb(tmp_path / "missing.pdb")
    assert len(lazy.collect_schema()) > 0
    with pytest.raises(Exception, match="missing.pdb"):
        lazy.collect()
    with pytest.raises(Exception, match="max_decoded_bytes"):
        pb.scan_pdb(DATA / "1ubq.pdb", max_decoded_bytes=10).collect()


def test_http_source_matches_local():
    from functools import partial
    from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), partial(SimpleHTTPRequestHandler, directory=str(DATA))
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        address = f"http://127.0.0.1:{server.server_port}/1ubq.cif"
        remote = (
            pb.scan_mmcif(address, level="residue")
            .select("auth_seq_id", "phi_deg")
            .collect()
        )
        local = (
            pb.scan_mmcif(DATA / "1ubq.cif", level="residue")
            .select(remote.columns)
            .collect()
        )
        assert_frame_equal(remote, local)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_binding_models_conformers_and_changed_sources(tmp_path):
    text = (DATA / "1ubq.pdb").read_text()
    atom = next(line for line in text.splitlines() if line.startswith("ATOM"))
    path = tmp_path / "models.pdb"
    path.write_text(f"MODEL        7\n{atom}\nENDMDL\nMODEL        9\n{atom}\nENDMDL\n")
    assert pb.read_pdb(path)["model_id"].to_list() == [7, 9]
    first = pb.read_pdb(path, model="first")
    assert first["model_id"].to_list() == [7]
    second = pb.read_pdb(path, model=9)
    assert second["model_index"].to_list() == [1]
    lazy = pb.scan_pdb(path)
    path.write_text(atom + "\n")
    assert lazy.collect().height == 1
    with pytest.raises(TypeError, match="model"):
        pb.scan_pdb(path, model=True)
    cif = tmp_path / "alternates.cif"
    cif.write_text(
        """data_alt
loop_
_atom_site.auth_atom_id
_atom_site.auth_comp_id
_atom_site.auth_asym_id
_atom_site.auth_seq_id
_atom_site.label_alt_id
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
N ALA A X1 . 0 0 0
CA ALA A X1 A 1 0 0
C ALA A X1 B 1 1 0
"""
    )
    assert pb.read_mmcif(cif).height == 3
    residue = pb.read_mmcif(cif, level="residue").row(0, named=True)
    assert residue["selected_alt_id"] == "A"
    assert residue["c_x"] is None
    assert residue["backbone_complete"] is False
    chosen = pb.read_mmcif(cif, level="residue", altloc="B").row(0, named=True)
    assert chosen["ca_x"] is None
    assert chosen["c_x"] == 1


def test_same_basename_sources_have_distinct_keys(tmp_path):
    paths = []
    for directory in ["first", "second"]:
        folder = tmp_path / directory
        folder.mkdir()
        path = folder / "protein.pdb"
        path.write_bytes((DATA / "1ubq.pdb").read_bytes())
        paths.append(path)
    result = pb.read_structures(paths)
    assert (
        result.select("source_index", "entry_index", "atom_index").unique().height
        == 1320
    )
