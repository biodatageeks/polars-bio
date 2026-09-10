"""Narrow issue #455 oracle probe; not a general structure normalizer."""

import argparse
import hashlib
import importlib.metadata
import json
import math
import platform
import subprocess
import urllib.request
from pathlib import Path

import Bio.PDB
import foldcomp
import gemmi
import numpy as np
from Bio.PDB.vectors import Vector, calc_angle, calc_dihedral

p = argparse.ArgumentParser()
p.add_argument("--work-dir", type=Path, required=True)
p.add_argument("--foldcomp-repo", type=Path, required=True)
a = p.parse_args()
a.work_dir.mkdir(parents=True, exist_ok=True)
report = {
    "observed_date": "2026-09-09",
    "python": platform.python_version(),
    "versions": {
        x: importlib.metadata.version(x)
        for x in ["gemmi", "biopython", "foldcomp", "numpy"]
    },
    "inputs": {},
}
report["foldcomp_fixture_commit"] = subprocess.check_output(
    ["git", "-C", str(a.foldcomp_repo), "rev-parse", "HEAD"], text=True
).strip()
report["foldcomp_fixture_hashes"] = {
    "example_db"
    + suffix: hashlib.sha256(
        (a.foldcomp_repo / ("test/example_db" + suffix)).read_bytes()
    ).hexdigest()
    for suffix in ["", ".index", ".lookup", ".dbtype", ".source"]
}

for ext in ["pdb", "cif"]:
    dst = a.work_dir / f"1ubq.{ext}"
    url = f"https://files.rcsb.org/download/1UBQ.{ext}"
    if not dst.exists():
        with urllib.request.urlopen(url, timeout=60) as response:
            dst.write_bytes(response.read())
    report["inputs"][dst.name] = {
        "url": url,
        "bytes": dst.stat().st_size,
        "sha256": hashlib.sha256(dst.read_bytes()).hexdigest(),
    }


def atom_rows(st):
    return [
        (
            mi,
            ch.name,
            str(r.seqid),
            r.name,
            at.name,
            at.altloc,
            at.pos.x,
            at.pos.y,
            at.pos.z,
        )
        for mi, m in enumerate(st)
        for ch in m
        for r in ch
        for at in r
    ]


def backbone_rows(st):
    ch = st[0]["A"]
    return [r for r in ch if r.het_flag == "A"]


def xyz(r, name):
    atoms = [at for at in r if at.name == name]
    return atoms[0].pos


def calc_pair(positions):
    g = (
        gemmi.calculate_dihedral(*positions)
        if len(positions) == 4
        else gemmi.calculate_angle(*positions)
    )
    vs = [Vector(pos.x, pos.y, pos.z) for pos in positions]
    b = calc_dihedral(*vs) if len(vs) == 4 else calc_angle(*vs)
    return math.degrees(g), math.degrees(b)


def circ(x, y):
    return abs((x - y + 180) % 360 - 180)


st = gemmi.read_structure(str(a.work_dir / "1ubq.pdb"))
cs = gemmi.read_structure(str(a.work_dir / "1ubq.cif"))
assert atom_rows(st) == atom_rows(cs)
rs = backbone_rows(st)
errs = []
golden = []
for i, r in enumerate(rs):
    n, ca, c = (xyz(r, x) for x in ["N", "CA", "C"])
    metrics = {"n_ca_c_deg": [n, ca, c]}
    if i:
        metrics["phi_deg"] = [xyz(rs[i - 1], "C"), n, ca, c]
        metrics["c_n_ca_deg"] = [xyz(rs[i - 1], "C"), n, ca]
    if i + 1 < len(rs):
        nn, nca = xyz(rs[i + 1], "N"), xyz(rs[i + 1], "CA")
        metrics.update(
            psi_deg=[n, ca, c, nn], omega_deg=[ca, c, nn, nca], ca_c_n_deg=[ca, c, nn]
        )
    row = {"auth_seq_id": str(r.seqid), "residue_name": r.name}
    for name in [
        "phi_deg",
        "psi_deg",
        "omega_deg",
        "n_ca_c_deg",
        "ca_c_n_deg",
        "c_n_ca_deg",
    ]:
        if name not in metrics:
            row[name] = None
            continue
        g, b = calc_pair(metrics[name])
        errs.append(circ(g, b) if len(metrics[name]) == 4 else abs(g - b))
        row[name] = g
    golden.append(row)
report["pdb_cif"] = {
    "atom_rows_exact_equal": True,
    "atoms": len(atom_rows(st)),
    "protein_residues": len(rs),
    "geometry_comparisons": len(errs),
    "max_gemmi_biopython_float64_difference_deg": max(errs),
}
assert max(errs) < 1e-6
bio_rs = [
    r
    for r in Bio.PDB.PDBParser(QUIET=True).get_structure(
        "1ubq", a.work_dir / "1ubq.pdb"
    )[0]["A"]
    if r.id[0] == " "
]
float32_errs = []
for i in range(1, len(rs)):
    b = math.degrees(
        calc_dihedral(
            bio_rs[i - 1]["C"].get_vector(),
            bio_rs[i]["N"].get_vector(),
            bio_rs[i]["CA"].get_vector(),
            bio_rs[i]["C"].get_vector(),
        )
    )
    float32_errs.append(circ(b, golden[i]["phi_deg"]))
report["pdb_cif"]["max_biopython_parser_phi_difference_deg"] = max(float32_errs)

fcz = foldcomp.compress("1ubq-probe", (a.work_dir / "1ubq.pdb").read_text())
(a.work_dir / "1ubq.fcz").write_bytes(fcz)
name, pdb_dec = foldcomp.decompress(fcz)
(a.work_dir / "1ubq.decoded.pdb").write_text(pdb_dec)
d = foldcomp.get_data(fcz)
pd = foldcomp.get_data((a.work_dir / "1ubq.pdb").read_text())
report["foldcomp"] = {
    "compressed_bytes": len(fcz),
    "decoded_name": name,
    "get_data_lengths_fcz": {k: len(v) for k, v in d.items()},
    "get_data_lengths_pdb": {k: len(v) for k, v in pd.items()},
    "first_phi_fcz": d["phi"][:3],
    "first_psi_fcz": d["psi"][:3],
    "first_phi_pdb": pd["phi"][:3],
    "first_psi_pdb": pd["psi"][:3],
    "gemmi_phi_first_four_residues": [r["phi_deg"] for r in golden[:4]],
    "gemmi_psi_first_four_residues": [r["psi_deg"] for r in golden[:4]],
}
ds = gemmi.read_pdb_string(pdb_dec)
orig_atoms = {
    (ch.name, str(r.seqid), r.name, at.name): (at.pos.x, at.pos.y, at.pos.z)
    for m in st
    for ch in m
    for r in ch
    if r.het_flag == "A"
    for at in r
}
dec_atoms = {
    (ch.name, str(r.seqid), r.name, at.name): (at.pos.x, at.pos.y, at.pos.z)
    for m in ds
    for ch in m
    for r in ch
    for at in r
}
common = sorted(orig_atoms.keys() & dec_atoms.keys())
e = np.array([dec_atoms[k] for k in common]) - np.array([orig_atoms[k] for k in common])
report["foldcomp"].update(
    original_protein_atoms=len(orig_atoms),
    decoded_atoms=len(dec_atoms),
    matching_atom_keys=len(common),
    unaligned_rmsd_angstrom=float(np.sqrt(np.mean(np.sum(e * e, axis=1)))),
    max_atom_displacement_angstrom=float(np.linalg.norm(e, axis=1).max()),
)
coords = np.asarray(d["coordinates"])
dec_xyz = np.asarray(
    [(at.pos.x, at.pos.y, at.pos.z) for m in ds for ch in m for r in ch for at in r]
)
report["foldcomp"]["get_data_vs_decoded_pdb_max_component_difference_angstrom"] = (
    float(abs(coords - dec_xyz).max())
    if coords.shape == dec_xyz.shape
    else "shape mismatch"
)
base = str(a.foldcomp_repo / "test/example_db")
with foldcomp.open(
    base, ids=["d1asha_", "d1it2a_"], decompress=False, err_on_missing=True
) as db:
    entries = list(db)
report["foldcomp"]["subset_raw"] = [
    {"bytes": len(b), "magic": b[:4].decode()} for b in entries
]
with foldcomp.open(base, ids=["d1asha_", "d1it2a_"], err_on_missing=True) as db:
    report["foldcomp"]["subset_names"] = [n for n, _ in db]
assert report["foldcomp"]["subset_names"] == ["d1asha_", "d1it2a_"]
with foldcomp.open(base, ids=[]) as db:
    report["foldcomp"]["empty_ids_length"] = len(db)
assert report["foldcomp"]["empty_ids_length"] == 24
try:
    with foldcomp.open(base, ids=["nonexistent-455"], err_on_missing=True) as db:
        list(db)
except Exception as exc:
    report["foldcomp"]["missing_id_error"] = type(exc).__name__
assert report["foldcomp"].get("missing_id_error") == "KeyError"

import io

from Bio.PDB.MMCIF2Dict import MMCIF2Dict

block = gemmi.cif.read_file(str(a.work_dir / "1ubq.cif")).sole_block()
col = block.find_values("_atom_site.auth_seq_id")
for i, val in enumerate(col):
    if val == "1":
        col[i] = "X1"
mutant = block.as_string()
raw_bio = MMCIF2Dict(io.StringIO(mutant))
assert raw_bio["_atom_site.auth_seq_id"][0] == "X1"
assert block.find_values("_atom_site.auth_seq_id")[0] == "X1"
report["nonnumeric_author_id"] = {"raw_gemmi_and_mmcif2dict": "X1"}
try:
    gemmi.make_structure_from_block(block)
    report["nonnumeric_author_id"]["gemmi_structure"] = "accepted"
except Exception as exc:
    report["nonnumeric_author_id"]["gemmi_structure"] = (
        type(exc).__name__ + ": " + str(exc)
    )
try:
    Bio.PDB.MMCIFParser(QUIET=True).get_structure("mutant", io.StringIO(mutant))
    report["nonnumeric_author_id"]["biopython_structure"] = "accepted"
except Exception as exc:
    report["nonnumeric_author_id"]["biopython_structure"] = (
        type(exc).__name__ + ": " + str(exc)
    )

(a.work_dir / "1ubq.geometry.gemmi.json").write_text(
    json.dumps(golden, indent=2, allow_nan=False) + "\n"
)
(a.work_dir / "probe-results.json").write_text(
    json.dumps(report, indent=2, allow_nan=False) + "\n"
)
print(json.dumps(report, indent=2, allow_nan=False))
