# Protein structures

PDB, mmCIF and Foldcomp readers return Polars LazyFrames with native DataFusion
execution. Atom output is the default; choose `level="residue"` for amino-acid
identities, backbone coordinates and angles.

```python
import polars as pl
import polars_bio as pb

atoms = pb.scan_structures(["experimental.pdb", "predictions/*.cif.gz"])
residues = pb.scan_mmcif("protein.cif", level="residue")
angles = residues.select("chain_id", "auth_seq_id", "phi_deg", "psi_deg", "omega_deg")
print(angles.collect())

subset = pb.scan_foldcomp("afdb", ids=["protein_1", "protein_2"], level="residue")
pb.register_foldcomp("proteins", "afdb", entry_keys=[10, 20], level="residue")
print(pb.sql("SELECT entry_name, one_letter_code, phi_deg FROM proteins").collect())
```

`scan_pdb`, `scan_mmcif`, `scan_structures`, and `scan_foldcomp` each have an eager
`read_*` counterpart. Text readers accept paths, PathLike objects, ordered lists,
and sorted local globs. Explicit HTTP/S3/GCS/Azure URLs accept
`object_storage_options=pb.ObjectStorageOptions(...)`; remote globs are unsupported.
Gzip text is detected from its header. Foldcomp accepts one local FCZ/database.

| Option | Default | Meaning |
|---|---|---|
| level | atom | atom or residue rows |
| model | all | all, first, or source model number |
| altloc | all for atoms; best_backbone for residues | preserve sites or select one coherent conformer; an explicit alternate ID is also accepted |
| include_non_peptide | False | retain ligand/water residue rows with null geometry |
| max_peptide_bond | 1.8 | maximum C–N link distance in Angstroms |
| max_input_bytes | 268435456 | encoded file/entry limit |
| max_decoded_bytes | 536870912 | decompressed text limit |
| max_atoms | 5000000 | per-entry atom limit |

Coordinates are Float64 **Angstroms**, angles Float64 **degrees**. Genomic
zero/one-based settings do not change these values or attach genomic metadata.
`pb.get_metadata(frame)` includes the structure schema version and physical units.

Author residue IDs are strings, including nonnumeric mmCIF IDs. Separate
`auth_*` and `label_*` columns preserve both naming systems; PDB label IDs stay
null. Blank chains stay empty strings. Source and structural ordinal columns
provide deterministic keys even when filenames or identifiers repeat. Sort by
these columns when row order matters.

Residue geometry uses the selected N/CA/C/O sites. Selection chooses a coherent
component/alternate by backbone completeness, occupancy, preference for A, then
lexical tie-breaking. It never combines incompatible alternate backbone atoms.
Missing atoms and termini produce nulls, not zero or NaN. Links cannot cross
models, chains, TER segments, standardized sequence gaps or excessive C–N distance.
An author numbering gap alone does not imply a break.

| Angle on residue i | Points |
|---|---|
| phi_deg | C(i-1), N(i), CA(i), C(i) |
| psi_deg | N(i), CA(i), C(i), N(i+1) |
| omega_deg | CA(i), C(i), N(i+1), CA(i+1), the outgoing bond |
| angle_n_ca_c_deg | N(i), CA(i), C(i) |
| angle_ca_c_n_deg | CA(i), C(i), N(i+1) |
| angle_c_n_ca_deg | C(i-1), N(i), CA(i) |

Foldcomp selectors `ids` and `entry_keys` are mutually exclusive. None selects all,
an empty sequence selects zero, duplicates select once, and missing/ambiguous
selections raise errors. Supported databases have an uncompressed type-12 payload,
a text index with unique increasing keys, and a lookup for name selection. Keep
these files unchanged while a LazyFrame is in use. Only selected payloads decode;
metadata selection still scans the index/lookup. Internal title, lookup name,
numeric key and database ordinal remain separate columns.

Foldcomp coordinates are lossy reconstructions. All angles are recomputed from
those coordinates; codec torsion arrays are not assumed to align with output rows.
B factors are not automatically interpreted as pLDDT.

Scans retain one complete structure file/entry per active worker before emitting
bounded Arrow batches. Projection reduces Arrow allocation; it does not turn text
or FCZ into columnar I/O. Geometry is computed before query filters, so a phi-only
projection or filtering out neighboring residues retains the same values as a
full scan. Repeated and concurrent `collect()` calls use fresh execution cursors.

The runtime has no Gemmi/Biopython/foldcomp Python dependency. Vendored native
libraries are built into the extension; building from source needs C++17 and Rust.
