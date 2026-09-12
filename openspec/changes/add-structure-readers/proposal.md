# Change: Read structural collections and compute residue descriptors (issue #455)

## Why

[polars-bio #455](https://github.com/biodatageeks/polars-bio/issues/455) requests loading collections of PDB/mmCIF structures and subsets of Foldcomp databases into DataFrames, including residue identities, coordinates, and bond angles. This requires a structural data model, geometry semantics, and indexed collection execution in addition to format parsing.

## What Changes

- Add lazy and eager PDB, PDBx/mmCIF, mixed structural collection, and Foldcomp readers. Provide an atom view and an explicit residue view with backbone coordinates, three bond angles, and phi/psi/omega torsions.
- Preserve author and standardized identifiers, models, alternate locations, and provenance. Coordinates use angstroms; derived angles use degrees and explicit nulls.
- Implement native atom/residue providers, grouping, conformer selection, connectivity, geometry, and Foldcomp decoding in `datafusion-bio-formats`. Implement Python/SQL integration in `polars-bio`.
- Decode only selected Foldcomp entries, using the upstream codec through a bounded native interface. Recompute public descriptors from reconstructed coordinates.
- Establish independent parser and numerical oracles, frozen input/output fixtures, and execution tests before optimizing.
- Deliver in stages: contracts and native dependency spike; PDB/mmCIF atoms; residue geometry; Python/SQL; Foldcomp; collection performance and release validation.

## Impact

- New capabilities: `structure-io`, `residue-descriptors`, `foldcomp-io`.
- Existing genomic APIs retain their coordinate-system behavior. Structural APIs have physical units and do not attach genomic coordinate metadata.
- New proposed crates in the formats workspace: `datafusion-bio-format-structure` and `datafusion-bio-format-foldcomp`. The structure crate contains shared residue and pure geometry modules; the Foldcomp crate reuses them. No new functions crate or functions release is required.
- Python integration touches `src/{option,scan,lib}.rs`, a new `src/structure.rs` for source/options/Arrow bindings, `polars_bio/{io,sql,__init__,metadata_extractors}.py`, and dedicated tests/docs. Shared scan identity and metadata handling need focused changes. Atom/residue provider selection remains in formats rather than a polars-bio geometry wrapper.
- Native dependencies require wheel feasibility checks on Linux, macOS, and Windows. Gemmi is the recommended mmCIF syntax backend, conditional on the packaging spike; unmodified `pdbtbx` does not preserve the proposed data contract.
- This directory owns the public API and semantic contract. The [formats proposal](../../../../datafusion-bio-formats/openspec/changes/add-structure-readers/proposal.md) owns native implementation tasks. Both repositories consume one versioned oracle corpus, owned by formats.

## Review material and status

- [Design and implementation analysis](design.md)
- [Oracle strategy, fixture matrix, and measured findings](oracles.md)
- [Implementation tasks and acceptance gates](tasks.md)
- [Native formats tasks](../../../../datafusion-bio-formats/openspec/changes/add-structure-readers/tasks.md)
- [Executable research probe](research/probe.py) and [recorded results](research/probe-results.json)

Status: planning only, revised 2026-09-09 to implement the user's agreed two-repository scope. The research probe was executed; the product feature and comprehensive fixture generator have not been implemented. Proposed API names and policies remain defaults for review.
