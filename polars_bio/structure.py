"""Protein structure readers backed by native DataFusion table providers.

Coordinates are Angstroms; angles are signed degrees. Author residue IDs remain
strings. Residue output selects coherent alternate conformers and computes geometry
before query filters. Foldcomp output describes reconstructed, lossy coordinates.
"""

from __future__ import annotations

import os
from collections.abc import Sequence

import polars as pl

from ._metadata import set_source_metadata
from .context import ctx
from .polars_bio import StructureReadOptions, py_scan_structure

Sources = str | os.PathLike[str] | Sequence[str | os.PathLike[str]]


def _paths(sources: Sources) -> list[str]:
    if isinstance(sources, (str, os.PathLike)):
        sources = [sources]
    if not isinstance(sources, Sequence) or isinstance(sources, bytes):
        raise TypeError("sources must be a path or a sequence of paths")
    result = [os.fspath(path) for path in sources]
    if any(not isinstance(path, str) or not path for path in result):
        raise ValueError("structure paths must be nonempty strings")
    return result


def _provider(
    sources: Sources,
    *,
    format: str,
    level: str = "atom",
    model: str | int = "all",
    altloc: str | None = None,
    include_non_peptide: bool = False,
    max_peptide_bond: float = 1.8,
    max_input_bytes: int = 256 * 1024 * 1024,
    max_decoded_bytes: int = 512 * 1024 * 1024,
    max_atoms: int = 5_000_000,
    object_storage_options=None,
    ids: Sequence[str] | None = None,
    entry_keys: Sequence[int] | None = None,
    table_name: str | None = None,
):
    if isinstance(model, bool) or not isinstance(model, (str, int)):
        raise TypeError("model must be 'all', 'first', or an integer")
    if ids is not None and (
        isinstance(ids, str) or any(not isinstance(x, str) for x in ids)
    ):
        raise TypeError("ids must be a sequence of strings")
    if entry_keys is not None and (
        isinstance(entry_keys, (str, bytes))
        or any(
            isinstance(x, bool) or not isinstance(x, int) or x < 0 for x in entry_keys
        )
    ):
        raise TypeError("entry_keys must be a sequence of nonnegative integers")
    if format != "foldcomp" and (ids is not None or entry_keys is not None):
        raise ValueError("ids and entry_keys are only supported for Foldcomp")
    options = StructureReadOptions(
        level,
        str(model),
        altloc,
        include_non_peptide,
        max_peptide_bond,
        max_input_bytes,
        max_decoded_bytes,
        max_atoms,
    )
    return py_scan_structure(
        ctx,
        _paths(sources),
        format,
        options,
        None if ids is None else list(ids),
        None if entry_keys is None else list(entry_keys),
        table_name,
        object_storage_options,
    )


def _scan(sources: Sources, *, format: str, **options) -> pl.LazyFrame:
    from .io import _lazy_scan

    provider = _provider(sources, format=format, **options)
    frame = _lazy_scan(provider, projection_pushdown=True, predicate_pushdown=True)
    metadata = {
        key.decode(): value.decode()
        for key, value in (provider.schema().metadata or {}).items()
    }
    set_source_metadata(frame, format=format, path=str(sources), header=metadata)
    return frame


def scan_structures(
    sources: Sources, *, level: str = "atom", **options
) -> pl.LazyFrame:
    """Scan PDB/mmCIF paths, ordered lists, or local globs without loading coordinates.

    ``level='residue'`` emits one peptide residue per row, with N/CA/C/O
    coordinates and phi/psi/omega plus three backbone bond angles. Geometry uses
    coherent conformers (``altloc='best_backbone'`` by default), a 1.8 Angstrom
    peptide-bond cutoff, and nulls at chain breaks or missing atoms. Omega belongs
    to the outgoing peptide bond. Set ``include_non_peptide=True`` to retain
    ligands/waters with null geometry. ``model`` accepts 'all', 'first', or an ID.

    Text gzip is detected from its header. Explicit HTTP/S3/GCS/Azure URLs use
    ``object_storage_options``; remote glob expansion is unsupported. Per-file
    input/decompressed/atom limits bound active structure buffers. List order and
    repeated source occurrences are preserved in ``source_index``. Rows have no
    guaranteed query order: sort by the source and structural ordinal columns.
    """
    return _scan(sources, format="auto", level=level, **options)


def scan_pdb(sources: Sources, *, level: str = "atom", **options) -> pl.LazyFrame:
    """Scan PDB files; see :func:`scan_structures` for shared options and semantics."""
    return _scan(sources, format="pdb", level=level, **options)


def scan_mmcif(sources: Sources, *, level: str = "atom", **options) -> pl.LazyFrame:
    """Scan mmCIF files, preserving raw author and standardized label identifiers."""
    return _scan(sources, format="mmcif", level=level, **options)


def scan_foldcomp(
    source: str | os.PathLike[str],
    *,
    ids: Sequence[str] | None = None,
    entry_keys: Sequence[int] | None = None,
    level: str = "atom",
    **options,
) -> pl.LazyFrame:
    """Scan a local FCZ file or select entries from a Foldcomp database.

    ``ids`` selects lookup names; ``entry_keys`` selects numeric index keys. They
    are mutually exclusive. None selects all entries; an empty sequence selects
    zero; duplicates decode once; missing or ambiguous selections raise. Metadata
    selection scans the index/lookup, then only selected payloads are decoded.
    Database files must remain unchanged between scan creation and collection.
    Geometry is computed from reconstructed coordinates; B factors are not
    automatically labeled as pLDDT. Other options match :func:`scan_structures`.
    """
    return _scan(
        source,
        format="foldcomp",
        ids=ids,
        entry_keys=entry_keys,
        level=level,
        **options,
    )


def read_structures(sources: Sources, **options) -> pl.DataFrame:
    """Read structures eagerly; accepts the same options as scan_structures."""
    return scan_structures(sources, **options).collect()


def read_pdb(sources: Sources, **options) -> pl.DataFrame:
    """Read PDB atoms or residues eagerly."""
    return scan_pdb(sources, **options).collect()


def read_mmcif(sources: Sources, **options) -> pl.DataFrame:
    """Read mmCIF atoms or residues eagerly."""
    return scan_mmcif(sources, **options).collect()


def read_foldcomp(source: str | os.PathLike[str], **options) -> pl.DataFrame:
    """Read selected Foldcomp atoms or residues eagerly."""
    return scan_foldcomp(source, **options).collect()


def register_structure(
    name: str, sources: Sources, *, format: str = "auto", **options
) -> None:
    """Register a native PDB/mmCIF provider for SQL with scan_structures options."""
    if format not in {"auto", "pdb", "mmcif"}:
        raise ValueError("format must be 'auto', 'pdb', or 'mmcif'")
    _provider(sources, format=format, table_name=name, **options)


def register_foldcomp(name: str, source: str | os.PathLike[str], **options) -> None:
    """Register a native Foldcomp provider for SQL with scan_foldcomp options."""
    _provider(source, format="foldcomp", table_name=name, **options)
