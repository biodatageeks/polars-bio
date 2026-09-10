# Test oracles, fixture generation, and executed experiments

## 1. Use several independent kinds of truth

No single structure library should define all expected outputs. Parsing, chemical interpretation, geometry, compression fidelity, and query execution are separate test targets.

| Layer | Primary oracle | Independent check | What it must not decide implicitly |
|---|---|---|---|
| PDB record fields | Tiny hand-audited fixed-column examples from the wwPDB layout | Gemmi plus Biopython PDBParser on compatible valid examples | Missing-value defaults, alternate selection, chain merging |
| mmCIF raw identifiers/values | Gemmi `cif.Document`/`Block`/table values | Biopython `MMCIF2Dict`, plus literal fixture expectations | High-level hierarchy coercions, auth-versus-label fallback, protein filtering |
| Normalized atom tables | Explicit mapping of source columns to the schema in design.md | Independently parsed keys/coordinates, full key-set comparison | Silent dropping of waters/HETATM/alternate sites |
| Three-point angles and four-point torsions | Analytical coordinates plus Biopython `calc_angle`/`calc_dihedral` over Float64 vectors | Gemmi `calculate_angle`/`calculate_dihedral`; optionally Biotite | Which residues are connected, which altloc wins, where omega is assigned |
| Protein/residue classification | Versioned entity/component/MODRES rules and hand-labeled cases | Source metadata and a pinned independent residue table | Treating all HETATM as nonprotein or all N/CA/C groups as protein |
| End-to-end conventional structures | Gemmi traversal + explicit policies and geometry definitions | Biopython hierarchy/PPBuilder or Biotite after normalizing all defaults | Nonnumeric author identifiers; those use low-level parsers |
| FCZ codec output | Pinned official Foldcomp `get_data(fcz)` coordinates and `decompress(fcz)` identity/order | CLI decode + independent PDB parsing, and coordinate-derived descriptors | Exact equality to original uncompressed coordinates; padded codec angle arrays |
| Foldcomp subset selection | Literal index/lookup manifest and official `open(..., ids=...)` for nonempty valid IDs | Full-decode-then-select on a tiny database | Upstream empty-ID or missing-ID policy when our API deliberately differs |
| DataFusion/Polars execution | Full materialization followed by the same filter/select/limit | SQL/eager/lazy equality, decode counters, instrumented storage | Assuming a query-plan string alone proves work was pruned |

The wwPDB sources define [PDB coordinate fields](https://www.wwpdb.org/documentation/file-format-content/format33/sect9.html) and [mmCIF identifier relationships](https://mmcif.wwpdb.org/docs/tutorials/intro/atom.htm). Preserve their distinction between label sequence identity and author nomenclature. `auth_seq_id` [need not be numeric](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_atom_site.auth_seq_id.html).

Gemmi documents [backbone torsion helpers](https://gemmi.readthedocs.io/en/stable/analysis.html). Biopython's [MMCIFParser options](https://biopython.org/docs/latest/api/Bio.PDB.MMCIFParser.html) alter author/label behavior; using label residue numbering can skip nonpolymer rows. Use raw category parsing for the full atom-table oracle. Biotite offers [explicit altloc/model choices](https://www.biotite-python.org/latest/apidoc/biotite.structure.io.pdbx.get_structure.html) and [backbone dihedrals](https://www.biotite-python.org/latest/apidoc/biotite.structure.dihedral_backbone.html), but reconcile conventions before adding it to CI.

If Gemmi becomes the production CIF tokenizer, Gemmi alone ceases to be an independent parser oracle. Keep `MMCIF2Dict` and hand-audited syntax/identity fixtures mandatory. If the upstream Foldcomp codec becomes the production codec, official decoding is still a compatibility oracle for the adapter; it does not independently prove that upstream compression is scientifically lossless.

## 2. Experiments actually run

Executed on 2026-09-09 with Python 3.12.2, Gemmi 0.7.5, Biopython 1.88, Foldcomp 1.0.0, and NumPy 2.5.3. The executable [probe](research/probe.py), [JSON results](research/probe-results.json), [76-row geometry output](research/1ubq.geometry.gemmi.json), and [hand-audited numerical inputs/outputs](research/analytical-geometry.json) are included. The research geometry JSON uses abbreviated bond-angle names; it is not the full proposed residue schema. These are research observations on one conventional protein and a tiny upstream database, not comprehensive production validation.

| Experiment | Observation | Implication |
|---|---|---|
| RCSB 1UBQ in PDB and mmCIF | 660 atom rows, 76 protein residues; keys/XYZ in the normalized comparison matched exactly | Useful paired-format seed fixture; compare only shared semantic fields, not absent PDB label IDs |
| Six backbone quantities on 1UBQ | 451 defined comparisons; maximum Gemmi/Biopython difference with identical Float64 coordinates was `5.97e-12` degrees | A `1e-6`-degree computational tolerance is practical for nondegenerate same-input math |
| Biopython PDBParser coordinates versus Float64 source values | Maximum phi difference was `1.85e-4` degrees | Do not use parsed Float32 coordinates to impose a `1e-6`-degree full-parser tolerance |
| Replace author residue ID 1 with `X1` in mmCIF | Both raw APIs preserve `X1`; Gemmi and Biopython high-level structure constructors reject it | Raw CIF category access is necessary for the complete identifier contract |
| Official Foldcomp compress/decompress of 1UBQ | 1,364-byte FCZ; 602 reconstructed protein atoms; original protein keys all matched | Codec output excludes the original water rows in this example |
| Original versus decoded 1UBQ | Unaligned atom RMSD `0.196282` angstrom; maximum displacement `1.129011` angstrom | Original PDB is not an exact decoding oracle; these are sample measurements, not universal error limits |
| `get_data(fcz)["coordinates"]` | 602 coordinate triples, not `76 * 3` backbone triples | Build backbone rows by atom identity; do not reshape the returned list as N/CA/C triples |
| Raw FCZ coordinates versus emitted PDB coordinates | Maximum component difference `0.000499741` angstrom | Official PDB formatting rounds coordinates; direct array parity needs a different tolerance |
| `get_data(PDB)` versus `get_data(FCZ)` arrays | phi/psi/omega lengths 75 versus 76 for 76 residues; bond-angle lengths 226 versus 228 | Do not zip these vectors directly into residue rows or accept terminal padding as defined geometry |
| Foldcomp phi alignment | First PDB phi approximately `-91.02025` degrees matches residue **2**, while residue 1 phi is undefined | `phi[j]` is shifted relative to a simple residue-indexed vector; psi starts at residue 1 |
| Official sample DB subset | Requested `d1asha_`, `d1it2a_`; raw payloads 2,306 and 2,249 bytes | Valid subset oracle; `.index` lengths are 2,307 and 2,250 including terminators |
| `foldcomp.open(..., ids=[])` | Opens all 24 sample entries | Our empty selection must short-circuit and deliberately differ |
| Missing entry with `err_on_missing=True` | `KeyError` | Useful explicit-error baseline; do not inherit permissive defaults |

The primary input URLs and SHA-256 hashes are recorded in the JSON. RCSB's [1UBQ entry](https://www.rcsb.org/structure/1UBQ) is the provenance for the experimental structure. The database fixture is from Foldcomp commit `89e37195d3c8ade8d40ead91ad82e6cd2964a967`; the codec observations are from the installed **1.0.0 wheel**, not a locally compiled version of that source commit. The upstream [Python binding source](https://github.com/steineggerlab/foldcomp/blob/89e37195d3c8ade8d40ead91ad82e6cd2964a967/foldcomp/foldcomp.cxx) and [codec angle construction](https://github.com/steineggerlab/foldcomp/blob/89e37195d3c8ade8d40ead91ad82e6cd2964a967/src/foldcomp.cpp#L484) explain the representation differences.

### Reproduce this probe

Use a disposable directory; these commands do not build or modify polars-bio. Replace `PLAN` with the path to this proposal directory.

```sh
PLAN=/path/to/polars-bio/openspec/changes/add-structure-readers
WORK=$(mktemp -d)
uv venv --python 3.12 "$WORK/venv"
uv pip install --python "$WORK/venv/bin/python" -r "$PLAN/research/requirements.txt"
git clone https://github.com/steineggerlab/foldcomp.git "$WORK/foldcomp"
git -C "$WORK/foldcomp" checkout 89e37195d3c8ade8d40ead91ad82e6cd2964a967
"$WORK/venv/bin/python" "$PLAN/research/probe.py" \
  --work-dir "$WORK/results" --foldcomp-repo "$WORK/foldcomp"
```

The probe downloads only the two 1UBQ structure files, uses the checked-out tiny example database, writes JSON and temporary PDB/FCZ outputs, and asserts its core parser/numerical comparisons. It is intentionally narrow: its traversal assumes the simple, complete chain A in 1UBQ. It is **not** the production fixture normalizer or an implementation of the generalized altloc/connectivity policy. Re-fetching an archive URL can change bytes; the future generator must enforce the committed expected hashes, not merely record new ones.

## 3. Deterministic synthetic corpus

Write expected identities and null masks from the specification first. Generate **input coordinates** from small deterministic arrays or an independent peptide builder, then serialize PDB and mmCIF independently. Store generator parameters and seeds. Do not generate expected output by invoking polars-bio, the Rust kernel, or a wrapper of the same production normalizer.

For simple geometry, exact literals are sufficient. Using A=(1,0,0), B=(0,0,0), C=(0,1,0):

| D | Expected signed A-B-C-D torsion |
|---|---|
| (0,1,1) | -90 degrees |
| (0,1,-1) | +90 degrees |
| (-1,1,0) | -180 degrees after canonical normalization; +180 is equivalent |

The A-B-C angle is 90 degrees. Collinear four-point examples have undefined dihedral planes and must yield null. A straight three-point angle with nonzero bonds is a valid 180 degrees. Distinguish those two cases. Independently perturb points near 0/180 degrees, and use translation/rotation/mirroring to test invariants.

For physically meaningful multi-residue inputs, start with a complete peptide fixture and vary exactly one property per case. Synthetic bond endpoints must satisfy the configured C-N link rule unless a chain break is the purpose. If a generated PDB rounds coordinates to three decimals, compute its golden geometry from the **serialized PDB values**, not the pre-serialization generator values.

| Family | Essential cases | Expected assertions |
|---|---|---|
| Small peptides | 1, 2, 3 residues; standard residue mixture; negative/zero author numbering | Row count, termini, incoming/outgoing angle placement, available local angles |
| Known geometry | Exact +/-90, equivalent +/-180, cis/trans omega, random well-conditioned points | Sign, units, circular comparison, bond-angle range |
| Missing atoms | Delete N, CA, C, O separately in middle/terminal residue; CA-only structure | Only dependent descriptors null; no residue dropped; atom triples remain aligned |
| Degeneracy | Duplicate points, zero-length bond, collinear planes, non-finite/malformed required fields | Undefined geometry null; invalid source numeric data errors |
| Connectivity | TER with same chain label, geometric break, different chains, label gap, author gap with continuous label sequence, threshold boundary | No cross-boundary angles; author gap alone does not disconnect |
| Identity | 10, 10A, 10B; negative and `X1` author IDs; auth/label chain mismatch; reused chain ID in different TER segments | Distinct keys; exact strings; stable ordinals; no invented label identifiers for PDB |
| Models | Absent MODEL, non-1 first model, several models with repeated atom serials and different coordinates | All/first/number selection; model boundaries; no angle leakage |
| Altloc | Shared blank atoms, A/B occupancy ties, higher occupancy B, incomplete higher-occupancy candidate, conflicting per-atom maxima | Deterministic whole-candidate choice; no mixed backbone; explicit-alt handling |
| Microheterogeneity | Two residue component identities at one sequence site, with separate conformers | Preserve all atom variants; one documented selected residue candidate; no merging names |
| Chemistry | MSE MODRES/parent, SEC/PYL, unknown peptide, water, ligand named CA, nucleic-acid chain | Correct peptide population/one-letter mapping; no peptide geometry for nonpeptides |
| CIF syntax | Reordered/mixed-case tags, arbitrary whitespace, quoted names, literal quoted `?`/`.`, semicolon text, unknown categories before/after atoms | Raw token handling, exact normalized fields, no false missing values |
| CIF shape | Several blocks with and without atom data; interleaved model/chain/residue rows; singleton category syntax | Explicit block handling, stable groups, no row loss; reject ambiguous unsupported layouts descriptively |
| CIF numbers | Scientific notation, standard uncertainties, missing optional occupancy/B/charge, missing required coordinates | Defined numeric interpretation, null optional fields, required-field errors |
| PDB syntax | Fixed-column atom-name alignment, blank chain, negative coordinates, CRLF, no final newline, blank optional fields | Correct slicing and nulls; no whitespace-parser behavior |
| Malformed inputs | Truncated loop, duplicate required tags, duplicate atom identity, invalid number, truncated gzip, empty file | Contextual error; no panic, silent row skipping, or fabricated zeros |
| Collection identity | Same basename in two dirs, mixed formats, duplicate explicit source, mixed empty/nonempty selections | Unique provenance and row keys; deterministic explicit-sort output |

A structure can contain non-coordinate CIF blocks. Enumerate coordinate-bearing blocks and retain their original block ordinal; error if the requested source contains none. Unsupported fractional-only coordinate blocks must not disappear silently. Model/chain/residue row reordering should preserve biological identity/geometry when standardized identity establishes sequence; for PDB-like sources without ordering metadata, arbitrary residue-order shuffling is not an invariant.

## 4. Real structures and codec fixtures

Use real files to complement synthetic cases, not to replace them. Download each source once, inspect the claimed feature, and freeze its bytes, source URL, revision/access date, checksum, and licensing/provenance metadata. Only 1UBQ and the upstream Foldcomp example database were exercised in this analysis; the following additional entries are **candidate fixtures to inspect**.

| Input | Purpose | Initial size policy |
|---|---|---|
| [1UBQ](https://www.rcsb.org/structure/1UBQ), PDB + mmCIF | Small complete protein and solvent; paired formats; basic descriptors | Commit the pair or audited compact extracts plus source hashes |
| [1D3Z](https://www.rcsb.org/structure/1D3Z) | NMR ensemble and model selection | Small representative subset for CI, full file for integration |
| [1EN2](https://www.rcsb.org/structure/1EN2) | Candidate disorder/microheterogeneity case; audit actual alternate records before labeling the fixture | Keep only small audited data in normal CI |
| [4HHB](https://www.rcsb.org/structure/4HHB) | Multiple chains and nonprotein components | PDB/mmCIF identity and residue-population check |
| Synthetic mmCIF with long chain and nonnumeric author IDs | Limits impossible to represent faithfully in legacy PDB | Tiny literal file; raw category oracles |
| One pinned predicted structure | Confidence provenance and predicted-protein ingestion | Freeze a specific dataset/model revision; no “latest” fetch in CI |
| Locally compressed 1UBQ/protein-only peptide | Official compressor-generated FCZ and decoded-coordinate reference | Small, deterministic codec fixture with exact compressor version/options |
| Upstream Foldcomp example DB | Name/key/offset/length and subset behavior | About 52 KB payload at the inspected commit; preserve all small sidecars |
| A generated 3–10-entry DB | Known order, distinct lengths/keys/titles, duplicates and missing IDs | Generate with pinned official CLI; retain a manually audited index manifest |

Avoid treating independent RCSB PDB/mmCIF downloads as universally bitwise equivalent; identifiers, representations, or precision may differ. Compare shared normalized fields only. A Gemmi PDB-to-CIF conversion is useful for testing serialization invariants, but cannot be the only CIF input when Gemmi is also the production tokenizer.

For codec corruption tests, mutate only fixture copies: short header, truncated payload, invalid record magic, out-of-bounds index, duplicate key, missing selected entry, lookup mismatch, and missing required sidecar. Run risky native corruption probes in subprocesses/sanitizer jobs; ordinary test failures must remain catchable errors. Test database numeric keys separately from display names and source titles.

## 5. Full generator design (to implement)

Place the canonical corpus and generation tools under `datafusion-bio-formats/testing/data/structure/` and `testing/oracles/structure/`. Both atom-provider and residue/geometry tests live in the formats workspace and consume that corpus. polars-bio integration tests consume the same corpus version, using a small checked-in subset plus its source manifest/hash so wheel tests do not require a sibling checkout. Regenerate in formats only; use a checked copy/sync step for polars-bio and fail on corpus-version/hash mismatch. The research outputs already saved here remain provenance, not a second independently maintained golden corpus.

Suggested generated layout:

```text
structure/
  manifest.json
  synthetic/*.pdb, *.cif, *.cif.gz
  real/1ubq.pdb, 1ubq.cif, ...
  foldcomp/tiny_db, tiny_db.index, tiny_db.lookup, tiny_db.dbtype
  expected/atoms/*.json
  expected/residues/*.json
  expected/errors.json
  expected/policies.json
  oracle-differences.json
```

JSON is easy to audit for small goldens and works in Rust/Python. Use explicit JSON nulls and prohibit NaN/Infinity. Optional Arrow IPC/Parquet copies can exercise typed schema conversion, but a binary format is not a substitute for human-reviewable key/value fixtures.

The manifest records input hash, URL or generator seed, generator revision, oracle package/binary versions, command/options, schema version, model/altloc/connectivity policy, coordinate precision, expected row counts, expected null counts, and expected-output hashes. Keep the independent raw oracle outputs as well as canonicalized expectations. A discrepancy ledger must explain each deliberate difference, such as nonnumeric-ID rejection by a high-level parser or our empty-ID behavior.

Generator stages:

1. Verify or obtain **pinned** inputs. Downloads are explicit; CI's normal correctness job uses local fixtures only.
2. Emit deterministic synthetic PDB and CIF with separately checked serialization. Use a fixed seed and stable ordering.
3. Extract raw PDB/CIF fields using independent oracles. Compare full keys/counts and audit disagreements before canonicalization.
4. Apply the written identity/classification/conformer/connectivity policy in a small Python reference implementation, independently of Rust. Cross-check policy behavior against hand-authored tiny expected outputs.
5. Calculate geometry from those selected coordinates with both vector libraries; compare signs, alignment, finite masks, and numeric differences.
6. Compress designated clean peptide inputs using the pinned official Foldcomp CLI or Python compressor. Decode to raw arrays and text; map atoms by verified identity/order; recompute expected geometry from raw decoded arrays.
7. Write full expected tables, types/null masks, hashes, and discrepancy ledger. Fail generation on an unexplained difference. Never average oracle disagreements or silently increase a tolerance.
8. A `--check` mode re-generates into a temporary directory and verifies committed content. Golden refreshes require review with a semantic diff, input revision, and oracle-version change summary.

Neither the Rust feature nor polars-bio is imported by the generator. CI compares product output to frozen expected results without installing all oracle tools; a dedicated generation/parity job runs the pinned tools and must fail if required oracle dependencies are unavailable. Avoid `importorskip` allowing the only correctness gate to disappear.

## 6. Comparison rules and tolerances

Always compare **complete key multisets, row counts, schema/types, and null masks before numbers**. An inner join alone can hide missing or extra residues. Then sort by stable source/entity/site identity and compare values. Where duplicate sites are intentionally preserved, include original row ordinal.

| Comparison | Initial rule |
|---|---|
| Raw identifiers, record type, categorical fields, source IDs | Exact string/integer equality |
| Null versus present | Exact; NaN is never a substitute for Arrow null |
| Same serialized PDB/mmCIF coordinates parsed to Float64 | `atol=1e-9` angstrom, `rtol=0`; optional decimal fields similarly checked |
| Same Float64 coordinates, well-conditioned computed angles | `atol=1e-6` degrees, `rtol=0`; circular error for torsions |
| Biopython Float32-parser comparison on 1UBQ-like magnitudes | `1e-3` degrees is a diagnostic starting bound, supported by the measured `1.85e-4` difference; do not use as the primary Float64 golden gate |
| Native Foldcomp adapter versus pinned codec raw arrays | Exact widening of identical Float32 results if builds agree; provisional `1e-6` angstrom until cross-platform evidence sets the bound |
| Raw FCZ arrays versus official three-decimal PDB output | Per-component `5.1e-4` angstrom for matched atom rows; compute geometry independently from each representation rather than applying the Float64 angle tolerance across them |
| Original structure versus decoded FCZ | Report codec-fidelity statistics separately; no universal identity tolerance or maximum-error guarantee inferred from 1UBQ |

Circular torsion error in degrees is `abs(((actual - expected + 180) % 360) - 180)`. This accepts equivalent -180/+180 representations while still checking the public canonical range separately. Near-degenerate geometry gets dedicated threshold tests, not a broad tolerance increase. Unknown or missing data must not become a numerically convenient zero.

For Foldcomp native builds, quantify how compiler/architecture changes affect raw reconstructed arrays and coordinate-derived angles in stage 0. If small coordinate differences become large angular differences near a singularity, classify the geometry by the agreed conditioning rule. Do not apply the codec's compression RMSD tolerance to bugs in our adapter.

## 7. Execution, metamorphic, and performance checks

Required execution equivalences:

- `read_*` equals `scan_*.collect()` equals SQL registration for both schemas.
- Projected/filtered scans equal full collection followed by the same operation; include a residue filter that removes both neighbors from output.
- Selecting only phi still gives the full-scan phi, even though no coordinate columns were requested.
- Atom/residue count-only and rootless projections preserve row counts across batch boundaries and empty selections.
- Batch sizes 1, 2, 3, and values splitting a residue; 1 versus several partitions; entry boundaries and terminal residues.
- Two scans of the same source with different level/model/altloc options; repeated and concurrent collection; no shared mutable cursor or catalog-name collision.
- `ids=None`, `ids=[]`, one/many/nonexistent/duplicate IDs; known numeric key selection; file title different from lookup name.
- Selection counters prove only K distinct selected FCZ payloads were decoded; metadata scan cost is measured separately.
- Plain/gzip equality; explicit remote text URLs with range/content-encoding behavior; local tests use a controlled HTTP fixture, not public internet.

Metamorphic checks cover rigid translation/rotation (geometry unchanged), reflection (dihedral sign changes and unsigned angles unchanged), duplicate independent models (independent repeated rows), source renaming (provenance changes only), CIF tag reordering/whitespace (unchanged semantics), and atom-row permutation where standardized residue ordering is available. For reordered inputs, compare biological identities and geometry while allowing original-row ordinals to change. Missing-atom mutations have explicitly bounded effects on descriptor null masks.

Benchmark representative parse-to-DataFrame and descriptor-to-DataFrame work, not just parser calls. A small correct scalar reference is allowed to be slow. Performance gates should prove bounded collection memory, no unselected payload decode, no collection-wide eager materialization, and compliance with the configured worker budget. Set numerical speed targets only after a measured baseline. No benchmark claims were produced by this planning task.
