## ADDED Requirements

### Requirement: Native provider ownership of residue output
polars-bio SHALL expose residue rows produced by datafusion-bio-formats providers, including grouping, conformer selection, connectivity and geometry. Python/SQL bindings SHALL pass native options and consume the resulting Arrow schema/batches without implementing a second residue algorithm or depending on a new functions crate.

#### Scenario: Shared Python and SQL geometry
- **WHEN** a user reads the same structure/options through eager, lazy or SQL APIs
- **THEN** all paths SHALL consume the same native provider residue output and share its descriptor/null semantics.

### Requirement: Observed residue descriptors
The system SHALL provide an explicit residue view containing observed peptide residue identity, one-letter/parent mapping, selected N/CA/C/O coordinates, three backbone torsions, and three backbone bond angles. It SHALL preserve incomplete observed residues and SHALL NOT synthesize unobserved residues or peptide geometry for nonpeptide entities.

#### Scenario: Missing backbone atom
- **WHEN** an observed peptide residue lacks CA but retains other source atoms
- **THEN** the residue SHALL remain in the result with null CA coordinates and null dependent descriptors
- **AND** unrelated available values SHALL remain present.

#### Scenario: Modified peptide residue
- **WHEN** source metadata identifies a modified peptide residue such as a supported MODRES-mapped MSE
- **THEN** the original component SHALL be preserved alongside the documented parent/one-letter mapping regardless of ATOM/HETATM classification.

### Requirement: Defined angle alignment and units
The system SHALL compute Float64 angles in degrees from the selected coordinates. For residue i, phi SHALL use C(i-1),N(i),CA(i),C(i); psi SHALL use N(i),CA(i),C(i),N(i+1); and omega SHALL use CA(i),C(i),N(i+1),CA(i+1). Bond angles SHALL use N(i),CA(i),C(i); CA(i),C(i),N(i+1); and C(i-1),N(i),CA(i). Torsions SHALL normalize to [-180,180), and bond angles SHALL lie in [0,180].

#### Scenario: Chain termini
- **WHEN** a complete open peptide chain is read
- **THEN** its first residue SHALL have null phi and incoming C-N-CA angle
- **AND** its last residue SHALL have null psi, outgoing omega, and outgoing CA-C-N angle.

#### Scenario: Degenerate geometry
- **WHEN** a required dihedral plane is undefined or a required bond has zero length
- **THEN** the affected descriptor SHALL be null rather than NaN or a fabricated zero.

### Requirement: Explicit conformer and connectivity policy
Residue computation SHALL use a documented deterministic component/conformer policy without mixing mutually exclusive alternate backbone atoms. It SHALL preserve entry/model/chain/segment boundaries, use available standardized polymer identity and sequence continuity, and apply the configured finite C-N link-distance rule. It SHALL expose selected conformer and link/availability status.

#### Scenario: Inconsistent per-atom occupancy maxima
- **WHEN** selecting each backbone atom independently would mix altloc A and B
- **THEN** residue computation SHALL select one coherent candidate according to the configured policy and leave missing atoms null.

#### Scenario: Author-number gap
- **WHEN** author numbering has a gap but standardized peptide sequence identity is consecutive and C-N geometry is valid
- **THEN** the author-number gap alone SHALL NOT break the peptide link.

#### Scenario: Known break
- **WHEN** residues are separated by TER, a known standardized sequence gap, a model/chain boundary, or a failed geometric link rule
- **THEN** descriptors requiring that link SHALL be null.

### Requirement: Geometry-preserving optimization
The residue execution plan SHALL retain neighboring atom dependencies across projections, filters, limits, batch boundaries, and entry partitions. It SHALL compute a selected residue's geometry from original neighboring context before unsafe row filtering.

#### Scenario: Middle-residue filter
- **WHEN** a query requests only residue 10 and only its phi/psi columns
- **THEN** its values SHALL match residue 10 in the full descriptor table even though residues 9 and 11 are absent from output.

#### Scenario: Batch split
- **WHEN** a residue's atoms or its neighbor context cross a record-batch boundary
- **THEN** its descriptors and null masks SHALL match an execution with a larger batch size.

#### Scenario: Count and limit
- **WHEN** a query counts residues with no projected atom fields or limits a residue result
- **THEN** row counts SHALL reflect residue rows
- **AND** the final selected row SHALL retain any required next-residue context.
