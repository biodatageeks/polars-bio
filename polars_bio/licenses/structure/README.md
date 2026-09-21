# Structure reader notices

PDB and mmCIF parsing use repository-owned Rust code under Apache-2.0.
The Rust Foldcomp decoder adapts packing, discretization, residue tables and
reconstruction from Foldcomp commit `89e37195d3c8ade8d40ead91ad82e6cd2964a967`,
under the MIT license. The complete MIT text and retained upstream copyright
notices accompany this file in `FOLDCOMP-LICENSE.txt`.

Copyright (c) 2022 Foldcomp Development Team
Copyright © 2021 Hyunbin Kim, All rights reserved
Upstream NeRF contributor: Milot Mirdita

No Gemmi, PEGTL, tcb::span or Foldcomp C++ implementation is bundled with these
readers. Optional development comparisons obtain the original code separately;
their historical licenses are retained with that reference checkout.

The exact Rust source is in the [datafusion-bio-formats repository](https://github.com/biodatageeks/datafusion-bio-formats)
at the immutable revision pinned in this release's Cargo.toml and Cargo.lock.
The decoder and its notices are in `datafusion/bio-format-foldcomp/src/fcz/`.
[Original Foldcomp source](https://github.com/steineggerlab/foldcomp/tree/89e37195d3c8ade8d40ead91ad82e6cd2964a967).
