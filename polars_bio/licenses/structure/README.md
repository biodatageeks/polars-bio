# Native structure reader notices

The polars-bio extension incorporates the following third-party native sources:

- Gemmi 0.7.5, commit `5cc1c23c6007e0e6cbd69289c6f7c0bff50e943e`, under MPL 2.0; its bundled PEGTL is MIT licensed.
- Foldcomp, commit `89e37195d3c8ade8d40ead91ad82e6cd2964a967`, under MIT; its bundled tcb::span is under the Boost Software License 1.0.
- The Windows build also uses the MIT-licensed dirent compatibility header bundled with Foldcomp; its copyright notice is reproduced below.

Full license texts accompany this file. The exact source distribution, native adapters, and build instructions are publicly available in the [datafusion-bio-formats repository](https://github.com/biodatageeks/datafusion-bio-formats), at the immutable revision recorded for its dependencies in this release's Cargo.toml and Cargo.lock. Gemmi's covered headers are vendored without modification in `datafusion/bio-format-structure/native/vendor`; Foldcomp sources are under `datafusion/bio-format-foldcomp/native/vendor`.

Upstream source: [Gemmi](https://github.com/project-gemmi/gemmi/tree/5cc1c23c6007e0e6cbd69289c6f7c0bff50e943e), [Foldcomp](https://github.com/steineggerlab/foldcomp/tree/89e37195d3c8ade8d40ead91ad82e6cd2964a967).

Dirent interface for Microsoft Visual Studio

Copyright (C) 1998-2019 Toni Ronkko
This file is part of dirent. Dirent may be freely distributed under the MIT
license. For all details and documentation, see https://github.com/tronkko/dirent

The full MIT permission and warranty terms are reproduced in FOLDCOMP-LICENSE.txt.
