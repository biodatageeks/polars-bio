## 1. Implementation
- [x] 1.1 Update `datafusion-bio-functions` git revision pins to `ab88d47c8f4e4b7e72da48797ac038c6c0439597`.
- [x] 1.2 Update annotation lookup internals to keep Python API stable while aligning lookup argument handling with the new backend behavior.
- [x] 1.3 Keep cache creation entrypoint as `pb.annotations.create_vep_cache` and validate native->parquet and native->fjall flows.
- [x] 1.4 Add/extend tests for `annotate_variants` match modes, validation paths, chr-prefixed output preservation, and cache conversion paths.
- [x] 1.5 Run targeted tests and OpenSpec validation in strict mode.
