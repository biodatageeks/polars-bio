## 1. Plumbing
- [ ] 1.1 `companion_cache` and `cache_dir` on `PgenReadOptions` and `native_pgen_options`; unknown mode is a `ValueError` naming the accepted values.
- [ ] 1.2 Arguments on `read_pgen`, `scan_pgen`, `read_pgen_matrix`, `describe_pgen`, `register_pgen`.

## 2. Docs and tests
- [ ] 2.1 `reading.md` section and CHANGELOG entry.
- [ ] 2.2 Forwarding tests; `read_write` on a `tmp_path` copy of the oracle creates the sidecar and a second read uses it; `off` creates nothing.
- [ ] 2.3 Real-panel check: second `describe_pgen` under a second with the sidecar present.
