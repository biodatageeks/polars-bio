# Design: PGEN genotype input

## The Python default narrows the provider default

`resolve_genotype_fields` returns every available field when the requested list
is absent, so `PgenReadOptions::default()` emits `GT`, `ALT_COUNT`, `PHASED`,
`DS`, `DS_STORED`, and `HDS` together. That is a defensible Rust default:
explicit, with no hidden narrowing.

It is a poor Python default. `read_pgen(path)` would decode six
representations of the same genotypes, and a benchmark run at default arguments
would report a figure no comparison corresponds to. The Python default is
therefore `("GT",)`, and the divergence is documented on the function and the
option class.

## Three range knobs are exposed; six size caps are not

`max_range_gap` defaults to 0, so no unselected gap is ever bridged when
coalescing PGEN byte ranges and a subset scan issues one read per contiguous
run. That, `max_range_bytes`, and `batch_soft_byte_limit` are the options that
move throughput on object storage, so they cross into Python.

The six remaining caps — `max_companion_bytes`,
`max_decompressed_companion_bytes`, `max_header_bytes`, `max_record_bytes`,
`max_variants`, and `max_samples` — are guardrails against malformed input
rather than tuning, and stay at their provider defaults.

An unset knob passes the provider default through, never a zero. A zero
`max_range_bytes` would disable coalescing entirely rather than mean "default".

## Enum options cross as lowercase strings

`missing_sample_policy` and `psam_id_mode` are strings rather than Python
enums, matching how BGEN passes `genotype_output` and `probability_layout`.
Conversion happens in `src/scan.rs` and an unrecognised value returns a
`DataFusionError::Plan`, so a typo surfaces as an exception rather than a
panic inside the extension.
