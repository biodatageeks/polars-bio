The `config_meta` compatibility API and `polars_plugin_meta` JSON/Parquet
representation in `polars_bio/_frame_metadata.py` are adapted from
[polars-config-meta](https://github.com/lmmx/polars-config-meta), version 0.3.4
(upstream commit `5739e3818d8e8625883419c0f5091a6a8b526f86`).

The upstream MIT notice is retained in LICENSE. polars-bio owns its instance
storage, bounded propagation, grouping, conflict handling, and regression tests;
it does not vendor or import the upstream package.
