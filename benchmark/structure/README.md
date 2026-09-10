# Structure scan smoke benchmark

Run `python scripts/benchmark_structures.py --build development` (or `--build
release` after building the release extension) from the repository environment.
Each format/level/worker case runs in a fresh subprocess, collects all columns and
rows, then repeats twice. PDB/mmCIF use 32 copies of 1UBQ; Foldcomp uses database
keys 0 and 7. The fixture manifest freezes input hashes. This script uses POSIX
resource reporting, so run it on Linux or macOS.

The checked-in run uses the development extension on macOS arm64, 2026-09-09.
It verifies the full Python API across 1/2/4/8 worker settings. RSS includes Python,
Arrow/DataFusion imports, caches and allocations in each fresh case process.
Filesystem caches were not flushed. No release-speedup claim follows from these
small development-build measurements. The native companion benchmark separates
parse, Arrow construction and first-batch/full-scan latency.
