"""Small process-isolated wrapper benchmark; not a genomic I/O benchmark.

Run once per mode (native, legacy, owned) in the same dependency environment.
Tagged native measurements provide a no-propagation reference only.
"""

import argparse
import importlib.util
import json
import platform
import statistics
import timeit
from pathlib import Path

import polars as pl


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("native", "legacy", "owned"))
    args = parser.parse_args()
    if args.mode == "legacy":
        import polars_config_meta  # noqa: F401
    elif args.mode == "owned":
        path = Path(__file__).resolve().parents[4] / "polars_bio" / "_frame_metadata.py"
        spec = importlib.util.spec_from_file_location("benchmark_frame_metadata", path)
        provider = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(provider)
        provider.initialize()

    # The legacy provider installs its wrappers on first namespace access, not
    # merely import. Compare active providers even for the untagged workload.
    if args.mode != "native":
        activation_frame = pl.DataFrame()
        activation_frame.config_meta.set(activate=True)

    metadata = {
        "coordinate_system_zero_based": True,
        "source_format": "vcf",
        "source_path": "example.vcf",
        "source_header": json.dumps({"samples": ["S1"], "header": "x" * 10_000}),
    }
    timings = {}
    for tagged in (False, True):
        frame = pl.DataFrame({"start": [1, 3, 5], "end": [2, 4, 6]})
        lazy = frame.lazy()
        if tagged:
            for obj in (frame, lazy):
                if args.mode == "native":
                    obj._benchmark_metadata = metadata.copy()
                else:
                    obj.config_meta.set(**metadata)

        def chain():
            return (
                lazy.filter(pl.col("start") > 1)
                .with_columns(length=pl.col("end") - pl.col("start"))
                .select("start", "end", "length")
            )

        for name, operation in (("lazy_chain", chain), ("eager_clone", frame.clone)):
            for _ in range(100):
                operation()
            samples = timeit.repeat(operation, number=2000, repeat=7)
            timings[f"{name}_{'tagged' if tagged else 'untagged'}"] = {
                "median_us": statistics.median(samples) * 1_000_000 / 2000,
                "samples_us": [sample * 1_000_000 / 2000 for sample in samples],
            }
    print(
        json.dumps(
            {
                "mode": args.mode,
                "python": platform.python_version(),
                "polars": pl.__version__,
                "iterations": 2000,
                "repetitions": 7,
                "provider_activated_before_measurement": args.mode != "native",
                "timings": timings,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
