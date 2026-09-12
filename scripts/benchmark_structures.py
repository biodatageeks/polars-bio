"""Reproducible Python-visible structure scan smoke benchmark (no speedup claims)."""

import argparse
import contextlib
import importlib.metadata
import io
import json
import platform
import resource
import subprocess
import sys
import time
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--build", required=True, choices=["development", "release"])
parser.add_argument("--case", choices=["pdb", "mmcif", "foldcomp"])
parser.add_argument("--workers", type=int, default=1)
parser.add_argument("--level", choices=["atom", "residue"], default="atom")
args = parser.parse_args()
if args.case:
    import polars_bio as pb

    data = Path(__file__).resolve().parents[1] / "tests/data/structure"
    pb.set_option("datafusion.execution.target_partitions", str(args.workers))
    timings = []
    rows = None
    with contextlib.redirect_stderr(io.StringIO()):
        for _ in range(3):
            start = time.perf_counter()
            if args.case == "foldcomp":
                frame = pb.scan_foldcomp(
                    data / "example_db", entry_keys=[0, 7], level=args.level
                )
            else:
                path = data / ("1ubq.pdb" if args.case == "pdb" else "1ubq.cif")
                frame = getattr(pb, "scan_" + args.case)([path] * 32, level=args.level)
            result = frame.collect()
            timings.append(time.perf_counter() - start)
            assert rows is None or rows == result.height
            rows = result.height
            del result, frame
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(
        json.dumps(
            {
                "format": args.case,
                "level": args.level,
                "workers": args.workers,
                "rows": rows,
                "seconds": timings,
                "peak_process_rss_bytes": (
                    rss if sys.platform == "darwin" else rss * 1024
                ),
            }
        )
    )
else:
    results = []
    for case in ["pdb", "mmcif", "foldcomp"]:
        for level in ["atom", "residue"]:
            for workers in [1, 2, 4, 8]:
                output = subprocess.check_output(
                    [
                        sys.executable,
                        __file__,
                        "--build",
                        args.build,
                        "--case",
                        case,
                        "--level",
                        level,
                        "--workers",
                        str(workers),
                    ],
                    text=True,
                )
                results.append(json.loads(output))
    print(
        json.dumps(
            {
                "build": args.build,
                "platform": platform.platform(),
                "python": platform.python_version(),
                "versions": {
                    name: importlib.metadata.version(name)
                    for name in ["polars", "pyarrow", "datafusion"]
                },
                "workload": "32 repeated 1UBQ paths or database keys [0,7]; full schema; all rows collected",
                "cache": "first execution and two repeats; filesystem caches are not flushed",
                "rss": "fresh process per case, includes Python/Arrow/DataFusion imports and allocations",
                "results": results,
            },
            indent=2,
        )
    )
