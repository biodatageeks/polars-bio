"""Fail the build when a distribution file is close to PyPI's per-file upload limit.

PyPI (Warehouse) rejects files larger than the project's per-file limit, which is
100 MiB unless a higher limit has been granted for the project. The budget is that
limit minus a safety margin, so a release is caught in CI instead of failing at
upload time.

Usage: python check_wheel_size.py DIST_DIR
Environment:
  PYPI_FILE_LIMIT_MIB  per-file limit for the project (default 100)
  SIZE_MARGIN_MIB      safety margin below the limit (default 5)
"""

import os
import sys
from pathlib import Path

MIB = 1024 * 1024


def main() -> int:
    dist = Path(sys.argv[1] if len(sys.argv) > 1 else "dist")
    limit = float(os.environ.get("PYPI_FILE_LIMIT_MIB", "100"))
    margin = float(os.environ.get("SIZE_MARGIN_MIB", "5"))
    budget = limit - margin

    files = sorted(p for p in dist.iterdir() if p.suffix in (".whl", ".gz"))
    if not files:
        print(f"no wheels or sdists found in {dist}", file=sys.stderr)
        return 1

    rows = []
    over = False
    for path in files:
        size = path.stat().st_size / MIB
        ok = size <= budget
        over |= not ok
        rows.append(
            f"| `{path.name}` | {size:.1f} MiB | {budget - size:+.1f} MiB | "
            f"{'ok' if ok else 'OVER BUDGET'} |"
        )

    report = "\n".join(
        [
            f"### Distribution size (budget {budget:.0f} MiB = "
            f"{limit:.0f} MiB PyPI limit - {margin:.0f} MiB margin)",
            "",
            "| file | size | headroom | status |",
            "|---|---|---|---|",
            *rows,
            "",
        ]
    )
    print(report)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(report + "\n")

    if over:
        print(
            "A distribution exceeds the size budget. Options: strip symbols "
            '([profile.release] strip = "symbols"), reduce dependencies, or '
            "request a higher per-file limit from PyPI and raise "
            "PYPI_FILE_LIMIT_MIB.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
