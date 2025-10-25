#!/usr/bin/env python3
"""
Generate metadata.json for a benchmark run.
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


def generate_metadata(args):
    """Generate metadata dictionary from arguments."""
    metadata = {
        "version": "1.0",
        "ref": args.ref,
        "ref_type": args.ref_type,
        "commit_sha": args.commit_sha,
        "timestamp": args.timestamp or datetime.utcnow().isoformat() + "Z",
        "runner": {"os": args.runner_os, "arch": args.runner_arch},
        "benchmark_suite": args.benchmark_suite,
        "benchmark_config": args.benchmark_config,
    }
    return metadata


def save_metadata(metadata, output_path):
    """Save metadata to JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(metadata, f, indent=2)


def main():
    parser = argparse.ArgumentParser(
        description="Generate benchmark metadata.json file"
    )
    parser.add_argument(
        "--ref", required=True, help="Git reference (tag or branch name)"
    )
    parser.add_argument(
        "--ref-type", required=True, choices=["tag", "branch"], help="Reference type"
    )
    parser.add_argument("--commit-sha", required=True, help="Full commit SHA")
    parser.add_argument("--runner-os", required=True, help="Runner OS (linux, macos)")
    parser.add_argument(
        "--runner-arch", required=True, help="Runner architecture (amd64, arm64)"
    )
    parser.add_argument(
        "--benchmark-suite", required=True, help="Benchmark suite (fast, full)"
    )
    parser.add_argument(
        "--benchmark-config", required=True, help="Benchmark config file path"
    )
    parser.add_argument("--timestamp", help="Timestamp (ISO 8601 UTC), defaults to now")
    parser.add_argument(
        "--output", type=Path, required=True, help="Output metadata.json path"
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()

    try:
        metadata = generate_metadata(args)
        save_metadata(metadata, args.output)

        if args.verbose:
            print(f"✅ Generated metadata: {args.output}")
            print(json.dumps(metadata, indent=2))
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
