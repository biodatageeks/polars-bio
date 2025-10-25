#!/usr/bin/env python3
"""
Update master index.json when new benchmark dataset is added.
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


def load_or_create_index(index_path):
    """Load existing index or create new one."""
    if index_path.exists():
        with open(index_path) as f:
            return json.load(f)
    else:
        return {
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "datasets": [],
            "tags": [],
            "latest_tag": None,
        }


def add_dataset(index, dataset_info, max_commits_per_branch=10):
    """Add or update dataset entry in index.

    Args:
        index: The index dictionary
        dataset_info: New dataset to add
        max_commits_per_branch: Maximum number of commits to keep per branch (default: 10)
    """
    # For tags: remove existing entry with same ID (only keep one per tag)
    # For branches: keep all entries (will be limited later)
    if dataset_info["ref_type"] == "tag":
        index["datasets"] = [
            d for d in index["datasets"] if d["id"] != dataset_info["id"]
        ]
    else:
        # For branches, remove only if same commit SHA (avoid duplicates)
        index["datasets"] = [
            d
            for d in index["datasets"]
            if not (
                d["ref_type"] == "branch"
                and d["ref"] == dataset_info["ref"]
                and d["runner"] == dataset_info["runner"]
                and d.get("commit_sha") == dataset_info.get("commit_sha")
            )
        ]

    # Add new entry
    index["datasets"].append(dataset_info)

    # Sort datasets: tags first (by timestamp desc), then branches (by timestamp desc)
    index["datasets"].sort(
        key=lambda d: (
            d["ref_type"] != "tag",  # tags first
            -datetime.fromisoformat(
                d["timestamp"].replace("Z", "+00:00")
            ).timestamp(),  # newest first
        )
    )

    # Limit commits per branch to max_commits_per_branch
    if max_commits_per_branch > 0:
        # Group datasets by (ref_type, ref, runner)
        from collections import defaultdict

        groups = defaultdict(list)

        for dataset in index["datasets"]:
            if dataset["ref_type"] == "branch":
                key = (dataset["ref"], dataset["runner"])
                groups[key].append(dataset)

        # Keep only the N most recent commits per branch+runner
        datasets_to_keep = []
        datasets_to_remove = set()

        for key, datasets in groups.items():
            # Sort by timestamp descending
            sorted_datasets = sorted(
                datasets,
                key=lambda d: -datetime.fromisoformat(
                    d["timestamp"].replace("Z", "+00:00")
                ).timestamp(),
            )
            # Keep first N
            for i, dataset in enumerate(sorted_datasets):
                if i < max_commits_per_branch:
                    datasets_to_keep.append(dataset["id"])
                else:
                    datasets_to_remove.add(dataset["id"])

        # Remove old commits
        index["datasets"] = [
            d
            for d in index["datasets"]
            if d["ref_type"] == "tag" or d["id"] in datasets_to_keep
        ]


def update_tags_list(index, ref, ref_type):
    """Update tags list if this is a tag."""
    if ref_type == "tag" and ref not in index["tags"]:
        index["tags"].append(ref)
        # Sort tags (assuming semantic versioning, latest first)
        index["tags"].sort(reverse=True)


def mark_latest_tag(index, ref):
    """Mark a tag as the latest."""
    # Unmark all other tags
    for dataset in index["datasets"]:
        if "is_latest_tag" in dataset:
            dataset["is_latest_tag"] = False

    # Mark this tag's datasets as latest
    for dataset in index["datasets"]:
        if dataset["ref"] == ref and dataset["ref_type"] == "tag":
            dataset["is_latest_tag"] = True

    # Update latest_tag field
    index["latest_tag"] = ref


def save_index(index, index_path):
    """Save index to JSON file."""
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index["last_updated"] = datetime.utcnow().isoformat() + "Z"

    with open(index_path, "w") as f:
        json.dump(index, f, indent=2)


def main():
    parser = argparse.ArgumentParser(
        description="Update benchmark index.json with new dataset"
    )
    parser.add_argument("--index", type=Path, required=True, help="Path to index.json")
    parser.add_argument("--dataset-id", required=True, help="Unique dataset ID")
    parser.add_argument(
        "--label", required=True, help="Display label (without architecture)"
    )
    parser.add_argument("--ref", required=True, help="Git reference")
    parser.add_argument(
        "--ref-type", required=True, choices=["tag", "branch"], help="Reference type"
    )
    parser.add_argument("--runner", required=True, help="Runner ID (linux, macos)")
    parser.add_argument("--runner-label", required=True, help="Runner display label")
    parser.add_argument(
        "--path", required=True, help="Relative path to dataset directory"
    )
    parser.add_argument("--timestamp", required=True, help="Timestamp (ISO 8601 UTC)")
    parser.add_argument("--commit-sha", help="Full commit SHA (optional)")
    parser.add_argument(
        "--is-latest-tag", action="store_true", help="Mark as latest tag"
    )
    parser.add_argument(
        "--max-commits",
        type=int,
        default=10,
        help="Maximum number of commits to keep per branch (default: 10)",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()

    try:
        # Load or create index
        index = load_or_create_index(args.index)

        # Create dataset entry
        dataset = {
            "id": args.dataset_id,
            "label": args.label,
            "ref": args.ref,
            "ref_type": args.ref_type,
            "timestamp": args.timestamp,
            "runner": args.runner,
            "runner_label": args.runner_label,
            "path": args.path,
        }

        if args.commit_sha:
            dataset["commit_sha"] = args.commit_sha

        if args.is_latest_tag:
            dataset["is_latest_tag"] = True

        # Add dataset
        add_dataset(index, dataset, max_commits_per_branch=args.max_commits)

        # Update tags list
        update_tags_list(index, args.ref, args.ref_type)

        # Mark latest tag if specified
        if args.is_latest_tag:
            mark_latest_tag(index, args.ref)

        # Save index
        save_index(index, args.index)

        if args.verbose:
            print(f"✅ Updated index: {args.index}")
            print(f"   Dataset ID: {args.dataset_id}")
            print(f"   Total datasets: {len(index['datasets'])}")

    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
