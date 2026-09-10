"""Copy the hash-checked native fixture/oracle corpus into Python tests."""

import argparse
import hashlib
import json
import shutil
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("formats_repo", type=Path)
args = parser.parse_args()
source = args.formats_repo
oracles = source / "testing/oracles/structure"
manifest = json.loads((oracles / "manifest.json").read_text())
destination = Path(__file__).resolve().parents[1] / "tests/data/structure"
destination.mkdir(parents=True, exist_ok=True)
for group, directory in [
    ("inputs", source / "testing/data/structure"),
    ("outputs", oracles),
]:
    for name, expected in manifest[group].items():
        path = directory / name
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected, name
        shutil.copy2(path, destination / name)
shutil.copy2(oracles / "manifest.json", destination / "manifest.json")
print(f"Exported schema version {manifest['schema_version']} to {destination}")
