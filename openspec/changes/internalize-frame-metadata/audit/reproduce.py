"""Isolated observations of polars-config-meta; not tests of a proposed port.

Run with the project Python. Each probe gets a fresh subprocess because importing
the dependency and accessing its namespace modifies global Polars classes.
Set PYTHONPATH to a checkout's src directory to inspect another upstream revision.
No network access, repository edits, or polars-bio extension import is needed.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import importlib.metadata
import importlib.util
import inspect
import json
import subprocess
import sys
import warnings
import weakref
from pathlib import Path

import polars as pl


def frame():
    return pl.DataFrame({"chrom": ["chr1", "chr1"], "start": [1, 2], "end": [2, 3]})


def metadata(obj):
    return dict(obj.config_meta.get_metadata())


def seed_stale_entry(plugin, obj):
    """Model failed cleanup deterministically, without waiting for id reuse."""

    class Dead:
        pass

    dead = Dead()
    reference = weakref.ref(dead)
    del dead
    assert reference() is None
    plugin.ConfigMetaPlugin._df_id_to_ref[id(obj)] = reference
    plugin.ConfigMetaPlugin._df_id_to_meta[id(obj)] = {
        "coordinate_system_zero_based": False,
        "source_path": "dead-frame.vcf",
    }


def probe(name):
    if name == "instance_storage":
        observations = {}
        for cls in (pl.DataFrame, pl.LazyFrame):
            obj = cls({"x": [1]})
            native = obj._df if isinstance(obj, pl.DataFrame) else obj._ldf
            obj._polars_bio_metadata = {"coordinate_system_zero_based": True}
            reference = weakref.ref(obj)
            observations[cls.__name__] = {
                "private_dictionary": obj.__dict__["_polars_bio_metadata"],
                "native_unchanged": native
                is (obj._df if isinstance(obj, pl.DataFrame) else obj._ldf),
            }
            del obj
            observations[cls.__name__]["released"] = reference() is None
        return observations

    before_signature = str(inspect.signature(pl.DataFrame.filter))
    import polars_config_meta as plugin

    if name == "signature":
        frame().config_meta.set(owner="trigger")
        return {
            "before": before_signature,
            "after": str(inspect.signature(pl.DataFrame.filter)),
            "patched_methods": len(plugin.discovery._ORIGINAL_METHODS),
            "has_wrapped_attribute": hasattr(pl.DataFrame.filter, "__wrapped__"),
        }

    if name in ("stale_read", "stale_copy"):
        trigger = frame()
        trigger.config_meta.set(owner="enable-patching")
        fresh = frame()
        seed_stale_entry(plugin, fresh)
        try:
            if name == "stale_read":
                return {"fresh_metadata": metadata(fresh)}
            # Crucially, do not access fresh.config_meta before transforming it.
            clone = fresh.clone()
            return {
                "clone_metadata": metadata(clone),
                "source_after_read": metadata(fresh),
            }
        finally:
            plugin.ConfigMetaPlugin._df_id_to_ref.pop(id(fresh), None)
            plugin.ConfigMetaPlugin._df_id_to_meta.pop(id(fresh), None)

    obj = frame()
    obj.config_meta.set(
        coordinate_system_zero_based=True,
        source_format="bed",
        source_path="source.bed",
        source_header='{"tag_types":{"X":"A"}}',
    )

    if name == "propagation":
        operations = {
            "filter": lambda: obj.filter(pl.col("start") > 1),
            "select": lambda: obj.select("chrom", "start"),
            "with_columns": lambda: obj.with_columns(extra=pl.lit(1)),
            "head": lambda: obj.head(1),
            "clone": obj.clone,
            "lazy_collect": lambda: obj.lazy().collect(),
            "getitem_frame": lambda: obj[["chrom", "start"]],
            "group_by_agg": lambda: obj.group_by("chrom").agg(pl.col("start").min()),
            "concat": lambda: pl.concat([obj, obj]),
            "partition_by": lambda: obj.partition_by("chrom")[0],
            "iter_slices": lambda: next(obj.iter_slices(1)),
            "collect_batches": lambda: next(obj.lazy().collect_batches()),
        }
        return {key: metadata(operation()) for key, operation in operations.items()}

    if name == "multi_input":
        right = frame()
        right.config_meta.set(
            coordinate_system_zero_based=False, source_path="other.vcf"
        )
        joined = obj.join(right, on="chrom")
        stacked = obj.vstack(right)
        return {
            "left": metadata(obj),
            "right": metadata(right),
            "join": metadata(joined),
            "vstack": metadata(stacked),
        }

    if name == "mutable_values":
        obj.config_meta.set(extra={"samples": ["S1"]})
        child = obj.filter(pl.col("start") > 0)
        child.config_meta.get_metadata()["extra"]["samples"].append("S2")
        snapshot = obj.config_meta.get_metadata()
        snapshot["coordinate_system_zero_based"] = False
        return {"parent": metadata(obj), "child": metadata(child)}

    if name == "set_return":
        return {"set_returns": obj.config_meta.set(coordinate_system_zero_based=False)}

    if name == "duplicate_namespace":
        source = Path(plugin.__file__)
        spec = importlib.util.spec_from_file_location(
            "audit_second_metadata_plugin",
            source,
            submodule_search_locations=[str(source.parent)],
        )
        second = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = second
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            spec.loader.exec_module(second)
        fresh = frame()
        fresh.config_meta.set(coordinate_system_zero_based=False)
        return {
            "warnings": [str(w.message) for w in caught],
            "old_frame": metadata(obj),
            "old_frame_clone": metadata(obj.clone()),
            "new_frame": metadata(fresh),
            "new_frame_clone": metadata(fresh.clone()),
        }

    if name == "stream_interface":
        batches = obj.lazy().collect_batches(lazy=True, engine="streaming")
        return {
            "iterator_type": type(batches).__name__,
            "has_inner": hasattr(batches, "_inner"),
            "inner_exports_arrow_stream": hasattr(batches._inner, "__arrow_c_stream__"),
        }

    raise ValueError(name)


PROBES = (
    "stale_read",
    "stale_copy",
    "propagation",
    "multi_input",
    "signature",
    "mutable_values",
    "set_return",
    "duplicate_namespace",
    "instance_storage",
    "stream_interface",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", choices=PROBES)
    args = parser.parse_args()
    if args.probe:
        print(json.dumps(probe(args.probe), sort_keys=True))
        return

    plugin_source = Path(importlib.util.find_spec("polars_config_meta").origin)
    report = {
        "python": sys.version.split()[0],
        "polars": pl.__version__,
        "plugin_distribution_version": importlib.metadata.version("polars-config-meta"),
        "plugin_source_sha256": hashlib.sha256(plugin_source.read_bytes()).hexdigest(),
        "note": "Distribution version may differ from source selected using PYTHONPATH.",
        "observations": {},
    }
    for name in PROBES:
        child = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--probe", name],
            capture_output=True,
            text=True,
            check=True,
            timeout=60,
        )
        report["observations"][name] = json.loads(child.stdout)
    gc.collect()
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
