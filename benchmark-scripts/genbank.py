"""Run one GenBank workload in a fresh process and emit JSON for comparison.

Example: python benchmark-scripts/genbank.py polars-bio faa assembly.gbff
For a plain-file, process-parallel Bio.SeqIO comparison, pass --threads 8 with
the narrow or wide workload. Serial Biopython also accepts .gz and .bgz files.
"""

import argparse
import gzip
import hashlib
import json
import mmap
import multiprocessing
import resource
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor
from importlib.metadata import version
from itertools import islice
from pathlib import Path

import polars as pl

NARROW = ["record_id", "type", "locus_tag"]
WIDE = [*NARROW, "gene", "product", "protein_id", "translation"]


def polars_rows(path, workload, threads):
    import polars_bio as pb

    pb.set_progress_bar(False)
    pb.set_option("datafusion.execution.target_partitions", str(threads))
    if workload == "full":
        return pb.read_genbank(path, "features")
    fields = WIDE if workload == "wide" else NARROW
    if workload == "faa":
        frame = (
            pb.scan_genbank(
                path, "features", qualifier_fields=["locus_tag", "translation"]
            )
            .filter((pl.col("type") == "CDS") & pl.col("translation").is_not_null())
            .select("locus_tag", "translation")
            .collect()
        )
        return frame.iter_rows()
    return (
        pb.scan_genbank(path, "features", qualifier_fields=fields[2:])
        .select(fields)
        .collect()
    )


def reference_rows(tool, path, workload):
    if tool == "biopython":
        from Bio import SeqIO

        opener = gzip.open if path.suffix in {".gz", ".bgz", ".bgzf"} else open
        with opener(path, "rt") as stream:
            yield from feature_rows(tool, SeqIO.parse(stream, "genbank"), workload)
    else:
        import gb_io

        yield from feature_rows(tool, gb_io.iter(str(path)), workload)


def feature_rows(tool, records, workload):
    columns = WIDE if workload == "wide" else NARROW
    keys = ["locus_tag", "translation"] if workload == "faa" else columns[2:]
    wanted = set(keys)
    for record in records:
        for feature in record.features:
            kind = feature.type if tool == "biopython" else feature.kind
            if workload == "faa" and kind != "CDS":
                continue
            if tool == "biopython":
                values = {
                    key: vs[0] for key in keys if (vs := feature.qualifiers.get(key))
                }
            else:
                values = {}
                for q in feature.qualifiers:
                    key = q.key
                    if key not in wanted or key in values:
                        continue
                    value = q.value
                    if value is not None and "\n" in value:
                        value = value.replace("\n", "" if key == "translation" else " ")
                    values[key] = value
                    if len(values) == len(keys):
                        break
            if workload == "faa":
                if values.get("translation"):
                    yield values.get("locus_tag"), values["translation"]
            else:
                yield record.name, kind, *(values.get(f) for f in keys)


def biopython_shard(args):
    from Bio import SeqIO

    path, workload, offset, count = args
    fields = WIDE if workload == "wide" else NARROW
    with path.open("rt") as source:
        source.seek(offset)
        records = islice(SeqIO.parse(source, "genbank"), count)
        return pl.DataFrame(
            feature_rows("biopython", records, workload),
            schema={field: pl.String for field in fields},
            orient="row",
        )


def biopython_parallel(path, workload, workers):
    """Split complete LOCUS records across processes and collect one frame."""
    from bisect import bisect_left

    size = path.stat().st_size
    if not size:
        fields = WIDE if workload == "wide" else NARROW
        return pl.DataFrame(schema={field: pl.String for field in fields})
    with (
        path.open("rb") as source,
        mmap.mmap(source.fileno(), 0, access=mmap.ACCESS_READ) as data,
    ):
        starts = [0] if data[:6] == b"LOCUS " else []
        pos = 0
        while (pos := data.find(b"\nLOCUS ", pos)) != -1:
            pos += 1
            starts.append(pos)
    if not starts:
        raise ValueError("no GenBank LOCUS records found")
    cuts = sorted(
        {
            0,
            len(starts),
            *(bisect_left(starts, size * i // workers) for i in range(1, workers)),
        }
    )
    shards = [(path, workload, starts[i], j - i) for i, j in zip(cuts, cuts[1:])]
    with ProcessPoolExecutor(
        max_workers=len(shards), mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        return pl.concat(pool.map(biopython_shard, shards))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "tool", choices=["polars-bio", "biopython", "gb-io", "microbiorust"]
    )
    parser.add_argument("workload", choices=["faa", "narrow", "wide", "full"])
    parser.add_argument("path", type=Path)
    parser.add_argument("--threads", type=int, default=1)
    args = parser.parse_args()
    if args.threads < 1:
        parser.error("--threads must be positive")
    parallel_bio = args.tool == "biopython" and args.threads > 1
    if parallel_bio and (
        args.workload not in {"wide", "narrow"}
        or args.path.suffix in {".gz", ".bgz", ".bgzf"}
    ):
        parser.error(
            "parallel Biopython requires plain input and the narrow or wide workload"
        )
    if args.workload == "full" and args.tool != "polars-bio":
        parser.error("full measures the native polars-bio feature schema")
    if args.tool == "microbiorust" and args.workload != "faa":
        print(
            json.dumps(
                {
                    "status": "unsupported",
                    "reason": "locus-tag keyed features lack the full feature table and type column",
                }
            )
        )
        return
    # Exclude module startup, include parser construction, partition discovery,
    # Arrow/DataFrame construction or writing the FASTA file.
    import importlib

    importlib.import_module(
        {
            "polars-bio": "polars_bio",
            "biopython": "Bio.SeqIO",
            "gb-io": "gb_io",
            "microbiorust": "microbiorust",
        }[args.tool]
    )
    size = args.path.stat().st_size
    with tempfile.TemporaryFile("w+b") as out:
        start = time.perf_counter()
        if args.tool == "polars-bio":
            rows = polars_rows(args.path, args.workload, args.threads)
        elif parallel_bio:
            rows = biopython_parallel(args.path, args.workload, args.threads)
        elif args.tool == "microbiorust":
            import microbiorust

            collection = microbiorust.parse_gbk(str(args.path))
            rows = (
                (v.locus_tag, v.faa)
                for r in collection.values()
                for v in r.faa().values()
                if v.faa
            )
        else:
            rows = reference_rows(args.tool, args.path, args.workload)
            if args.workload != "faa":
                fields = WIDE if args.workload == "wide" else NARROW
                rows = pl.DataFrame(
                    rows, schema={name: pl.String for name in fields}, orient="row"
                )

        if args.workload == "faa":
            count = 0
            for tag, translation in rows:
                out.write(f">{tag or '.'}\n{translation}\n".encode())
                count += 1
        else:
            count = rows.height
        elapsed = time.perf_counter() - start
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if args.workload == "faa":
            out.seek(0)
            items = (head + seq for head, seq in zip(out, out))
        else:
            if args.workload == "full":

                def comparable(row):
                    qualifiers = row["qualifiers"]
                    if isinstance(qualifiers, list):
                        qualifiers = {q["key"]: q["value"] for q in qualifiers}
                    return (
                        row["record_id"],
                        row["type"],
                        *(qualifiers.get(k, [None])[0] for k in WIDE[2:]),
                    )

                canonical = (comparable(row) for row in rows.iter_rows(named=True))
            else:
                canonical = rows.iter_rows()
            items = (json.dumps(row, ensure_ascii=False).encode() for row in canonical)
        # Physical partitions can complete in different orders. Include repeated
        # rows in the multiset digest, so a dropped duplicate changes the checksum.
        checksum = hashlib.sha256(
            b"".join(sorted(hashlib.sha256(row).digest() for row in items))
        ).hexdigest()
    print(
        json.dumps(
            {
                "tool": args.tool,
                "version": version(args.tool),
                "input": args.path.name,
                "workload": args.workload,
                "threads": args.threads,
                "rows": count,
                "sha256": checksum,
                "seconds": round(elapsed, 4),
                "MiB_s": round(size / 2**20 / elapsed, 2),
                "peak_MiB": (
                    None
                    if parallel_bio
                    else round(rss / (2**20 if sys.platform == "darwin" else 1024), 1)
                ),
            }
        )
    )


if __name__ == "__main__":
    main()
