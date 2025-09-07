#!/usr/bin/env python3
"""
Shared GFF parsing helpers for benchmarks.

Provides consistent pandas and polars readers that parse the attributes
column into a structured representation for fair comparisons.
"""

from pathlib import Path
from typing import List, NamedTuple, Optional, Union

import pandas as pd
import polars as pl


class AttributeStruct(NamedTuple):
    """Struct-like representation of GFF attribute key-value pair"""

    key: Optional[str]
    value: Optional[str]


def pandas_read_gff(path: Union[str, Path]) -> pd.DataFrame:
    """Read GFF file using pandas with attributes parsing into list of structs"""
    cols = [
        "seqid",
        "source",
        "type",
        "start",
        "end",
        "score",
        "strand",
        "phase",
        "attributes",
    ]
    dtypes = {
        "seqid": "string",
        "source": "string",
        "type": "string",
        "start": "UInt32",
        "end": "UInt32",
        "score": "Float32",
        "strand": "string",
        "phase": "UInt32",
        "attributes": "string",
    }

    df = pd.read_csv(
        path,
        sep="\t",
        names=cols,
        header=None,
        comment="#",
        na_values=".",
        dtype=dtypes,
        engine="c",
        compression="gzip",
    )

    # Parse attributes into list of structs (matching Polars format)
    def _parse_attrs(attr: Optional[str]) -> List[AttributeStruct]:
        if pd.isna(attr) or attr == "":
            return []
        out: List[AttributeStruct] = []
        for part in str(attr).split(";"):
            if not part:
                continue
            if "=" in part:
                k, v = part.split("=", 1)
            else:
                k, v = part, None
            out.append(AttributeStruct(key=k, value=v))
        return out

    df["attributes"] = df["attributes"].map(_parse_attrs)
    return df


def polars_scan_gff(path: Union[str, Path]) -> pl.LazyFrame:
    """Read GFF file using vanilla Polars with attributes parsing"""
    schema = pl.Schema(
        [
            ("seqid", pl.String),
            ("source", pl.String),
            ("type", pl.String),
            ("start", pl.UInt32),
            ("end", pl.UInt32),
            ("score", pl.Float32),
            ("strand", pl.String),
            ("phase", pl.UInt32),
            ("attributes", pl.String),
        ]
    )

    reader = pl.scan_csv(
        path,
        has_header=False,
        separator="\t",
        comment_prefix="#",
        schema=schema,
        null_values=["."],
    )

    # Parse attributes into List[Struct{key, value}]
    reader = reader.with_columns(
        pl.col("attributes")
        .str.split(";")
        .list.eval(
            pl.element()
            .str.split("=")
            .list.to_struct(n_field_strategy="max_width", fields=["key", "value"])
        )
        .alias("attributes")
    )

    return reader


__all__ = ["AttributeStruct", "pandas_read_gff", "polars_scan_gff"]
