import json
import logging
import weakref as _weakref
from contextlib import contextmanager
from typing import Any, Dict, Iterator, NamedTuple, Optional, Sequence, Union
from uuid import uuid4

import polars as pl

logger = logging.getLogger(__name__)
from datafusion import DataFrame
from polars.io.plugins import register_io_source
from tqdm.auto import tqdm

from polars_bio.polars_bio import (
    BamReadOptions,
    BamWriteOptions,
    BedReadOptions,
    BgenReadOptions,
    BigBedReadOptions,
    BigWigReadOptions,
    CoolReadOptions,
    CramReadOptions,
    CramWriteOptions,
    FastaReadOptions,
    FastaWriteOptions,
    FastqReadOptions,
    FastqWriteOptions,
    GffReadOptions,
    GtfReadOptions,
    InputFormat,
    OutputFormat,
    PairsReadOptions,
    PgenReadOptions,
    PyObjectStorageOptions,
    ReadOptions,
    VcfReadOptions,
    VcfWriteOptions,
    VcfZarrReadOptions,
    WriteOptions,
    py_describe_bam,
    py_describe_cool,
    py_describe_cram,
    py_describe_vcf,
    py_describe_vcf_zarr,
    py_from_polars,
    py_get_table_schema,
    py_read_sql,
    py_read_table,
    py_register_table,
    py_write_table,
)

from ._metadata import get_vcf_metadata, set_coordinate_system, set_vcf_metadata
from ._path_utils import strip_url_parameters
from .context import _resolve_zero_based, ctx
from .predicate_translator import (
    BAM_INT32_COLUMNS,
    BAM_STRING_COLUMNS,
    BAM_UINT32_COLUMNS,
    BIGBED_FLOAT32_COLUMNS,
    BIGBED_STRING_COLUMNS,
    BIGBED_UINT32_COLUMNS,
    BIGWIG_FLOAT32_COLUMNS,
    BIGWIG_STRING_COLUMNS,
    BIGWIG_UINT32_COLUMNS,
    COOL_FLOAT32_COLUMNS,
    COOL_STRING_COLUMNS,
    COOL_UINT64_COLUMNS,
    GFF_FLOAT32_COLUMNS,
    GFF_STRING_COLUMNS,
    GFF_UINT32_COLUMNS,
    PAIRS_FLOAT32_COLUMNS,
    PAIRS_STRING_COLUMNS,
    PAIRS_UINT32_COLUMNS,
    VCF_STRING_COLUMNS,
    VCF_UINT32_COLUMNS,
)

# Mapping from format name to (string_cols, uint32_cols, float32_cols) for predicate validation.
# Uses string keys because PyO3 InputFormat is not hashable.
_FORMAT_COLUMN_TYPES = {
    "Bam": (BAM_STRING_COLUMNS, BAM_UINT32_COLUMNS | BAM_INT32_COLUMNS, None),
    "Sam": (BAM_STRING_COLUMNS, BAM_UINT32_COLUMNS | BAM_INT32_COLUMNS, None),
    "Cram": (BAM_STRING_COLUMNS, BAM_UINT32_COLUMNS | BAM_INT32_COLUMNS, None),
    "Vcf": (VCF_STRING_COLUMNS, VCF_UINT32_COLUMNS, None),
    "VcfZarr": (VCF_STRING_COLUMNS, VCF_UINT32_COLUMNS, None),
    "Gff": (GFF_STRING_COLUMNS, GFF_UINT32_COLUMNS, GFF_FLOAT32_COLUMNS),
    "Gtf": (GFF_STRING_COLUMNS, GFF_UINT32_COLUMNS, GFF_FLOAT32_COLUMNS),
    "Pairs": (PAIRS_STRING_COLUMNS, PAIRS_UINT32_COLUMNS, PAIRS_FLOAT32_COLUMNS),
    "BigWig": (BIGWIG_STRING_COLUMNS, BIGWIG_UINT32_COLUMNS, BIGWIG_FLOAT32_COLUMNS),
    "BigBed": (BIGBED_STRING_COLUMNS, BIGBED_UINT32_COLUMNS, BIGBED_FLOAT32_COLUMNS),
    "Cool": (COOL_STRING_COLUMNS, COOL_UINT64_COLUMNS, COOL_FLOAT32_COLUMNS),
}

_VALID_SAM_SCALAR_TYPE_CODES = {"A", "c", "C", "s", "S", "i", "I", "f", "Z", "H"}
_VALID_SAM_ARRAY_SUBTYPE_CODES = {"c", "C", "s", "S", "i", "I", "f"}
_VALID_SAM_TYPE_CODES = _VALID_SAM_SCALAR_TYPE_CODES | {"B"}


def _supported_sam_type_message() -> str:
    return (
        "Supported scalar types: A, c, C, s, S, i, I, f, Z, H. "
        "Array types: B or B:<subtype> where subtype is one of c, C, s, S, i, I, f."
    )


def _validate_tag_name(tag: str, parameter_name: str, raw_value: str) -> None:
    if len(tag) != 2:
        raise ValueError(
            f"Invalid {parameter_name} '{raw_value}': TAG must be exactly 2 characters."
        )


def _validate_sam_type_spec(
    type_spec: str,
    parameter_name: str,
    raw_value: str,
) -> None:
    parts = type_spec.split(":")

    if len(parts) == 1:
        type_code = parts[0]
        if type_code not in _VALID_SAM_TYPE_CODES:
            raise ValueError(
                f"Invalid {parameter_name} '{raw_value}': unsupported SAM type "
                f"'{type_code}'. {_supported_sam_type_message()}"
            )
        return

    if len(parts) == 2 and parts[0] == "B":
        subtype = parts[1]
        if subtype in _VALID_SAM_ARRAY_SUBTYPE_CODES:
            return
        raise ValueError(
            f"Invalid {parameter_name} '{raw_value}': unsupported SAM array subtype "
            f"'{subtype}'. {_supported_sam_type_message()}"
        )

    raise ValueError(
        f"Invalid {parameter_name} '{raw_value}': expected scalar TYPE, 'B', or "
        f"'B:SUBTYPE'. {_supported_sam_type_message()}"
    )


def _validate_tag_type_hints(tag_type_hints: list[str]) -> None:
    """Validate tag_type_hints format before passing to Rust.

    Each hint must be one of:
    - TAG:TYPE
    - TAG:B
    - TAG:B:SUBTYPE
    """
    for hint in tag_type_hints:
        parts = hint.split(":")
        if len(parts) not in (2, 3) or not parts[0]:
            raise ValueError(
                f"Invalid tag_type_hint '{hint}': expected 'TAG:TYPE', 'TAG:B', "
                f"or 'TAG:B:SUBTYPE' format. {_supported_sam_type_message()}"
            )
        tag = parts[0]
        _validate_tag_name(tag, "tag_type_hint", hint)
        _validate_sam_type_spec(":".join(parts[1:]), "tag_type_hint", hint)


def _normalize_read_tag_type_hints(
    tag_type_hints: Optional[list[str]],
) -> Optional[list[str]]:
    """Normalize read-side hints to the stricter upstream parser contract.

    Upstream now requires array hints to include a subtype (`TAG:B:<subtype>`),
    but polars-bio intentionally keeps accepting bare `TAG:B` as the default
    integer-array hint for backward compatibility. Rewrite those hints to
    `TAG:B:i` before they reach the Rust table providers.
    """
    if tag_type_hints is None:
        return None

    normalized_hints = []
    for hint in tag_type_hints:
        if hint.endswith(":B") and hint.count(":") == 1:
            normalized_hints.append(f"{hint}:i")
        else:
            normalized_hints.append(hint)
    return normalized_hints


def _validate_tag_type_overrides(tag_type_overrides: Dict[str, str]) -> None:
    """Validate BAM/SAM write-time tag type overrides."""
    for tag, type_spec in tag_type_overrides.items():
        _validate_tag_name(tag, "tag_type_override", f"{tag}={type_spec}")
        _validate_sam_type_spec(type_spec, "tag_type_override", f"{tag}={type_spec}")


SCHEMAS = {
    "bed3": ["chrom", "start", "end"],
    "bed4": ["chrom", "start", "end", "name"],
    "bed5": ["chrom", "start", "end", "name", "score"],
    "bed6": ["chrom", "start", "end", "name", "score", "strand"],
    "bed7": ["chrom", "start", "end", "name", "score", "strand", "thickStart"],
    "bed8": [
        "chrom",
        "start",
        "end",
        "name",
        "score",
        "strand",
        "thickStart",
        "thickEnd",
    ],
    "bed9": [
        "chrom",
        "start",
        "end",
        "name",
        "score",
        "strand",
        "thickStart",
        "thickEnd",
        "itemRgb",
    ],
    "bed12": [
        "chrom",
        "start",
        "end",
        "name",
        "score",
        "strand",
        "thickStart",
        "thickEnd",
        "itemRgb",
        "blockCount",
        "blockSizes",
        "blockStarts",
    ],
}


def _quote_sql_identifier(identifier: str) -> str:
    """Quote a SQL identifier for DataFusion SQL text."""
    escaped = str(identifier).replace('"', '""')
    return f'"{escaped}"'


def _normalize_bigbed_schema_mode(schema: str) -> str:
    normalized = str(schema).lower()
    if normalized not in {"auto", "rest"}:
        raise ValueError("schema must be either 'auto' or 'rest'")
    return normalized


def _validate_variant_input_path(
    path: str, expected_format: str, operation: str = "read"
) -> None:
    """Keep the public VCF and BCF entry points format-specific.

    The shared upstream provider selects its physical decoder from the path.
    Query strings and fragments are ignored so signed object-store URLs work in
    the same way as local paths.
    """
    normalized_path = strip_url_parameters(path).lower()
    is_bcf = normalized_path.endswith(".bcf")

    if expected_format == "vcf" and is_bcf:
        if operation == "describe":
            raise ValueError(
                "BCF input must be described with describe_bcf(), not describe_vcf()"
            )
        if operation == "register":
            raise ValueError(
                "BCF input must be registered with register_bcf(), not register_vcf()"
            )
        raise ValueError(
            "BCF input must be read with read_bcf() or scan_bcf(), not the VCF APIs"
        )
    if expected_format == "bcf" and not is_bcf:
        if operation == "describe":
            raise ValueError("describe_bcf() requires a path ending in '.bcf'")
        if operation == "register":
            raise ValueError("register_bcf() requires a path ending in '.bcf'")
        raise ValueError("read_bcf() and scan_bcf() require a path ending in '.bcf'")


def _validate_bcf_genotype_output(
    genotype_output: str,
    format_fields: Union[list[str], None] = None,
) -> None:
    if genotype_output not in {"string", "dosage"}:
        raise ValueError(
            "genotype_output must be either 'string' or 'dosage', "
            f"got {genotype_output!r}"
        )
    if (
        genotype_output == "dosage"
        and format_fields is not None
        and format_fields != ["GT"]
    ):
        raise ValueError(
            'BCF genotype_output="dosage" requires GT as the only selected '
            'FORMAT field (format_fields=["GT"]); '
            f"got format_fields={format_fields!r}"
        )


def _validate_bgen_input_path(path: str, operation: str = "read") -> None:
    """Keep the BGEN entry points format-specific."""
    if not strip_url_parameters(path).lower().endswith(".bgen"):
        raise ValueError(
            f"BGEN {operation} requires a path ending in '.bgen', got {path!r}"
        )


def _validate_bgen_probability_layout(probability_layout: str) -> None:
    if probability_layout not in {"nested", "fixed"}:
        raise ValueError(
            "probability_layout must be either 'nested' or 'fixed', "
            f"got {probability_layout!r}"
        )


BGEN_GENOTYPE_FIELDS = ("DS", "GP", "PLOIDY")


def _validate_bgen_genotype_fields(
    genotype_fields: Union[Sequence[str], None],
) -> None:
    """Catch an empty or misspelled selection before a file is opened.

    Only the names are checked here. *Which* of them a given call may ask for
    depends on `genotype_output` — `DS` for dosage, `GP` for probability — and
    that rule stays with the provider, which states it precisely and cannot
    drift from itself. Duplicating it would put two answers in the repository
    for one question.
    """
    if genotype_fields is None:
        return
    if not genotype_fields:
        raise ValueError(
            "genotype_fields must name at least one of "
            f"{', '.join(BGEN_GENOTYPE_FIELDS)}"
        )
    unknown = [name for name in genotype_fields if name not in BGEN_GENOTYPE_FIELDS]
    if unknown:
        raise ValueError(
            f"unsupported BGEN genotype field(s) {unknown!r}; "
            f"available fields: {', '.join(BGEN_GENOTYPE_FIELDS)}"
        )


def _validate_bgen_genotype_output(genotype_output: str) -> None:
    if genotype_output not in {"probability", "dosage"}:
        raise ValueError(
            "genotype_output must be either 'probability' or 'dosage', "
            f"got {genotype_output!r}"
        )


PGEN_GENOTYPE_FIELDS = ("GT", "ALT_COUNT", "PHASED", "DS", "DS_STORED", "HDS")


def _validate_pgen_input_path(path: str, operation: str = "read") -> None:
    """Keep the PGEN entry points format-specific."""
    if not strip_url_parameters(path).lower().endswith(".pgen"):
        raise ValueError(
            f"PGEN {operation} requires a path ending in '.pgen', got {path!r}"
        )


def _validate_pgen_genotype_fields(genotype_fields: Sequence[str]) -> None:
    if not genotype_fields:
        raise ValueError(
            "genotype_fields must name at least one of "
            f"{', '.join(PGEN_GENOTYPE_FIELDS)}"
        )
    unknown = [name for name in genotype_fields if name not in PGEN_GENOTYPE_FIELDS]
    if unknown:
        raise ValueError(
            f"unsupported PGEN genotype field(s) {unknown!r}; "
            f"available fields: {', '.join(PGEN_GENOTYPE_FIELDS)}"
        )


PGEN_PSAM_ID_MODES = ("iid", "fid_iid", "fid_iid_sid")
PGEN_MISSING_SAMPLE_POLICIES = ("error", "ignore")


def _validate_pgen_psam_id_mode(psam_id_mode: str) -> None:
    if psam_id_mode not in PGEN_PSAM_ID_MODES:
        raise ValueError(
            "psam_id_mode must be one of "
            f"{', '.join(repr(mode) for mode in PGEN_PSAM_ID_MODES)}, "
            f"got {psam_id_mode!r}"
        )


def _validate_pgen_missing_sample_policy(missing_sample_policy: str) -> None:
    if missing_sample_policy not in PGEN_MISSING_SAMPLE_POLICIES:
        raise ValueError(
            "missing_sample_policy must be either 'error' or 'ignore', "
            f"got {missing_sample_policy!r}"
        )


def _check_room(row: int, count: int, variants: int) -> int:
    """Guard the destination against a scan longer than the companions declare."""
    if row + count > variants:
        raise RuntimeError(
            f"PGEN scan emitted more than the {variants} variants the provider reported"
        )
    return count


class PgenMatrix(NamedTuple):
    """A dense genotype matrix and the labels for its axes.

    `values` is a C-contiguous NumPy array with one row per variant and one
    column per selected sample; `positions` is a NumPy array labelling the rows
    and `sample_names` a list labelling the columns.

    The fields are typed loosely because NumPy is not a polars-bio dependency —
    it is imported only by `read_pgen_matrix`, which is the only thing that
    produces this.
    """

    values: Any
    positions: Any
    sample_names: list


class IOOperations:
    @staticmethod
    def read_fasta(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
    ) -> pl.DataFrame:
        """

        Read a FASTA file into a DataFrame.

        Parameters:
            path: The path to the FASTA file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the FASTA file. If not specified, it will be detected automatically based on the file extension. BGZF and GZIP compressions are supported ('bgz', 'gz').
            projection_pushdown: Enable column projection pushdown optimization. When True, only requested columns are processed at the DataFusion execution level, improving performance and reducing memory usage.

        !!! Example
            ```shell
            wget https://www.ebi.ac.uk/ena/browser/api/fasta/BK006935.2?download=true -O /tmp/test.fasta
            ```

            ```python
            import polars_bio as pb
            pb.read_fasta("/tmp/test.fasta").limit(1)
            ```
            ```shell
             shape: (1, 3)
            ┌─────────────────────────┬─────────────────────────────────┬─────────────────────────────────┐
            │ name                    ┆ description                     ┆ sequence                        │
            │ ---                     ┆ ---                             ┆ ---                             │
            │ str                     ┆ str                             ┆ str                             │
            ╞═════════════════════════╪═════════════════════════════════╪═════════════════════════════════╡
            │ ENA|BK006935|BK006935.2 ┆ TPA_inf: Saccharomyces cerevis… ┆ CCACACCACACCCACACACCCACACACCAC… │
            └─────────────────────────┴─────────────────────────────────┴─────────────────────────────────┘
            ```
        """
        return IOOperations.scan_fasta(
            path,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            compression_type,
            projection_pushdown,
        ).collect()

    @staticmethod
    def scan_fasta(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
    ) -> pl.LazyFrame:
        """

        Lazily read a FASTA file into a LazyFrame.

        Parameters:
            path: The path to the FASTA file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the FASTA file. If not specified, it will be detected automatically based on the file extension. BGZF and GZIP compressions are supported ('bgz', 'gz').
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.

        !!! Example
            ```shell
            wget https://www.ebi.ac.uk/ena/browser/api/fasta/BK006935.2?download=true -O /tmp/test.fasta
            ```

            ```python
            import polars_bio as pb
            pb.scan_fasta("/tmp/test.fasta").limit(1).collect()
            ```
            ```shell
             shape: (1, 3)
            ┌─────────────────────────┬─────────────────────────────────┬─────────────────────────────────┐
            │ name                    ┆ description                     ┆ sequence                        │
            │ ---                     ┆ ---                             ┆ ---                             │
            │ str                     ┆ str                             ┆ str                             │
            ╞═════════════════════════╪═════════════════════════════════╪═════════════════════════════════╡
            │ ENA|BK006935|BK006935.2 ┆ TPA_inf: Saccharomyces cerevis… ┆ CCACACCACACCCACACACCCACACACCAC… │
            └─────────────────────────┴─────────────────────────────────┴─────────────────────────────────┘
            ```
        """
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )
        fasta_read_options = FastaReadOptions(
            object_storage_options=object_storage_options
        )
        read_options = ReadOptions(fasta_read_options=fasta_read_options)
        return _read_file(path, InputFormat.Fasta, read_options, projection_pushdown)

    @staticmethod
    def read_vcf(
        path: str,
        info_fields: Union[list[str], None] = None,
        format_fields: Union[list[str], None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        samples: Union[list[str], None] = None,
    ) -> pl.DataFrame:
        """
        Read a text VCF file into a DataFrame.

        !!! hint "Parallelism & Indexed Reads"
            Indexed parallel reads and predicate pushdown are automatic when a VCF TBI/CSI
            index is present. See [File formats support](/polars-bio/features/#file-formats-support),
            [Indexed reads](/polars-bio/features/#indexed-reads-predicate-pushdown),
            and [Automatic parallel partitioning](/polars-bio/features/#automatic-parallel-partitioning) for details.

        Parameters:
            path: The path to the VCF file.
            info_fields: List of INFO field names to include. If *None*, all INFO fields from the VCF header are included by default. Use this to limit fields for better performance.
            format_fields: List of FORMAT field names to include (per-sample genotype data). If *None*, all FORMAT fields are included by default. For **single-sample** VCFs, FORMAT fields are top-level columns (e.g., `GT`, `DP`). For **multi-sample** VCFs, FORMAT data is exposed as a nested `genotypes` column (`struct<GT: list, DP: list, ...>`) with sample names in `meta["header"]["sample_names"]`.
            samples: Optional list of sample names to include from the VCF header. Matching is exact and case-sensitive. Missing sample names are skipped with a warning. The output follows the requested sample order.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the VCF file. If not specified, it will be detected automatically..
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            predicate_pushdown: Enable predicate pushdown using VCF TBI/CSI index files for efficient region-based filtering. Index files are auto-discovered (for example, `file.vcf.gz.tbi`). Only simple predicates are pushed down (equality, comparisons, IN); complex predicates like `.str.contains()` or OR logic are filtered client-side. Correctness is always guaranteed.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.

        !!! Example "Reading VCF with INFO and FORMAT fields"
            ```python
            import polars_bio as pb

            # Read VCF with both INFO and FORMAT fields
            df = pb.read_vcf(
                "sample.vcf.gz",
                info_fields=["END"],              # INFO field
                format_fields=["GT", "DP", "GQ"]  # FORMAT fields
            )

            # Single-sample VCF: FORMAT fields are top-level columns (GT, DP, GQ)
            print(df.select(["chrom", "start", "ref", "alt", "END", "GT", "DP", "GQ"]))
            # Output:
            # shape: (10, 8)
            # ┌───────┬───────┬─────┬─────┬──────┬─────┬─────┬─────┐
            # │ chrom ┆ start ┆ ref ┆ alt ┆ END  ┆ GT  ┆ DP  ┆ GQ  │
            # │ str   ┆ u32   ┆ str ┆ str ┆ i32  ┆ str ┆ i32 ┆ i32 │
            # ╞═══════╪═══════╪═════╪═════╪══════╪═════╪═════╪═════╡
            # │ 1     ┆ 10009 ┆ A   ┆ .   ┆ null ┆ 0/0 ┆ 10  ┆ 27  │
            # │ 1     ┆ 10015 ┆ A   ┆ .   ┆ null ┆ 0/0 ┆ 17  ┆ 35  │
            # └───────┴───────┴─────┴─────┴──────┴─────┴─────┴─────┘

            # Multi-sample VCF: FORMAT data is nested in "genotypes"
            df = pb.read_vcf("multisample.vcf", format_fields=["GT", "DP"])
            print(df.select(["chrom", "start", "genotypes"]))
            ```
        """
        lf = IOOperations.scan_vcf(
            path=path,
            info_fields=info_fields,
            format_fields=format_fields,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
            projection_pushdown=projection_pushdown,
            predicate_pushdown=predicate_pushdown,
            use_zero_based=use_zero_based,
            samples=samples,
        )
        # Get metadata before collecting (polars-config-meta doesn't preserve through collect)
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        # Set metadata on the collected DataFrame
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_vcf(
        path: str,
        info_fields: Union[list[str], None] = None,
        format_fields: Union[list[str], None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        samples: Union[list[str], None] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a text VCF file into a LazyFrame.

        !!! hint "Parallelism & Indexed Reads"
            Indexed parallel reads and predicate pushdown are automatic when a VCF TBI/CSI
            index is present. See [File formats support](/polars-bio/features/#file-formats-support),
            [Indexed reads](/polars-bio/features/#indexed-reads-predicate-pushdown),
            and [Automatic parallel partitioning](/polars-bio/features/#automatic-parallel-partitioning) for details.

        Parameters:
            path: The path to the VCF file.
            info_fields: List of INFO field names to include. If *None*, all INFO fields from the VCF header are included by default. Use this to limit fields for better performance.
            format_fields: List of FORMAT field names to include (per-sample genotype data). If *None*, all FORMAT fields are included by default. For **single-sample** VCFs, FORMAT fields are top-level columns (e.g., `GT`, `DP`). For **multi-sample** VCFs, FORMAT data is exposed as a nested `genotypes` column (`struct<GT: list, DP: list, ...>`) with sample names in `meta["header"]["sample_names"]`.
            samples: Optional list of sample names to include from the VCF header. Matching is exact and case-sensitive. Missing sample names are skipped with a warning. The output follows the requested sample order.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the VCF file. If not specified, it will be detected automatically..
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            predicate_pushdown: Enable predicate pushdown using VCF TBI/CSI index files for efficient region-based filtering. Index files are auto-discovered (for example, `file.vcf.gz.tbi`). Only simple predicates are pushed down (equality, comparisons, IN); complex predicates like `.str.contains()` or OR logic are filtered client-side. Correctness is always guaranteed.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.

        !!! Example "Lazy scanning VCF with INFO and FORMAT fields"
            ```python
            import polars_bio as pb

            # Lazily scan VCF with both INFO and FORMAT fields
            lf = pb.scan_vcf(
                "sample.vcf.gz",
                info_fields=["END"],              # INFO field
                format_fields=["GT", "DP", "GQ"]  # FORMAT fields
            )

            # Apply filters and collect only what's needed
            df = lf.filter(pl.col("DP") > 20).select(
                ["chrom", "start", "ref", "alt", "GT", "DP", "GQ"]
            ).collect()

            # Single-sample VCF: FORMAT fields are top-level columns (GT, DP, GQ)
            # Multi-sample VCF: FORMAT data is nested in "genotypes"
            ```
        """
        _validate_variant_input_path(path, "vcf")
        return IOOperations._scan_variant(
            path=path,
            info_fields=info_fields,
            format_fields=format_fields,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
            projection_pushdown=projection_pushdown,
            predicate_pushdown=predicate_pushdown,
            use_zero_based=use_zero_based,
            samples=samples,
            genotype_output="string",
            source_format="vcf",
        )

    @staticmethod
    def read_bcf(
        path: str,
        info_fields: Union[list[str], None] = None,
        format_fields: Union[list[str], None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        samples: Union[list[str], None] = None,
        genotype_output: str = "string",
    ) -> pl.DataFrame:
        """Read a BCF file into a DataFrame.

        Parameters:
            path: The path to the BCF file. The path must end in `.bcf`.
            info_fields: INFO fields to include. If *None*, all header-defined INFO fields are included.
            format_fields: FORMAT fields to include. Single-sample fields are top-level columns; multisample fields are nested in `genotypes`.
            samples: Optional sample names to include, in requested order.
            chunk_size: Object-store chunk size in MB.
            concurrent_fetches: Number of concurrent object-store fetches.
            allow_anonymous: Allow anonymous object-store access.
            enable_request_payer: Enable AWS request-payer access.
            max_retries: Maximum number of object-store retries.
            timeout: Object-store timeout in seconds.
            compression_type: Compression override. The default detects BCF automatically.
            projection_pushdown: Push column projection into the BCF reader.
            predicate_pushdown: Use a neighboring `.bcf.csi` index for genomic predicate pushdown when available.
            use_zero_based: Select 0-based half-open (`True`) or 1-based closed (`False`) coordinates. *None* uses global configuration.
            genotype_output: GT representation. `"string"` (default) returns VCF-style calls such as `"0/1"`. `"dosage"` returns the number of ALT alleles per sample as nullable `Int8` (normally 0, 1, or 2 for diploid calls); any missing allele yields null. Dosage requires GT to be the only selected FORMAT field and requires biallelic records. When `format_fields` is *None*, all header-defined FORMAT fields are selected, so pass `format_fields=["GT"]` when the header declares additional fields. Multiallelic records are rejected.

        !!! note
            BCF is input-only. Use `write_vcf` or `sink_vcf` to write text VCF.
        """
        lf = IOOperations.scan_bcf(
            path=path,
            info_fields=info_fields,
            format_fields=format_fields,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
            projection_pushdown=projection_pushdown,
            predicate_pushdown=predicate_pushdown,
            use_zero_based=use_zero_based,
            samples=samples,
            genotype_output=genotype_output,
        )
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_bcf(
        path: str,
        info_fields: Union[list[str], None] = None,
        format_fields: Union[list[str], None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        samples: Union[list[str], None] = None,
        genotype_output: str = "string",
    ) -> pl.LazyFrame:
        """Lazily read a BCF file into a LazyFrame.

        BCF CSI range pushdown, projection pushdown, and configured input
        partition parallelism are preserved. `genotype_output="string"` returns
        VCF-style GT calls and remains the default. `genotype_output="dosage"`
        returns the number of ALT alleles per sample as nullable `Int8` (normally
        0, 1, or 2 for diploid calls); any missing allele yields null. Dosage
        requires GT to be the only selected FORMAT field and requires biallelic
        records. When `format_fields` is `None`, all header-defined FORMAT
        fields are selected, so pass `format_fields=["GT"]` when the header
        declares additional fields. Multiallelic records are rejected.
        """
        _validate_bcf_genotype_output(genotype_output, format_fields)
        _validate_variant_input_path(path, "bcf")
        return IOOperations._scan_variant(
            path=path,
            info_fields=info_fields,
            format_fields=format_fields,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
            projection_pushdown=projection_pushdown,
            predicate_pushdown=predicate_pushdown,
            use_zero_based=use_zero_based,
            samples=samples,
            genotype_output=genotype_output,
            source_format="bcf",
        )

    @staticmethod
    def _scan_variant(
        path: str,
        info_fields: Union[list[str], None],
        format_fields: Union[list[str], None],
        chunk_size: int,
        concurrent_fetches: int,
        allow_anonymous: bool,
        enable_request_payer: bool,
        max_retries: int,
        timeout: int,
        compression_type: str,
        projection_pushdown: bool,
        predicate_pushdown: bool,
        use_zero_based: Optional[bool],
        samples: Union[list[str], None],
        genotype_output: str,
        source_format: str,
    ) -> pl.LazyFrame:
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        # Upstream VCF reader projects all INFO fields by default when info_fields is None.
        initial_info_fields = info_fields

        zero_based = _resolve_zero_based(use_zero_based)
        vcf_read_options = VcfReadOptions(
            info_fields=initial_info_fields,
            format_fields=format_fields,
            samples=samples,
            object_storage_options=object_storage_options,
            zero_based=zero_based,
            genotype_output=genotype_output,
        )
        read_options = ReadOptions(vcf_read_options=vcf_read_options)
        lf = _read_file(
            path,
            InputFormat.Vcf,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero_based,
        )
        lf.config_meta.set(source_format=source_format)
        return lf

    @staticmethod
    def read_vcf_zarr(
        path: str,
        info_fields: Union[list[str], None] = None,
        format_fields: Union[list[str], None] = None,
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        samples: Union[list[str], None] = None,
        genotype_encoding_raw: bool = True,
    ) -> pl.DataFrame:
        """
        Read a local VCF Zarr store into a DataFrame.

        Parameters:
            path: The path to the VCF Zarr store directory.
            info_fields: Optional list of INFO field names to include. If None, local INFO arrays are discovered automatically. Use [] to disable INFO fields.
            format_fields: Optional list of FORMAT field names to include. If None, local FORMAT arrays are discovered automatically. Use [] to disable FORMAT fields.
            projection_pushdown: Enable column projection pushdown at the DataFusion level.
            predicate_pushdown: Enable predicate pushdown at the DataFusion level.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None, uses the global configuration.
            samples: Optional list of sample names to include.
            genotype_encoding_raw: If True, output GT as raw typed allele calls. If False, output VCF-style GT strings.
        """
        lf = IOOperations.scan_vcf_zarr(
            path=path,
            info_fields=info_fields,
            format_fields=format_fields,
            projection_pushdown=projection_pushdown,
            predicate_pushdown=predicate_pushdown,
            use_zero_based=use_zero_based,
            samples=samples,
            genotype_encoding_raw=genotype_encoding_raw,
        )
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_vcf_zarr(
        path: str,
        info_fields: Union[list[str], None] = None,
        format_fields: Union[list[str], None] = None,
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        samples: Union[list[str], None] = None,
        genotype_encoding_raw: bool = True,
    ) -> pl.LazyFrame:
        """
        Lazily read a local VCF Zarr store into a LazyFrame.

        Parameters:
            path: The path to the VCF Zarr store directory.
            info_fields: Optional list of INFO field names to include. If None, local INFO arrays are discovered automatically. Use [] to disable INFO fields.
            format_fields: Optional list of FORMAT field names to include. If None, local FORMAT arrays are discovered automatically. Use [] to disable FORMAT fields.
            projection_pushdown: Enable column projection pushdown at the DataFusion level.
            predicate_pushdown: Enable predicate pushdown at the DataFusion level.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None, uses the global configuration.
            samples: Optional list of sample names to include.
            genotype_encoding_raw: If True, output GT as raw typed allele calls. If False, output VCF-style GT strings.
        """
        zero_based = _resolve_zero_based(use_zero_based)
        vcf_zarr_read_options = VcfZarrReadOptions(
            info_fields=info_fields,
            format_fields=format_fields,
            samples=samples,
            zero_based=zero_based,
            genotype_encoding_raw=genotype_encoding_raw,
        )
        read_options = ReadOptions(vcf_zarr_read_options=vcf_zarr_read_options)
        return _read_file(
            path,
            InputFormat.VcfZarr,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def read_gff(
        path: str,
        attr_fields: Union[list[str], None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Read a GFF file into a DataFrame.

        Parameters:
            path: The path to the GFF file.
            attr_fields: List of attribute field names to extract as separate columns. If *None*, attributes will be kept as a nested structure. Use this to extract specific attributes like 'ID', 'gene_name', 'gene_type', etc. as direct columns for easier access.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the GFF file. If not specified, it will be detected automatically..
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            predicate_pushdown: Enable predicate pushdown using index files (TBI/CSI) for efficient region-based filtering. Index files are auto-discovered (e.g., `file.gff.gz.tbi`). Only simple predicates are pushed down (equality, comparisons, IN); complex predicates like `.str.contains()` or OR logic are filtered client-side. Correctness is always guaranteed.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
        """
        lf = IOOperations.scan_gff(
            path,
            attr_fields,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            compression_type,
            projection_pushdown,
            predicate_pushdown,
            use_zero_based,
        )
        # Get metadata before collecting (polars-config-meta doesn't preserve through collect)
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        # Set metadata on the collected DataFrame
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_gff(
        path: str,
        attr_fields: Union[list[str], None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a GFF file into a LazyFrame.

        Parameters:
            path: The path to the GFF file.
            attr_fields: List of attribute field names to extract as separate columns. If *None*, attributes will be kept as a nested structure. Use this to extract specific attributes like 'ID', 'gene_name', 'gene_type', etc. as direct columns for easier access.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large-scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the GFF file. If not specified, it will be detected automatically.
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            predicate_pushdown: Enable predicate pushdown using index files (TBI/CSI) for efficient region-based filtering. Index files are auto-discovered (e.g., `file.gff.gz.tbi`). Only simple predicates are pushed down (equality, comparisons, IN); complex predicates like `.str.contains()` or OR logic are filtered client-side. Correctness is always guaranteed.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
        """
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        zero_based = _resolve_zero_based(use_zero_based)
        gff_read_options = GffReadOptions(
            attr_fields=attr_fields,
            object_storage_options=object_storage_options,
            zero_based=zero_based,
        )
        read_options = ReadOptions(gff_read_options=gff_read_options)
        _store_py_object_storage_options(read_options, object_storage_options)
        return _read_file(
            path,
            InputFormat.Gff,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def read_gtf(
        path: str,
        attr_fields: Union[list[str], None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Read a GTF file into a DataFrame.

        GTF (Gene Transfer Format) shares the same 9-column structure as GFF but uses
        different attribute syntax (``key "value"`` vs GFF's ``key=value``).

        Parameters:
            path: The path to the GTF file.
            attr_fields: List of attribute field names to extract as separate columns.
                If *None*, attributes will be kept as a nested structure.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large-scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the GTF file. If not specified, it will be detected automatically.
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            predicate_pushdown: Enable predicate pushdown using index files (TBI/CSI) for efficient region-based filtering. Index files are auto-discovered (e.g., `file.gtf.gz.tbi`). Only simple predicates are pushed down (equality, comparisons, IN); complex predicates like `.str.contains()` or OR logic are filtered client-side. Correctness is always guaranteed.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
        """
        lf = IOOperations.scan_gtf(
            path,
            attr_fields,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            compression_type,
            projection_pushdown,
            predicate_pushdown,
            use_zero_based,
        )
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_gtf(
        path: str,
        attr_fields: Union[list[str], None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a GTF file into a LazyFrame.

        GTF (Gene Transfer Format) shares the same 9-column structure as GFF but uses
        different attribute syntax (``key "value"`` vs GFF's ``key=value``).

        Parameters:
            path: The path to the GTF file.
            attr_fields: List of attribute field names to extract as separate columns.
                If *None*, attributes will be kept as a nested structure.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large-scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the GTF file. If not specified, it will be detected automatically.
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            predicate_pushdown: Enable predicate pushdown using index files (TBI/CSI) for efficient region-based filtering. Index files are auto-discovered (e.g., `file.gtf.gz.tbi`). Only simple predicates are pushed down (equality, comparisons, IN); complex predicates like `.str.contains()` or OR logic are filtered client-side. Correctness is always guaranteed.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
        """
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        zero_based = _resolve_zero_based(use_zero_based)
        gtf_read_options = GtfReadOptions(
            attr_fields=attr_fields,
            object_storage_options=object_storage_options,
            zero_based=zero_based,
        )
        read_options = ReadOptions(gtf_read_options=gtf_read_options)
        _store_py_object_storage_options(read_options, object_storage_options)
        return _read_file(
            path,
            InputFormat.Gtf,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def read_bam(
        path: str,
        tag_fields: Union[list[str], None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        infer_tag_types: bool = True,
        infer_tag_sample_size: int = 100,
        tag_type_hints: Optional[list[str]] = None,
    ) -> pl.DataFrame:
        """
        Read a BAM file into a DataFrame.

        !!! hint "Parallelism & Indexed Reads"
            Indexed parallel reads and predicate pushdown are automatic when a BAI/CSI index
            is present. See [File formats support](/polars-bio/features/#file-formats-support),
            [Indexed reads](/polars-bio/features/#indexed-reads-predicate-pushdown),
            and [Automatic parallel partitioning](/polars-bio/features/#automatic-parallel-partitioning) for details.

        Parameters:
            path: The path to the BAM file.
            tag_fields: List of BAM tag names to include as columns (e.g., ["NM", "MD", "AS"]). If None, no optional tags are parsed (default). Common tags include: NM (edit distance), MD (mismatch string), AS (alignment score), XS (secondary alignment score), RG (read group), CB (cell barcode), UB (UMI barcode).
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large-scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large-scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            predicate_pushdown: Enable predicate pushdown using index files (BAI/CSI) for efficient region-based filtering. Index files are auto-discovered (e.g., `file.bam.bai`). Only simple predicates are pushed down (equality, comparisons, IN); complex predicates like `.str.contains()` or OR logic are filtered client-side. Correctness is always guaranteed.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.
            infer_tag_types: If True (default), sample the file to auto-detect types for custom/unknown tags. This prevents integer tags from being decoded as ASCII characters.
            infer_tag_sample_size: Number of records to sample for tag type inference (default: 100).
            tag_type_hints: Explicit SAM-style type hints for tags (e.g., ["pt:i", "ML:B:C", "FZ:B:S"]). Used as fallback when inference is disabled or a tag is not found in sampled records. Supported forms: TAG:TYPE, TAG:B, or TAG:B:SUBTYPE where TYPE is one of A, c, C, s, S, i, I, f, Z, H and SUBTYPE is one of c, C, s, S, i, I, f.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
        """
        lf = IOOperations.scan_bam(
            path,
            tag_fields,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            projection_pushdown,
            predicate_pushdown,
            use_zero_based,
            infer_tag_types,
            infer_tag_sample_size,
            tag_type_hints,
        )
        # Get metadata before collecting (polars-config-meta doesn't preserve through collect)
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        # Set metadata on the collected DataFrame
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_bam(
        path: str,
        tag_fields: Union[list[str], None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        infer_tag_types: bool = True,
        infer_tag_sample_size: int = 100,
        tag_type_hints: Optional[list[str]] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a BAM file into a LazyFrame.

        !!! hint "Parallelism & Indexed Reads"
            Indexed parallel reads and predicate pushdown are automatic when a BAI/CSI index
            is present. See [File formats support](/polars-bio/features/#file-formats-support),
            [Indexed reads](/polars-bio/features/#indexed-reads-predicate-pushdown),
            and [Automatic parallel partitioning](/polars-bio/features/#automatic-parallel-partitioning) for details.

        Parameters:
            path: The path to the BAM file.
            tag_fields: List of BAM tag names to include as columns (e.g., ["NM", "MD", "AS"]). If None, no optional tags are parsed (default). Common tags include: NM (edit distance), MD (mismatch string), AS (alignment score), XS (secondary alignment score), RG (read group), CB (cell barcode), UB (UMI barcode).
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            predicate_pushdown: Enable predicate pushdown using index files (BAI/CSI) for efficient region-based filtering. Index files are auto-discovered (e.g., `file.bam.bai`). Only simple predicates are pushed down (equality, comparisons, IN); complex predicates like `.str.contains()` or OR logic are filtered client-side. Correctness is always guaranteed.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.
            infer_tag_types: If True (default), sample the file to auto-detect types for custom/unknown tags. This prevents integer tags from being decoded as ASCII characters.
            infer_tag_sample_size: Number of records to sample for tag type inference (default: 100).
            tag_type_hints: Explicit SAM-style type hints for tags (e.g., ["pt:i", "ML:B:C", "FZ:B:S"]). Used as fallback when inference is disabled or a tag is not found in sampled records. Supported forms: TAG:TYPE, TAG:B, or TAG:B:SUBTYPE where TYPE is one of A, c, C, s, S, i, I, f, Z, H and SUBTYPE is one of c, C, s, S, i, I, f.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
        """
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type="auto",
        )

        zero_based = _resolve_zero_based(use_zero_based)
        if tag_type_hints is not None:
            _validate_tag_type_hints(tag_type_hints)
            tag_type_hints = _normalize_read_tag_type_hints(tag_type_hints)
        bam_read_options = BamReadOptions(
            object_storage_options=object_storage_options,
            zero_based=zero_based,
            tag_fields=tag_fields,
            infer_tag_types=infer_tag_types,
            infer_tag_sample_size=infer_tag_sample_size,
            tag_type_hints=tag_type_hints,
        )
        read_options = ReadOptions(bam_read_options=bam_read_options)
        return _read_file(
            path,
            InputFormat.Bam,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def read_cram(
        path: str,
        reference_path: str = None,
        tag_fields: Union[list[str], None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        infer_tag_types: bool = True,
        infer_tag_sample_size: int = 100,
        tag_type_hints: Optional[list[str]] = None,
    ) -> pl.DataFrame:
        """
        Read a CRAM file into a DataFrame.

        !!! hint "Parallelism & Indexed Reads"
            Indexed parallel reads and predicate pushdown are automatic when a CRAI index
            is present. See [File formats support](/polars-bio/features/#file-formats-support),
            [Indexed reads](/polars-bio/features/#indexed-reads-predicate-pushdown),
            and [Automatic parallel partitioning](/polars-bio/features/#automatic-parallel-partitioning) for details.

        Parameters:
            path: The path to the CRAM file (local or cloud storage: S3, GCS, Azure Blob).
            reference_path: Optional path to external FASTA reference file (**local path only**, cloud storage not supported). If not provided, the CRAM file must contain embedded reference sequences. The FASTA file must have an accompanying index file (.fai) in the same directory. Create the index using: `samtools faidx reference.fasta`
            tag_fields: List of CRAM tag names to include as columns (e.g., ["NM", "MD", "AS"]). If None, no optional tags are parsed (default). Common tags include: NM (edit distance), MD (mismatch string), AS (alignment score), XS (secondary alignment score), RG (read group), CB (cell barcode), UB (UMI barcode).
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            projection_pushdown: Enable column projection pushdown optimization. When True, only requested columns are processed at the DataFusion execution level, improving performance and reducing memory usage.
            predicate_pushdown: Enable predicate pushdown using index files (CRAI) for efficient region-based filtering. Index files are auto-discovered (e.g., `file.cram.crai`). Only simple predicates are pushed down (equality, comparisons, IN); complex predicates like `.str.contains()` or OR logic are filtered client-side. Correctness is always guaranteed.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.
            infer_tag_types: If True (default), sample the file to auto-detect types for custom/unknown tags. This prevents integer tags from being decoded as ASCII characters.
            infer_tag_sample_size: Number of records to sample for tag type inference (default: 100).
            tag_type_hints: Explicit SAM-style type hints for tags (e.g., ["pt:i", "ML:B:C", "FZ:B:S"]). Used as fallback when inference is disabled or a tag is not found in sampled records. Supported forms: TAG:TYPE, TAG:B, or TAG:B:SUBTYPE where TYPE is one of A, c, C, s, S, i, I, f, Z, H and SUBTYPE is one of c, C, s, S, i, I, f.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.

        !!! warning "Known Limitation: MD and NM Tags"
            Due to a limitation in the underlying noodles-cram library, **MD (mismatch descriptor) and NM (edit distance) tags are not accessible** from CRAM files, even when stored in the file. These tags can be seen with samtools but are not exposed through the noodles-cram record.data() interface.

            Other optional tags (RG, MQ, AM, OQ, etc.) work correctly. This issue is tracked at: https://github.com/biodatageeks/datafusion-bio-formats/issues/54

            **Workaround**: Use BAM format if MD/NM tags are required for your analysis.

        !!! example "Using External Reference"
            ```python
            import polars_bio as pb

            # Read CRAM with external reference
            df = pb.read_cram(
                "/path/to/file.cram",
                reference_path="/path/to/reference.fasta"
            )
            ```

        !!! example "Public CRAM File Example"
            Download and read a public CRAM file from 42basepairs:
            ```bash
            # Download the CRAM file and reference
            wget https://42basepairs.com/download/s3/gatk-test-data/wgs_cram/NA12878_20k_hg38/NA12878.cram
            wget https://storage.googleapis.com/genomics-public-data/resources/broad/hg38/v0/Homo_sapiens_assembly38.fasta

            # Create FASTA index (required)
            samtools faidx Homo_sapiens_assembly38.fasta
            ```

            ```python
            import polars_bio as pb

            # Read first 5 reads from the CRAM file
            df = pb.scan_cram(
                "NA12878.cram",
                reference_path="Homo_sapiens_assembly38.fasta"
            ).limit(5).collect()

            print(df.select(["name", "chrom", "start", "end", "cigar"]))
            ```

        !!! example "Creating CRAM with Embedded Reference"
            To create a CRAM file with embedded reference using samtools:
            ```bash
            samtools view -C -o output.cram --output-fmt-option embed_ref=1 input.bam
            ```

        Returns:
            A Polars DataFrame with the following schema:
                - name: Read name (String)
                - chrom: Chromosome/contig name (String)
                - start: Alignment start position, 1-based (UInt32)
                - end: Alignment end position, 1-based (UInt32)
                - flags: SAM flags (UInt32)
                - cigar: CIGAR string (String)
                - mapping_quality: Mapping quality (UInt32)
                - mate_chrom: Mate chromosome/contig name (String)
                - mate_start: Mate alignment start position, 1-based (UInt32)
                - sequence: Read sequence (String)
                - quality_scores: Base quality scores (String)
        """
        lf = IOOperations.scan_cram(
            path,
            reference_path,
            tag_fields,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            projection_pushdown,
            predicate_pushdown,
            use_zero_based,
            infer_tag_types,
            infer_tag_sample_size,
            tag_type_hints,
        )
        # Get metadata before collecting (polars-config-meta doesn't preserve through collect)
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        # Set metadata on the collected DataFrame
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_cram(
        path: str,
        reference_path: str = None,
        tag_fields: Union[list[str], None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        infer_tag_types: bool = True,
        infer_tag_sample_size: int = 100,
        tag_type_hints: Optional[list[str]] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a CRAM file into a LazyFrame.

        !!! hint "Parallelism & Indexed Reads"
            Indexed parallel reads and predicate pushdown are automatic when a CRAI index
            is present. See [File formats support](/polars-bio/features/#file-formats-support),
            [Indexed reads](/polars-bio/features/#indexed-reads-predicate-pushdown),
            and [Automatic parallel partitioning](/polars-bio/features/#automatic-parallel-partitioning) for details.

        Parameters:
            path: The path to the CRAM file (local or cloud storage: S3, GCS, Azure Blob).
            reference_path: Optional path to external FASTA reference file (**local path only**, cloud storage not supported). If not provided, the CRAM file must contain embedded reference sequences. The FASTA file must have an accompanying index file (.fai) in the same directory. Create the index using: `samtools faidx reference.fasta`
            tag_fields: List of CRAM tag names to include as columns (e.g., ["NM", "MD", "AS"]). If None, no optional tags are parsed (default). Common tags include: NM (edit distance), MD (mismatch string), AS (alignment score), XS (secondary alignment score), RG (read group), CB (cell barcode), UB (UMI barcode).
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            projection_pushdown: Enable column projection pushdown optimization. When True, only requested columns are processed at the DataFusion execution level, improving performance and reducing memory usage.
            predicate_pushdown: Enable predicate pushdown using index files (CRAI) for efficient region-based filtering. Index files are auto-discovered (e.g., `file.cram.crai`). Only simple predicates are pushed down (equality, comparisons, IN); complex predicates like `.str.contains()` or OR logic are filtered client-side. Correctness is always guaranteed.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.
            infer_tag_types: If True (default), sample the file to auto-detect types for custom/unknown tags. This prevents integer tags from being decoded as ASCII characters.
            infer_tag_sample_size: Number of records to sample for tag type inference (default: 100).
            tag_type_hints: Explicit SAM-style type hints for tags (e.g., ["pt:i", "ML:B:C", "FZ:B:S"]). Used as fallback when inference is disabled or a tag is not found in sampled records. Supported forms: TAG:TYPE, TAG:B, or TAG:B:SUBTYPE where TYPE is one of A, c, C, s, S, i, I, f, Z, H and SUBTYPE is one of c, C, s, S, i, I, f.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.

        !!! warning "Known Limitation: MD and NM Tags"
            Due to a limitation in the underlying noodles-cram library, **MD (mismatch descriptor) and NM (edit distance) tags are not accessible** from CRAM files, even when stored in the file. These tags can be seen with samtools but are not exposed through the noodles-cram record.data() interface.

            Other optional tags (RG, MQ, AM, OQ, etc.) work correctly. This issue is tracked at: https://github.com/biodatageeks/datafusion-bio-formats/issues/54

            **Workaround**: Use BAM format if MD/NM tags are required for your analysis.

        !!! example "Using External Reference"
            ```python
            import polars_bio as pb

            # Lazy scan CRAM with external reference
            lf = pb.scan_cram(
                "/path/to/file.cram",
                reference_path="/path/to/reference.fasta"
            )

            # Apply transformations and collect
            df = lf.filter(pl.col("chrom") == "chr1").collect()
            ```

        !!! example "Public CRAM File Example"
            Download and read a public CRAM file from 42basepairs:
            ```bash
            # Download the CRAM file and reference
            wget https://42basepairs.com/download/s3/gatk-test-data/wgs_cram/NA12878_20k_hg38/NA12878.cram
            wget https://storage.googleapis.com/genomics-public-data/resources/broad/hg38/v0/Homo_sapiens_assembly38.fasta

            # Create FASTA index (required)
            samtools faidx Homo_sapiens_assembly38.fasta
            ```

            ```python
            import polars_bio as pb
            import polars as pl

            # Lazy scan and filter for chromosome 20 reads
            df = pb.scan_cram(
                "NA12878.cram",
                reference_path="Homo_sapiens_assembly38.fasta"
            ).filter(
                pl.col("chrom") == "chr20"
            ).select(
                ["name", "chrom", "start", "end", "mapping_quality"]
            ).limit(10).collect()

            print(df)
            ```

        !!! example "Creating CRAM with Embedded Reference"
            To create a CRAM file with embedded reference using samtools:
            ```bash
            samtools view -C -o output.cram --output-fmt-option embed_ref=1 input.bam
            ```

        Returns:
            A Polars LazyFrame with the following schema:
                - name: Read name (String)
                - chrom: Chromosome/contig name (String)
                - start: Alignment start position, 1-based (UInt32)
                - end: Alignment end position, 1-based (UInt32)
                - flags: SAM flags (UInt32)
                - cigar: CIGAR string (String)
                - mapping_quality: Mapping quality (UInt32)
                - mate_chrom: Mate chromosome/contig name (String)
                - mate_start: Mate alignment start position, 1-based (UInt32)
                - sequence: Read sequence (String)
                - quality_scores: Base quality scores (String)
        """
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type="auto",
        )

        zero_based = _resolve_zero_based(use_zero_based)
        if tag_type_hints is not None:
            _validate_tag_type_hints(tag_type_hints)
            tag_type_hints = _normalize_read_tag_type_hints(tag_type_hints)
        cram_read_options = CramReadOptions(
            reference_path=reference_path,
            object_storage_options=object_storage_options,
            zero_based=zero_based,
            tag_fields=tag_fields,
            infer_tag_types=infer_tag_types,
            infer_tag_sample_size=infer_tag_sample_size,
            tag_type_hints=tag_type_hints,
        )
        read_options = ReadOptions(cram_read_options=cram_read_options)
        return _read_file(
            path,
            InputFormat.Cram,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def describe_bam(
        path: str,
        sample_size: int = 100,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Get schema information for a BAM file with automatic tag discovery.

        Samples the first N records to discover all available tags and their types.
        Returns detailed schema information including column names, data types,
        nullability, category (standard/tag), SAM type, and descriptions.

        Parameters:
            path: The path to the BAM file.
            sample_size: Number of records to sample for tag discovery (default: 100).
                Use higher values for more comprehensive tag discovery.
            chunk_size: The size in MB of a chunk when reading from object storage.
            concurrent_fetches: The number of concurrent fetches when reading from object storage.
            allow_anonymous: Whether to allow anonymous access to object storage.
            enable_request_payer: Whether to enable request payer for object storage.
            max_retries: The maximum number of retries for reading the file.
            timeout: The timeout in seconds for reading the file.
            compression_type: The compression type of the file. If "auto" (default), compression is detected automatically.
            use_zero_based: If True, output 0-based coordinates. If False, 1-based coordinates.

        Returns:
            DataFrame with columns:
            - column_name: Name of the column/field
            - data_type: Arrow data type (e.g., "Utf8", "Int32")
            - nullable: Whether the field can be null
            - category: "core" for fixed columns, "tag" for optional SAM tags
            - sam_type: SAM type code (e.g., "Z", "i") for tags, null for core columns
            - description: Human-readable description of the field

        Example:
            ```python
            import polars_bio as pb

            # Auto-discover all tags present in the file
            schema = pb.describe_bam("file.bam", sample_size=100)
            print(schema)
            # Output:
            # shape: (15, 6)
            # ┌─────────────┬───────────┬──────────┬──────────┬──────────┬──────────────────────┐
            # │ column_name ┆ data_type ┆ nullable ┆ category ┆ sam_type ┆ description          │
            # │ ---         ┆ ---       ┆ ---      ┆ ---      ┆ ---      ┆ ---                  │
            # │ str         ┆ str       ┆ bool     ┆ str      ┆ str      ┆ str                  │
            # ╞═════════════╪═══════════╪══════════╪══════════╪══════════╪══════════════════════╡
            # │ name        ┆ Utf8      ┆ true     ┆ core     ┆ null     ┆ Query name           │
            # │ chrom       ┆ Utf8      ┆ true     ┆ core     ┆ null     ┆ Reference name       │
            # │ ...         ┆ ...       ┆ ...      ┆ ...      ┆ ...      ┆ ...                  │
            # │ NM          ┆ Int32     ┆ true     ┆ tag      ┆ i        ┆ Edit distance        │
            # │ AS          ┆ Int32     ┆ true     ┆ tag      ┆ i        ┆ Alignment score      │
            # └─────────────┴───────────┴──────────┴──────────┴──────────┴──────────────────────┘
            ```
        """
        # Build object storage options
        object_storage_options = PyObjectStorageOptions(
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        # Resolve zero_based setting
        zero_based = _resolve_zero_based(use_zero_based)

        # Call Rust function with tag auto-discovery (tag_fields=None)
        df = py_describe_bam(
            ctx,  # PyBioSessionContext
            path,
            object_storage_options,
            zero_based,
            None,  # tag_fields=None enables auto-discovery
            sample_size,
        )

        # Convert DataFusion DataFrame to Polars DataFrame
        return pl.from_arrow(df.to_arrow_table())

    @staticmethod
    def describe_cram(
        path: str,
        reference_path: str = None,
        sample_size: int = 100,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Get schema information for a CRAM file with automatic tag discovery.

        Samples the first N records to discover all available tags and their types.
        Returns detailed schema information including column names, data types,
        nullability, category (core/tag), SAM type, and descriptions.

        Parameters:
            path: The path to the CRAM file.
            reference_path: Optional path to external FASTA reference file.
            sample_size: Number of records to sample for tag discovery (default: 100).
            chunk_size: The size in MB of a chunk when reading from object storage.
            concurrent_fetches: The number of concurrent fetches when reading from object storage.
            allow_anonymous: Whether to allow anonymous access to object storage.
            enable_request_payer: Whether to enable request payer for object storage.
            max_retries: The maximum number of retries for reading the file.
            timeout: The timeout in seconds for reading the file.
            compression_type: The compression type of the file. If "auto" (default), compression is detected automatically.
            use_zero_based: If True, output 0-based coordinates. If False, 1-based coordinates.

        Returns:
            DataFrame with columns:
            - column_name: Name of the column/field
            - data_type: Arrow data type (e.g., "Utf8", "Int32")
            - nullable: Whether the field can be null
            - category: "core" for fixed columns, "tag" for optional SAM tags
            - sam_type: SAM type code (e.g., "Z", "i") for tags, null for core columns
            - description: Human-readable description of the field

        !!! warning "Known Limitation: MD and NM Tags"
            Due to a limitation in the underlying noodles-cram library, **MD (mismatch descriptor) and NM (edit distance) tags are not discoverable** from CRAM files, even when stored. Automatic tag discovery will not include MD/NM tags. Other optional tags (RG, MQ, AM, OQ, etc.) are discovered correctly. See: https://github.com/biodatageeks/datafusion-bio-formats/issues/54

        Example:
            ```python
            import polars_bio as pb

            # Auto-discover all tags present in the file
            schema = pb.describe_cram("file.cram", sample_size=100)
            print(schema)

            # Filter to see only tag columns
            tags = schema.filter(schema["category"] == "tag")
            print(tags["column_name"])
            ```
        """
        # Build object storage options
        object_storage_options = PyObjectStorageOptions(
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        # Resolve zero_based setting
        zero_based = _resolve_zero_based(use_zero_based)

        # Call Rust function with tag auto-discovery (tag_fields=None)
        df = py_describe_cram(
            ctx,
            path,
            reference_path,
            object_storage_options,
            zero_based,
            None,  # tag_fields=None enables auto-discovery
            sample_size,
        )

        # Convert DataFusion DataFrame to Polars DataFrame
        return pl.from_arrow(df.to_arrow_table())

    @staticmethod
    def read_fastq(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
    ) -> pl.DataFrame:
        """
        Read a FASTQ file into a DataFrame.

        !!! hint "Parallelism & Compression"
            See [File formats support](/polars-bio/features/#file-formats-support),
            [Compression](/polars-bio/features/#compression),
            and [Automatic parallel partitioning](/polars-bio/features/#automatic-parallel-partitioning) for details on parallel reads and supported compression types.

        Parameters:
            path: The path to the FASTQ file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the FASTQ file. If not specified, it will be detected automatically based on the file extension. BGZF and GZIP compressions are supported ('bgz', 'gz').
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
        """
        return IOOperations.scan_fastq(
            path,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            compression_type,
            projection_pushdown,
        ).collect()

    @staticmethod
    def scan_fastq(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
    ) -> pl.LazyFrame:
        """
        Lazily read a FASTQ file into a LazyFrame.

        !!! hint "Parallelism & Compression"
            See [File formats support](/polars-bio/features/#file-formats-support),
            [Compression](/polars-bio/features/#compression),
            and [Automatic parallel partitioning](/polars-bio/features/#automatic-parallel-partitioning) for details on parallel reads and supported compression types.

        Parameters:
            path: The path to the FASTQ file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the FASTQ file. If not specified, it will be detected automatically based on the file extension. BGZF and GZIP compressions are supported ('bgz', 'gz').
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
        """
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        fastq_read_options = FastqReadOptions(
            object_storage_options=object_storage_options,
        )
        read_options = ReadOptions(fastq_read_options=fastq_read_options)
        return _read_file(path, InputFormat.Fastq, read_options, projection_pushdown)

    @staticmethod
    def read_pairs(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Read a Pairs (Hi-C) file into a DataFrame.

        The Pairs format (4DN project) stores chromatin contact data with columns:
        readID, chr1, pos1, chr2, pos2, strand1, strand2.

        !!! hint "Parallelism & Indexed Reads"
            Indexed parallel reads and predicate pushdown are automatic when a TBI index
            is present. See [File formats support](/polars-bio/features/#file-formats-support)
            and [Indexed reads](/polars-bio/features/#indexed-reads-predicate-pushdown) for details.

        Parameters:
            path: The path to the Pairs file (.pairs, .pairs.gz, .pairs.bgz).
            chunk_size: The size in MB of a chunk when reading from an object store.
            concurrent_fetches: The number of concurrent fetches when reading from an object store.
            allow_anonymous: Whether to allow anonymous access to object storage.
            enable_request_payer: Whether to enable request payer for object storage.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type. If not specified, it will be detected automatically.
            projection_pushdown: Enable column projection pushdown to optimize query performance.
            predicate_pushdown: Enable predicate pushdown using index files (TBI) for efficient region-based filtering. Index files are auto-discovered (e.g., `file.pairs.gz.tbi`). Only simple predicates are pushed down (equality, comparisons, IN); complex predicates are filtered client-side. Correctness is always guaranteed.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
        """
        lf = IOOperations.scan_pairs(
            path,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            compression_type,
            projection_pushdown,
            predicate_pushdown,
            use_zero_based,
        )
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_pairs(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a Pairs (Hi-C) file into a LazyFrame.

        The Pairs format (4DN project) stores chromatin contact data with columns:
        readID, chr1, pos1, chr2, pos2, strand1, strand2.

        !!! hint "Parallelism & Indexed Reads"
            Indexed parallel reads and predicate pushdown are automatic when a TBI index
            is present. See [File formats support](/polars-bio/features/#file-formats-support)
            and [Indexed reads](/polars-bio/features/#indexed-reads-predicate-pushdown) for details.

        Parameters:
            path: The path to the Pairs file (.pairs, .pairs.gz, .pairs.bgz).
            chunk_size: The size in MB of a chunk when reading from an object store.
            concurrent_fetches: The number of concurrent fetches when reading from an object store.
            allow_anonymous: Whether to allow anonymous access to object storage.
            enable_request_payer: Whether to enable request payer for object storage.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type. If not specified, it will be detected automatically.
            projection_pushdown: Enable column projection pushdown to optimize query performance.
            predicate_pushdown: Enable predicate pushdown using index files (TBI) for efficient region-based filtering. Index files are auto-discovered (e.g., `file.pairs.gz.tbi`). Only simple predicates are pushed down (equality, comparisons, IN); complex predicates are filtered client-side. Correctness is always guaranteed.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
        """
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        zero_based = _resolve_zero_based(use_zero_based)
        pairs_read_options = PairsReadOptions(
            object_storage_options=object_storage_options,
            zero_based=zero_based,
        )
        read_options = ReadOptions(pairs_read_options=pairs_read_options)
        return _read_file(
            path,
            InputFormat.Pairs,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def read_bgen(
        path: str,
        genotype_output: str = "probability",
        probability_layout: str = "nested",
        samples: Union[list[str], None] = None,
        genotype_fields: Union[list[str], None] = None,
        sample_path: Union[str, None] = None,
        bgi_path: Union[str, None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Read a BGEN file into a DataFrame.

        One row is one BGEN variant. Encoded alleles stay ordered in `alleles`
        and are not given reference/alternate semantics.

        Parameters:
            path: The path to the BGEN file. The path must end in `.bgen`.
            genotype_output: Genotype representation. `"probability"` (default) keeps every format-defined state in `genotypes.GP`. `"dosage"` emits `genotypes.DS`, the expected copy count of `alleles[1]`, and rejects multiallelic variants.
            probability_layout: How probability states are stored. `"nested"` (default) gives each sample a variable-length list and reads every BGEN file. `"fixed"` gives each sample a fixed-width list, dropping the per-sample offsets that are about a quarter of the emitted probability bytes for a diploid biallelic cohort; it requires every variant to store the same number of states and rejects a file that mixes them. Ignored when `genotype_output="dosage"`.
            samples: Sample identifiers to emit, in requested order. If *None*, all samples are emitted in file order.
            genotype_fields: Children of the `genotypes` struct to emit, from the output mode's value child — `"DS"` for dosage, `"GP"` for probability — and `"PLOIDY"`, in the requested order. If *None*, all of them are emitted. `"PLOIDY"` is a byte per genotype, 2.53 GB on a whole 1000 Genomes chromosome 22, and a NumPy view of the result keeps the whole struct alive, so pass `["DS"]` when only the dosages are wanted.
            sample_path: An explicit Oxford `.sample` companion. Used only when the BGEN has no embedded sample identifiers.
            bgi_path: An explicit `.bgi` index. A neighbouring `file.bgen.bgi` is discovered automatically.
            chunk_size: The size in MB of a chunk when reading from an object store.
            concurrent_fetches: The number of concurrent fetches when reading from an object store.
            allow_anonymous: Whether to allow anonymous access to object storage.
            enable_request_payer: Whether to enable request payer for object storage.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression override. BGEN block compression is read from the file header.
            projection_pushdown: Enable column projection pushdown. Metadata-only scans do not read or decompress probability blocks.
            predicate_pushdown: Use a `.bgi` index for `chrom`, `rsid`, `id`, `start`, and `end` predicate pushdown when one is available.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration.

        !!! note
            BGEN is input-only.
        """
        lf = IOOperations.scan_bgen(
            path=path,
            genotype_output=genotype_output,
            probability_layout=probability_layout,
            samples=samples,
            genotype_fields=genotype_fields,
            sample_path=sample_path,
            bgi_path=bgi_path,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
            projection_pushdown=projection_pushdown,
            predicate_pushdown=predicate_pushdown,
            use_zero_based=use_zero_based,
        )
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_bgen(
        path: str,
        genotype_output: str = "probability",
        probability_layout: str = "nested",
        samples: Union[list[str], None] = None,
        genotype_fields: Union[list[str], None] = None,
        sample_path: Union[str, None] = None,
        bgi_path: Union[str, None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a BGEN file into a LazyFrame.

        BGI range pushdown, projection pushdown, and configured input partition
        parallelism are preserved. See `read_bgen` for the parameters.
        """
        _validate_bgen_genotype_output(genotype_output)
        _validate_bgen_probability_layout(probability_layout)
        _validate_bgen_genotype_fields(genotype_fields)
        _validate_bgen_input_path(path)
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        zero_based = _resolve_zero_based(use_zero_based)
        bgen_read_options = BgenReadOptions(
            object_storage_options=object_storage_options,
            genotype_output=genotype_output,
            probability_layout=probability_layout,
            samples=samples,
            genotype_fields=genotype_fields,
            sample_path=sample_path,
            bgi_path=bgi_path,
            zero_based=zero_based,
        )
        read_options = ReadOptions(bgen_read_options=bgen_read_options)
        return _read_file(
            path,
            InputFormat.Bgen,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def read_pgen(
        path: str,
        genotype_fields: Sequence[str] = ("GT",),
        samples: Union[list[str], None] = None,
        missing_sample_policy: str = "error",
        psam_id_mode: str = "iid",
        pvar_path: Union[str, None] = None,
        psam_path: Union[str, None] = None,
        pgi_path: Union[str, None] = None,
        max_range_gap: Union[int, None] = None,
        max_range_bytes: Union[int, None] = None,
        batch_soft_byte_limit: Union[int, None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Read a PLINK 2 PGEN fileset into a DataFrame.

        One row is one PVAR variant. The `.pvar` and `.psam` companions are
        discovered from the `.pgen` basename.

        Parameters:
            path: The path to the PGEN file. The path must end in `.pgen`. A neighbouring `.pvar` (or `.pvar.zst`) and `.psam` are discovered automatically.
            genotype_fields: Genotype children to emit, from `"GT"`, `"ALT_COUNT"`, `"PHASED"`, `"DS"`, `"DS_STORED"`, and `"HDS"`, in the requested order. Defaults to `("GT",)`. Note this narrows the provider default, which emits all of them. `"ALT_COUNT"` is the hardcall ALT allele count as `int8`, one byte per genotype rather than the four `"DS"` uses; prefer it when the fileset stores only hardcalls.
            samples: Sample identifiers to emit, in requested order. If *None*, all samples are emitted in PSAM order.
            missing_sample_policy: `"error"` (default) rejects a requested sample name absent from the PSAM; `"ignore"` omits it from the selection.
            psam_id_mode: How selectable sample names are built from PSAM identifiers. `"iid"` (default) uses IID alone and rejects duplicates; `"fid_iid"` uses `FID:IID`; `"fid_iid_sid"` uses `FID:IID:SID`. A PSAM without FID or SID columns defaults those parts to `"0"`.
            pvar_path: An explicit `.pvar` companion. A neighbouring `.pvar` then `.pvar.zst` is discovered otherwise.
            psam_path: An explicit `.psam` companion. The shared-basename `.psam` is used otherwise.
            pgi_path: An explicit `.pgi` index, for a PGEN that uses an external index.
            max_range_gap: The largest run of unselected bytes bridged when coalescing reads, in bytes. The provider default is 0, which never bridges a gap and issues one read per contiguous run of selected variants. Raising it trades wasted bytes for fewer requests, which matters most on object storage. If *None*, the provider default is used.
            max_range_bytes: The largest coalesced read, in bytes. If *None*, the provider default is used.
            batch_soft_byte_limit: A soft target for genotype bytes in one RecordBatch. If *None*, the provider default is used.
            chunk_size: The size in MB of a chunk when reading from an object store.
            concurrent_fetches: The number of concurrent fetches when reading from an object store.
            allow_anonymous: Whether to allow anonymous access to object storage.
            enable_request_payer: Whether to enable request payer for object storage.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression override. PGEN record compression is read from the file header.
            projection_pushdown: Enable column projection pushdown. Metadata-only scans do not read genotype records.
            predicate_pushdown: Push `chrom`, `id`, `start`, and `end` predicates into variant selection.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration.

        !!! note
            PGEN is input-only.
        """
        lf = IOOperations.scan_pgen(
            path=path,
            genotype_fields=genotype_fields,
            samples=samples,
            missing_sample_policy=missing_sample_policy,
            psam_id_mode=psam_id_mode,
            pvar_path=pvar_path,
            psam_path=psam_path,
            pgi_path=pgi_path,
            max_range_gap=max_range_gap,
            max_range_bytes=max_range_bytes,
            batch_soft_byte_limit=batch_soft_byte_limit,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
            projection_pushdown=projection_pushdown,
            predicate_pushdown=predicate_pushdown,
            use_zero_based=use_zero_based,
        )
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_pgen(
        path: str,
        genotype_fields: Sequence[str] = ("GT",),
        samples: Union[list[str], None] = None,
        missing_sample_policy: str = "error",
        psam_id_mode: str = "iid",
        pvar_path: Union[str, None] = None,
        psam_path: Union[str, None] = None,
        pgi_path: Union[str, None] = None,
        max_range_gap: Union[int, None] = None,
        max_range_bytes: Union[int, None] = None,
        batch_soft_byte_limit: Union[int, None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a PLINK 2 PGEN fileset into a LazyFrame.

        Projection pushdown and configured input partition parallelism are
        preserved. See `read_pgen` for the parameters.
        """
        _validate_pgen_input_path(path)
        _validate_pgen_genotype_fields(genotype_fields)
        _validate_pgen_psam_id_mode(psam_id_mode)
        _validate_pgen_missing_sample_policy(missing_sample_policy)
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        zero_based = _resolve_zero_based(use_zero_based)
        pgen_read_options = PgenReadOptions(
            object_storage_options=object_storage_options,
            genotype_fields=list(genotype_fields),
            zero_based=zero_based,
            samples=samples,
            missing_sample_policy=missing_sample_policy,
            psam_id_mode=psam_id_mode,
            pvar_path=pvar_path,
            psam_path=psam_path,
            pgi_path=pgi_path,
            max_range_gap=max_range_gap,
            max_range_bytes=max_range_bytes,
            batch_soft_byte_limit=batch_soft_byte_limit,
        )
        read_options = ReadOptions(pgen_read_options=pgen_read_options)
        return _read_file(
            path,
            InputFormat.Pgen,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def read_pgen_matrix(
        path: str,
        field: str = "ALT_COUNT",
        samples: Union[list[str], None] = None,
        missing: Union[int, float, None] = None,
        missing_sample_policy: str = "error",
        psam_id_mode: str = "iid",
        pvar_path: Union[str, None] = None,
        psam_path: Union[str, None] = None,
        pgi_path: Union[str, None] = None,
        max_range_gap: Union[int, None] = None,
        max_range_bytes: Union[int, None] = None,
        batch_soft_byte_limit: Union[int, None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        use_zero_based: Optional[bool] = None,
        copy_threads: Union[int, None] = None,
    ) -> "PgenMatrix":
        """
        Read one genotype field of a PGEN fileset into a dense NumPy matrix.

        The whole-cohort matrix is what association testing, PCA, and relatedness
        pipelines consume, and going through a DataFrame to get one costs a
        second full copy of every value: the scan builds Arrow batches, and
        something then has to consolidate them into a contiguous array. The
        decoder here writes genotypes at their final address instead, so they
        are written once.

        On chromosome 22 of 1000 Genomes (993,881 variants x 2,548 samples) the
        `DS` matrix takes **1.29 s** and 12.6 GB, against 3.2 s and 22.3 GB
        through `read_pgen`. `ALT_COUNT` takes 0.70 s. Both are faster than
        PLINK 2's own `pgenlib` at one thread, and roughly three times faster
        again given eight partitions.

        Parameters:
            path: The path to the PGEN file. The path must end in `.pgen`.
            field: The genotype field to materialize: `"ALT_COUNT"` (`int8` hardcall ALT allele count) or `"DS"` (`float32` ALT dosage). Fields with more than one value per sample — `"GT"`, `"HDS"` — have no dense matrix form, and `"DS_STORED"` has no decoder on this path; read those with `read_pgen`.
            samples: Sample identifiers to emit, in requested order. If *None*, all samples are emitted in PSAM order. The matrix has one column per selected sample.
            missing: The value written where a genotype is missing. Defaults to `-9` for `"ALT_COUNT"`, matching PLINK's sentinel, and to NaN for the float fields.
            missing_sample_policy: `"error"` (default) rejects a requested sample name absent from the PSAM; `"ignore"` omits it.
            psam_id_mode: How selectable sample names are built from PSAM identifiers. See `read_pgen`.
            pvar_path: An explicit `.pvar` companion.
            psam_path: An explicit `.psam` companion.
            pgi_path: An explicit `.pgi` index.
            max_range_gap: The largest run of unselected bytes bridged when coalescing reads.
            max_range_bytes: The largest coalesced read, in bytes.
            batch_soft_byte_limit: A soft target for genotype bytes in one RecordBatch.
            chunk_size: The size in MB of a chunk when reading from an object store.
            concurrent_fetches: The number of concurrent fetches when reading from an object store.
            allow_anonymous: Whether to allow anonymous access to object storage.
            enable_request_payer: Whether to enable request payer for object storage.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression override.
            use_zero_based: If True, report 0-based positions. If False, 1-based. If None (default), uses the global configuration.
            copy_threads: How many threads decode into the result. They write disjoint row ranges, so they never contend. If *None* (default), this follows `datafusion.execution.target_partitions`, so a single-partition read stays single-threaded end to end.

        Returns:
            A `PgenMatrix` of `values` (a C-contiguous `(variants, samples)`
            array), `positions` (one per row), and `sample_names` (one per
            column).

        !!! note
            Rows are in PVAR order at every partition count: each variant is
            written at its own row index rather than in the order it finished
            decoding. This differs from `read_pgen`, whose row order may
            interleave above one partition.

        Example:
            ```python
            import polars_bio as pb

            matrix = pb.read_pgen_matrix("cohort.pgen", field="ALT_COUNT")
            matrix.values.shape       # (variants, samples)
            matrix.values.mean(axis=1)  # per-variant ALT frequency * 2
            ```
        """
        # Imported here rather than at module scope: NumPy is not a polars-bio
        # dependency, and only this function needs it.
        try:
            import numpy as np
        except ImportError as error:  # pragma: no cover - environment-dependent
            raise ImportError(
                "read_pgen_matrix returns NumPy arrays and needs NumPy installed"
            ) from error

        dtypes = {
            "ALT_COUNT": np.int8,
            "DS": np.float32,
        }
        if field not in dtypes:
            raise ValueError(
                f"read_pgen_matrix supports {sorted(dtypes)}, not {field!r}. "
                "Fields with more than one value per sample have no dense matrix "
                "form, and DS_STORED has no decoder on this path; read them with "
                "read_pgen."
            )
        dtype = np.dtype(dtypes[field])
        if missing is None:
            missing = -9 if dtype == np.int8 else np.nan
        elif field == "ALT_COUNT":
            # The sentinel crosses into Rust as an f64 and is written with
            # `as i8`, which saturates out-of-range values and turns NaN into
            # 0 — silently indistinguishable from a homozygous-reference call.
            # Reject what that cast would corrupt rather than write it.
            sentinel = float(missing)
            if (
                not np.isfinite(sentinel)
                or sentinel != int(sentinel)
                or not -128 <= sentinel <= 127
            ):
                raise ValueError(
                    f"missing={missing!r} is not representable as the int8 "
                    "ALT_COUNT matrix stores; pass a whole number in "
                    "[-128, 127] (PLINK's own sentinel is -9)"
                )

        # Built directly rather than through `scan_pgen`, because this path does
        # not register a table: the reader opens the fileset itself and answers
        # shape, names and positions from it, so the PVAR is parsed once.
        decode_options = PgenReadOptions(
            object_storage_options=PyObjectStorageOptions(
                allow_anonymous=allow_anonymous,
                enable_request_payer=enable_request_payer,
                chunk_size=chunk_size,
                concurrent_fetches=concurrent_fetches,
                max_retries=max_retries,
                timeout=timeout,
                compression_type=compression_type,
            ),
            genotype_fields=[field],
            zero_based=_resolve_zero_based(use_zero_based),
            samples=samples,
            missing_sample_policy=missing_sample_policy,
            psam_id_mode=psam_id_mode,
            pvar_path=pvar_path,
            psam_path=psam_path,
            pgi_path=pgi_path,
            max_range_gap=max_range_gap,
            max_range_bytes=max_range_bytes,
            batch_soft_byte_limit=batch_soft_byte_limit,
        )

        from polars_bio.context import get_option
        from polars_bio.polars_bio import PgenMatrixReader

        reader = PgenMatrixReader(path, decode_options)
        variants, columns = reader.shape()

        if copy_threads is None:
            try:
                copy_threads = int(get_option("datafusion.execution.target_partitions"))
            except (TypeError, ValueError):
                copy_threads = 1
        copy_threads = max(1, int(copy_threads))

        values = np.empty((variants, columns), dtype=dtype)
        # The array itself is handed over, not its address: the reader checks
        # dtype, C-contiguity, writability and length at the boundary, which is
        # the only place a caller cannot route around.
        reader.read_into(field, values, copy_threads, float(missing))

        positions = np.asarray(reader.positions(), dtype=np.int64)
        if positions.shape[0] != variants:
            raise RuntimeError(
                f"PGEN reported {variants} variants but {positions.shape[0]} positions"
            )
        return PgenMatrix(
            values=values, positions=positions, sample_names=list(reader.sample_names())
        )

    @staticmethod
    def read_bgen_matrix(
        path: str,
        samples: Union[list[str], None] = None,
        missing: Union[float, None] = None,
        sample_path: Union[str, None] = None,
        bgi_path: Union[str, None] = None,
        threads: Union[int, None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        use_zero_based: Optional[bool] = None,
    ) -> PgenMatrix:
        """
        Read a BGEN's ALT dosages into a dense NumPy matrix.

        The counterpart of `read_pgen_matrix`. `scan_bgen` returns Arrow batches
        that a caller wanting one array must then consolidate, and on a whole
        chromosome that consolidation is a serial pass over 10 GB — it does not
        parallelise, so it becomes the ceiling as partitions are added. This
        decodes each variant at its final address instead, which on chromosome
        22 scales 6.0x from one thread to eight against the Arrow path's 4.2x.

        Dosage only: BGEN probabilities are variable width and have no single
        dense shape. Use `scan_bgen(genotype_output="probability")` for those.

        Parameters:
            path: The path to the BGEN file. The path must end in `.bgen`.
            samples: Sample identifiers to emit, in requested order. If *None*, all samples are emitted in file order.
            missing: Written where a sample has no called genotype. Defaults to `NaN`.
            sample_path: An explicit Oxford `.sample` companion, used only when the BGEN has no embedded sample identifiers.
            bgi_path: An explicit `.bgi` index. A neighbouring `file.bgen.bgi` is discovered automatically.
            threads: Decoder threads. Defaults to the configured `datafusion.execution.target_partitions`.
            use_zero_based: Output coordinate convention for the returned positions.

        Example:
            ```python
            import polars_bio as pb

            matrix = pb.read_bgen_matrix("chr22.bgen")
            matrix.values.shape          # (variants, samples), float32
            matrix.values.mean(axis=1)   # per-variant mean dosage
            ```
        """
        # Imported here rather than at module scope: NumPy is not a polars-bio
        # dependency, and only the matrix readers need it.
        try:
            import numpy as np
        except ImportError as error:  # pragma: no cover - environment-dependent
            raise ImportError(
                "read_bgen_matrix returns NumPy arrays and needs NumPy installed"
            ) from error

        _validate_bgen_input_path(path)
        dtype = np.dtype(np.float32)
        if missing is None:
            missing = np.nan

        decode_options = BgenReadOptions(
            object_storage_options=PyObjectStorageOptions(
                allow_anonymous=allow_anonymous,
                enable_request_payer=enable_request_payer,
                chunk_size=chunk_size,
                concurrent_fetches=concurrent_fetches,
                max_retries=max_retries,
                timeout=timeout,
                compression_type=compression_type,
            ),
            genotype_output="dosage",
            probability_layout="nested",
            samples=samples,
            genotype_fields=["DS"],
            sample_path=sample_path,
            bgi_path=bgi_path,
            zero_based=_resolve_zero_based(use_zero_based),
        )

        from polars_bio.context import get_option
        from polars_bio.polars_bio import BgenMatrixReader

        reader = BgenMatrixReader(path, decode_options)
        variants, columns = reader.shape()

        if threads is None:
            try:
                threads = int(get_option("datafusion.execution.target_partitions"))
            except (TypeError, ValueError):
                threads = 1
        threads = max(1, int(threads))

        values = np.empty((variants, columns), dtype=dtype)
        # As in `read_pgen_matrix`: the array goes across, not its address, and
        # the reader validates it before decoding.
        reader.read_into(values, threads, float(missing))

        positions = np.asarray(reader.positions(), dtype=np.int64)
        if positions.shape[0] != variants:
            raise RuntimeError(
                f"BGEN reported {variants} variants but {positions.shape[0]} positions"
            )
        return PgenMatrix(
            values=values, positions=positions, sample_names=list(reader.sample_names())
        )

    @staticmethod
    def read_bed(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Read a BED file into a DataFrame.

        Parameters:
            path: The path to the BED file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the BED file. If not specified, it will be detected automatically based on the file extension. BGZF compressions is supported ('bgz').
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! Note
            Only **BED4** format is supported. It extends the basic BED format (BED3) by adding a name field, resulting in four columns: chromosome, start position, end position, and name.
            Also unlike other text formats, **GZIP** compression is not supported.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
        """
        lf = IOOperations.scan_bed(
            path,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            compression_type,
            projection_pushdown,
            use_zero_based,
        )
        # Get metadata before collecting (polars-config-meta doesn't preserve through collect)
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        # Set metadata on the collected DataFrame
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_bed(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a BED file into a LazyFrame.

        Parameters:
            path: The path to the BED file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the BED file. If not specified, it will be detected automatically based on the file extension. BGZF compressions is supported ('bgz').
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! Note
            Only **BED4** format is supported. It extends the basic BED format (BED3) by adding a name field, resulting in four columns: chromosome, start position, end position, and name.
            Also unlike other text formats, **GZIP** compression is not supported.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
        """
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        zero_based = _resolve_zero_based(use_zero_based)
        bed_read_options = BedReadOptions(
            object_storage_options=object_storage_options,
            zero_based=zero_based,
        )
        read_options = ReadOptions(bed_read_options=bed_read_options)
        return _read_file(
            path,
            InputFormat.Bed,
            read_options,
            projection_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def read_bigwig(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Read a BigWig file into a DataFrame.

        BigWig rows are exposed as ``chrom``, ``start``, ``end``, and ``value``.

        Parameters:
            path: The path to the BigWig file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the BigWig file. If not specified, it will be detected automatically based on the file extension.
            projection_pushdown: Enable column projection pushdown optimization. When True, only requested columns are processed at the DataFusion execution level, improving performance and reducing memory usage.
            predicate_pushdown: Enable predicate pushdown on the genomic coordinate columns so range filters are evaluated at the DataFusion execution level.
            use_zero_based: Coordinate system override. BigWig is natively 0-based half-open; set to *False* to emit 1-based closed coordinates, or *None* to use the global default.
        """
        lf = IOOperations.scan_bigwig(
            path,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            compression_type,
            projection_pushdown,
            predicate_pushdown,
            use_zero_based,
        )
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_bigwig(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a BigWig file into a LazyFrame.

        BigWig is natively 0-based half-open. Set ``use_zero_based=False`` to emit
        1-based closed coordinates.

        Parameters:
            path: The path to the BigWig file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the BigWig file. If not specified, it will be detected automatically based on the file extension.
            projection_pushdown: Enable column projection pushdown optimization. When True, only requested columns are processed at the DataFusion execution level, improving performance and reducing memory usage.
            predicate_pushdown: Enable predicate pushdown on the genomic coordinate columns so range filters are evaluated at the DataFusion execution level.
            use_zero_based: Coordinate system override. BigWig is natively 0-based half-open; set to *False* to emit 1-based closed coordinates, or *None* to use the global default.
        """
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        zero_based = _resolve_zero_based(use_zero_based)
        bigwig_read_options = BigWigReadOptions(
            object_storage_options=object_storage_options,
            zero_based=zero_based,
        )
        read_options = ReadOptions(bigwig_read_options=bigwig_read_options)
        return _read_file(
            path,
            InputFormat.BigWig,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def read_bigbed(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        schema: str = "auto",
    ) -> pl.DataFrame:
        """
        Read a BigBed file into a DataFrame.

        ``schema="auto"`` uses supported autoSQL fields when available.
        ``schema="rest"`` exposes the raw trailing fields in ``rest``.

        Parameters:
            path: The path to the BigBed file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the BigBed file. If not specified, it will be detected automatically based on the file extension.
            projection_pushdown: Enable column projection pushdown optimization. When True, only requested columns are processed at the DataFusion execution level, improving performance and reducing memory usage.
            predicate_pushdown: Enable predicate pushdown on the genomic coordinate columns so range filters are evaluated at the DataFusion execution level.
            use_zero_based: Coordinate system override. BigBed is natively 0-based half-open; set to *False* to emit 1-based closed coordinates, or *None* to use the global default.
            schema: Schema mode. ``"auto"`` exposes the supported autoSQL fields when available; ``"rest"`` exposes the raw trailing fields in a single ``rest`` column.
        """
        lf = IOOperations.scan_bigbed(
            path,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            compression_type,
            projection_pushdown,
            predicate_pushdown,
            use_zero_based,
            schema,
        )
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_bigbed(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        schema: str = "auto",
    ) -> pl.LazyFrame:
        """
        Lazily read a BigBed file into a LazyFrame.

        BigBed is natively 0-based half-open. Set ``use_zero_based=False`` to emit
        1-based closed coordinates.

        Parameters:
            path: The path to the BigBed file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the BigBed file. If not specified, it will be detected automatically based on the file extension.
            projection_pushdown: Enable column projection pushdown optimization. When True, only requested columns are processed at the DataFusion execution level, improving performance and reducing memory usage.
            predicate_pushdown: Enable predicate pushdown on the genomic coordinate columns so range filters are evaluated at the DataFusion execution level.
            use_zero_based: Coordinate system override. BigBed is natively 0-based half-open; set to *False* to emit 1-based closed coordinates, or *None* to use the global default.
            schema: Schema mode. ``"auto"`` exposes the supported autoSQL fields when available; ``"rest"`` exposes the raw trailing fields in a single ``rest`` column.
        """
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        zero_based = _resolve_zero_based(use_zero_based)
        bigbed_read_options = BigBedReadOptions(
            object_storage_options=object_storage_options,
            zero_based=zero_based,
            schema=_normalize_bigbed_schema_mode(schema),
        )
        read_options = ReadOptions(bigbed_read_options=bigbed_read_options)
        return _read_file(
            path,
            InputFormat.BigBed,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def read_cool(
        path: str,
        resolution: Optional[int] = None,
        join_bins: bool = True,
        include_weights: bool = False,
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Read a Cooler (`.cool`/`.mcool`) Hi-C contact matrix into a DataFrame.

        See `scan_cool` for parameter semantics.

        Parameters:
            path: The path to the `.cool`/`.mcool` file, or a cooler URI (`file.mcool::/resolutions/10000`).
            resolution: Bin size selecting an `.mcool` data collection. Optional for `.cool` files and single-resolution `.mcool` files.
            join_bins: If *True* (default), join pixels with bin coordinates (`chrom1`, `start1`, `end1`, `chrom2`, `start2`, `end2`, `count`); if *False*, return the raw COO triple (`bin1_id`, `bin2_id`, `count`).
            include_weights: If *True*, expose balancing weights as `weight1`/`weight2` (requires a balanced cooler).
            projection_pushdown: Enable column projection pushdown optimization.
            predicate_pushdown: Enable predicate pushdown on the first-axis genomic columns (`chrom1`, `start1`, `end1`) so range filters prune pixel row ranges through the cooler indexes.
            use_zero_based: Coordinate system override. Cooler is natively 0-based half-open; set to *False* to emit 1-based closed coordinates, or *None* to use the global default.
        """
        lf = IOOperations.scan_cool(
            path,
            resolution=resolution,
            join_bins=join_bins,
            include_weights=include_weights,
            projection_pushdown=projection_pushdown,
            predicate_pushdown=predicate_pushdown,
            use_zero_based=use_zero_based,
        )
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_cool(
        path: str,
        resolution: Optional[int] = None,
        join_bins: bool = True,
        include_weights: bool = False,
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a Cooler (`.cool`/`.mcool`) Hi-C contact matrix into a LazyFrame.

        One row per stored pixel (upper-triangle contact), joined with bin
        coordinates by default. `.mcool` files store one data collection per
        resolution: select one with ``resolution`` or the cooler URI syntax
        ``file.mcool::/resolutions/10000``; an `.mcool` with several
        resolutions and no selection raises an error listing the available
        ones. Only local filesystem paths are supported.

        Cooler is natively 0-based half-open. Set ``use_zero_based=False`` to
        emit 1-based closed coordinates.

        Parameters:
            path: The path to the `.cool`/`.mcool` file, or a cooler URI (`file.mcool::/resolutions/10000`).
            resolution: Bin size selecting an `.mcool` data collection. Optional for `.cool` files and single-resolution `.mcool` files.
            join_bins: If *True* (default), join pixels with bin coordinates (`chrom1`, `start1`, `end1`, `chrom2`, `start2`, `end2`, `count`); if *False*, return the raw COO triple (`bin1_id`, `bin2_id`, `count`).
            include_weights: If *True*, expose balancing weights as `weight1`/`weight2` (requires a balanced cooler).
            projection_pushdown: Enable column projection pushdown optimization. Only HDF5 datasets required by the requested columns are read, and `count(*)` is served from the cooler index without touching pixel data.
            predicate_pushdown: Enable predicate pushdown on the first-axis genomic columns (`chrom1`, `start1`, `end1`) so range filters prune pixel row ranges through the cooler indexes.
            use_zero_based: Coordinate system override. Cooler is natively 0-based half-open; set to *False* to emit 1-based closed coordinates, or *None* to use the global default.

        !!! Example
            ```python
            import polars as pl
            import polars_bio as pb

            pb.scan_cool("contacts.mcool", resolution=10000).filter(
                pl.col("chrom1") == "chr1"
            ).collect()
            ```
        """
        zero_based = _resolve_zero_based(use_zero_based)
        cool_read_options = CoolReadOptions(
            resolution=resolution,
            join_bins=join_bins,
            include_weights=include_weights,
            zero_based=zero_based,
        )
        read_options = ReadOptions(cool_read_options=cool_read_options)
        return _read_file(
            path,
            InputFormat.Cool,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def describe_cool(path: str) -> pl.DataFrame:
        """
        Describe the data collections of a Cooler (`.cool`/`.mcool`) file.

        Returns one row per stored data collection (one for `.cool`, one per
        resolution for `.mcool`) with `group_path`, `resolution` (bin size),
        `bin_type`, `format_version`, `assembly`, `nbins`, `nnz`, `sum`, and
        `nchroms`, read from file metadata without scanning pixel data. `sum`
        is Int64/UInt64 for integer-count collections and Float64 for
        float-count collections. Files mixing those storage classes use an
        exact Decimal column (or an exact string for values outside Arrow's
        Decimal128 range), preserving wide integer totals alongside fractions.

        Parameters:
            path: The path to the `.cool`/`.mcool` file, or a cooler URI
                (`file.mcool::/resolutions/10000`) to describe a single data
                collection.
        """
        return py_describe_cool(ctx, path).to_polars()

    @staticmethod
    def read_table(path: str, schema: Dict = None, **kwargs) -> pl.DataFrame:
        """
         Read a tab-delimited (i.e. BED) file into a Polars DataFrame.
         Tries to be compatible with Bioframe's [read_table](https://bioframe.readthedocs.io/en/latest/guide-io.html)
         but faster. Schema should follow the Bioframe's schema [format](https://github.com/open2c/bioframe/blob/2b685eebef393c2c9e6220dcf550b3630d87518e/bioframe/io/schemas.py#L174).

        Parameters:
            path: The path to the file.
            schema: Schema should follow the Bioframe's schema [format](https://github.com/open2c/bioframe/blob/2b685eebef393c2c9e6220dcf550b3630d87518e/bioframe/io/schemas.py#L174).
        """
        return IOOperations.scan_table(path, schema, **kwargs).collect()

    @staticmethod
    def scan_table(path: str, schema: Dict = None, **kwargs) -> pl.LazyFrame:
        """
         Lazily read a tab-delimited (i.e. BED) file into a Polars LazyFrame.
         Tries to be compatible with Bioframe's [read_table](https://bioframe.readthedocs.io/en/latest/guide-io.html)
         but faster and lazy. Schema should follow the Bioframe's schema [format](https://github.com/open2c/bioframe/blob/2b685eebef393c2c9e6220dcf550b3630d87518e/bioframe/io/schemas.py#L174).

        Parameters:
            path: The path to the file.
            schema: Schema should follow the Bioframe's schema [format](https://github.com/open2c/bioframe/blob/2b685eebef393c2c9e6220dcf550b3630d87518e/bioframe/io/schemas.py#L174).
        """
        df = pl.scan_csv(path, separator="\t", has_header=False, **kwargs)
        if schema is not None:
            columns = SCHEMAS[schema]
            if len(columns) != len(df.collect_schema()):
                raise ValueError(
                    f"Schema incompatible with the input. Expected {len(columns)} columns in a schema, got {len(df.collect_schema())} in the input data file. Please provide a valid schema."
                )
            for i, c in enumerate(columns):
                df = df.rename({f"column_{i + 1}": c})
        return df

    @staticmethod
    def describe_vcf(
        path: str,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
    ) -> pl.DataFrame:
        """
        Describe a text VCF INFO and FORMAT schema.

        Parameters:
            path: The path to the text VCF file.
            allow_anonymous: Whether to allow anonymous access to object storage (GCS and S3 supported).
            enable_request_payer: Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            compression_type: The compression type of the VCF file. If not specified, it will be detected automatically..
        """
        _validate_variant_input_path(path, "vcf", operation="describe")
        return IOOperations._describe_variant(
            path,
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            compression_type=compression_type,
        )

    @staticmethod
    def describe_bcf(
        path: str,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
    ) -> pl.DataFrame:
        """Describe a BCF INFO and FORMAT schema.

        Parameters:
            path: The path to the BCF file. The path must end in `.bcf`.
            allow_anonymous: Whether to allow anonymous access to object storage (GCS and S3 supported).
            enable_request_payer: Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            compression_type: The compression override. The default detects BCF automatically.
        """
        _validate_variant_input_path(path, "bcf", operation="describe")
        return IOOperations._describe_variant(
            path,
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            compression_type=compression_type,
        )

    @staticmethod
    def _describe_variant(
        path: str,
        allow_anonymous: bool,
        enable_request_payer: bool,
        compression_type: str,
    ) -> pl.DataFrame:
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=8,
            concurrent_fetches=1,
            max_retries=1,
            timeout=10,
            compression_type=compression_type,
        )
        return py_describe_vcf(ctx, path, object_storage_options).to_polars()

    @staticmethod
    def describe_bgen(
        path: str,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
        sample_path: Union[str, None] = None,
        bgi_path: Union[str, None] = None,
    ) -> pl.DataFrame:
        """
        Describe the schema a BGEN file produces.

        BGEN has no INFO/FORMAT header, so instead of a field dictionary this
        returns one row per emitted column, plus the file-level properties the
        provider records in the Arrow schema metadata: the BGEN layout, whether
        a `.bgi` index was used, whether sample identifiers were generated, and
        the coordinate system.

        Parameters:
            path: The path to the BGEN file. The path must end in `.bgen`.
            allow_anonymous: Whether to allow anonymous access to object storage.
            enable_request_payer: Whether to enable request payer for object storage.
            compression_type: The compression override.
            sample_path: An explicit Oxford `.sample` companion, used only when the BGEN has no embedded sample identifiers.
            bgi_path: An explicit `.bgi` index. Pass it for an index stored away from the file, so the reported `index` property reflects the index a read would actually use.

        !!! note
            The reported schema is the one the default `probability_layout="nested"`
            produces, because that layout describes every BGEN file. Reading with
            `probability_layout="fixed"` gives `genotypes.GP` a fixed-width state
            list instead.
        """
        _validate_bgen_input_path(path, operation="describe")
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=8,
            concurrent_fetches=1,
            max_retries=1,
            timeout=10,
            compression_type=compression_type,
        )
        bgen_read_options = BgenReadOptions(
            object_storage_options=object_storage_options,
            genotype_output="probability",
            probability_layout="nested",
            samples=None,
            sample_path=sample_path,
            bgi_path=bgi_path,
            zero_based=_resolve_zero_based(None),
        )
        # Registering under the derived name would deregister and replace a
        # table the caller already registered for the same file, so describe
        # uses a private name and removes it again.
        describe_name = f"_pb_bgen_describe_{uuid4().hex}"
        table = py_register_table(
            ctx,
            path,
            describe_name,
            InputFormat.Bgen,
            ReadOptions(bgen_read_options=bgen_read_options),
        )
        try:
            schema = py_get_table_schema(ctx, table.name)
        finally:
            ctx.deregister_table(table.name)
        metadata = {
            (key.decode() if isinstance(key, bytes) else key): (
                value.decode() if isinstance(value, bytes) else value
            )
            for key, value in (schema.metadata or {}).items()
        }
        described = pl.DataFrame(
            {
                "name": [field.name for field in schema],
                "type": [str(field.type) for field in schema],
            }
        )
        properties = {
            "layout": metadata.get("bio.bgen.layout"),
            "index": metadata.get("bio.bgen.index"),
            "sample_names_synthetic": metadata.get("bio.bgen.sample_names.synthetic"),
            "coordinate_system_zero_based": metadata.get(
                "bio.coordinate_system_zero_based"
            ),
        }
        return described.with_columns(
            [pl.lit(value).alias(name) for name, value in properties.items()]
        )

    @staticmethod
    def describe_pgen(
        path: str,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
        pvar_path: Union[str, None] = None,
        psam_path: Union[str, None] = None,
        pgi_path: Union[str, None] = None,
    ) -> pl.DataFrame:
        """
        Describe the schema a PLINK 2 PGEN fileset produces.

        PGEN has no embedded header, so instead of a field dictionary this
        returns one row per emitted column, plus the file-level properties the
        provider records in the Arrow schema metadata: the storage mode,
        whether the index is embedded or external, the specification baseline,
        and the coordinate system.

        Parameters:
            path: The path to the PGEN file. The path must end in `.pgen`.
            allow_anonymous: Whether to allow anonymous access to object storage.
            enable_request_payer: Whether to enable request payer for object storage.
            compression_type: The compression override.
            pvar_path: An explicit `.pvar` companion.
            psam_path: An explicit `.psam` companion.
            pgi_path: An explicit `.pgi` index, for a PGEN that uses an external index. Without it, such a fileset cannot be opened here at all.

        !!! note
            The reported schema is the one the default `genotype_fields=("GT",)`
            produces. Selecting other genotype fields changes the children of
            the `genotypes` struct.
        """
        _validate_pgen_input_path(path, operation="describe")
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=8,
            concurrent_fetches=1,
            max_retries=1,
            timeout=10,
            compression_type=compression_type,
        )
        pgen_read_options = PgenReadOptions(
            object_storage_options=object_storage_options,
            genotype_fields=["GT"],
            pvar_path=pvar_path,
            psam_path=psam_path,
            pgi_path=pgi_path,
            zero_based=_resolve_zero_based(None),
        )
        # Registering under the derived name would deregister and replace a
        # table the caller already registered for the same file, so describe
        # uses a private name and removes it again.
        describe_name = f"_pb_pgen_describe_{uuid4().hex}"
        table = py_register_table(
            ctx,
            path,
            describe_name,
            InputFormat.Pgen,
            ReadOptions(pgen_read_options=pgen_read_options),
        )
        try:
            schema = py_get_table_schema(ctx, table.name)
        finally:
            ctx.deregister_table(table.name)
        metadata = {
            (key.decode() if isinstance(key, bytes) else key): (
                value.decode() if isinstance(value, bytes) else value
            )
            for key, value in (schema.metadata or {}).items()
        }
        described = pl.DataFrame(
            {
                "name": [field.name for field in schema],
                "type": [str(field.type) for field in schema],
            }
        )
        properties = {
            "storage_mode": metadata.get("bio.pgen.storage_mode"),
            "index": metadata.get("bio.pgen.index"),
            "specification_baseline": metadata.get("bio.pgen.specification_baseline"),
            "coordinate_system_zero_based": metadata.get(
                "bio.coordinate_system_zero_based"
            ),
        }
        return described.with_columns(
            [pl.lit(value).alias(name) for name, value in properties.items()]
        )

    @staticmethod
    def describe_vcf_zarr(path: str) -> pl.DataFrame:
        """
        Describe VCF Zarr INFO and FORMAT schema.

        Parameters:
            path: The path to the local VCF Zarr store directory.
        """
        return py_describe_vcf_zarr(ctx, path).to_polars()

    @staticmethod
    def from_polars(name: str, df: Union[pl.DataFrame, pl.LazyFrame]) -> None:
        """
        Register a Polars DataFrame as a DataFusion table.

        Parameters:
            name: The name of the table.
            df: The Polars DataFrame.
        """
        reader = (
            df.to_arrow()
            if isinstance(df, pl.DataFrame)
            else df.collect().to_arrow().to_reader()
        )
        py_from_polars(ctx, name, reader)

    @staticmethod
    def write_vcf(
        df: Union[pl.DataFrame, pl.LazyFrame],
        path: str,
    ) -> int:
        """
        Write a DataFrame to VCF format.

        Coordinate system is automatically read from DataFrame metadata (set during
        read_vcf). Compression is auto-detected from the file extension.

        Parameters:
            df: The DataFrame or LazyFrame to write.
            path: The output file path. Compression is auto-detected from extension
                  (.vcf.bgz for BGZF, .vcf.gz for GZIP, .vcf for uncompressed).

        Returns:
            The number of rows written.

        !!! Example "Writing VCF files"
            ```python
            import polars_bio as pb

            # Read a VCF file
            df = pb.read_vcf("input.vcf")

            # Write to uncompressed VCF
            pb.write_vcf(df, "output.vcf")

            # Write to BGZF-compressed VCF
            pb.write_vcf(df, "output.vcf.bgz")

            # Write to GZIP-compressed VCF
            pb.write_vcf(df, "output.vcf.gz")
            ```
        """
        return _write_file(df, path, OutputFormat.Vcf)

    @staticmethod
    def sink_vcf(
        lf: pl.LazyFrame,
        path: str,
    ) -> None:
        """
        Streaming write a LazyFrame to VCF format.

        This method executes the LazyFrame immediately and writes the results
        to the specified path. Unlike `write_vcf`, it doesn't return the row count.

        Coordinate system is automatically read from LazyFrame metadata (set during
        scan_vcf). Compression is auto-detected from the file extension.

        Parameters:
            lf: The LazyFrame to write.
            path: The output file path. Compression is auto-detected from extension
                  (.vcf.bgz for BGZF, .vcf.gz for GZIP, .vcf for uncompressed).

        !!! Example "Streaming write VCF"
            ```python
            import polars_bio as pb

            # Lazy read and filter, then sink to VCF
            lf = pb.scan_vcf("large_input.vcf").filter(pl.col("qual") > 30)
            pb.sink_vcf(lf, "filtered_output.vcf.bgz")
            ```
        """
        _write_file(lf, path, OutputFormat.Vcf)

    @staticmethod
    def write_fasta(
        df: Union[pl.DataFrame, pl.LazyFrame],
        path: str,
    ) -> int:
        """
        Write a DataFrame to FASTA format.

        Compression is auto-detected from the file extension.

        Parameters:
            df: The DataFrame or LazyFrame to write. Must have columns:
                - name: Sequence name/identifier
                - sequence: DNA/RNA sequence
                Optional: description (added after name on header line)
            path: The output file path. Compression is auto-detected from extension
                  (.fasta.bgz for BGZF, .fasta.gz/.fa.gz for GZIP, .fasta/.fa for uncompressed).

        Returns:
            The number of rows written.

        !!! Example "Writing FASTA files"
            ```python
            import polars_bio as pb

            # Read a FASTA file
            df = pb.read_fasta("input.fasta")

            # Write to uncompressed FASTA
            pb.write_fasta(df, "output.fasta")

            # Write to GZIP-compressed FASTA
            pb.write_fasta(df, "output.fasta.gz")
            ```
        """
        return _write_file(df, path, OutputFormat.Fasta)

    @staticmethod
    def sink_fasta(
        lf: pl.LazyFrame,
        path: str,
    ) -> None:
        """
        Streaming write a LazyFrame to FASTA format.

        Compression is auto-detected from the file extension.

        Parameters:
            lf: The LazyFrame to write.
            path: The output file path. Compression is auto-detected from extension
                  (.fasta.bgz for BGZF, .fasta.gz/.fa.gz for GZIP, .fasta/.fa for uncompressed).

        !!! Example "Streaming write FASTA"
            ```python
            import polars_bio as pb

            # Lazy read, filter, then sink
            lf = pb.scan_fasta("large_input.fasta.gz")
            pb.sink_fasta(lf.limit(1000), "sample_output.fasta")
            ```
        """
        _write_file(lf, path, OutputFormat.Fasta)

    @staticmethod
    def write_fastq(
        df: Union[pl.DataFrame, pl.LazyFrame],
        path: str,
    ) -> int:
        """
        Write a DataFrame to FASTQ format.

        Compression is auto-detected from the file extension.

        Parameters:
            df: The DataFrame or LazyFrame to write. Must have columns:
                - name: Read name/identifier
                - sequence: DNA sequence
                - quality_scores: Quality scores string
                Optional: description (added after name on header line)
            path: The output file path. Compression is auto-detected from extension
                  (.fastq.bgz for BGZF, .fastq.gz for GZIP, .fastq for uncompressed).

        Returns:
            The number of rows written.

        !!! Example "Writing FASTQ files"
            ```python
            import polars_bio as pb

            # Read a FASTQ file
            df = pb.read_fastq("input.fastq")

            # Write to uncompressed FASTQ
            pb.write_fastq(df, "output.fastq")

            # Write to GZIP-compressed FASTQ
            pb.write_fastq(df, "output.fastq.gz")
            ```
        """
        return _write_file(df, path, OutputFormat.Fastq)

    @staticmethod
    def sink_fastq(
        lf: pl.LazyFrame,
        path: str,
    ) -> None:
        """
        Streaming write a LazyFrame to FASTQ format.

        Compression is auto-detected from the file extension.

        Parameters:
            lf: The LazyFrame to write.
            path: The output file path. Compression is auto-detected from extension
                  (.fastq.bgz for BGZF, .fastq.gz for GZIP, .fastq for uncompressed).

        !!! Example "Streaming write FASTQ"
            ```python
            import polars_bio as pb

            # Lazy read, filter by quality, then sink
            lf = pb.scan_fastq("large_input.fastq.gz")
            pb.sink_fastq(lf.limit(1000), "sample_output.fastq")
            ```
        """
        _write_file(lf, path, OutputFormat.Fastq)

    @staticmethod
    def write_bam(
        df: Union[pl.DataFrame, pl.LazyFrame],
        path: str,
        sort_on_write: bool = False,
        tag_type_overrides: Optional[dict[str, str]] = None,
    ) -> int:
        """
        Write a DataFrame to BAM/SAM format.

        Compression is auto-detected from file extension:
        - .sam → Uncompressed SAM (plain text)
        - .bam → BGZF-compressed BAM

        For CRAM format, use `write_cram()` instead.

        Parameters:
            df: DataFrame or LazyFrame with 11 core BAM columns + optional tag columns
            path: Output file path (.bam or .sam)
            sort_on_write: If True, sort records by (chrom, start) and set header SO:coordinate.
                If False (default), set header SO:unsorted.
            tag_type_overrides: Optional exact SAM tag type specifications for ambiguous
                or newly created tag columns, e.g. {"tp": "A", "XH": "H", "ML": "B:C"}.
                Overrides take precedence over preserved source metadata and Arrow dtype inference.

        Returns:
            Number of rows written

        !!! Example "Write BAM files"
            ```python
            import polars_bio as pb
            df = pb.read_bam("input.bam", tag_fields=["NM", "AS"])
            pb.write_bam(df, "output.bam")
            pb.write_bam(df, "output.sam")
            ```
        """
        return _write_bam_file(
            df,
            path,
            OutputFormat.Bam,
            None,
            sort_on_write=sort_on_write,
            tag_type_overrides=tag_type_overrides,
        )

    @staticmethod
    def sink_bam(
        lf: pl.LazyFrame,
        path: str,
        sort_on_write: bool = False,
        tag_type_overrides: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Streaming write a LazyFrame to BAM/SAM format.

        For CRAM format, use `sink_cram()` instead.

        Parameters:
            lf: LazyFrame to write
            path: Output file path (.bam or .sam)
            sort_on_write: If True, sort records by (chrom, start) and set header SO:coordinate.
                If False (default), set header SO:unsorted.
            tag_type_overrides: Optional exact SAM tag type specifications for ambiguous
                or newly created tag columns, e.g. {"tp": "A", "XH": "H", "ML": "B:C"}.
                Overrides take precedence over preserved source metadata and Arrow dtype inference.

        !!! Example "Streaming write BAM"
            ```python
            import polars_bio as pb
            lf = pb.scan_bam("input.bam").filter(pl.col("mapping_quality") > 20)
            pb.sink_bam(lf, "filtered.bam")
            ```
        """
        _write_bam_file(
            lf,
            path,
            OutputFormat.Bam,
            None,
            sort_on_write=sort_on_write,
            tag_type_overrides=tag_type_overrides,
        )

    @staticmethod
    def read_sam(
        path: str,
        tag_fields: Union[list[str], None] = None,
        projection_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        infer_tag_types: bool = True,
        infer_tag_sample_size: int = 100,
        tag_type_hints: Optional[list[str]] = None,
    ) -> pl.DataFrame:
        """
        Read a SAM file into a DataFrame.

        SAM (Sequence Alignment/Map) is the plain-text counterpart of BAM.
        This function reuses the BAM reader, which auto-detects the format
        from the file extension.

        Parameters:
            path: The path to the SAM file.
            tag_fields: List of SAM tag names to include as columns (e.g., ["NM", "MD", "AS"]).
                If None, no optional tags are parsed (default).
            projection_pushdown: Enable column projection pushdown to optimize query performance.
            use_zero_based: If True, output 0-based half-open coordinates.
                If False, output 1-based closed coordinates.
                If None (default), uses the global configuration.
            infer_tag_types: If True (default), sample the file to auto-detect types for custom/unknown tags.
            infer_tag_sample_size: Number of records to sample for tag type inference (default: 100).
            tag_type_hints: Explicit SAM-style type hints for tags (e.g., ["pt:i", "ML:B:C", "FZ:B:S"]). Supported forms: TAG:TYPE, TAG:B, or TAG:B:SUBTYPE where TYPE is one of A, c, C, s, S, i, I, f, Z, H and SUBTYPE is one of c, C, s, S, i, I, f.

        !!! note
            By default, coordinates are output in **1-based closed** format.
        """
        lf = IOOperations.scan_sam(
            path,
            tag_fields,
            projection_pushdown,
            use_zero_based,
            infer_tag_types,
            infer_tag_sample_size,
            tag_type_hints,
        )
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_sam(
        path: str,
        tag_fields: Union[list[str], None] = None,
        projection_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        infer_tag_types: bool = True,
        infer_tag_sample_size: int = 100,
        tag_type_hints: Optional[list[str]] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a SAM file into a LazyFrame.

        SAM (Sequence Alignment/Map) is the plain-text counterpart of BAM.
        This function reuses the BAM reader, which auto-detects the format
        from the file extension.

        Parameters:
            path: The path to the SAM file.
            tag_fields: List of SAM tag names to include as columns (e.g., ["NM", "MD", "AS"]).
                If None, no optional tags are parsed (default).
            projection_pushdown: Enable column projection pushdown to optimize query performance.
            use_zero_based: If True, output 0-based half-open coordinates.
                If False, output 1-based closed coordinates.
                If None (default), uses the global configuration.
            infer_tag_types: If True (default), sample the file to auto-detect types for custom/unknown tags.
            infer_tag_sample_size: Number of records to sample for tag type inference (default: 100).
            tag_type_hints: Explicit SAM-style type hints for tags (e.g., ["pt:i", "ML:B:C", "FZ:B:S"]). Supported forms: TAG:TYPE, TAG:B, or TAG:B:SUBTYPE where TYPE is one of A, c, C, s, S, i, I, f, Z, H and SUBTYPE is one of c, C, s, S, i, I, f.

        !!! note
            By default, coordinates are output in **1-based closed** format.
        """
        zero_based = _resolve_zero_based(use_zero_based)
        if tag_type_hints is not None:
            _validate_tag_type_hints(tag_type_hints)
            tag_type_hints = _normalize_read_tag_type_hints(tag_type_hints)
        bam_read_options = BamReadOptions(
            zero_based=zero_based,
            tag_fields=tag_fields,
            infer_tag_types=infer_tag_types,
            infer_tag_sample_size=infer_tag_sample_size,
            tag_type_hints=tag_type_hints,
        )
        read_options = ReadOptions(bam_read_options=bam_read_options)
        return _read_file(
            path,
            InputFormat.Sam,
            read_options,
            projection_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def describe_sam(
        path: str,
        sample_size: int = 100,
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Get schema information for a SAM file with automatic tag discovery.

        Samples the first N records to discover all available tags and their types.
        Reuses the BAM describe logic, which auto-detects SAM from the file extension.

        Parameters:
            path: The path to the SAM file.
            sample_size: Number of records to sample for tag discovery (default: 100).
            use_zero_based: If True, output 0-based coordinates. If False, 1-based coordinates.

        Returns:
            DataFrame with columns: column_name, data_type, nullable, category, sam_type, description
        """
        zero_based = _resolve_zero_based(use_zero_based)

        df = py_describe_bam(
            ctx,
            path,
            None,
            zero_based,
            None,
            sample_size,
        )

        return pl.from_arrow(df.to_arrow_table())

    @staticmethod
    def write_sam(
        df: Union[pl.DataFrame, pl.LazyFrame],
        path: str,
        sort_on_write: bool = False,
        tag_type_overrides: Optional[dict[str, str]] = None,
    ) -> int:
        """
        Write a DataFrame to SAM format (plain text).

        Parameters:
            df: DataFrame or LazyFrame with 11 core BAM/SAM columns + optional tag columns
            path: Output file path (.sam)
            sort_on_write: If True, sort records by (chrom, start) and set header SO:coordinate.
                If False (default), set header SO:unsorted.
            tag_type_overrides: Optional exact SAM tag type specifications for ambiguous
                or newly created tag columns, e.g. {"tp": "A", "XH": "H", "ML": "B:C"}.
                Overrides take precedence over preserved source metadata and Arrow dtype inference.

        Returns:
            Number of rows written

        !!! Example "Write SAM files"
            ```python
            import polars_bio as pb
            df = pb.read_bam("input.bam", tag_fields=["NM", "AS"])
            pb.write_sam(df, "output.sam")
            ```
        """
        return _write_bam_file(
            df,
            path,
            OutputFormat.Sam,
            None,
            sort_on_write=sort_on_write,
            tag_type_overrides=tag_type_overrides,
        )

    @staticmethod
    def sink_sam(
        lf: pl.LazyFrame,
        path: str,
        sort_on_write: bool = False,
        tag_type_overrides: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Streaming write a LazyFrame to SAM format (plain text).

        Parameters:
            lf: LazyFrame to write
            path: Output file path (.sam)
            sort_on_write: If True, sort records by (chrom, start) and set header SO:coordinate.
                If False (default), set header SO:unsorted.
            tag_type_overrides: Optional exact SAM tag type specifications for ambiguous
                or newly created tag columns, e.g. {"tp": "A", "XH": "H", "ML": "B:C"}.
                Overrides take precedence over preserved source metadata and Arrow dtype inference.

        !!! Example "Streaming write SAM"
            ```python
            import polars_bio as pb
            lf = pb.scan_bam("input.bam").filter(pl.col("mapping_quality") > 20)
            pb.sink_sam(lf, "filtered.sam")
            ```
        """
        _write_bam_file(
            lf,
            path,
            OutputFormat.Sam,
            None,
            sort_on_write=sort_on_write,
            tag_type_overrides=tag_type_overrides,
        )

    @staticmethod
    def write_cram(
        df: Union[pl.DataFrame, pl.LazyFrame],
        path: str,
        reference_path: str,
        sort_on_write: bool = False,
    ) -> int:
        """
        Write a DataFrame to CRAM format.

        CRAM uses reference-based compression, storing only differences from the
        reference sequence. This achieves 30-60% better compression than BAM.

        Parameters:
            df: DataFrame or LazyFrame with 11 core BAM columns + optional tag columns
            path: Output CRAM file path
            reference_path: Path to reference FASTA file (required). The reference must
                contain all sequences referenced by the alignment data.
            sort_on_write: If True, sort records by (chrom, start) and set header SO:coordinate.
                If False (default), set header SO:unsorted.

        Returns:
            Number of rows written

        !!! warning "Known Limitation: MD and NM Tags"
            Due to a limitation in the underlying noodles-cram library, **MD and NM tags cannot be read back from CRAM files** after writing, even though they are written to the file. If you need MD/NM tags for downstream analysis, use BAM format instead. Other optional tags (RG, MQ, AM, OQ, AS, etc.) work correctly. See: https://github.com/biodatageeks/datafusion-bio-formats/issues/54

        !!! Example "Write CRAM files"
            ```python
            import polars_bio as pb

            df = pb.read_bam("input.bam", tag_fields=["NM", "AS"])

            # Write CRAM with reference (required)
            pb.write_cram(df, "output.cram", reference_path="reference.fasta")

            # For sorted output
            pb.write_cram(df, "output.cram", reference_path="reference.fasta", sort_on_write=True)
            ```
        """
        return _write_bam_file(
            df, path, OutputFormat.Cram, reference_path, sort_on_write=sort_on_write
        )

    @staticmethod
    def sink_cram(
        lf: pl.LazyFrame,
        path: str,
        reference_path: str,
        sort_on_write: bool = False,
    ) -> None:
        """
        Streaming write a LazyFrame to CRAM format.

        CRAM uses reference-based compression, storing only differences from the
        reference sequence. This method streams data without materializing all
        rows in memory.

        Parameters:
            lf: LazyFrame to write
            path: Output CRAM file path
            reference_path: Path to reference FASTA file (required). The reference must
                contain all sequences referenced by the alignment data.
            sort_on_write: If True, sort records by (chrom, start) and set header SO:coordinate.
                If False (default), set header SO:unsorted.

        !!! warning "Known Limitation: MD and NM Tags"
            Due to a limitation in the underlying noodles-cram library, **MD and NM tags cannot be read back from CRAM files** after writing, even though they are written to the file. If you need MD/NM tags for downstream analysis, use BAM format instead. Other optional tags (RG, MQ, AM, OQ, AS, etc.) work correctly. See: https://github.com/biodatageeks/datafusion-bio-formats/issues/54

        !!! Example "Streaming write CRAM"
            ```python
            import polars_bio as pb
            import polars as pl

            lf = pb.scan_bam("large_input.bam")
            lf = lf.filter(pl.col("mapping_quality") > 30)

            # Write CRAM with reference (required)
            pb.sink_cram(lf, "filtered.cram", reference_path="reference.fasta")

            # For sorted output
            pb.sink_cram(lf, "filtered.cram", reference_path="reference.fasta", sort_on_write=True)
            ```
        """
        _write_bam_file(
            lf, path, OutputFormat.Cram, reference_path, sort_on_write=sort_on_write
        )


def _cleanse_fields(t: Union[list[str], None]) -> Union[list[str], None]:
    if t is None:
        return None
    return [x.strip() for x in t]


_FASTQ_COLUMNS = ["name", "description", "sequence", "quality_scores"]
_FASTQ_REQUIRED_COLUMNS = ["name", "sequence", "quality_scores"]


def _normalize_fastq_columns(
    df: Union[pl.DataFrame, pl.LazyFrame],
) -> Union[pl.DataFrame, pl.LazyFrame]:
    columns = (
        df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
    )
    missing = [column for column in _FASTQ_REQUIRED_COLUMNS if column not in columns]
    if missing:
        raise ValueError(
            "FASTQ write requires columns: "
            + ", ".join(_FASTQ_REQUIRED_COLUMNS)
            + f"; missing: {', '.join(missing)}"
        )

    if "description" in columns:
        df = df.with_columns(pl.col("description").cast(pl.String))
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.String).alias("description"))

    return df.select(_FASTQ_COLUMNS)


def _write_file(
    df: Union[pl.DataFrame, pl.LazyFrame],
    path: str,
    output_format: OutputFormat,
) -> int:
    """
    Internal helper to write DataFrame to a file with TRUE STREAMING.

    This function now streams data directly from LazyFrame to file without
    materializing the entire dataset in memory. This is critical for large files!

    Coordinate system is read from DataFrame/LazyFrame metadata.
    Compression is auto-detected from file extension.

    Parameters:
        df: The DataFrame or LazyFrame to write.
        path: The output file path.
        output_format: The output format.

    Returns:
        The number of rows written.
    """
    import json

    from ._metadata import get_coordinate_system, get_metadata

    # Get metadata WITHOUT collecting (works for both DataFrame and LazyFrame)
    source_meta = None
    vcf_header = None
    zero_based = None

    try:
        source_meta = get_metadata(df)
        if output_format == OutputFormat.Vcf:
            vcf_header = source_meta.get("header") if source_meta else None
    except (KeyError, AttributeError, TypeError):
        pass

    # Get coordinate system from metadata
    try:
        zero_based = get_coordinate_system(df)
    except (KeyError, AttributeError, TypeError):
        pass

    if zero_based is None:
        zero_based = _resolve_zero_based(None)

    # Build write options based on format
    if output_format == OutputFormat.Vcf:
        # Extract VCF metadata from source_header
        info_fields_json = None
        format_fields_json = None
        sample_names_json = None
        contigs_json = None
        if vcf_header:
            if vcf_header.get("info_fields"):
                info_fields_json = json.dumps(vcf_header["info_fields"])
            if vcf_header.get("format_fields"):
                format_fields_json = json.dumps(vcf_header["format_fields"])
            if vcf_header.get("sample_names"):
                sample_names_json = json.dumps(vcf_header["sample_names"])
            if vcf_header.get("contigs"):
                contigs_json = json.dumps(vcf_header["contigs"])

        vcf_opts = VcfWriteOptions(
            zero_based=zero_based,
            info_fields_metadata=info_fields_json,
            format_fields_metadata=format_fields_json,
            sample_names=sample_names_json,
            contigs_metadata=contigs_json,
        )
        write_options = WriteOptions(vcf_write_options=vcf_opts)
    elif output_format == OutputFormat.Fasta:
        fasta_opts = FastaWriteOptions()
        write_options = WriteOptions(fasta_write_options=fasta_opts)
    elif output_format == OutputFormat.Fastq:
        fastq_opts = FastqWriteOptions()
        write_options = WriteOptions(fastq_write_options=fastq_opts)
        df = _normalize_fastq_columns(df)
    else:
        write_options = None

    # ✅ TRUE STREAMING: Use collect_batches pattern with Utf8View → LargeUtf8 conversion
    # This works for filtered/transformed LazyFrames
    # NOTE: Filtering currently materializes all data - predicate pushdown to DataFusion not yet implemented
    if isinstance(df, pl.LazyFrame):
        import pyarrow as pa
        import pyarrow.compute as pc

        # Get streaming batches from Polars
        batches_iter = df.collect_batches(lazy=True, engine="streaming")
        stream = batches_iter._inner

        # We need to convert Utf8View to LargeUtf8 on the Rust side
        # Pass the stream and let Rust handle the conversion
        return py_write_table(ctx, stream, path, output_format, write_options)
    else:
        # Already a DataFrame
        arrow_table = df.to_arrow()
        reader = arrow_table.to_reader()
        return py_write_table(ctx, reader, path, output_format, write_options)


def _write_bam_file(
    df: Union[pl.DataFrame, pl.LazyFrame],
    path: str,
    output_format: OutputFormat,
    reference_path: Optional[str] = None,
    sort_on_write: bool = False,
    tag_type_overrides: Optional[Dict[str, str]] = None,
) -> int:
    """Internal helper for BAM/CRAM write with streaming."""
    import json

    from ._metadata import get_coordinate_system, get_metadata

    # Extract metadata
    source_meta = None
    bam_header = None
    zero_based = None

    try:
        source_meta = get_metadata(df)
        if source_meta:
            bam_header = source_meta.get("header")
    except (KeyError, AttributeError, TypeError):
        pass

    try:
        zero_based = get_coordinate_system(df)
    except (KeyError, AttributeError, TypeError):
        pass

    if zero_based is None:
        zero_based = _resolve_zero_based(None)

    if tag_type_overrides is not None:
        _validate_tag_type_overrides(tag_type_overrides)

    tag_type_overrides_json = (
        json.dumps(tag_type_overrides) if tag_type_overrides else None
    )

    # Build write options
    if output_format == OutputFormat.Cram:
        # reference_path is optional - None means reference-free CRAM
        cram_opts = CramWriteOptions(
            reference_path=reference_path,
            zero_based=zero_based,
            tag_fields=None,
            header_metadata=json.dumps(bam_header) if bam_header else None,
            sort_on_write=sort_on_write,
            tag_type_overrides=tag_type_overrides_json,
        )
        write_options = WriteOptions(cram_write_options=cram_opts)
    else:
        bam_opts = BamWriteOptions(
            zero_based=zero_based,
            tag_fields=None,
            header_metadata=json.dumps(bam_header) if bam_header else None,
            sort_on_write=sort_on_write,
            tag_type_overrides=tag_type_overrides_json,
        )
        write_options = WriteOptions(bam_write_options=bam_opts)

    # Stream write
    if isinstance(df, pl.LazyFrame):
        batches_iter = df.collect_batches(lazy=True, engine="streaming")
        stream = batches_iter._inner
        return py_write_table(ctx, stream, path, output_format, write_options)
    else:
        arrow_table = df.to_arrow()
        reader = arrow_table.to_reader()
        return py_write_table(ctx, reader, path, output_format, write_options)


def _apply_combined_pushdown_via_sql(
    ctx,
    table_name,
    original_df,
    predicate,
    projected_columns,
    predicate_pushdown,
    projection_pushdown,
):
    """Apply both predicate and projection pushdown using SQL approach."""
    from polars_bio.polars_bio import py_read_sql

    # Build SQL query with combined optimizations
    select_clause = "*"
    if projection_pushdown and projected_columns:
        select_clause = ", ".join([f'"{c}"' for c in projected_columns])

    where_clause = ""
    if predicate_pushdown and predicate is not None:
        try:
            # Use the proven regex-based predicate translation
            where_clause = _build_sql_where_from_predicate_safe(predicate)
        except Exception as e:
            where_clause = ""

    # No fallback - if we can't parse to SQL, just use projection only
    # This keeps us in pure SQL mode for maximum performance

    # Construct optimized SQL query
    quoted_table = _quote_sql_identifier(table_name)
    if where_clause:
        sql = f"SELECT {select_clause} FROM {quoted_table} WHERE {where_clause}"
    else:
        sql = f"SELECT {select_clause} FROM {quoted_table}"

    # Execute with DataFusion - this leverages the proven 4x+ optimization
    return py_read_sql(ctx, sql)


def _build_sql_where_from_predicate_safe(predicate):
    """Build SQL WHERE clause by parsing all individual conditions and connecting with AND."""
    import re

    pred_str = str(predicate).strip("[]")

    # Find all individual conditions in the nested structure
    conditions = []

    # String equality/inequality patterns (including empty strings)
    # Accept both with and without surrounding parentheses in Polars repr
    str_eq_patterns = [
        r'\(col\("([^"]+)"\)\)\s*==\s*\("([^"]*)"\)',  # (col("x")) == ("v")
        r'col\("([^"]+)"\)\s*==\s*"([^"]*)"',  # col("x") == "v"
    ]
    for pat in str_eq_patterns:
        for column, value in re.findall(pat, pred_str):
            conditions.append(f"\"{column}\" = '{value}'")

    # Numeric comparison patterns (handle both formats: with and without "dyn int:")
    numeric_patterns = [
        (r'\(col\("([^"]+)"\)\)\s*>\s*\((?:dyn int:\s*)?(\d+)\)', ">"),
        (r'\(col\("([^"]+)"\)\)\s*<\s*\((?:dyn int:\s*)?(\d+)\)', "<"),
        (r'\(col\("([^"]+)"\)\)\s*>=\s*\((?:dyn int:\s*)?(\d+)\)', ">="),
        (r'\(col\("([^"]+)"\)\)\s*<=\s*\((?:dyn int:\s*)?(\d+)\)', "<="),
        (r'\(col\("([^"]+)"\)\)\s*!=\s*\((?:dyn int:\s*)?(\d+)\)', "!="),
        (r'\(col\("([^"]+)"\)\)\s*==\s*\((?:dyn int:\s*)?(\d+)\)', "="),
        (r'col\("([^"]+)"\)\s*>\s*(\d+)', ">"),
        (r'col\("([^"]+)"\)\s*<\s*(\d+)', "<"),
        (r'col\("([^"]+)"\)\s*>=\s*(\d+)', ">="),
        (r'col\("([^"]+)"\)\s*<=\s*(\d+)', "<="),
        (r'col\("([^"]+)"\)\s*!=\s*(\d+)', "!="),
        (r'col\("([^"]+)"\)\s*==\s*(\d+)', "="),
    ]

    for pattern, op in numeric_patterns:
        matches = re.findall(pattern, pred_str)
        for column, value in matches:
            conditions.append(f'"{column}" {op} {value}')

    # Float comparison patterns (handle both formats: with and without "dyn float:")
    float_patterns = [
        (r'\(col\("([^"]+)"\)\)\s*>\s*\((?:dyn float:\s*)?([\d.]+)\)', ">"),
        (r'\(col\("([^"]+)"\)\)\s*<\s*\((?:dyn float:\s*)?([\d.]+)\)', "<"),
        (r'\(col\("([^"]+)"\)\)\s*>=\s*\((?:dyn float:\s*)?([\d.]+)\)', ">="),
        (r'\(col\("([^"]+)"\)\)\s*<=\s*\((?:dyn float:\s*)?([\d.]+)\)', "<="),
        (r'\(col\("([^"]+)"\)\)\s*!=\s*\((?:dyn float:\s*)?([\d.]+)\)', "!="),
        (r'\(col\("([^"]+)"\)\)\s*==\s*\((?:dyn float:\s*)?([\d.]+)\)', "="),
        (r'col\("([^"]+)"\)\s*>\s*([\d.]+)', ">"),
        (r'col\("([^"]+)"\)\s*<\s*([\d.]+)', "<"),
        (r'col\("([^"]+)"\)\s*>=\s*([\d.]+)', ">="),
        (r'col\("([^"]+)"\)\s*<=\s*([\d.]+)', "<="),
        (r'col\("([^"]+)"\)\s*!=\s*([\d.]+)', "!="),
        (r'col\("([^"]+)"\)\s*==\s*([\d.]+)', "="),
    ]

    for pattern, op in float_patterns:
        matches = re.findall(pattern, pred_str)
        for column, value in matches:
            conditions.append(f'"{column}" {op} {value}')

    # IN list pattern: col("x").is_in([v1, v2, ...])
    in_matches = re.findall(r'col\("([^"]+)"\)\.is_in\(\[(.*?)\]\)', pred_str)
    for column, values_str in in_matches:
        # Tokenize values: quoted strings or numbers
        tokens = re.findall(r"'(?:[^']*)'|\"(?:[^\"]*)\"|\d+(?:\.\d+)?", values_str)
        items = []
        for t in tokens:
            if t.startswith('"') and t.endswith('"'):
                items.append("'" + t[1:-1] + "'")
            else:
                items.append(t)
        if items:
            conditions.append(f'"{column}" IN ({", ".join(items)})')

    # Join all conditions with AND
    if conditions:
        where = " AND ".join(conditions)
        # Clean up any residual bracketed list formatting from IN clause (defensive)
        where = (
            where.replace("IN ([", "IN (")
            .replace("])", ")")
            .replace("[ ", "")
            .replace(" ]", "")
        )
        # Collapse simple >= and <= pairs into BETWEEN when possible
        try:
            import re as _re

            where = _re.sub(
                r'"([^"]+)"\s*>=\s*([\d.]+)\s*AND\s*"\1"\s*<=\s*([\d.]+)',
                r'"\1" BETWEEN \2 AND \3',
                where,
            )
            where = _re.sub(
                r'"([^"]+)"\s*<=\s*([\d.]+)\s*AND\s*"\1"\s*>=\s*([\d.]+)',
                r'"\1" BETWEEN \3 AND \2',
                where,
            )
        except Exception:
            pass
        return where

    return ""


def _lazy_scan(
    schema_or_df,  # Either: PyArrow schema (from py_get_table_schema) or DataFusion DataFrame (from py_read_sql for SQL path)
    projection_pushdown: bool = True,
    predicate_pushdown: bool = False,
    table_name: str = None,
    input_format: InputFormat = None,
    file_path: str = None,
    read_options: ReadOptions = None,
    force_empty_projection: bool = False,
) -> pl.LazyFrame:
    # Handle both PyArrow schema (new streaming path) and DataFusion DataFrame (old SQL path)
    import pyarrow as pa

    df_for_stream = None  # Used for SQL path

    # Check if it's a DataFusion DataFrame by checking for schema() method
    # We use hasattr because there are multiple DataFrame classes in datafusion package
    is_datafusion_df = hasattr(schema_or_df, "schema") and hasattr(
        schema_or_df, "execute_stream"
    )

    if isinstance(schema_or_df, pa.Schema):
        # PyArrow schema (from py_get_table_schema or df.schema())
        # Convert to Polars schema dict for register_io_source
        empty_table = pa.table(
            {field.name: pa.array([], type=field.type) for field in schema_or_df}
        )
        temp_df = pl.from_arrow(empty_table)
        original_schema = dict(temp_df.schema)  # Convert to dict for register_io_source
    elif is_datafusion_df:
        # DataFusion DataFrame from py_read_sql (sql() function)
        # Extract PyArrow schema and convert to Polars schema dict
        df_for_stream = schema_or_df
        pa_schema = schema_or_df.schema()
        empty_table = pa.table(
            {field.name: pa.array([], type=field.type) for field in pa_schema}
        )
        temp_df = pl.from_arrow(empty_table)
        original_schema = dict(temp_df.schema)  # Convert to dict for register_io_source
    else:
        # Fallback: already a Polars schema
        original_schema = (
            dict(schema_or_df) if not isinstance(schema_or_df, dict) else schema_or_df
        )

    if force_empty_projection:
        # Polars cannot express a zero-column request through the Python IO
        # callback: count-only plans are represented by an arbitrary physical
        # source column. Advertise an empty schema so the provider can preserve
        # row counts without decoding any Cooler datasets.
        original_schema = {}

    def _overlap_source(
        with_columns: Union[pl.Expr, None],
        predicate: Union[pl.Expr, None],
        n_rows: Union[int, None],
        _batch_size: Union[int, None],
    ) -> Iterator[pl.DataFrame]:
        from polars_bio.polars_bio import py_read_table, py_register_table

        from .context import ctx as _ctx
        from .pushdown import (
            apply_predicate_pushdown,
            apply_projection_pushdown,
            extract_source_columns,
        )

        table_refreshed = False

        # === GFF/GTF pre-step ===
        # GFF/GTF "attributes" column contains semi-structured key=value pairs
        # that can be parsed into individual columns. Unlike BAM/VCF/CRAM which
        # have fixed schemas, attribute columns must be configured at table
        # registration time. If projection requests specific attribute columns
        # we must re-register the table with those attr_fields.
        table_to_query = table_name
        if input_format in (InputFormat.Gff, InputFormat.Gtf) and file_path is not None:
            from polars_bio.polars_bio import GffReadOptions
            from polars_bio.polars_bio import GtfReadOptions as _GtfReadOptions
            from polars_bio.polars_bio import PyObjectStorageOptions
            from polars_bio.polars_bio import ReadOptions as _ReadOptions

            is_gff = input_format == InputFormat.Gff
            opts_field = "gff_read_options" if is_gff else "gtf_read_options"

            requested_cols = (
                _extract_column_names_from_expr(with_columns)
                if with_columns is not None
                else []
            )

            STATIC = {
                "chrom",
                "start",
                "end",
                "type",
                "source",
                "score",
                "strand",
                "phase",
                "attributes",
            }
            attr_fields = [c for c in requested_cols if c not in STATIC]

            # Derive zero_based from read_options
            zero_based = False
            if read_options is not None:
                try:
                    gopt = getattr(read_options, opts_field, None)
                    if gopt is not None:
                        zb = getattr(gopt, "zero_based", None)
                        if zb is not None:
                            zero_based = zb
                except Exception:
                    pass

            obj = _extract_py_object_storage_options(read_options)

            # When both the raw nested ``attributes`` column and parsed fields
            # are requested, use the reader's "attributes" sentinel to emit both
            # from a single registration.
            if "attributes" in requested_cols and attr_fields:
                _attr = attr_fields + ["attributes"]
            elif "attributes" in requested_cols:
                _attr = None
            elif attr_fields:
                _attr = attr_fields
            else:
                _attr = []

            if is_gff:
                fmt_opts = GffReadOptions(
                    attr_fields=_attr,
                    object_storage_options=obj,
                    zero_based=zero_based,
                )
                ropts = _ReadOptions(gff_read_options=fmt_opts)
            else:
                fmt_opts = _GtfReadOptions(
                    attr_fields=_attr,
                    object_storage_options=obj,
                    zero_based=zero_based,
                )
                ropts = _ReadOptions(gtf_read_options=fmt_opts)

            if projection_pushdown and requested_cols:
                table_obj = py_register_table(
                    _ctx, file_path, table_name, input_format, ropts
                )
                table_to_query = table_obj.name
                table_refreshed = True

        # === Unified path for ALL formats ===

        # 1. Get base DataFusion DataFrame
        if df_for_stream is not None:
            query_df = df_for_stream
        else:
            # Re-register file-backed sources on each execution so every collect()
            # sees a fresh provider state for this LazyFrame.
            should_register = (
                file_path is not None
                and not table_refreshed
                and table_to_query is not None
            )
            if should_register and input_format == InputFormat.Cool:
                # A LazyFrame may be collected concurrently by separate Polars
                # plans or Python threads. Give every callback invocation its
                # own catalog identity so one lease cannot replace or remove
                # another invocation's provider between registration and lookup.
                lease_name = f"_pb_cool_collect_{uuid4().hex}"
                with _registered_table_lease(
                    _ctx,
                    file_path,
                    lease_name,
                    input_format,
                    read_options,
                ):
                    query_df = py_read_table(_ctx, lease_name)
            else:
                if should_register:
                    py_register_table(
                        _ctx, file_path, table_to_query, input_format, read_options
                    )
                query_df = py_read_table(_ctx, table_to_query)

        if force_empty_projection:
            query_df = query_df.select()

        # 2. Predicate pushdown (optimization only; the client-side filter below
        #    is the source of truth). The shared helper pushes the faithfully
        #    translatable AND-conjuncts and reports whether the full predicate
        #    must still be reapplied client-side.
        _fmt_key = str(input_format).rsplit(".", 1)[-1]
        _scols, _ucols, _fcols = _FORMAT_COLUMN_TYPES.get(_fmt_key, (None, None, None))
        needs_client_filter = predicate is not None
        if predicate_pushdown and predicate is not None:
            query_df, needs_client_filter = apply_predicate_pushdown(
                query_df,
                predicate,
                {"string_cols": _scols, "uint32_cols": _ucols, "float32_cols": _fcols},
                log=logger,
            )

        # 3. Projection pushdown (optimization only; client-side select is truth)
        needs_client_select = with_columns is not None
        if projection_pushdown and with_columns is not None:
            query_df, needs_client_select = apply_projection_pushdown(
                query_df,
                with_columns,
                log=logger,
                retain_for_client=predicate if needs_client_filter else None,
            )

        projection_columns, projection_complete = extract_source_columns(with_columns)
        rootless_client_projection = (
            with_columns is not None
            and needs_client_select
            and projection_complete
            and not projection_columns
        )

        # 4. Limit
        # A limit cannot cross a predicate that still runs client-side: doing so
        # would inspect only the first N unfiltered rows. The loop below instead
        # keeps consuming batches until it has N matching rows.
        if n_rows and n_rows > 0 and not needs_client_filter:
            query_df = query_df.limit(int(n_rows))

        # 5. Stream with safety net
        df_stream = query_df.execute_stream()
        progress_bar = tqdm(unit="rows")
        remaining = int(n_rows) if n_rows is not None else None
        rootless_row_count = 0
        for r in df_stream:
            out = pl.DataFrame(r.to_pyarrow())
            # Source of truth: reapply the full predicate/projection client-side
            # unless the helper certified the pushdown was complete.
            if predicate is not None and needs_client_filter:
                out = out.filter(predicate)

            if remaining is not None:
                if remaining <= 0:
                    break
                if len(out) > remaining:
                    out = out.head(remaining)
                remaining -= len(out)

            # Rootless projections such as ``pl.len()`` are whole-stream
            # expressions. Applying them to each RecordBatch would emit partial
            # aggregates, so retain only the total input height and evaluate once
            # after the stream is exhausted. Arrow's NullArray has no value
            # buffer, keeping this metadata-only even for very large counts.
            if rootless_client_projection:
                rootless_row_count += len(out)
                if remaining is not None and remaining <= 0:
                    break
                continue

            if with_columns is not None and needs_client_select:
                out = out.select(with_columns)

            progress_bar.update(len(out))
            yield out
            if remaining is not None and remaining <= 0:
                return

        if rootless_client_projection:
            virtual_rows = pa.record_batch(
                [pa.nulls(rootless_row_count)], names=["__polars_bio_row"]
            ).drop_columns(["__polars_bio_row"])
            out = pl.DataFrame(virtual_rows).select(with_columns)
            progress_bar.update(len(out))
            yield out

    return register_io_source(_overlap_source, schema=original_schema)


# Module-level weak store for PyObjectStorageOptions keyed by ReadOptions id.
# PyO3 structs don't support arbitrary Python attributes, so we store the
# original options here during scan_gff/scan_gtf and retrieve them when
# re-registering tables in pre-steps or LazyFrameWrapper.select().
_object_storage_options_store: dict = {}


def _store_py_object_storage_options(read_options, obj_opts):
    """Attach PyObjectStorageOptions to a ReadOptions instance via module dict."""
    _object_storage_options_store[id(read_options)] = obj_opts
    # Clean up when read_options is garbage-collected (best-effort).
    try:
        _weakref.finalize(
            read_options, _object_storage_options_store.pop, id(read_options), None
        )
    except TypeError:
        pass  # PyO3 objects may not support weak references


def _extract_py_object_storage_options(read_options):
    """Extract stored PyObjectStorageOptions from read_options, or return defaults."""
    from polars_bio.polars_bio import PyObjectStorageOptions

    stored = _object_storage_options_store.get(id(read_options))
    if stored is not None:
        return stored
    return PyObjectStorageOptions(
        allow_anonymous=True,
        enable_request_payer=False,
        chunk_size=8,
        concurrent_fetches=1,
        max_retries=5,
        timeout=300,
        compression_type="auto",
    )


def _extract_column_names_from_expr(with_columns: Union[pl.Expr, list]) -> list[str]:
    """Extract column names from Polars expressions."""
    if with_columns is None:
        return []

    # Handle different types of with_columns input
    if hasattr(with_columns, "__iter__") and not isinstance(with_columns, str):
        # It's a list of expressions or strings
        column_names = []
        for item in with_columns:
            if isinstance(item, str):
                column_names.append(item)
            elif hasattr(item, "meta") and hasattr(item.meta, "output_name"):
                # Polars expression with output name
                try:
                    column_names.append(item.meta.output_name())
                except Exception:
                    pass
        return column_names
    elif isinstance(with_columns, str):
        return [with_columns]
    elif hasattr(with_columns, "meta") and hasattr(with_columns.meta, "output_name"):
        # Single Polars expression
        try:
            return [with_columns.meta.output_name()]
        except Exception:
            pass

    return []


def _extract_vcf_metadata_from_schema(schema) -> dict:
    """Extract VCF field metadata from a PyArrow schema.

    This extracts the VCF-specific metadata (vcf_number, vcf_type, vcf_description)
    from Arrow field metadata and organizes it for storage in Polars config_meta.

    Args:
        schema: PyArrow schema with VCF field metadata

    Returns:
        Dict with 'info_fields', 'format_fields', and 'sample_names'
    """
    info_fields = {}
    format_fields = {}
    sample_names = []
    seen_samples = set()

    for field in schema:
        if not field.metadata:
            continue

        # Decode bytes to strings
        metadata = {
            k.decode("utf-8") if isinstance(k, bytes) else k: (
                v.decode("utf-8") if isinstance(v, bytes) else v
            )
            for k, v in field.metadata.items()
        }

        field_type = metadata.get("vcf_field_type")
        if field_type == "INFO":
            info_fields[field.name] = {
                "number": metadata.get("vcf_number", "."),
                "type": metadata.get("vcf_type", "String"),
                "description": metadata.get("vcf_description", ""),
            }
        elif field_type == "FORMAT":
            format_id = metadata.get("vcf_format_id", field.name)
            if format_id not in format_fields:
                format_fields[format_id] = {
                    "number": metadata.get("vcf_number", "1"),
                    "type": metadata.get("vcf_type", "String"),
                    "description": metadata.get("vcf_description", ""),
                }

            # Extract sample name from column name pattern: {sample}_{format}
            if field.name.endswith(f"_{format_id}"):
                sample = field.name[: -len(format_id) - 1]
                if sample and sample not in seen_samples:
                    seen_samples.add(sample)
                    sample_names.append(sample)

    # Handle single-sample VCFs where column name equals format_id (no sample prefix)
    # In this case, we infer the sample name. The bio-formats library uses a default
    # single sample name, so we check if any FORMAT fields exist without sample prefixes.
    if format_fields and not sample_names:
        # Check if any FORMAT column name matches a format_id directly
        format_ids = set(format_fields.keys())
        for field in schema:
            if field.name in format_ids:
                # Single-sample VCF detected - use "sample" as default name
                sample_names = ["sample"]
                break

    return {
        "info_fields": info_fields if info_fields else None,
        "format_fields": format_fields if format_fields else None,
        "sample_names": sample_names if sample_names else None,
    }


def _extract_vcf_header_extras(schema) -> dict:
    """Extract VCF schema-level metadata from Arrow schema.

    Based on datafusion-bio-formats PR #47 naming convention: bio.vcf.*
    Extracts schema-level metadata that provides provenance and validation info.

    Args:
        schema: PyArrow schema with VCF schema-level metadata

    Returns:
        Dict with optional keys:
        - "version": VCF version (e.g., "VCFv4.2")
        - "contigs": List of contig definitions
        - "filters": List of filter definitions
        - "alt_definitions": List of ALT allele definitions

    Schema-level metadata keys (from datafusion-bio-formats):
        - bio.vcf.file_format: VCF version string
        - bio.vcf.contigs: JSON array of ContigMetadata
        - bio.vcf.filters: JSON array of FilterMetadata
        - bio.vcf.alternative_alleles: JSON array of AltAlleleMetadata
        - bio.vcf.samples: JSON array of sample names (redundant with column-based extraction)
    """
    import json

    extras = {}
    schema_meta = schema.metadata or {}

    # Helper to safely decode bytes or string keys
    def get_meta(key: str):
        # Try string key first, then bytes
        value = schema_meta.get(key) or schema_meta.get(key.encode())
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value

    # Extract version (plain string)
    version = get_meta("bio.vcf.file_format")
    if version:
        extras["version"] = version

    # Extract JSON-encoded schema-level metadata
    json_fields = [
        ("bio.vcf.contigs", "contigs"),
        ("bio.vcf.filters", "filters"),
        ("bio.vcf.alternative_alleles", "alt_definitions"),
    ]

    for key, target_key in json_fields:
        value = get_meta(key)
        if value:
            try:
                extras[target_key] = json.loads(value)
            except json.JSONDecodeError:
                # Silently skip malformed JSON
                pass

    return extras


def _format_to_string(input_format: InputFormat) -> str:
    """Convert InputFormat enum to string identifier for metadata storage.

    Args:
        input_format: InputFormat enum value

    Returns:
        String identifier (e.g., "vcf", "fastq", "bam")
    """
    # Use string comparison since InputFormat is not hashable
    format_str = str(input_format)
    if "VcfZarr" in format_str:
        return "vcf_zarr"
    elif "Vcf" in format_str:
        return "vcf"
    elif "Sam" in format_str:
        return "sam"
    elif "Bam" in format_str:
        return "bam"
    elif "Cram" in format_str:
        return "cram"
    elif "Fastq" in format_str:
        return "fastq"
    elif "Fasta" in format_str:
        return "fasta"
    elif "Gtf" in format_str:
        return "gtf"
    elif "Gff" in format_str:
        return "gff"
    # NOTE: BigWig/BigBed must be checked before Bed — "Bed" is a substring of
    # "BigBed", so reordering these branches would silently misclassify BigBed as bed.
    elif "BigWig" in format_str:
        return "bigwig"
    elif "BigBed" in format_str:
        return "bigbed"
    elif "Cool" in format_str:
        return "cool"
    elif "Bed" in format_str:
        return "bed"
    elif "Pairs" in format_str:
        return "pairs"
    elif "Bgen" in format_str:
        return "bgen"
    elif "Pgen" in format_str:
        return "pgen"
    else:
        return "unknown"


@contextmanager
def _registered_table_lease(context, path, name, input_format, read_options):
    """Keep a private table registered only while acquiring its query plan."""
    table = py_register_table(context, path, name, input_format, read_options)
    try:
        yield table
    finally:
        context.deregister_table(table.name)


def _read_file(
    path: str,
    input_format: InputFormat,
    read_options: ReadOptions,
    projection_pushdown: bool = True,
    predicate_pushdown: bool = False,
    zero_based: bool = True,
) -> pl.LazyFrame:
    # Each Cooler LazyFrame must retain its own provider. Different resolutions
    # of one .mcool have the same filename-derived default table name and schema,
    # so re-registering that shared name from concurrent IO callbacks can make
    # one scan read another scan's resolution without raising an error.
    table_name = (
        f"_pb_cool_scan_{uuid4().hex}" if input_format == InputFormat.Cool else None
    )
    if table_name is not None:
        with _registered_table_lease(
            ctx, path, table_name, input_format, read_options
        ) as table:
            # Get schema WITHOUT materializing data - critical for large files!
            schema = py_get_table_schema(ctx, table.name)
    else:
        table = py_register_table(ctx, path, None, input_format, read_options)
        schema = py_get_table_schema(ctx, table.name)

    # Extract ALL metadata from schema (works for all formats!)
    from polars_bio.metadata_extractors import extract_all_schema_metadata

    full_metadata = extract_all_schema_metadata(schema)

    # Build format-specific header metadata for backward compatibility
    header_metadata = None
    format_str = _format_to_string(input_format)

    # Extract format-specific metadata from the comprehensive extraction
    format_specific = full_metadata.get("format_specific", {})

    # SAM and CRAM use the same schema metadata keys as BAM (bio.bam.*),
    # so look up "bam" in format_specific when reading SAM or CRAM files.
    if format_str == "vcf_zarr":
        metadata_key = "vcf"
    else:
        metadata_key = "bam" if format_str in ("sam", "cram") else format_str

    if metadata_key in format_specific:
        # Use the parsed format-specific metadata
        if metadata_key == "vcf":
            vcf_meta = format_specific["vcf"]
            header_metadata = {
                "info_fields": vcf_meta.get("info_fields"),
                "format_fields": vcf_meta.get("format_fields"),
                "sample_names": vcf_meta.get("sample_names"),
                "version": vcf_meta.get("version"),
                "contigs": vcf_meta.get("contigs"),
                "filters": vcf_meta.get("filters"),
                "alt_definitions": vcf_meta.get("alt_definitions"),
            }
        elif metadata_key in [
            "fastq",
            "bam",
            "gff",
            "gtf",
            "fasta",
            "bed",
            "cram",
            "bigwig",
            "bigbed",
            "bgen",
            "pgen",
            "cool",
        ]:
            # For other formats (including SAM via "bam" key), include their specific metadata
            header_metadata = format_specific.get(metadata_key, {})

    # Note: We don't store _full_metadata to avoid duplication
    # All relevant metadata is already parsed into user-friendly fields
    # (info_fields, format_fields, sample_names, version, etc.)

    lf = _lazy_scan(
        schema,
        projection_pushdown,
        predicate_pushdown,
        table.name,
        input_format,
        path,
        read_options,
    )

    # Set coordinate system metadata
    set_coordinate_system(lf, zero_based)

    # Set source metadata (replaces old VCF-specific metadata setting)
    from polars_bio._metadata import set_source_metadata

    format_str = _format_to_string(input_format)

    # Store DataFusion table name for debugging
    if header_metadata is None:
        header_metadata = {}
    header_metadata["_datafusion_table_name"] = table.name

    set_source_metadata(lf, format=format_str, path=path, header=header_metadata)

    # Wrap GFF/GTF LazyFrames with projection-aware wrapper for consistent attribute field handling
    if input_format == InputFormat.Gff:
        return GffLazyFrameWrapper(
            lf, path, read_options, projection_pushdown, predicate_pushdown
        )
    if input_format == InputFormat.Gtf:
        return GtfLazyFrameWrapper(
            lf, path, read_options, projection_pushdown, predicate_pushdown
        )
    if input_format == InputFormat.Cool:
        return CoolLazyFrameWrapper(
            lf,
            schema,
            table.name,
            path,
            read_options,
            projection_pushdown,
        )

    return lf


def _is_len_expr(expr) -> bool:
    """Return whether an expression is exactly ``pl.len()``, optionally aliased."""
    if not isinstance(expr, pl.Expr):
        return False
    try:
        serialized = json.loads(expr.meta.serialize(format="json"))
    except Exception:
        return False
    return serialized == "Len" or (
        isinstance(serialized, dict)
        and isinstance(serialized.get("Alias"), list)
        and len(serialized["Alias"]) == 2
        and serialized["Alias"][0] == "Len"
    )


def _is_len_projection(exprs, named_exprs) -> bool:
    """Return whether a select consists only of row-count expressions."""
    items = []
    for expr in exprs:
        if isinstance(expr, (list, tuple)):
            items.extend(expr)
        else:
            items.append(expr)
    items.extend(named_exprs.values())
    return bool(items) and all(_is_len_expr(expr) for expr in items)


class CoolLazyFrameWrapper(pl.LazyFrame):
    """Preserve Cooler count-only intent before Polars projection rewriting.

    Polars' Python IO optimizer requests an arbitrary physical column for
    ``select(pl.len())``. By the time the IO callback runs, that request is
    indistinguishable from a real one-column projection. Intercepting the exact
    count projection here lets DataFusion execute a zero-column Cooler scan and
    retain only RecordBatch row counts.
    """

    def __init__(
        self,
        base_lf: pl.LazyFrame,
        schema,
        table_name: str,
        file_path: str,
        read_options: ReadOptions,
        projection_pushdown: bool = True,
    ):
        # Remain a real LazyFrame so Polars APIs that dispatch by type (for
        # example, pl.concat) accept Cooler scans without special handling.
        self._ldf = base_lf._ldf
        self._base_lf = base_lf
        self._schema = schema
        self._table_name = table_name
        self._file_path = file_path
        self._read_options = read_options
        self._projection_pushdown = projection_pushdown
        metadata = base_lf.config_meta.get_metadata()
        if metadata:
            self.config_meta.set(**metadata)

    @classmethod
    def _from_pyldf(cls, ldf):
        # Any operation other than the direct select intercepted below returns
        # an ordinary LazyFrame. Subclass instances constructed by Polars would
        # otherwise lack the Cooler source state stored by __init__.
        return pl.LazyFrame._from_pyldf(ldf)

    def select(self, *exprs, **named_exprs):
        if self._projection_pushdown and _is_len_projection(exprs, named_exprs):
            count_lf = _lazy_scan(
                self._schema,
                False,
                False,
                self._table_name,
                InputFormat.Cool,
                self._file_path,
                self._read_options,
                force_empty_projection=True,
            )
            metadata = self.config_meta.get_metadata()
            if metadata:
                count_lf.config_meta.set(**metadata)
            return count_lf.select(*exprs, **named_exprs)
        return super().select(*exprs, **named_exprs)


class AnnotationLazyFrameWrapper:
    """Unified wrapper for GFF/GTF LazyFrames with projection-aware attribute handling.

    Pushdown is decided exclusively inside the io_source callback based on
    with_columns and predicate; this wrapper only keeps chain type stable.
    Parameterized by format_type ("gff" or "gtf") to handle the minor
    differences in ReadOptions class, field name, InputFormat, and view prefix.
    """

    # InputFormat enum is not stored here because PyO3 enums can't be
    # referenced at class-definition time; it's resolved at runtime via
    # is_gff = self._format_type == "gff" in select().
    _FORMAT_CONFIG = {
        "gff": {
            "opts_field": "gff_read_options",
            "view_prefix": "_pb_gff_proj_",
        },
        "gtf": {
            "opts_field": "gtf_read_options",
            "view_prefix": "_pb_gtf_proj_",
        },
    }
    _PRESERVE_DEFERRED_PREDICATE = object()

    def __init__(
        self,
        base_lf: pl.LazyFrame,
        file_path: str,
        read_options: ReadOptions,
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        format_type: str = "gff",
        deferred_predicate: Optional[pl.Expr] = None,
    ):
        self._base_lf = base_lf
        self._file_path = file_path
        self._read_options = read_options
        self._projection_pushdown = projection_pushdown
        self._predicate_pushdown = predicate_pushdown
        self._format_type = format_type
        self._config = self._FORMAT_CONFIG[format_type]
        self._deferred_predicate = deferred_predicate

    def _make_wrapper(
        self,
        base_lf,
        projection_pushdown=None,
        predicate_pushdown=None,
        deferred_predicate=_PRESERVE_DEFERRED_PREDICATE,
    ):
        """Create a new wrapper of the same concrete type."""
        return type(self)(
            base_lf,
            self._file_path,
            self._read_options,
            (
                projection_pushdown
                if projection_pushdown is not None
                else self._projection_pushdown
            ),
            (
                predicate_pushdown
                if predicate_pushdown is not None
                else self._predicate_pushdown
            ),
            (
                self._deferred_predicate
                if deferred_predicate is self._PRESERVE_DEFERRED_PREDICATE
                else deferred_predicate
            ),
        )

    def select(self, exprs):
        # Source columns the projection needs (root names, NOT output names).
        # An aliased/computed expression like (pl.col("start") + 1).alias("s1")
        # needs the SOURCE column "start"; the alias/computation is applied
        # client-side over the original ``exprs`` and must never be mistaken for
        # a (here: attribute) column name to re-register the reader with.
        from .pushdown import extract_source_columns

        columns, _proj_complete = extract_source_columns(exprs)

        STATIC = {
            "chrom",
            "start",
            "end",
            "type",
            "source",
            "score",
            "strand",
            "phase",
            "attributes",
        }
        predicate_columns = self._extract_predicate_column_names()
        scan_columns = list(dict.fromkeys(columns + predicate_columns))
        attr_cols = [c for c in scan_columns if c not in STATIC]

        # If selecting attribute fields, run one-shot SQL projection with proper attr_fields
        if columns and (attr_cols or "attributes" in columns):
            from polars_bio.polars_bio import GffReadOptions
            from polars_bio.polars_bio import GtfReadOptions as _GtfReadOptions
            from polars_bio.polars_bio import InputFormat as _InputFormat
            from polars_bio.polars_bio import ReadOptions as _ReadOptions
            from polars_bio.polars_bio import py_read_table, py_register_table

            from .context import ctx

            is_gff = self._format_type == "gff"
            input_fmt = _InputFormat.Gff if is_gff else _InputFormat.Gtf

            # Pull zero_based from original read options
            zero_based = False
            try:
                gopt = getattr(self._read_options, self._config["opts_field"], None)
                if gopt is not None:
                    zb = getattr(gopt, "zero_based", None)
                    if zb is not None:
                        zero_based = zb
            except Exception:
                pass

            obj = _extract_py_object_storage_options(self._read_options)

            # ``scan_columns`` (projection + deferred-predicate roots) may need
            # both the raw nested ``attributes`` column and parsed attribute
            # fields at once, e.g.
            #   scan_gff(...).filter(pl.col("attributes")...).select("ID")
            #   scan_gff(..., attr_fields=["ID"]).filter(pl.col("ID")...).select("attributes")
            # The reader supports the "attributes" sentinel: including it in
            # ``attr_fields`` emits the nested ``attributes`` column alongside
            # the flattened fields in a single registration.
            needs_raw_attributes = "attributes" in scan_columns
            if needs_raw_attributes and attr_cols:
                _attr = attr_cols + ["attributes"]
            elif needs_raw_attributes:
                _attr = None
            elif attr_cols:
                _attr = attr_cols
            else:
                _attr = []

            if is_gff:
                fmt_opts = GffReadOptions(
                    attr_fields=_attr,
                    object_storage_options=obj,
                    zero_based=zero_based,
                )
                ropts = _ReadOptions(gff_read_options=fmt_opts)
            else:
                fmt_opts = _GtfReadOptions(
                    attr_fields=_attr,
                    object_storage_options=obj,
                    zero_based=zero_based,
                )
                ropts = _ReadOptions(gtf_read_options=fmt_opts)

            table = py_register_table(ctx, self._file_path, None, input_fmt, ropts)

            query_df = py_read_table(ctx, table.name)
            datafusion_predicate_applied = False
            if self._predicate_pushdown and self._deferred_predicate is not None:
                from .pushdown import apply_predicate_pushdown

                _fmt_key = str(input_fmt).rsplit(".", 1)[-1]
                _scols, _ucols, _fcols = _FORMAT_COLUMN_TYPES.get(
                    _fmt_key, (None, None, None)
                )
                query_df, _needs_client = apply_predicate_pushdown(
                    query_df,
                    self._deferred_predicate,
                    {
                        "string_cols": _scols,
                        "uint32_cols": _ucols,
                        "float32_cols": _fcols,
                    },
                    log=logger,
                )
                datafusion_predicate_applied = not _needs_client

            # SQL-level column pruning: keep the projection's source columns,
            # plus the predicate roots when the filter is reapplied client-side.
            datafusion_columns = (
                columns if datafusion_predicate_applied else scan_columns
            )
            if datafusion_columns:
                select_exprs = [
                    query_df.parse_sql_expr(f'"{c}"') for c in datafusion_columns
                ]
                query_df = query_df.select(*select_exprs)

            new_lf = _lazy_scan(
                query_df,
                False,
                False,
                table.name,
                input_fmt,
                self._file_path,
                self._read_options,
            )
            if (
                self._deferred_predicate is not None
                and not datafusion_predicate_applied
            ):
                new_lf = new_lf.filter(self._deferred_predicate)
            # Apply the ORIGINAL projection client-side so aliases and computed
            # expressions resolve correctly (the scan above only pruned columns).
            new_lf = new_lf.select(exprs)
            return self._make_wrapper(
                new_lf, projection_pushdown=False, deferred_predicate=None
            )

        # Otherwise delegate to Polars
        return self._make_wrapper(self._base_lf.select(exprs))

    def filter(self, *predicates):
        if not predicates:
            return self
        pred = predicates[0]
        for p in predicates[1:]:
            pred = pred & p
        deferred_predicate = (
            pred
            if self._deferred_predicate is None
            else self._deferred_predicate & pred
        )
        return self._make_wrapper(
            self._base_lf.filter(pred), deferred_predicate=deferred_predicate
        )

    def _extract_predicate_column_names(self):
        if self._deferred_predicate is None:
            return []
        try:
            return list(self._deferred_predicate.meta.root_names())
        except Exception:
            return []

    def __getattr__(self, name):
        return getattr(self._base_lf, name)


class GffLazyFrameWrapper(AnnotationLazyFrameWrapper):
    def __init__(
        self,
        base_lf,
        file_path,
        read_options,
        projection_pushdown=True,
        predicate_pushdown=True,
        deferred_predicate=None,
    ):
        super().__init__(
            base_lf,
            file_path,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            "gff",
            deferred_predicate,
        )


class GtfLazyFrameWrapper(AnnotationLazyFrameWrapper):
    def __init__(
        self,
        base_lf,
        file_path,
        read_options,
        projection_pushdown=True,
        predicate_pushdown=True,
        deferred_predicate=None,
    ):
        super().__init__(
            base_lf,
            file_path,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            "gtf",
            deferred_predicate,
        )
