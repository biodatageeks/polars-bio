"""
Comprehensive metadata extraction from Arrow schemas.

This module provides a unified metadata extraction framework that:
1. Extracts ALL metadata from Arrow schemas (schema-level and field-level)
2. Provides format-specific parsers (VCF, FASTQ, BAM, etc.)
3. Returns comprehensive metadata dictionaries

The extraction is non-destructive - all metadata is preserved.
"""

import json
from typing import Any, Dict, List, Optional

import pyarrow as pa


def _decode_metadata_value(value: Any) -> Any:
    """Decode bytes to strings in metadata values."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _decode_metadata_dict(metadata: dict) -> dict:
    """Decode all keys and values in a metadata dictionary from bytes to strings."""
    if not metadata:
        return {}

    return {
        _decode_metadata_value(k): _decode_metadata_value(v)
        for k, v in metadata.items()
    }


def extract_all_schema_metadata(schema: pa.Schema) -> Dict[str, Any]:
    """
    Extract ALL metadata from a PyArrow schema.

    Returns a comprehensive dictionary with:
    - raw_schema_metadata: All schema-level metadata (key-value pairs)
    - raw_field_metadata: All field-level metadata for each field
    - fields_summary: Summary of all fields with their types
    - format_specific: Parsed format-specific metadata (VCF, FASTQ, etc.)

    Args:
        schema: PyArrow schema with metadata

    Returns:
        Comprehensive metadata dictionary
    """
    result = {
        "raw_schema_metadata": {},
        "raw_field_metadata": {},
        "fields_summary": [],
        "format_specific": {},
    }

    # Extract schema-level metadata (ALL of it)
    if schema.metadata:
        result["raw_schema_metadata"] = _decode_metadata_dict(schema.metadata)

    # Extract field-level metadata (ALL of it)
    for field in schema:
        field_info = {
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable,
        }

        if field.metadata:
            decoded_meta = _decode_metadata_dict(field.metadata)
            result["raw_field_metadata"][field.name] = decoded_meta
            field_info["metadata"] = decoded_meta

        result["fields_summary"].append(field_info)

    # Detect format and extract format-specific metadata
    result["format_specific"] = _extract_format_specific_metadata(
        schema, result["raw_schema_metadata"], result["raw_field_metadata"]
    )

    return result


def _extract_format_specific_metadata(
    schema: pa.Schema, schema_meta: dict, field_meta: dict
) -> Dict[str, Any]:
    """
    Extract format-specific metadata based on detected format.

    Args:
        schema: PyArrow schema
        schema_meta: Decoded schema-level metadata
        field_meta: Decoded field-level metadata

    Returns:
        Dictionary with format-specific parsed metadata
    """
    result = {}

    # Detect format from schema metadata
    if any(key.startswith("bio.vcf") for key in schema_meta.keys()):
        result["vcf"] = _extract_vcf_specific_metadata(schema, schema_meta, field_meta)

    if any(key.startswith("bio.fastq") for key in schema_meta.keys()):
        result["fastq"] = _extract_fastq_specific_metadata(
            schema, schema_meta, field_meta
        )

    if any(key.startswith("bio.bam") for key in schema_meta.keys()):
        result["bam"] = _extract_bam_specific_metadata(schema, schema_meta, field_meta)

    if any(key.startswith("bio.gff") for key in schema_meta.keys()):
        result["gff"] = _extract_gff_specific_metadata(schema, schema_meta, field_meta)

    return result


def _extract_vcf_specific_metadata(
    schema: pa.Schema, schema_meta: dict, field_meta: dict
) -> Dict[str, Any]:
    """
    Extract VCF-specific metadata.

    This includes:
    - File format version
    - Contigs
    - Filters
    - Alternative alleles
    - INFO fields
    - FORMAT fields
    - Sample names

    Args:
        schema: PyArrow schema
        schema_meta: Schema-level metadata
        field_meta: Field-level metadata

    Returns:
        VCF-specific metadata dictionary
    """
    vcf_meta = {
        "version": None,
        "contigs": [],
        "filters": [],
        "alt_definitions": [],
        "info_fields": {},
        "format_fields": {},
        "sample_names": [],
    }

    # Extract schema-level metadata
    vcf_meta["version"] = schema_meta.get("bio.vcf.file_format")

    # Extract JSON-encoded arrays
    json_fields = [
        ("bio.vcf.contigs", "contigs"),
        ("bio.vcf.filters", "filters"),
        ("bio.vcf.alternative_alleles", "alt_definitions"),
    ]

    for key, target_key in json_fields:
        value = schema_meta.get(key)
        if value:
            try:
                parsed = json.loads(value)
                vcf_meta[target_key] = parsed if parsed else []
            except (json.JSONDecodeError, TypeError):
                vcf_meta[target_key] = []

    # Extract field-level metadata (INFO/FORMAT fields)
    # The actual metadata uses "bio.vcf.field.*" prefix
    info_fields = {}
    format_fields = {}
    sample_names = []
    seen_samples = set()

    for field_name, metadata in field_meta.items():
        # Check for bio.vcf.field.field_type (the actual key name)
        field_type = metadata.get("bio.vcf.field.field_type")

        if field_type == "INFO":
            info_fields[field_name] = {
                "number": metadata.get("bio.vcf.field.number", "."),
                "type": metadata.get("bio.vcf.field.type", "String"),
                "description": metadata.get("bio.vcf.field.description", ""),
                "id": metadata.get("bio.vcf.field.info_id", field_name),
            }

        elif field_type == "FORMAT":
            format_id = metadata.get("bio.vcf.field.format_id", field_name)

            if format_id not in format_fields:
                format_fields[format_id] = {
                    "number": metadata.get("bio.vcf.field.number", "1"),
                    "type": metadata.get("bio.vcf.field.type", "String"),
                    "description": metadata.get("bio.vcf.field.description", ""),
                }

            # Extract sample name from column name pattern: {sample}_{format}
            if field_name.endswith(f"_{format_id}"):
                sample = field_name[: -len(format_id) - 1]
                if sample and sample not in seen_samples:
                    seen_samples.add(sample)
                    sample_names.append(sample)

    # Handle single-sample VCFs where column name equals format_id
    if format_fields and not sample_names:
        format_ids = set(format_fields.keys())
        for field in schema:
            if field.name in format_ids:
                sample_names = ["sample"]
                break

    vcf_meta["info_fields"] = info_fields if info_fields else {}
    vcf_meta["format_fields"] = format_fields if format_fields else {}
    vcf_meta["sample_names"] = sample_names if sample_names else []

    return vcf_meta


def _extract_fastq_specific_metadata(
    schema: pa.Schema, schema_meta: dict, field_meta: dict
) -> Dict[str, Any]:
    """Extract FASTQ-specific metadata."""
    fastq_meta = {}

    # Extract any bio.fastq.* metadata
    for key, value in schema_meta.items():
        if key.startswith("bio.fastq"):
            # Remove prefix for cleaner keys
            clean_key = key.replace("bio.fastq.", "")
            fastq_meta[clean_key] = value

    return fastq_meta


def _extract_bam_specific_metadata(
    schema: pa.Schema, schema_meta: dict, field_meta: dict
) -> Dict[str, Any]:
    """Extract BAM-specific metadata."""
    bam_meta = {}

    # Extract any bio.bam.* metadata
    for key, value in schema_meta.items():
        if key.startswith("bio.bam"):
            clean_key = key.replace("bio.bam.", "")
            bam_meta[clean_key] = value

    return bam_meta


def _extract_gff_specific_metadata(
    schema: pa.Schema, schema_meta: dict, field_meta: dict
) -> Dict[str, Any]:
    """Extract GFF-specific metadata."""
    gff_meta = {}

    # Extract any bio.gff.* metadata
    for key, value in schema_meta.items():
        if key.startswith("bio.gff"):
            clean_key = key.replace("bio.gff.", "")
            gff_meta[clean_key] = value

    return gff_meta


def get_metadata_summary(full_metadata: dict) -> dict:
    """
    Get a user-friendly summary of metadata.

    Args:
        full_metadata: Full metadata from extract_all_schema_metadata()

    Returns:
        Simplified metadata summary for display
    """
    summary = {
        "total_fields": len(full_metadata.get("fields_summary", [])),
        "fields_with_metadata": len(full_metadata.get("raw_field_metadata", {})),
        "schema_metadata_keys": list(
            full_metadata.get("raw_schema_metadata", {}).keys()
        ),
        "detected_formats": list(full_metadata.get("format_specific", {}).keys()),
    }

    return summary


def format_metadata_for_display(
    full_metadata: dict, format_type: Optional[str] = None
) -> str:
    """
    Format metadata for human-readable display.

    Args:
        full_metadata: Full metadata from extract_all_schema_metadata()
        format_type: Specific format to display (vcf, fastq, etc.) or None for all

    Returns:
        Formatted string representation
    """
    lines = []
    lines.append("=" * 70)
    lines.append("Schema Metadata Summary")
    lines.append("=" * 70)

    # Fields summary
    lines.append(f"\nTotal Fields: {len(full_metadata.get('fields_summary', []))}")
    lines.append(
        f"Fields with Metadata: {len(full_metadata.get('raw_field_metadata', {}))}"
    )

    # Schema-level metadata
    schema_meta = full_metadata.get("raw_schema_metadata", {})
    if schema_meta:
        lines.append(f"\nSchema-Level Metadata ({len(schema_meta)} keys):")
        for key, value in sorted(schema_meta.items()):
            # Truncate long values
            value_str = str(value)
            if len(value_str) > 100:
                value_str = value_str[:100] + "..."
            lines.append(f"  {key}: {value_str}")

    # Format-specific metadata
    format_specific = full_metadata.get("format_specific", {})
    if format_specific:
        lines.append(f"\nDetected Formats: {', '.join(format_specific.keys())}")

        for fmt, fmt_meta in format_specific.items():
            if format_type and fmt != format_type:
                continue

            lines.append(f"\n{fmt.upper()} Metadata:")
            if fmt == "vcf":
                lines.append(f"  Version: {fmt_meta.get('version')}")
                lines.append(f"  INFO Fields: {len(fmt_meta.get('info_fields', {}))}")
                lines.append(
                    f"  FORMAT Fields: {len(fmt_meta.get('format_fields', {}))}"
                )
                lines.append(f"  Samples: {len(fmt_meta.get('sample_names', []))}")
                lines.append(f"  Contigs: {len(fmt_meta.get('contigs', []))}")
                lines.append(f"  Filters: {len(fmt_meta.get('filters', []))}")
            else:
                for key, value in fmt_meta.items():
                    lines.append(f"  {key}: {value}")

    lines.append("\n" + "=" * 70)
    return "\n".join(lines)
