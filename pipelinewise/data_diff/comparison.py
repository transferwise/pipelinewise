"""Database-neutral type compatibility and aggregate-check semantics."""

from decimal import Decimal


_EXACT_NUMERIC_TYPES = {
    "bigint", "bigserial", "decimal", "dec", "fixed", "int", "integer",
    "mediumint", "number", "numeric", "serial", "smallint", "smallserial",
    "tinyint",
}
_APPROXIMATE_NUMERIC_TYPES = {
    "double", "double precision", "float", "float4", "float8", "real",
}
_STRING_TYPES = {
    "blob", "bpchar", "char", "character", "character varying", "enum",
    "longblob", "mediumblob", "string", "text", "tinyblob", "uuid", "varchar",
}
_BINARY_TYPES = {"binary", "bytea", "varbinary"}
_SEMISTRUCTURED_TYPES = {
    "array", "geometry", "geometrycollection", "json", "jsonb", "linestring",
    "multilinestring", "multipoint", "multipolygon", "object", "point", "polygon",
    "variant",
}


class UnsupportedComparisonError(RuntimeError):
    """Raised when selected physical types cannot be compared safely."""


def column_type_name(column: dict) -> str:
    """Return the most specific normalized database type name available."""
    data_type = str(column.get("data_type") or "").lower()
    udt_name = str(column.get("udt_name") or "").lower()
    if data_type == "tinyint" and udt_name.startswith("tinyint(1)"):
        return "boolean"
    if data_type in ("user-defined", "array") and udt_name:
        return udt_name
    return data_type


# pylint: disable=too-many-return-statements
def type_family(column: dict) -> str:
    """Collapse route-specific physical types into comparable families."""
    type_name = column_type_name(column)
    if column.get("missing"):
        return "missing"
    if type_name in _EXACT_NUMERIC_TYPES:
        return "exact_numeric"
    if type_name in _APPROXIMATE_NUMERIC_TYPES:
        return "approximate_numeric"
    if type_name in _STRING_TYPES:
        return "string"
    if type_name in ("bool", "boolean", "bit"):
        return "boolean"
    if type_name == "date":
        return "date"
    if type_name.startswith("timestamp") or type_name == "datetime":
        return "timestamp"
    if type_name.startswith("time"):
        return "time"
    if type_name in _BINARY_TYPES:
        return "binary"
    if type_name in _SEMISTRUCTURED_TYPES or type_name.startswith("_"):
        return "semistructured"
    return "unknown"


def _schema_families_compatible(source_family: str, target_family: str) -> bool:
    if "missing" in (source_family, target_family):
        return False
    if source_family == target_family:
        return True
    if {source_family, target_family} <= {"exact_numeric", "approximate_numeric"}:
        return True
    # Singer targets can represent a source DATE using a timestamp type.
    if source_family == "date" and target_family == "timestamp":
        return True
    return False


def _column_evidence(logical_name: str, column: dict) -> dict:
    return {
        "logical_name": logical_name,
        "resolved_name": None if column.get("missing") else column.get("name"),
        "data_type": column.get("data_type"),
        "type_family": type_family(column),
        "numeric_precision": column.get("numeric_precision"),
        "numeric_scale": column.get("numeric_scale"),
        "datetime_precision": column.get("datetime_precision"),
        "nullable": column.get("is_nullable"),
    }


def schema_compatibility_result(column_pairs: list) -> dict:
    """Compare selected source and target metadata without reading table rows."""
    source_evidence = []
    target_evidence = []
    findings = []
    for logical_name, source_column, target_column in column_pairs:
        source_family = type_family(source_column)
        target_family = type_family(target_column)
        source_evidence.append(_column_evidence(logical_name, source_column))
        target_evidence.append(_column_evidence(logical_name, target_column))
        if not _schema_families_compatible(source_family, target_family):
            findings.append(
                f"{logical_name}: source {source_family} is not compatible with "
                f"target {target_family}"
            )
    return {
        "check_type": "schema_compatibility",
        "status": "PASS" if not findings else "FAIL",
        "source_value": {"columns": source_evidence},
        "target_value": {"columns": target_evidence},
        "source_query_seconds": None,
        "target_query_seconds": None,
        "error": "; ".join(findings) if findings else None,
    }


def _numeric_scale(column: dict):
    scale = column.get("numeric_scale")
    integer_types = {
        "bigint", "bigserial", "int", "integer", "mediumint", "serial",
        "smallint", "smallserial", "tinyint",
    }
    if scale is None and column_type_name(column) in integer_types:
        return 0
    return int(scale) if scale is not None else None


def checksum_columns(column_pairs: list) -> tuple:
    """Return dialect-independent checksum descriptors or raise on unsafe types."""
    source_descriptors = []
    target_descriptors = []
    for logical_name, source_column, target_column in column_pairs:
        source_family = type_family(source_column)
        target_family = type_family(target_column)
        if not _schema_families_compatible(source_family, target_family):
            raise UnsupportedComparisonError(
                f"row_checksum column '{logical_name}' has incompatible source "
                f"and target types ({source_family}, {target_family})"
            )

        checksum_kind = source_family
        scale = 0
        if checksum_kind == "exact_numeric":
            source_scale = _numeric_scale(source_column)
            target_scale = _numeric_scale(target_column)
            if source_scale is None or target_scale is None:
                raise UnsupportedComparisonError(
                    f"row_checksum column '{logical_name}' needs declared numeric scales"
                )
            # The wider scale: rounding down to the target's would make a target
            # that lost precision hash identically to its source.
            scale = max(source_scale, target_scale)
        elif checksum_kind not in {
            "string", "boolean", "date", "timestamp", "time", "binary",
        }:
            raise UnsupportedComparisonError(
                f"row_checksum column '{logical_name}' has unsupported type family "
                f"'{checksum_kind}'"
            )
        if checksum_kind == "date":
            checksum_kind = "timestamp"

        common = {"checksum_kind": checksum_kind, "checksum_scale": scale}
        source_descriptors.append({**source_column, **common})
        target_descriptors.append({**target_column, **common})
    return source_descriptors, target_descriptors


def metric_passes(check_type: str, source_value, target_value) -> bool:
    """Apply equality checks and side-specific key-integrity invariants."""
    if check_type in ("null_key_count", "duplicate_key_count"):
        try:
            return Decimal(str(source_value)) == 0 and Decimal(str(target_value)) == 0
        except (ArithmeticError, ValueError):
            return False
    return source_value == target_value
