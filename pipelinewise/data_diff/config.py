"""Normalize YAML data-diff definitions before storing them in a backend.

The duration grammar and half-open window terminology deliberately follow the
Datafold data-diff project. This PipelineWise subsystem keeps a deliberately
narrower aggregate-check surface than its row-level diff engine.
"""

import hashlib
import json
import logging
import re

from dataclasses import asdict, dataclass
from typing import Iterable, List, Optional

from croniter import croniter


LOGGER = logging.getLogger(__name__)


SUPPORTED_CHECKS = (
    "schema_compatibility",
    "row_count",
    "distinct_key_count",
    "null_key_count",
    "duplicate_key_count",
    "min_key",
    "max_key",
    "row_checksum",
)
SUPPORTED_ROUTES = {
    ("tap-mysql", "target-postgres"),
    ("tap-mysql", "target-snowflake"),
    ("tap-postgres", "target-postgres"),
    ("tap-postgres", "target-snowflake"),
}
_DURATION_PART = re.compile(r"(\d+)(s|min|h|d|w)")
_DURATION_MULTIPLIERS = {
    "s": 1,
    "min": 60,
    "h": 60 * 60,
    "d": 24 * 60 * 60,
    "w": 7 * 24 * 60 * 60,
}


class DataDiffConfigError(ValueError):
    """Raised when a data-diff definition is unsafe or ambiguous."""


def parse_duration(value: str, *, allow_zero: bool = False) -> int:
    """Parse a composable duration such as ``1d6h`` into integer seconds."""
    if not isinstance(value, str) or not value:
        raise DataDiffConfigError("Duration must be a non-empty string such as '1h' or '1d6h'")

    offset = 0
    seconds = 0
    used_units = set()
    while offset < len(value):
        match = _DURATION_PART.match(value, offset)
        if not match:
            raise DataDiffConfigError(f"Invalid duration '{value}'. Supported units are s, min, h, d and w")
        count, unit = match.groups()
        if unit in used_units:
            raise DataDiffConfigError(f"Duration unit '{unit}' is specified more than once in '{value}'")
        used_units.add(unit)
        seconds += int(count) * _DURATION_MULTIPLIERS[unit]
        offset = match.end()

    if seconds == 0 and not allow_zero:
        raise DataDiffConfigError(f"Duration '{value}' must be greater than zero")
    return seconds


def _target_identifier(value: str, target_type: str) -> str:
    if target_type == "target-postgres":
        return value.lower()
    return value.upper()


def _validate_frequency(frequency: str, full_check_name: str) -> str:
    """Reject a frequency croniter cannot parse.

    Validated at import so a bad expression fails ``validate``/``import_config``
    rather than the scheduler, where it would be raised per run.
    """
    if not croniter.is_valid(frequency):
        raise DataDiffConfigError(
            f"frequency must be a crontab expression, got '{frequency}' in check "
            f"'{full_check_name}'. Example: '0 */6 * * *' for every six hours"
        )
    return frequency


def _parse_window_end(raw: dict, window_start_seconds: int, full_check_name: str) -> int:
    """Parse window_end and validate it is closer to fire time than window_start."""
    window_end_seconds = parse_duration(raw.get("window_end", "0s").lstrip("-"), allow_zero=True)
    if window_end_seconds >= window_start_seconds:
        raise DataDiffConfigError(
            f"window_end must be closer to fire time than window_start "
            f"(window_start={window_start_seconds}s, window_end={window_end_seconds}s) "
            f"in check '{full_check_name}'"
        )
    return window_end_seconds


def _target_table_name(
    source_schema: str,
    table_name: str,
    target_type: str,
) -> str:
    """Mirror supported targets' stream parsing and table normalization."""
    stream_parts = f"{source_schema}-{table_name}".split("-")
    if len(stream_parts) == 2:
        parsed_table = stream_parts[1]
    elif len(stream_parts) > 2:
        parsed_table = "_".join(stream_parts[2:])
    else:  # pragma: no cover - supported source stream names always include a schema
        parsed_table = table_name
    normalized = parsed_table.replace(".", "_").replace("-", "_")
    return _target_identifier(normalized, target_type)


@dataclass(frozen=True)
class CheckDefinition:
    """A normalized, credential-free version of one table check."""

    full_check_name: str
    target_id: str
    tap_id: str
    source_type: str
    target_type: str
    source_database: str
    target_database: str
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    source_key_column: str
    target_key_column: str
    source_timestamp_column: str
    target_timestamp_column: str
    source_compare_columns: tuple
    target_compare_columns: tuple
    checks: tuple
    frequency: str
    window_start_seconds: int
    window_end_seconds: int
    statement_timeout_seconds: int

    @property
    def canonical_config(self) -> dict:
        """Return a deterministic, non-secret definition snapshot."""
        value = asdict(self)
        value.pop("full_check_name")
        value["checks"] = list(self.checks)
        value["source_compare_columns"] = list(self.source_compare_columns)
        value["target_compare_columns"] = list(self.target_compare_columns)
        return value

    @property
    def config_hash(self) -> str:
        """Return the SHA-256 fingerprint of the canonical definition."""
        payload = json.dumps(
            self.canonical_config,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


def extract_check_definitions(  # noqa: PLR0912
    global_config: dict,
    targets: dict,
    selected_taps: Optional[Iterable[str]] = None,
) -> List[CheckDefinition]:
    """Extract and validate checks from plain configuration dictionaries."""
    selected = set(selected_taps or ["*"])
    include_all = "*" in selected
    definitions = []

    for target_id in sorted(targets):
        target = targets[target_id]
        target_type = target["type"]
        for tap in sorted(target.get("taps", []), key=lambda item: item["id"]):
            tap_id = tap["id"]
            if not include_all and tap_id not in selected:
                continue

            source_type = tap["type"]
            for schema in tap.get("schemas", []):
                source_schema = schema["source_schema"]
                target_schema = schema["target_schema"]
                for table in schema.get("tables", []):
                    table_config = table.get("data_diff")
                    if table_config is None:
                        continue
                    raw = {**tap.get("data_diff_defaults", {}), **table_config}
                    missing_timing = [field for field in ("frequency", "window_start") if not raw.get(field)]
                    if missing_timing:
                        raise DataDiffConfigError(
                            "Data-diff timing must be defined on the table or inherited "
                            f"from tap data_diff_defaults: {', '.join(missing_timing)}"
                        )

                    route = (source_type, target_type)
                    if route not in SUPPORTED_ROUTES:
                        raise DataDiffConfigError(
                            "Data diff supports only MySQL/MariaDB or PostgreSQL sources "
                            "with PostgreSQL or Snowflake targets; "
                            f"got {source_type} -> {target_type} "
                            f"for tap '{tap_id}'"
                        )

                    checks = tuple(raw["checks"])
                    unsupported = sorted(set(checks) - set(SUPPORTED_CHECKS))
                    if unsupported:
                        raise DataDiffConfigError(f"Unsupported data-diff checks: {unsupported}")

                    key_column = raw["key_column"]
                    timestamp_column = raw["timestamp_column"]
                    compare_columns = tuple(raw.get("compare_columns", []))
                    if "row_checksum" in checks and not compare_columns:
                        raise DataDiffConfigError(
                            "row_checksum requires at least one compare_columns entry for "
                            f"{source_schema}.{table['table_name']}"
                        )
                    transformed_columns = {
                        transformation["column"] for transformation in table.get("transformations", [])
                    }
                    incomparable = sorted(
                        {key_column, timestamp_column, *compare_columns}.intersection(transformed_columns)
                    )
                    if incomparable:
                        raise DataDiffConfigError(
                            f"Data-diff columns cannot be transformed for {source_schema}."
                            f"{table['table_name']}: {', '.join(incomparable)}"
                        )

                    full_check_name = "/".join((target_id, tap_id, source_schema, table["table_name"]))
                    frequency = _validate_frequency(raw["frequency"], full_check_name)
                    window_start_seconds = parse_duration(raw["window_start"].lstrip("-"))
                    definitions.append(
                        CheckDefinition(
                            full_check_name=full_check_name,
                            target_id=target_id,
                            tap_id=tap_id,
                            source_type=source_type,
                            target_type=target_type,
                            source_database=tap["db_conn"]["dbname"],
                            target_database=target["db_conn"]["dbname"],
                            source_schema=source_schema,
                            source_table=table["table_name"],
                            target_schema=_target_identifier(target_schema, target_type),
                            target_table=_target_table_name(source_schema, table["table_name"], target_type),
                            source_key_column=key_column,
                            target_key_column=_target_identifier(key_column, target_type),
                            source_timestamp_column=timestamp_column,
                            target_timestamp_column=_target_identifier(timestamp_column, target_type),
                            source_compare_columns=compare_columns,
                            target_compare_columns=tuple(
                                _target_identifier(column, target_type) for column in compare_columns
                            ),
                            checks=checks,
                            frequency=frequency,
                            window_start_seconds=window_start_seconds,
                            window_end_seconds=_parse_window_end(raw, window_start_seconds, full_check_name),
                            statement_timeout_seconds=parse_duration(raw.get("statement_timeout", "5min")),
                        )
                    )

    if definitions and not global_config.get("backend_db"):
        # Data-diff is opt-in through backend_db. Without it there is nowhere to
        # persist definitions, so drop them rather than blocking replication.
        LOGGER.warning(
            "Ignoring %s data_diff definition(s): config.yml does not define backend_db. Replication is unaffected.",
            len(definitions),
        )
        return []

    full_check_names = [definition.full_check_name for definition in definitions]
    if len(full_check_names) != len(set(full_check_names)):
        raise DataDiffConfigError("Data-diff check names must be unique for each table")

    return sorted(definitions, key=lambda definition: definition.full_check_name)
