"""Plan and classify a tap's Snowflake tables for native-to-Iceberg conversion."""

import re
from dataclasses import dataclass, field, replace
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from singer.decimal_support import is_decimal_schema

from pipelinewise.fastsync.commons import utils as fastsync_utils
from pipelinewise.fastsync.commons.snowflake_iceberg_coordination import RecoveryCoordinator
from pipelinewise.fastsync.commons.snowflake_iceberg_inspection import (
    SnowflakeTableInspector,
    physical_table_format,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_model import (
    SnowflakeObjectName,
    _row_value,
    _snowflake_boolean,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import (
    RecoveryManifestError,
    TableFormatDiscoveryError,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_versions import parse_exact_integer_metadata


STATE_NEW = 'new'
STATE_ICEBERG = 'iceberg'
STATE_NATIVE = 'native'
STATE_PENDING = 'pending'
STATE_BLOCKED = 'blocked'
TEXT = 'TEXT'
VARIANT = 'VARIANT'

_MISSING = 'missing'
_PREFERRED_SCHEMA_TYPES = ('string', 'array', 'object')
_NATIVE_TYPE_CLASSES = {
    **dict.fromkeys(('TEXT', 'VARCHAR', 'STRING', 'CHAR', 'CHARACTER'), TEXT),
    **dict.fromkeys(('VARIANT', 'OBJECT', 'ARRAY'), VARIANT),
}


@dataclass(frozen=True)
class ConversionTarget:
    """One selected stream and the exact Snowflake table its writers use."""

    stream_id: str
    table: Optional[SnowflakeObjectName]
    writer_types: Dict[str, str] = field(default_factory=dict, compare=False)
    blockers: Tuple[str, ...] = ()


@dataclass(frozen=True)
class ClassifiedTarget:
    """A planned table with its physical state, size, and any reason to stop."""

    target: ConversionTarget
    state: str
    reasons: Tuple[str, ...] = ()
    rows: Optional[int] = None
    bytes: Optional[int] = None
    primary: str = 'missing'


def selected_stream_ids(catalog: Mapping[str, Any]) -> List[str]:
    """Return the tap_stream_id of every stream selected in a Singer catalog."""
    stream_ids = []
    for stream in catalog.get('streams', []):
        root = next(
            (
                entry.get('metadata', {})
                for entry in stream.get('metadata', [])
                if isinstance(entry, dict) and not entry.get('breadcrumb')
            ),
            {},
        )
        if root.get('selected') is True and stream.get('tap_stream_id'):
            stream_ids.append(stream['tap_stream_id'])
    return stream_ids


def _preferred_type(type_value: Any) -> Optional[str]:
    if isinstance(type_value, str):
        return type_value if type_value in _PREFERRED_SCHEMA_TYPES else None
    if isinstance(type_value, list):
        return next((value for value in _PREFERRED_SCHEMA_TYPES if value in type_value), None)
    return None


def _level_zero_schema(schema_property: Any) -> Mapping[str, Any]:
    """Mirror target-snowflake's flatten_schema for one property at level 0."""
    if not isinstance(schema_property, dict):
        return {'type': ['null', 'string']}
    if 'type' in schema_property:
        return schema_property
    if 'anyOf' in schema_property:
        candidates = schema_property['anyOf'] if isinstance(schema_property['anyOf'], list) else []
    elif schema_property:
        first_value = next(iter(schema_property.values()))
        candidates = first_value[:1] if isinstance(first_value, list) else []
    else:
        candidates = []
    for candidate in candidates:
        preferred = _preferred_type(candidate.get('type')) if isinstance(candidate, dict) else None
        if preferred:
            return {**candidate, 'type': ['null', preferred]}
    return {'type': ['null', 'string']}


def singer_text_variant_class(schema_property: Any) -> Optional[str]:
    """Return TEXT or VARIANT when target-snowflake maps a level-0 property to that class."""
    schema = _level_zero_schema(schema_property)
    if schema.get('format') == 'singer.year' or is_decimal_schema(schema):
        return None
    property_type = schema.get('type', [])
    if 'object' in property_type or 'array' in property_type:
        return VARIANT
    if schema.get('format') in ('date-time', 'date', 'time', 'binary'):
        return None
    if 'number' in property_type:
        return None
    if 'integer' in property_type and 'string' in property_type:
        return TEXT
    if 'integer' in property_type or 'boolean' in property_type:
        return None
    return TEXT


def level_zero_column_name(name: str) -> str:
    """Mirror target-snowflake's flatten_key, which shortens names of 255 or more characters."""
    if len(name) < 255:
        return name.upper()
    camelized = re.sub(r'(?:^|_)(.)', lambda match: match.group(1).upper(), name)
    reduced = re.sub(r'[a-z]', '', camelized)
    return (reduced if len(reduced) > 1 else name[0:3]).upper()


def splits_into_child_columns(schema_property: Any, max_level: Any) -> bool:
    """Mirror target-snowflake, which expands a top-level object with properties below max_level."""
    if not isinstance(schema_property, dict) or not isinstance(max_level, (int, float)) or max_level <= 0:
        return False
    if 'type' in schema_property:
        return _preferred_type(schema_property['type']) == 'object' and 'properties' in schema_property
    candidates = schema_property.get('anyOf') if 'anyOf' in schema_property else None
    for candidate in candidates if isinstance(candidates, list) else []:
        if isinstance(candidate, dict) and (preferred := _preferred_type(candidate.get('type'))):
            return preferred == 'object' and 'properties' in candidate
    return False


def native_text_variant_class(data_type: str) -> Optional[str]:
    """Return the TEXT or VARIANT class of a native Snowflake column type."""
    return _NATIVE_TYPE_CLASSES.get(str(data_type).upper())


def text_variant_blockers(columns: Iterable[Any], writer_types: Mapping[str, str]) -> List[str]:
    """Return native columns whose TEXT/VARIANT class differs from the current tap catalog."""
    blockers = []
    for column in columns:
        expected = writer_types.get(column.name)
        actual = native_text_variant_class(column.data_type)
        if expected is not None and actual is not None and expected != actual:
            blockers.append(
                f'Column {column.name} is {column.data_type} but the tap catalog maps it to '
                f'{expected}; the Iceberg writers would refuse the converted table'
            )
    return blockers


def _configured_database(target_config: Mapping[str, Any]) -> str:
    database = target_config.get('dbname')
    if not isinstance(database, str) or not database:
        raise ValueError('The target configuration does not define a Snowflake database')
    if database.startswith('"') and database.endswith('"'):
        return database[1:-1].replace('""', '"')
    return database.upper()


def same_snowflake_database(first: Mapping[str, Any], second: Mapping[str, Any]) -> bool:
    """Return whether two target configurations write the same account and database."""
    try:
        return (
            str(first.get('account') or '').lower() == str(second.get('account') or '').lower()
            and _configured_database(first) == _configured_database(second)
        )
    except ValueError:
        return False


def singer_target_table(
    stream_id: str,
    target_config: Mapping[str, Any],
) -> SnowflakeObjectName:
    """Mirror target-snowflake stream parsing, schema mapping, and table normalization."""
    parts = stream_id.split('-')
    source_schema, table_name = None, stream_id
    if len(parts) == 2:
        source_schema, table_name = parts
    elif len(parts) > 2:
        source_schema, table_name = parts[1], '_'.join(parts[2:])
    schema_mapping = target_config.get('schema_mapping') or {}
    default_schema = (target_config.get('default_target_schema') or '').strip()
    if schema_mapping and source_schema in schema_mapping:
        target_schema = schema_mapping[source_schema].get('target_schema')
    else:
        target_schema = default_schema
    if not isinstance(target_schema, str) or not target_schema:
        raise ValueError(f'No target schema is configured for stream {stream_id}')
    return SnowflakeObjectName(
        _configured_database(target_config),
        target_schema.upper(),
        table_name.replace('.', '_').replace('-', '_').upper(),
    )


def fastsync_target_table(
    source_table: str,
    target_config: Mapping[str, Any],
) -> SnowflakeObjectName:
    """Return the table FullSync and PartialSync publish for one schema.table selection."""
    return SnowflakeObjectName(
        _configured_database(target_config),
        fastsync_utils.get_target_schema(target_config, source_table).upper(),
        fastsync_utils.tablename_to_dict(source_table)['table_name'].upper(),
    )


def _writer_types(stream: Mapping[str, Any], max_level: Any = 0) -> Dict[str, str]:
    """Predict the top-level TEXT/VARIANT columns; objects split into child columns are skipped."""
    properties = (stream.get('schema') or {}).get('properties') or {}
    return {
        level_zero_column_name(name): column_class
        for name, schema_property in properties.items()
        if not splits_into_child_columns(schema_property, max_level)
        and (column_class := singer_text_variant_class(schema_property)) is not None
    }


def _resolved_tables(
    stream_id: str,
    target_config: Mapping[str, Any],
    fastsync_capable: bool,
) -> Tuple[Optional[SnowflakeObjectName], Optional[SnowflakeObjectName], List[str]]:
    """Resolve a stream's Singer and FastSync tables independently, keeping each failure."""
    errors = []
    try:
        table = singer_target_table(stream_id, target_config)
    except Exception as exc:  # a malformed schema mapping can raise more than ValueError
        table = None
        errors.append(str(exc))
    fastsync_table = table
    if fastsync_capable:
        try:
            fastsync_table = fastsync_target_table(stream_id.replace('-', '.', 1), target_config)
        except Exception as exc:  # get_target_schema raises a bare Exception
            fastsync_table = None
            errors.append(f'FastSync cannot name the table of stream {stream_id}: {exc}')
    return table, fastsync_table, errors


def plan_conversion_targets(
    catalog: Mapping[str, Any],
    target_config: Mapping[str, Any],
    stream_ids: Iterable[str],
    fastsync_capable: bool,
) -> List[ConversionTarget]:
    """Map selected streams to the tables both writers use, refusing any disagreement."""
    streams = {stream.get('tap_stream_id'): stream for stream in catalog.get('streams', [])}
    targets = []
    for stream_id in dict.fromkeys(stream_ids):
        singer_table, fastsync_table, blockers = _resolved_tables(stream_id, target_config, fastsync_capable)
        # A known table keeps its recovery state visible, so a refusal can follow a restore.
        table = singer_table or fastsync_table
        if table is None:
            targets.append(ConversionTarget(stream_id, None, blockers=tuple(blockers)))
            continue
        if singer_table is not None and fastsync_table is not None and fastsync_table != singer_table:
            blockers.append(
                f'FastSync writes {fastsync_table.key} but Singer writes {table.key}; '
                'conversion cannot choose between them'
            )
        targets.append(ConversionTarget(
            stream_id,
            table,
            _writer_types(streams.get(stream_id) or {}, target_config.get('data_flattening_max_level') or 0),
            tuple(blockers),
        ))
    return targets


def plan_written_tables(
    catalog: Mapping[str, Any],
    target_config: Mapping[str, Any],
    stream_ids: Iterable[str],
    fastsync_capable: bool,
) -> List[ConversionTarget]:
    """Map another tap's streams to every table its writers use, FastSync names included."""
    streams = {stream.get('tap_stream_id'): stream for stream in catalog.get('streams', [])}
    targets = []
    for stream_id in dict.fromkeys(stream_ids):
        # Each writer that can name a table may write it, even if the other writer cannot.
        tables = {
            table for table in _resolved_tables(stream_id, target_config, fastsync_capable)[:2]
            if table is not None
        }
        writer_types = _writer_types(streams.get(stream_id) or {}, target_config.get('data_flattening_max_level') or 0)
        targets.extend(
            ConversionTarget(stream_id, table, writer_types)
            for table in sorted(tables, key=lambda table: table.key)
        )
    return targets


def _table_format(row: Optional[Mapping[str, Any]]) -> str:
    if row is None:
        return _MISSING
    return physical_table_format(
        row,
        lambda value: _snowflake_boolean(value, 'is_iceberg'),
        STATE_ICEBERG,
        STATE_NATIVE,
    )


def _size(row: Optional[Mapping[str, Any]]) -> Tuple[Optional[int], Optional[int]]:
    """Return SHOW TABLES row and byte counts, which only inform reports and ordering."""
    values = []
    for name in ('rows', 'bytes'):
        try:
            values.append(parse_exact_integer_metadata(_row_value(row or {}, name, '')))
        except ValueError:
            values.append(None)
    return values[0], values[1]


def _classify(
    target: ConversionTarget,
    rows: Mapping[str, Mapping[str, Any]],
    coordinator: RecoveryCoordinator,
    fastsync_rebuild: bool,
) -> ClassifiedTarget:
    table = target.table
    primary, staging, backup = (
        _table_format(rows.get(table.with_suffix(suffix).table))
        for suffix in ('', '_ICEBERG', '_NATIVE')
    )
    size = _size(rows.get(table.table))

    def classified(state: str, reason: Optional[str] = None) -> ClassifiedTarget:
        return ClassifiedTarget(target, state, (reason,) if reason else (), *size, primary)

    try:
        manifest, pointer = coordinator.recovery_store(table).peek()
    except RecoveryManifestError as exc:
        return classified(STATE_BLOCKED, str(exc))
    if manifest is not None:
        if pointer is not None or manifest.kind != 'manual_conversion':
            return classified(
                STATE_BLOCKED,
                f'{table.quoted} has conflicting recovery state; inspect it before replicating',
            )
        if manifest.manifest_payload.eventual != STATE_ICEBERG:
            return classified(
                STATE_BLOCKED,
                f'A conversion of {table.quoted} with --eventual native is pending; finish it '
                'with copy_native_to_iceberg --eventual native first',
            )
        return classified(STATE_PENDING)
    if pointer is not None and primary == STATE_NATIVE:
        return classified(
            STATE_BLOCKED, f'A FastSync Iceberg attempt is pending for native table {table.quoted}'
        )
    if primary == _MISSING:
        # FastSync rebuilds a missing table from its source, so leftover companions cannot hide rows.
        if not fastsync_rebuild and (staging != _MISSING or backup != _MISSING):
            return classified(
                STATE_BLOCKED,
                f'{table.quoted} is missing but its _ICEBERG or _NATIVE companion exists '
                'without a conversion manifest; inspect them before replicating',
            )
        return classified(STATE_NEW)
    if primary == STATE_ICEBERG:
        return classified(STATE_ICEBERG)
    if backup != _MISSING:
        return classified(
            STATE_BLOCKED,
            f'{table.with_suffix("_NATIVE").quoted} already exists, and conversion needs that '
            'name for the native backup. Find out what it holds before renaming or dropping it',
        )
    if staging == STATE_NATIVE:
        return classified(
            STATE_BLOCKED,
            f'{table.with_suffix("_ICEBERG").quoted} is a native table, and conversion needs '
            'that name for its Iceberg copy. Find out what it holds before renaming it',
        )
    if staging != _MISSING:
        return classified(STATE_BLOCKED, unmanifested_copy_reason(table))
    return classified(STATE_NATIVE)


def unmanifested_copy_reason(table: SnowflakeObjectName) -> str:
    """Return the advice for an Iceberg companion that has no conversion manifest."""
    return (
        f'{table.with_suffix("_ICEBERG").quoted} is an Iceberg table without a conversion manifest. '
        'Find out what it holds. Only if it is a copy left by copy_native_to_iceberg --eventual native '
        'or by an abandoned conversion, drop it so conversion can rebuild it, or promote it while the '
        'tap is stopped with copy_native_to_iceberg --eventual iceberg'
    )


def with_refusals(item: ClassifiedTarget, refusals: Sequence[str]) -> ClassifiedTarget:
    """Stop a table for these reasons, but let an interrupted cutover be resumed first."""
    if not refusals:
        return item
    reasons = tuple(dict.fromkeys((*refusals, *item.reasons)))
    if item.state == STATE_PENDING and item.primary == _MISSING:
        # The converter restores the name before it applies the reasons, which then stop a new cutover.
        return replace(item, reasons=reasons)
    return replace(item, state=STATE_BLOCKED, reasons=reasons)


def _companion_collision(
    target: ConversionTarget,
    planned: Mapping[str, ConversionTarget],
    rows: Mapping[str, Mapping[str, Any]],
) -> Optional[str]:
    """Return why a table's reserved companion names collide with another table."""
    table = target.table
    for suffix in ('_ICEBERG', '_NATIVE'):
        companion = planned.get(table.with_suffix(suffix).key)
        if companion is not None:
            return (
                f'{table.with_suffix(suffix).quoted} is the table of stream {companion.stream_id}, '
                f'so it cannot hold the conversion companion of {table.quoted}; change one '
                "stream's schema mapping or deselect it"
            )
    for suffix in ('_ICEBERG', '_NATIVE'):
        if table.table.endswith(suffix) and len(table.table) > len(suffix):
            base = table.with_table(table.table[:-len(suffix)])
            if base.key in planned or base.table in rows:
                return (
                    f'{table.quoted} uses the {suffix} name that conversion reserves for '
                    f"{base.quoted}; change the stream's schema mapping or deselect it"
                )
    return None


def classify_conversion_targets(
    targets: Sequence[ConversionTarget],
    inspector: SnowflakeTableInspector,
    coordinator: RecoveryCoordinator,
    fastsync_rebuild: bool = False,
) -> List[ClassifiedTarget]:
    """Classify planned tables with one SHOW TABLES per schema and lock-free manifest reads."""
    listings = {}
    classified = []
    planned = {target.table.key: target for target in targets if target.table is not None}
    for target in targets:
        if target.table is None:
            classified.append(ClassifiedTarget(target, STATE_BLOCKED, target.blockers))
            continue
        schema = (target.table.database, target.table.schema)
        if schema not in listings:
            listings[schema] = inspector.discover_schema_table_rows(*schema)
        rows = listings[schema]
        if rows is None:
            # A schema too large for one listing is read per table, companions included.
            rows = inspector.discover_schema_table_rows(*schema, starts_with=target.table.table)
            if rows is None:
                raise TableFormatDiscoveryError(
                    f'Snowflake lists too many tables starting with {target.table.quoted} '
                    'to classify it safely'
                )
        item = _classify(target, rows, coordinator, fastsync_rebuild)
        refusals = list(target.blockers)
        if not fastsync_rebuild and item.state in (STATE_NATIVE, STATE_PENDING, STATE_BLOCKED):
            # Conversion renames into the reserved names, so they must not be another table's.
            collision = _companion_collision(target, planned, rows)
            if collision is not None:
                refusals.insert(0, collision)
                stale = unmanifested_copy_reason(target.table)
                item = replace(item, reasons=tuple(reason for reason in item.reasons if reason != stale))
        classified.append(with_refusals(item, refusals))
    return classified
