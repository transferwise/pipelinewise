"""Tap-scoped native-to-Iceberg conversion, its checks, and feasibility reports."""

import json
import logging
import time
from dataclasses import asdict, dataclass, replace
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from tabulate import tabulate

from pipelinewise.cli.errors import NativeToIcebergConversionException, PreRunChecksException
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    SnowflakeConversionQueryAdapter,
    SnowflakeQueryAdapter,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_conversion_plan import (
    STATE_BLOCKED,
    STATE_ICEBERG,
    STATE_NATIVE,
    STATE_NEW,
    STATE_PENDING,
    ClassifiedTarget,
    ConversionTarget,
    classify_conversion_targets,
    fastsync_target_table,
    plan_conversion_targets,
    selected_stream_ids,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_converter import (
    EVENTUAL_ICEBERG,
    NativeToIcebergConversionError,
    SnowflakeNativeToIcebergConverter,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_coordination import RecoveryCoordinator
from pipelinewise.fastsync.commons.snowflake_iceberg_inspection import SnowflakeTableInspector
from pipelinewise.fastsync.commons.snowflake_iceberg_model import SnowflakeObjectName


ACTIONS = {
    STATE_NEW: 'none: the writers create it as Iceberg',
    STATE_ICEBERG: 'none: already Iceberg',
    STATE_NATIVE: 'convert',
    STATE_PENDING: 'resume the pending conversion',
    STATE_BLOCKED: 'resolve the blockers',
}
# Exceptions and alerts name at most this many tables; the log lists every one.
MAX_LISTED_TABLES = 20
LOGGER = logging.getLogger(__name__)


def next_action(target_id: str = '<target_id>', tap_id: str = '<tap_id>') -> str:
    """Return the operator guidance shared by every stopped conversion."""
    return (
        'Replication did not start, and tables converted earlier stay converted. Fix the cause, '
        'keep native_to_iceberg_conversion enabled, and rerun run_tap, which resumes any pending '
        'conversion. Check the remaining tables '
        f'with pipelinewise copy_native_to_iceberg --target {target_id} --tap {tap_id} '
        '--preflight-only --iceberg-version 3.'
    )


def _listed(details: Sequence[str]) -> str:
    """Log every detail, then return a bounded summary for exceptions and alerts."""
    for detail in details:
        LOGGER.error('%s', detail)
    summary = '; '.join(details[:MAX_LISTED_TABLES])
    if len(details) > MAX_LISTED_TABLES:
        summary += f'; and {len(details) - MAX_LISTED_TABLES} more tables listed in the log'
    return summary


@dataclass(frozen=True)
class OtherWriter:
    """A stream of another imported tap that writes one of the planned tables."""

    target: ConversionTarget
    refusal: Optional[str] = None


@dataclass(frozen=True)
class TableReport:
    """The feasibility of converting one planned table."""

    stream: str
    table: str
    state: str
    action: str
    rows: Optional[int]
    bytes: Optional[int]
    blockers: Tuple[str, ...]


def tap_targets(
    catalog: Mapping[str, Any],
    target_config: Mapping[str, Any],
    fastsync_capable: bool,
    stream_ids: Optional[Sequence[str]] = None,
) -> List[ConversionTarget]:
    """Plan the selected streams of one tap, or the given stream IDs."""
    if stream_ids is None:
        stream_ids = selected_stream_ids(catalog)
    return plan_conversion_targets(catalog, target_config, stream_ids, fastsync_capable)


def table_targets(fqtn: str, imported: Sequence[ConversionTarget]) -> List[ConversionTarget]:
    """Plan one named table as the imported streams that write it, or bare without a catalog."""
    table = SnowflakeObjectName.parse(fqtn)
    return [target for target in imported if target.table == table] or [ConversionTarget('', table)]


def writer_types_for_table(fqtn: str, imported: Sequence[ConversionTarget]) -> Dict[str, str]:
    """Return the TEXT/VARIANT classes every imported stream writing one table expects."""
    table = SnowflakeObjectName.parse(fqtn)
    merged, conflicts = _merge_writer_types([target for target in imported if target.table == table])
    if conflicts.get(table.key):
        raise PreRunChecksException(f'Cannot convert {table.key}: ' + '; '.join(conflicts[table.key]))
    return merged.get(table.key, {})


def tap_blockers(target_config: Mapping[str, Any]) -> List[str]:
    """Return tap-wide reasons that automatic conversion cannot be enabled yet."""
    blockers = []
    version = target_config.get('iceberg_version')
    if target_config.get('target_table_format') != STATE_ICEBERG:
        blockers.append(
            'The tap is not imported with target_table_format: iceberg yet, so its catalog '
            'may still map MariaDB JSON aliases to TEXT. Pause the tap, import the Iceberg '
            'settings with native_to_iceberg_conversion enabled: false, and run this report again'
        )
    elif version != 3 or isinstance(version, bool):
        blockers.append(f'iceberg_version is {version!r}; native_to_iceberg_conversion requires 3')
    level = target_config.get('data_flattening_max_level') or 0
    if level != 0 or isinstance(level, bool):
        blockers.append(
            f'data_flattening_max_level is {level!r}; native_to_iceberg_conversion '
            'requires 0'
        )
    return blockers


def assert_imported_for_conversion(target_config: Mapping[str, Any], tap_id: str) -> None:
    """Stop before any Snowflake work unless the tap's generated files carry its Iceberg settings."""
    if blockers := tap_blockers(target_config):
        raise NativeToIcebergConversionException(
            'Automatic native-to-Iceberg conversion stopped before any Snowflake work: the '
            f'imported configuration of tap {tap_id} is not ready for it. {_listed(blockers)}. '
            'Run import_config for the tap, then rerun run_tap. Replication did not start, and '
            'no table was changed.'
        )


def other_tap_writers(
    tap_label: str,
    written: Sequence[ConversionTarget],
    planned: Sequence[ConversionTarget],
    refusal: Optional[str] = None,
) -> List[OtherWriter]:
    """Keep another tap's streams that write a planned table; a refusal blocks converting it."""
    keys = {target.table.key for target in planned if target.table is not None}
    return [
        OtherWriter(
            replace(target, stream_id=f'{target.stream_id} of tap {tap_label}'),
            f'{target.table.quoted} is also written by tap {tap_label}, which {refusal}' if refusal else None,
        )
        for target in written
        if target.table is not None and target.table.key in keys
    ]


def _with_other_writers(
    classified: Sequence[ClassifiedTarget],
    writers: Sequence[OtherWriter],
) -> List[ClassifiedTarget]:
    """Block every native table that another tap writes in a form the converted table breaks."""
    refusals: Dict[str, List[str]] = {}
    for writer in writers:
        if writer.refusal:
            refusals.setdefault(writer.target.table.key, []).append(writer.refusal)
    result = []
    for item in classified:
        reasons = refusals.get(item.target.table.key) if item.target.table else None
        # An interrupted cutover is still closed first; only a native primary is held back.
        if reasons and item.primary == STATE_NATIVE:
            item = replace(item, state=STATE_BLOCKED, reasons=(*item.reasons, *dict.fromkeys(reasons)))
        result.append(item)
    return result


def _writer_classes(
    classified: Sequence[ClassifiedTarget],
    writers: Sequence[OtherWriter],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, List[str]]]:
    """Merge the column classes of this tap's streams and of other Iceberg taps writing its tables."""
    return _merge_writer_types(
        [item.target for item in classified]
        + [writer.target for writer in writers if writer.refusal is None]
    )


def classify(
    targets: Sequence[ConversionTarget],
    snowflake,
    runtime_dir: str,
    fastsync_rebuild: bool = False,
) -> List[ClassifiedTarget]:
    """Classify planned tables in Snowflake and the target's local recovery state."""
    return classify_conversion_targets(
        targets,
        SnowflakeTableInspector(snowflake),
        RecoveryCoordinator(runtime_dir),
        fastsync_rebuild=fastsync_rebuild,
    )


def _merge_writer_types(
    targets: Sequence[ConversionTarget],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, List[str]]]:
    """Combine the column classes of every stream writing a table, and their conflicts."""
    merged: Dict[str, Dict[str, str]] = {}
    sources: Dict[Tuple[str, str], str] = {}
    conflicts: Dict[str, List[str]] = {}
    for target in targets:
        if target.table is None:
            continue
        key = target.table.key
        columns = merged.setdefault(key, {})
        for column, column_class in target.writer_types.items():
            expected = columns.setdefault(column, column_class)
            sources.setdefault((key, column), target.stream_id)
            if expected != column_class:
                conflicts.setdefault(key, []).append(
                    f'Column {column} is {expected} for stream {sources[(key, column)]} but '
                    f'{column_class} for stream {target.stream_id}'
                )
    return merged, conflicts


def _preflight_blockers(
    converter: SnowflakeNativeToIcebergConverter,
    item: ClassifiedTarget,
    writer_types: Mapping[str, str],
) -> Tuple[str, ...]:
    try:
        return tuple(converter.preflight(item.target.table.quoted, writer_types))
    except Exception as exc:  # report the failure and keep checking other tables
        return (f'Cannot inspect the table: {exc}',)


def preflight_report(
    target_config: Mapping[str, Any],
    runtime_dir: str,
    targets: Sequence[ConversionTarget],
    conversion_warehouse: Optional[str] = None,
    other_writers: Sequence[OtherWriter] = (),
) -> List[TableReport]:
    """Classify and preflight planned tables without locks, manifests, or DDL."""
    snowflake = SnowflakeConversionQueryAdapter(dict(target_config), conversion_warehouse)
    converter = SnowflakeNativeToIcebergConverter(snowflake, runtime_dir)
    reports = []
    classified = _with_other_writers(classify(targets, snowflake, runtime_dir), other_writers)
    writer_types, conflicts = _writer_classes(classified, other_writers)
    for item in classified:
        key = item.target.table.key if item.target.table else None
        blockers = tuple(item.reasons) + tuple(conflicts.get(key, ()))
        if item.primary == STATE_NATIVE and item.state != STATE_ICEBERG:
            # Blocked and pending native tables are preflighted too, so every blocker shows.
            blockers += _preflight_blockers(converter, item, writer_types.get(key, {}))
        reports.append(TableReport(
            item.target.stream_id,
            item.target.table.key if item.target.table else '',
            item.state,
            ACTIONS[STATE_BLOCKED if blockers else item.state],
            item.rows,
            item.bytes,
            tuple(blockers),
        ))
    return reports


def _label(item: ClassifiedTarget) -> str:
    table = item.target.table.key if item.target.table else 'an unplanned table'
    return f'{table} (stream {item.target.stream_id})' if item.target.stream_id else table


def _raise_if_any(
    failures: Sequence[Tuple[ClassifiedTarget, Sequence[str]]],
    problem: str,
    action: str,
) -> None:
    if failures:
        details = [
            f'{_label(item)} {problem}: {" | ".join(reasons) or item.state}'
            for item, reasons in failures
        ]
        raise NativeToIcebergConversionException(
            f'Automatic native-to-Iceberg conversion stopped. {_listed(details)}. {action}'
        )


def _unique_tables(classified: Sequence[ClassifiedTarget]) -> List[ClassifiedTarget]:
    """Keep one entry per physical table when several streams map to it."""
    seen = set()
    unique = []
    for item in classified:
        key = item.target.table.key if item.target.table else None
        if key is None or key not in seen:
            unique.append(item)
            seen.add(key)
    return unique


def _convert(
    converter: SnowflakeNativeToIcebergConverter,
    item: ClassifiedTarget,
    writer_types: Mapping[str, str],
    verb: str,
    action: str,
) -> None:
    table = item.target.table
    LOGGER.info('%s %s (%s rows, %s bytes)', verb, table.key, item.rows, item.bytes)
    started = time.monotonic()
    try:
        converter.convert(table.quoted, eventual=EVENTUAL_ICEBERG, writer_types=writer_types)
    except Exception as exc:
        if not isinstance(exc, NativeToIcebergConversionError):
            LOGGER.exception('Unexpected error while converting %s', table.key)
        raise NativeToIcebergConversionException(
            f'Automatic native-to-Iceberg conversion of {_label(item)} failed: {exc}. {action}'
        ) from exc
    LOGGER.info('Converted %s in %.0f seconds', table.key, time.monotonic() - started)


def _convert_tap_tables(
    target_config: Mapping[str, Any],
    runtime_dir: str,
    targets: Sequence[ConversionTarget],
    conversion_warehouse: Optional[str],
    action: str,
    writers: Sequence[OtherWriter],
) -> None:
    snowflake = SnowflakeConversionQueryAdapter(dict(target_config), conversion_warehouse)
    converter = SnowflakeNativeToIcebergConverter(snowflake, runtime_dir)
    classified = _with_other_writers(classify(targets, snowflake, runtime_dir), writers)
    # Every stream's blockers and column classes count; only physical work is de-duplicated.
    writer_types, conflicts = _writer_classes(classified, writers)
    unique = _unique_tables(classified)
    # Filter before de-duplicating, so a blocked sibling stream cannot hide a table's state.
    pending = _unique_tables([
        item for item in classified
        if item.state == STATE_PENDING and item.target.table.key not in conflicts
    ])
    for item in pending:
        # Interrupted cutovers are closed first, so no other refusal leaves a name missing.
        _convert(
            converter, item, writer_types.get(item.target.table.key, {}),
            'Resuming the pending conversion of', action,
        )
    _raise_if_any(
        [(item, item.reasons) for item in classified if item.state == STATE_BLOCKED]
        + [(item, tuple(conflicts[item.target.table.key])) for item in unique
           if item.target.table and item.target.table.key in conflicts],
        'needs an operator',
        action,
    )
    candidates = sorted(
        _unique_tables([item for item in classified if item.state == STATE_NATIVE]),
        key=lambda item: (item.bytes is None, item.bytes or 0, item.target.table.key),
    )
    _raise_if_any(
        [
            (item, blockers)
            for item in candidates
            if (blockers := converter.preflight(
                item.target.table.quoted, writer_types.get(item.target.table.key, {}),
            ))
        ],
        'cannot be converted',
        action,
    )
    for index, item in enumerate(candidates, start=1):
        _convert(
            converter, item, writer_types.get(item.target.table.key, {}),
            f'Converting table {index} of {len(candidates)}:', action,
        )
    if pending or candidates:
        _raise_if_any(
            [
                (item, item.reasons)
                for item in classify(targets, snowflake, runtime_dir)
                if item.state not in (STATE_NEW, STATE_ICEBERG)
            ],
            'is still not Iceberg after conversion',
            action,
        )


def convert_tap_tables(
    target_config: Mapping[str, Any],
    runtime_dir: str,
    targets: Sequence[ConversionTarget],
    conversion_warehouse: Optional[str] = None,
    *,
    other_writers: Sequence[OtherWriter] = (),
    target_id: str = '<target_id>',
    tap_id: str = '<tap_id>',
) -> None:
    """Leave every planned table new or managed Iceberg, or stop before any writer runs."""
    action = next_action(target_id, tap_id)
    try:
        _convert_tap_tables(target_config, runtime_dir, targets, conversion_warehouse, action, other_writers)
    except NativeToIcebergConversionException:
        raise
    except Exception as exc:
        LOGGER.exception('Automatic native-to-Iceberg conversion stopped unexpectedly')
        raise NativeToIcebergConversionException(
            f'Automatic native-to-Iceberg conversion stopped: {exc}. {action}'
        ) from exc


def assert_fastsync_targets_ready(
    target_config: Mapping[str, Any],
    runtime_dir: str,
    source_tables: Sequence[str],
) -> None:
    """Refuse FastSync while a selected target is native or has pending conversion state."""
    targets = [
        ConversionTarget(source_table, fastsync_target_table(source_table, target_config))
        for source_table in source_tables
    ]
    snowflake = SnowflakeQueryAdapter(dict(target_config))
    not_ready = [
        item for item in classify(targets, snowflake, runtime_dir, fastsync_rebuild=True)
        if item.state not in (STATE_NEW, STATE_ICEBERG)
    ]
    if not_ready:
        details = [
            f'{item.target.table.key} is {item.state}'
            + (f' ({item.reasons[0]})' if item.reasons else '')
            for item in not_ready
        ]
        raise PreRunChecksException(
            f'FastSync can only publish to new or Iceberg tables: {_listed(details)}. Convert '
            'them first with run_tap and native_to_iceberg_conversion enabled, or with '
            'copy_native_to_iceberg --eventual iceberg. No source or state changes were made.'
        )


def render_preflight_report(
    reports: Sequence[TableReport],
    blockers: Sequence[str],
    output_format: str,
) -> str:
    """Render a feasibility report as a table or JSON."""
    ready = not blockers and not any(report.blockers for report in reports)
    if output_format == 'json':
        payload: Dict[str, Any] = {
            'ready': ready,
            'blockers': list(blockers),
            'tables': [asdict(report) for report in reports],
        }
        return json.dumps(payload, indent=2)
    rows = [
        [
            report.stream,
            report.table,
            report.state,
            report.action,
            report.rows,
            report.bytes,
            '\n'.join(report.blockers),
        ]
        for report in reports
    ]
    lines = [f'Blocker: {blocker}' for blocker in blockers]
    lines.append(tabulate(
        rows,
        headers=['Stream', 'Table', 'State', 'Action', 'Rows', 'Bytes', 'Blockers'],
    ))
    lines.append(
        'Ready: no blockers found.' if ready
        else 'Not ready: resolve every blocker before enabling native_to_iceberg_conversion.'
    )
    return '\n'.join(lines)
