"""Tap-scoped native-to-Iceberg conversion, its checks, and feasibility reports."""

import json
import logging
import time
from dataclasses import asdict, dataclass
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
        'then rerun run_tap, which resumes any pending conversion. Check the remaining tables '
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


def table_target(fqtn: str) -> ConversionTarget:
    """Plan one explicitly named table without a tap catalog."""
    return ConversionTarget('', SnowflakeObjectName.parse(fqtn))


def tap_blockers(target_config: Mapping[str, Any]) -> List[str]:
    """Return tap-wide reasons that automatic conversion cannot be enabled yet."""
    blockers = []
    if target_config.get('target_table_format') != STATE_ICEBERG:
        blockers.append(
            'The tap is not imported with target_table_format: iceberg yet, so its catalog '
            'may still map MariaDB JSON aliases to TEXT. Pause the tap, import the Iceberg '
            'settings with native_to_iceberg_conversion enabled: false, and run this report again'
        )
    level = target_config.get('data_flattening_max_level') or 0
    if level != 0 or isinstance(level, bool):
        blockers.append(
            f'data_flattening_max_level is {level!r}; native_to_iceberg_conversion '
            'requires 0'
        )
    return blockers


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


def _preflight_blockers(
    converter: SnowflakeNativeToIcebergConverter,
    item: ClassifiedTarget,
) -> Tuple[str, ...]:
    try:
        return tuple(converter.preflight(item.target.table.quoted, item.target.writer_types))
    except Exception as exc:  # report the failure and keep checking other tables
        return (f'Cannot inspect the table: {exc}',)


def preflight_report(
    target_config: Mapping[str, Any],
    runtime_dir: str,
    targets: Sequence[ConversionTarget],
    warehouse: Optional[str] = None,
) -> List[TableReport]:
    """Classify and preflight planned tables without locks, manifests, or DDL."""
    snowflake = SnowflakeConversionQueryAdapter(dict(target_config), warehouse)
    converter = SnowflakeNativeToIcebergConverter(snowflake, runtime_dir)
    reports = []
    for item in classify(targets, snowflake, runtime_dir):
        blockers = tuple(item.reasons)
        if item.primary == STATE_NATIVE and item.state in (STATE_NATIVE, STATE_BLOCKED):
            # A blocked native table is preflighted too, so its other blockers show at once.
            blockers += _preflight_blockers(converter, item)
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
    verb: str,
    action: str,
) -> None:
    table = item.target.table
    LOGGER.info('%s %s (%s rows, %s bytes)', verb, table.key, item.rows, item.bytes)
    started = time.monotonic()
    try:
        converter.convert(table.quoted, eventual=EVENTUAL_ICEBERG)
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
    warehouse: Optional[str],
    action: str,
) -> None:
    snowflake = SnowflakeConversionQueryAdapter(dict(target_config), warehouse)
    converter = SnowflakeNativeToIcebergConverter(snowflake, runtime_dir)
    classified = _unique_tables(classify(targets, snowflake, runtime_dir))
    pending = [item for item in classified if item.state == STATE_PENDING]
    for item in pending:
        # Interrupted cutovers are closed first, so no other refusal leaves a name missing.
        _convert(converter, item, 'Resuming the pending conversion of', action)
    _raise_if_any(
        [(item, item.reasons) for item in classified if item.state == STATE_BLOCKED],
        'needs an operator',
        action,
    )
    candidates = sorted(
        (item for item in classified if item.state == STATE_NATIVE),
        key=lambda item: (item.bytes is None, item.bytes or 0, item.target.table.key),
    )
    _raise_if_any(
        [
            (item, blockers)
            for item in candidates
            if (blockers := converter.preflight(item.target.table.quoted, item.target.writer_types))
        ],
        'cannot be converted',
        action,
    )
    for index, item in enumerate(candidates, start=1):
        _convert(converter, item, f'Converting table {index} of {len(candidates)}:', action)
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
    warehouse: Optional[str] = None,
    *,
    target_id: str = '<target_id>',
    tap_id: str = '<tap_id>',
) -> None:
    """Leave every planned table new or managed Iceberg, or stop before any writer runs."""
    action = next_action(target_id, tap_id)
    try:
        _convert_tap_tables(target_config, runtime_dir, targets, warehouse, action)
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
