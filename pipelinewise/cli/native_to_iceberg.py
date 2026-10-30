"""Tap-scoped native-to-Iceberg conversion checks and feasibility reports."""

import json
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from tabulate import tabulate

from pipelinewise.cli.errors import PreRunChecksException
from pipelinewise.fastsync.commons.snowflake_iceberg import SnowflakeQueryAdapter
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
    """Return tap-wide reasons that automatic conversion cannot be enabled."""
    level = target_config.get('data_flattening_max_level') or 0
    if level != 0 or isinstance(level, bool):
        return [
            f'data_flattening_max_level is {level!r}; native_to_iceberg_conversion '
            'requires 0'
        ]
    return []


def classify(
    targets: Sequence[ConversionTarget],
    snowflake,
    runtime_dir: str,
) -> List[ClassifiedTarget]:
    """Classify planned tables in Snowflake and the target's local recovery state."""
    return classify_conversion_targets(
        targets,
        SnowflakeTableInspector(snowflake),
        RecoveryCoordinator(runtime_dir),
    )


def preflight_report(
    target_config: Mapping[str, Any],
    runtime_dir: str,
    targets: Sequence[ConversionTarget],
) -> List[TableReport]:
    """Classify and preflight planned tables without locks, manifests, or DDL."""
    snowflake = SnowflakeQueryAdapter(dict(target_config))
    converter = SnowflakeNativeToIcebergConverter(snowflake, runtime_dir)
    reports = []
    for item in classify(targets, snowflake, runtime_dir):
        blockers = item.reasons
        if item.state == STATE_NATIVE:
            try:
                blockers = converter.preflight(item.target.table.quoted, item.target.writer_types)
            except Exception as exc:  # report the failure and keep checking other tables
                blockers = (f'Cannot inspect the table: {exc}',)
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
        item for item in classify(targets, snowflake, runtime_dir)
        if item.state not in (STATE_NEW, STATE_ICEBERG)
    ]
    if not_ready:
        details = '; '.join(
            f'{item.target.table.key} is {item.state}'
            + (f' ({item.reasons[0]})' if item.reasons else '')
            for item in not_ready
        )
        raise PreRunChecksException(
            f'FastSync can only publish to new or Iceberg tables: {details}. Convert them '
            'first with run_tap and native_to_iceberg_conversion enabled, or with '
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
