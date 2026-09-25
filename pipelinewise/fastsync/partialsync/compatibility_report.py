"""Read-only deployment report for native Snowflake PartialSync column types."""

import argparse
from contextlib import contextmanager
import json
import logging
from pathlib import Path
import sys

import pymysql

from pipelinewise.fastsync.commons import utils as common_utils
from pipelinewise.fastsync.commons.rdbms_source import RdbmsSnowflakeSource
from pipelinewise.fastsync.commons.snowflake_iceberg_inspection import SnowflakeTableInspector
from pipelinewise.fastsync.commons.snowflake_iceberg_model import SnowflakeObjectName
from pipelinewise.fastsync.commons.snowflake_sql_client import SnowflakeSqlClient
from pipelinewise.fastsync.commons.tap_mysql import FastSyncTapMySql, resolve_source_engine
from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres
from pipelinewise.fastsync.mysql_to_snowflake import tap_type_to_target_type as mysql_type
from pipelinewise.fastsync.postgres_to_snowflake import tap_type_to_target_type as postgres_type
from pipelinewise.fastsync.partialsync.utils import report_source_target_columns
from pipelinewise.fastsync.partialsync.report_diagnostics import ReportMetadataError, report_error


class ReportInputError(ValueError):
    """Generated input files or requested table selection are invalid."""


class MetadataSnowflakeClient(SnowflakeSqlClient):
    """Authenticate without constructing S3 clients or running publication code."""

    def create_query_tag(self, query_tag_props=None):
        return json.dumps({'ppw_component': 'partialsync_compatibility_report'})

    def query(self, query, params=None, query_tag_props=None):
        return self.query_with_timeout(query, params, 30)


def _catalog_tables(properties):
    """Retain exact physical names while matching stream IDs as import_config does."""
    available = {}
    streams = properties.get('streams', [])
    if not isinstance(streams, list):
        raise ReportInputError('Catalog streams must be a list')
    for stream in streams:
        rows = stream.get('metadata', []) if isinstance(stream, dict) else None
        if not isinstance(stream, dict) or not isinstance(rows, list):
            raise ReportInputError('Catalog streams must contain metadata lists')
        if any(not isinstance(row, dict) or not isinstance(row.get('metadata', {}), dict) for row in rows):
            raise ReportInputError('Catalog metadata entries must be objects')
        metadata = next((row.get('metadata', {}) for row in rows if row.get('breadcrumb') == []), {})
        if not metadata.get('selected'):
            continue
        schema = metadata.get('schema-name') or metadata.get('database-name')
        table = stream.get('table_name') or stream.get('stream')
        if any(not isinstance(name, str) or not name or '.' in name for name in (schema, table)):
            raise ReportInputError('Report requires unambiguous schema.table identifiers')
        if not isinstance(stream.get('tap_stream_id'), str) or not stream['tap_stream_id']:
            raise ReportInputError('Selected streams require a string tap_stream_id')
        available[f'{schema}.{table}'] = stream['tap_stream_id'].lower()
    return available


def _resolve_tables(available, selection, requested=None):
    """Resolve exact catalog identifiers without splitting hyphenated stream IDs."""
    if not isinstance(selection, list):
        raise ReportInputError('Selection entries require stream IDs and object boundaries')
    bounded_streams = set()
    for row in selection:
        boundary = row.get('sync_start_from') if isinstance(row, dict) else None
        if (
            not isinstance(row, dict)
            or not isinstance(row.get('tap_stream_id'), str)
            or not row['tap_stream_id']
            or (boundary is not None and not isinstance(boundary, dict))
        ):
            raise ReportInputError('Selection entries require stream IDs and object boundaries')
        if boundary:
            bounded_streams.add(row['tap_stream_id'].lower())
    if requested is not None:
        if set(requested) - available.keys():
            raise ReportInputError('Requested report tables must be selected in properties.json')
        return sorted(set(requested))
    return sorted(table for table, stream_id in available.items() if stream_id in bounded_streams)


def selected_tables(properties, selection, requested=None):
    """Resolve report tables from the generated catalog and selection files."""
    return _resolve_tables(_catalog_tables(properties), selection, requested)


@contextmanager
def source_connection(tap_type, config):
    """Open one metadata connection; never initialize custom SQL or sync state."""
    if tap_type == 'tap-postgres':
        connection = FastSyncTapPostgres.get_connection(config, prioritize_primary=False)
        try:
            connection.set_session(readonly=True, autocommit=True)
            with connection.cursor() as cursor:
                cursor.execute('SET statement_timeout = 30000')
            yield connection
        finally:
            connection.close()
    else:
        source = FastSyncTapMySql(config, mysql_type)
        parameters, _ = source.get_connection_parameters()
        connection = pymysql.connect(
            **parameters, ssl={'': True}, autocommit=True,
            init_command='SET SESSION TRANSACTION READ ONLY',
            connect_timeout=10, read_timeout=30, write_timeout=30,
        )
        try:
            yield connection
        finally:
            connection.close()


def native_source(connection, tap_type, config, target):
    """Use the runtime factory and real configuration without opening a sync lifecycle."""
    if tap_type == 'tap-postgres':
        adapter = RdbmsSnowflakeSource.postgres(FastSyncTapPostgres, postgres_type)
    else:
        adapter = RdbmsSnowflakeSource.mysql(FastSyncTapMySql, mysql_type)
        config = {**config, 'engine': resolve_source_engine(connection, config.get('engine'))}
    return adapter.create(argparse.Namespace(tap=config, target=target, transform=None), iceberg_version=None)


def mapped_source_columns(connection, source, table):
    """Reuse runtime metadata and mapping; the executor cannot reconnect or run custom SQL."""
    def metadata_query(sql, params):
        cursor_args = (pymysql.cursors.DictCursor,) if isinstance(source, FastSyncTapMySql) else ()
        with connection.cursor(*cursor_args) as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    columns = source.get_table_columns(table, metadata_query=metadata_query)
    if not columns:
        raise ReportMetadataError('Source column metadata is missing or inaccessible')
    return source.map_table_columns(columns)


def _table_report(connection, source, target, table):
    result = {'source_table': table}
    operation = 'target_format'
    try:
        schema = common_utils.get_target_schema(target.connection_config, table).upper()
        table_name = table.split('.')[1].upper()
        target_name = SnowflakeObjectName(target.connection_config['dbname'].upper(), schema, table_name)
        result['target_table'] = target_name.quoted
        table_format = SnowflakeTableInspector(target).discover_table_format(schema, table_name)
        if table_format == 'missing':
            return {**result, 'status': 'missing_target', 'reason': 'PartialSync would create this target'}
        if table_format != 'native':
            return {**result, 'status': 'skipped', 'reason': 'Only native Snowflake target tables are supported'}
        operation = 'source_columns'
        mapped = mapped_source_columns(connection, source, table)
        operation = 'target_columns'
        columns = target.query(f'SHOW COLUMNS IN TABLE {target_name.quoted}')
        if not columns:
            raise ReportMetadataError('Target column metadata is missing or inaccessible')
        operation = 'compare_columns'
        column_report = report_source_target_columns({'schema': schema, 'table': table_name}, mapped, columns)
        status = 'incompatible' if any(row['status'] == 'incompatible' for row in column_report) else 'compatible'
        return {**result, 'status': status, 'columns': column_report}
    except Exception as exc:
        return {**result, **report_error(exc, operation)}


def build_report(tap_type, tap, target, properties, selection, tables=None, target_type='target-snowflake'):
    """Return all table outcomes; report does not certify publication or permissions."""
    if tap_type not in ('tap-postgres', 'tap-mysql') or target_type != 'target-snowflake':
        return [{'status': 'skipped', 'reason': 'Only tap-postgres/tap-mysql to Snowflake are supported'}]
    if target.get('target_table_format', tap.get('target_table_format', 'native')) != 'native':
        return [{'status': 'skipped', 'reason': 'Configured Iceberg targets are outside this native-only report'}]
    catalog = _catalog_tables(properties)
    requested_tables = _resolve_tables(catalog, selection, tables)
    if not requested_tables:
        return [{'status': 'skipped', 'reason': 'No selected sync_start_from tables; use --tables to select others'}]
    replacements = {
        row['tap_stream_id'].lower() for row in selection
        if (row.get('sync_start_from') or {}).get('drop_target_table') is True
    } if tables is None else set()
    results = []
    tables_to_check = []
    for table in requested_tables:
        if catalog[table] in replacements:
            results.append({
                'source_table': table, 'status': 'skipped',
                'reason': 'Configured PartialSync replaces this target; existing column types are not reused',
            })
        else:
            tables_to_check.append(table)
    if not tables_to_check:
        return results
    operation = 'source_connection'
    try:
        with source_connection(tap_type, tap) as connection:
            source = native_source(connection, tap_type, tap, target)
            operation = 'target_connection'
            snowflake = MetadataSnowflakeClient(target)
            for table in tables_to_check:
                try:
                    results.append(_table_report(connection, source, snowflake, table))
                except Exception as exc:
                    results.append({'source_table': table, **report_error(exc, 'table_report')})
            operation = 'source_connection'
    except Exception as exc:
        results.append(report_error(exc, operation))
    return results


def main(argv=None):
    """Print JSON; exit 1 for incompatibility/error and 2 for invalid input."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tap-type', required=True)
    parser.add_argument('--target-type', default='target-snowflake')
    parser.add_argument('--tap-dir', required=True, type=Path, help='Generated tap directory from import_config')
    parser.add_argument('--target', required=True, type=Path, help='Generated target config.json')
    parser.add_argument(
        '--tables', help='Comma-separated selected schema.table names; defaults to sync_start_from tables',
    )
    args = parser.parse_args(argv)
    try:
        inputs = {}
        files = {'tap': args.tap_dir / 'config.json', 'target': args.target,
                 'properties': args.tap_dir / 'properties.json', 'selection': args.tap_dir / 'selection.json',
                 'inheritable': args.tap_dir / 'inheritable_config.json'}
        for name, path in files.items():
            with path.open(encoding='utf8') as handle:
                inputs[name] = json.load(handle)
        if not all(isinstance(value, dict) for value in inputs.values()):
            raise ReportInputError('Generated config and catalog files must contain objects')
        inputs['target'].update(inputs.pop('inheritable'))
        inputs['selection'] = inputs['selection'].get('selection')
        if not isinstance(inputs['selection'], list):
            raise ReportInputError('Selection must contain a list')
        tables = [name.strip() for name in args.tables.split(',')] if args.tables is not None else None
    except (OSError, UnicodeError, json.JSONDecodeError, ReportInputError):
        print('Invalid report input; supply generated JSON config, catalog and selection files.', file=sys.stderr)
        return 2
    # Existing FastSync loggers write to stdout; JSON reports must not include connector logs.
    previous_logging = logging.root.manager.disable
    try:
        logging.disable(logging.CRITICAL)
        report = build_report(args.tap_type, **inputs, tables=tables, target_type=args.target_type)
    except ReportInputError:
        print('Invalid report input; supply generated JSON config, catalog and selection files.', file=sys.stderr)
        return 2
    except Exception as exc:
        report = [report_error(exc, 'build_report')]
    finally:
        logging.disable(previous_logging)
    print(json.dumps(report, indent=2))
    return int(any(row['status'] in ('incompatible', 'error') for row in report))


if __name__ == '__main__':
    sys.exit(main())
