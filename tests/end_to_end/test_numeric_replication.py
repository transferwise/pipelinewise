"""Preserve decimal and float rows, identities and supported precision through bulk and Singer."""

import json
import math
import os
import subprocess
from argparse import Namespace
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from pathlib import Path
from uuid import uuid4

import pytest

from pipelinewise.cli import mysql_float_conditions
from pipelinewise.cli.config import Config
from pipelinewise.data_diff.engine import run_check
from pipelinewise.fastsync import (
    mysql_to_postgres, mysql_to_snowflake, postgres_to_postgres, postgres_to_snowflake, yugabyte_to_postgres,
    yugabyte_to_snowflake,
)
from pipelinewise.fastsync.commons.target_postgres import FastSyncTargetPostgres
from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake
from pipelinewise.fastsync.commons.source_numeric import postgres_float_expression
from pipelinewise.fastsync.partialsync import mysql_to_snowflake as partial_mysql
from pipelinewise.fastsync.partialsync import postgres_to_snowflake as partial_postgres
from pipelinewise.fastsync.partialsync import yugabyte_to_snowflake as partial_yugabyte
from tests.end_to_end.target_snowflake.test_source_transformation_exports import (
    RAW_SECRET, SOURCE_MODES, _SourceExport, float32,
)
from tests.end_to_end.target_snowflake.test_source_transformation_exports import source_export as source_export
from tests.end_to_end.target_snowflake.test_source_transformation_publication import (
    _connector_python, _remove_s3_prefix, _target_config,
)


EXACT = '12345678901234567890.123456789012345678'
NEXT = '98765432109876543210.987654321098765432'
SNOWFLAKE_FULL = {'postgres': postgres_to_snowflake, 'yugabyte': yugabyte_to_snowflake}
SNOWFLAKE_PARTIAL = {'postgres': partial_postgres, 'yugabyte': partial_yugabyte}
POSTGRES_FULL = {'postgres': postgres_to_postgres, 'yugabyte': yugabyte_to_postgres}
FLOAT_VALUES = (math.nan, math.inf, -math.inf, 1.5)
# Rows 5-8 repeat these four array patterns, so Singer loads every array FastSync loaded into rows 1-4.
# The last two cross NULL and empty arrays between the two array columns.
FLOAT_ARRAYS = ([math.nan, -math.inf], [math.inf, 1.5], None, [])
# numeric[] literals and the array_to_json text (lower bounds stripped) FastSync exports and decimal-route taps carry.
# YugabyteDB arrays keep NaN, infinities and 1.10, though its scalar NUMERIC rejects the tokens and trims scale.
NUMERIC_ARRAYS = (
    ('{1,1.10,12345678901234567890.123,0.00000012,NaN,Infinity,-Infinity,NULL}',
     '[1,1.10,12345678901234567890.123,0.00000012,"NaN","Infinity","-Infinity",null]'),
    ('[0:1][0:1]={{1,2},{3,4}}', '[[1,2],[3,4]]'),
    ('{}', '[]'),
    (None, None),
)
# Column type, row-1 literal and decimal-route record value; FastSync and then Singer load each into one jsonb.
POSTGRES_TARGET_ARRAYS = {
    'numbers': ('NUMERIC[]', *NUMERIC_ARRAYS[0]),
    'no_numbers': ('NUMERIC[]', *NUMERIC_ARRAYS[2]),
    'null_numbers': ('NUMERIC[]', *NUMERIC_ARRAYS[3]),
    'null_doubles': ('DOUBLE PRECISION[]', None, None),
}
# MAX() of the first four keys is NaN, which sorts above Infinity: FastSync must bookmark the finite 2.0,
# so Singer re-reads the Infinity and NaN keys and leaves -Infinity behind.
FLOAT_KEYS = (-math.inf, 2.0, math.inf, math.nan, 5.0, 6.0, 7.0, 8.0)
# Six-digit text renders the FLOAT maximum 1234567.75 as 1234570, above every key Singer must read next.
MYSQL_FLOAT_KEYS = (0.5, 1.5, 2.5, 1234567.8, 1234568.0, 1234568.5, 1234569.0, 1234569.5)


def _source_mode_id(mode):
    return f'{mode[0]}-{"v3" if mode[1] else "native"}'


@pytest.mark.parametrize('source_export', [('postgres', False)], indirect=True)
def test_postgres_decimal_float_underflow(source_export):
    """Round source-side casts correctly at the exact binary64 underflow midpoint."""
    with localcontext() as context:
        context.prec = 1100
        minimum = Decimal.from_float(float.fromhex('0x0.0000000000001p-1022'))
        midpoint = minimum / 2
        epsilon = Decimal('1e-1100')
        positive = [
            midpoint - epsilon, midpoint, midpoint + epsilon, Decimal('2.47032822920623275e-324'),
            minimum, Decimal('5e-324'), Decimal('1e-323'),
        ]
    values = positive + [value.copy_negate() for value in positive] + [Decimal(0), None]
    expression = postgres_float_expression('amount')

    rows = source_export.source.query(
        f'SELECT {expression} AS amount FROM unnest(%s::numeric[]) '
        'WITH ORDINALITY AS inputs(amount, position) ORDER BY position',
        params=(values,),
    )

    assert [row['amount'] for row in rows] == [float(value) if value is not None else None for value in values]


def _add_data_diff_window(source: _SourceExport) -> None:
    """Replicate a settled UTC timestamp with the numeric fixture, using its own index."""
    source.execute(
        f'ALTER TABLE {source.quoted_table} ADD COLUMN updated_at TIMESTAMP '
        "DEFAULT '2026-01-01 00:00:00'"
    )
    order = ' ASC' if source.engine == 'yugabyte' else ''
    source.execute(f'CREATE INDEX {source.table_name}_dd_time ON {source.quoted_table} (updated_at{order})')


def _assert_float_data_diff(
    source: _SourceExport, config: dict, target: FastSyncTargetSnowflake, compare_columns: list[str],
    *, key: str = 'id', mutation: tuple[str, int] | None = None,
) -> None:
    """Check actual replicated values, then detect an independent one-ULP target mutation."""
    schema = config['default_target_schema']
    table = f'"{schema}"."{source.table_name.upper()}"'
    check = {
        'check_id': str(uuid4()),
        'source_type': {'postgres': 'tap-postgres', 'yugabyte': 'tap-yugabyte'}.get(source.engine, 'tap-mysql'),
        'target_type': 'target-snowflake',
        'source_schema': source.schema, 'source_table': source.table_name,
        'target_schema': schema, 'target_table': source.table_name.upper(),
        'source_key_column': key, 'target_key_column': key.upper(),
        'source_timestamp_column': 'updated_at', 'target_timestamp_column': 'UPDATED_AT',
        'source_compare_columns': list(compare_columns),
        'target_compare_columns': [column.upper() for column in compare_columns],
        'statement_timeout_seconds': 60,
        'checks': ('schema_compatibility', 'row_count', 'row_checksum'),
    }

    def compare(start_day: int = 1, end_day: int = 2) -> tuple[dict[str, dict], str | None]:
        preflight, results, status = run_check(
            check, source.source.connection_config, config,
            datetime(2026, 1, start_day, tzinfo=timezone.utc), datetime(2026, 1, end_day, tzinfo=timezone.utc),
        )
        assert preflight['status'] == 'PASS', preflight
        return {result['check_type']: result for result in results}, status

    results, status = compare()
    assert status == 'PASS', results
    assert all(result['status'] == 'PASS' for result in results.values()), results
    if mutation is None:
        return
    column, row_key = mutation
    selected = f'"{column.upper()}"'
    predicate = f'"{key.upper()}" = %s'
    original = target.query(f'SELECT {selected} AS VALUE FROM {table} WHERE {predicate}', (row_key,))[0]['VALUE']
    assert isinstance(original, float) and math.isfinite(original)
    changed = math.nextafter(original, math.inf)
    assert changed != original
    try:
        target.query(f'UPDATE {table} SET {selected} = %s WHERE {predicate}', (changed, row_key))
        stored = target.query(f'SELECT {selected} AS VALUE FROM {table} WHERE {predicate}', (row_key,))[0]['VALUE']
        assert stored == changed
        results, status = compare()
        assert status == 'FAIL', results
        assert results['row_checksum']['status'] == 'FAIL', results
        assert results['row_count']['status'] == 'PASS', results
        assert results['schema_compatibility']['status'] == 'PASS', results
        results, status = compare(2, 3)
        assert status == 'PASS', results
        assert results['row_count']['source_value'] == results['row_count']['target_value'] == '0'
    finally:
        target.query(f'UPDATE {table} SET {selected} = %s WHERE {predicate}', (original, row_key))


def _args(source, config, tmp_path, namespace, replication_key='amount'):
    runtime = tmp_path / namespace
    runtime.mkdir()
    return Namespace(
        tap={**source.source.connection_config, 'tap_id': namespace}, target=config,
        transform={'transformations': []}, temp_dir=str(tmp_path),
        state=str(runtime / 'state.json'), end_value='4',
        properties={'streams': [{'stream': source.table_name, 'metadata': [{
            'breadcrumb': [], 'metadata': {
                'database-name': source.source.connection_config['dbname'],
                'schema-name': source.schema, 'replication-method': 'INCREMENTAL',
                'replication-key': replication_key,
            },
        }]}]},
    )


def _catalog_path(args):
    """Locate the catalog _tap_extract saves: the selected stream exactly as discovery reported it."""
    return Path(args.state).with_name('tap_catalog.json')


def _tap_extract(
    source, target_type, args, *, dimensions=(38, 18), start=EXACT, end=NEXT, replication_method='INCREMENTAL',
):
    """Discover and run the installed SQL tap, using FastSync's bookmark for incremental reads."""
    tap_type = {'postgres': 'tap-postgres', 'yugabyte': 'tap-yugabyte'}.get(source.engine, 'tap-mysql')
    tap = {'type': tap_type, 'db_conn': source.source.connection_config}
    config = Config.generate_tap_connection_config(tap, {}, f'target-{target_type}')
    config['filter_schemas' if source.postgres_dialect else 'filter_dbs'] = source.schema
    assert config['decimal_target'] == target_type
    config_path = Path(args.state).with_name('tap_config.json')
    catalog_path = _catalog_path(args)
    config_path.write_text(json.dumps(config), encoding='utf-8')
    executable = Path(os.environ['PIPELINEWISE_HOME']) / '.virtualenvs' / tap_type / 'bin' / tap_type

    def run(*options):
        result = subprocess.run(
            [str(executable), '--config', str(config_path), *options],
            capture_output=True, text=True, check=False, timeout=300,
        )
        assert result.returncode == 0, f'{tap_type} failed:\n{result.stderr}'
        return result.stdout

    catalog = json.loads(run('--discover'))
    selected = [stream for stream in catalog['streams'] if (
        stream.get('table_name', stream.get('table', stream['stream'])) == source.table_name
    )]
    assert len(selected) == 1
    stream = selected[0]
    replication_key = args.properties['streams'][0]['metadata'][0]['metadata']['replication-key']
    for entry in stream['metadata']:
        entry['metadata']['selected'] = True
        if not entry['breadcrumb']:
            entry['metadata']['replication-method'] = replication_method
            if replication_method == 'INCREMENTAL':
                entry['metadata']['replication-key'] = replication_key
            else:
                entry['metadata'].pop('replication-key', None)
    if dimensions is not None:
        assert stream['schema']['properties'][replication_key]['decimal'] == {
            'precision': dimensions[0], 'scale': dimensions[1],
        }
    if replication_method == 'INCREMENTAL':
        bookmark = json.loads(Path(args.state).read_text(encoding='utf-8'))['bookmarks'][stream['tap_stream_id']]
        assert bookmark['replication_key_value'] == start
    catalog_path.write_text(json.dumps({'streams': selected}), encoding='utf-8')
    options = ['--catalog', str(catalog_path)]
    if replication_method == 'INCREMENTAL':
        options.extend(('--state', args.state))
    messages = [json.loads(line) for line in run(*options).splitlines()]
    records = [message['record'] for message in messages if message['type'] == 'RECORD']
    assert records
    if dimensions is not None:
        assert all(isinstance(record[replication_key], str) for record in records)
    if replication_method == 'INCREMENTAL':
        final_state = [message['value'] for message in messages if message['type'] == 'STATE'][-1]
        assert final_state['bookmarks'][stream['tap_stream_id']]['replication_key_value'] == end
    return messages


def _load_singer(config, target_type, messages, transformations=()):
    """Pass actual tap protocol output through the installed transform-field, with any transformations, and target."""
    transformed = _connector_python('transform-field', '''
import json
import sys
from transform_field import TransformField
payload = json.load(sys.stdin)
TransformField({'transformations': payload['transformations']}).consume(
    json.dumps(message) for message in payload['messages']
)
''', {'messages': messages, 'transformations': list(transformations)})
    target_program = '''
import json
import sys
payload = json.load(sys.stdin)
'''
    if target_type == 'snowflake':
        target_program += '''
from target_snowflake import get_snowflake_statics, persist_lines
cache, file_format = get_snowflake_statics(payload['config'])
persist_lines(payload['config'], payload['lines'], cache, file_format)
'''
    else:
        target_program += '''
from target_postgres import persist_lines
persist_lines(payload['config'], payload['lines'])
'''
    _connector_python(f'target-{target_type}', target_program, {
        'config': {**config, 'parallelism': 1}, 'lines': transformed.splitlines(),
    })


@pytest.mark.parametrize(
    'source_export', [*SOURCE_MODES, ('yugabyte', False)], indirect=True, ids=_source_mode_id,
)
def test_decimal_fastsync_snowflake(source_export, tmp_path):
    """Native/v3 FullSync, PartialSync versioning and Singer keep all 38 digits."""
    source = source_export
    namespace = f'ppw_decimal_{uuid4().hex[:12]}'
    schema = namespace.upper()
    config = {**_target_config(schema, namespace), 'archive_load_files': False}
    if source.iceberg:
        config.update(target_table_format='iceberg', iceberg_version=3)
    target = FastSyncTargetSnowflake(config)
    source.create([
        ('id', 'INTEGER PRIMARY KEY'), ('amount', 'NUMERIC(18,2)'), ('exact_amount', 'NUMERIC(38,18)'),
    ], [(1, '12.34', '1.000000000000000001'), (2, '56.78', EXACT)])
    args = _args(source, config, tmp_path, namespace, replication_key='exact_amount')
    full = SNOWFLAKE_FULL.get(source.engine, mysql_to_snowflake)
    partial = SNOWFLAKE_PARTIAL.get(source.engine, partial_mysql)
    table = f'"{schema}"."{source.table_name.upper()}"'
    try:
        result = full.sync_table(source.table, args)
        assert result is True, result
        assert Decimal(target.query(
            f'SELECT TO_VARCHAR("EXACT_AMOUNT") AS AMOUNT FROM {table} WHERE "ID" = 2',
        )[0]['AMOUNT']) == Decimal(EXACT)
        alter = 'ALTER COLUMN amount TYPE' if source.postgres_dialect else 'MODIFY COLUMN amount'
        source.execute(f'ALTER TABLE {source.quoted_table} {alter} NUMERIC(38,18)')
        source.execute(f'UPDATE {source.quoted_table} SET amount = %s WHERE id = 2', (EXACT,))
        source.execute(f'INSERT INTO {source.quoted_table} VALUES (%s,%s,%s)', (3, NEXT, NEXT))
        request = (source.table, {
            'column': 'id', 'start_value': '<S>2', 'end_value': '<S>4', 'drop_target_table': False,
        })
        result = partial.partial_sync_table(request, args)
        assert result is True, result
        rows = target.query(f'SELECT "ID", TO_VARCHAR("AMOUNT") AS AMOUNT FROM {table} ORDER BY "ID"')
        assert rows[0]['AMOUNT'] is None
        assert Decimal(rows[1]['AMOUNT']) == Decimal(EXACT)
        assert Decimal(rows[2]['AMOUNT']) == Decimal(NEXT)
        columns = target.query(
            'SELECT COLUMN_NAME, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS '
            'WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s', (schema, source.table_name.upper()),
        )
        archives = [row['COLUMN_NAME'] for row in columns if row['COLUMN_NAME'].startswith('AMOUNT_')]
        assert len(archives) == 1
        assert next(row for row in columns if row['COLUMN_NAME'] == 'AMOUNT') == {
            'COLUMN_NAME': 'AMOUNT', 'NUMERIC_PRECISION': 38, 'NUMERIC_SCALE': 18,
        }
        history = target.query(f'SELECT "{archives[0]}" AS OLD FROM {table} WHERE "ID" = 1')
        assert history[0]['OLD'] == Decimal('12.34')
        result = partial.partial_sync_table(request, args)
        assert result is True, result
        source.execute(f'INSERT INTO {source.quoted_table} VALUES (%s,%s,%s)', (4, EXACT, NEXT))
        messages = _tap_extract(source, 'snowflake', args)
        assert {message['record']['id'] for message in messages if message['type'] == 'RECORD'} == {2, 3, 4}
        _load_singer(config, 'snowflake', messages)
        assert Decimal(target.query(f'SELECT TO_VARCHAR("AMOUNT") AS AMOUNT FROM {table} WHERE "ID" = 4')[0][
            'AMOUNT'
        ]) == Decimal(EXACT)
    finally:
        try:
            target.query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            _remove_s3_prefix(target, config['s3_key_prefix'])


def test_legacy_decimal_float_migration_requires_opt_in(source_export, tmp_path):
    """Keep legacy decimal FLOAT columns until the tap explicitly versions them."""
    source = source_export
    namespace = f'ppw_decimal_migration_{uuid4().hex[:12]}'
    schema = namespace.upper()
    config = {**_target_config(schema, namespace), 'archive_load_files': False}
    if source.iceberg:
        config.update(target_table_format='iceberg', iceberg_version=3)
    target = FastSyncTargetSnowflake(config)
    source.create(
        [('id', 'INTEGER PRIMARY KEY'), ('amount', 'DECIMAL(65,30)'), ('position', 'INTEGER')],
        [(1, '12.34', 1), (2, '56.78', 2)],
    )
    _add_data_diff_window(source)
    args = _args(source, config, tmp_path, namespace, replication_key='position')
    full = SNOWFLAKE_FULL.get(source.engine, mysql_to_snowflake)
    partial = SNOWFLAKE_PARTIAL.get(source.engine, partial_mysql)
    table = f'"{schema}"."{source.table_name.upper()}"'
    request = (source.table, {
        'column': 'position', 'start_value': '<S>1', 'end_value': '<S>2', 'drop_target_table': False,
    })

    def amount_columns():
        return [
            column for column in _fallback_columns(target, schema, source.table_name)
            if column['COLUMN_NAME'] == 'AMOUNT' or column['COLUMN_NAME'].startswith('AMOUNT_')
        ]

    try:
        result = full.sync_table(source.table, args)
        assert result is True, result
        columns = amount_columns()
        assert len(columns) == 1
        assert columns[0]['COLUMN_NAME'] == 'AMOUNT'
        assert columns[0]['DATA_TYPE'] == 'FLOAT'
        _assert_float_data_diff(source, config, target, ['amount', 'position'])

        alter = 'ALTER COLUMN amount TYPE' if source.postgres_dialect else 'MODIFY COLUMN amount'
        source.execute(f'ALTER TABLE {source.quoted_table} {alter} DECIMAL(18,2)')
        result = partial.partial_sync_table(request, args)
        assert result is True, result
        columns = amount_columns()
        assert len(columns) == 1
        assert columns[0]['COLUMN_NAME'] == 'AMOUNT'
        assert columns[0]['DATA_TYPE'] == 'FLOAT'
        _assert_float_data_diff(source, config, target, ['amount', 'position'], mutation=('amount', 1))

        config['version_legacy_float_columns'] = True
        result = partial.partial_sync_table(request, args)
        assert result is True, result
        columns = amount_columns()
        current = next(column for column in columns if column['COLUMN_NAME'] == 'AMOUNT')
        assert current == {
            'COLUMN_NAME': 'AMOUNT', 'DATA_TYPE': 'NUMBER',
            'NUMERIC_PRECISION': 18, 'NUMERIC_SCALE': 2,
        }
        archives = [column for column in columns if column['COLUMN_NAME'].startswith('AMOUNT_')]
        assert len(archives) == 1
        assert archives[0]['DATA_TYPE'] == 'FLOAT'
        rows = target.query(f'SELECT "ID", TO_VARCHAR("AMOUNT") AS AMOUNT FROM {table} ORDER BY "ID"')
        assert [(row['ID'], Decimal(row['AMOUNT'])) for row in rows] == [
            (1, Decimal('12.34')), (2, Decimal('56.78')),
        ]
    finally:
        try:
            target.query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            _remove_s3_prefix(target, config['s3_key_prefix'])


@pytest.mark.parametrize('source_export', [('postgres', False), ('postgres', True)], indirect=True)
def test_postgres_bounded_numeric_key_snowflake(source_export, tmp_path):
    """Keep finite and NaN PostgreSQL keys stable through native/v3 bulk and Singer loads."""
    source = source_export
    namespace = f'ppw_decimal_key_{uuid4().hex[:12]}'
    schema = namespace.upper()
    config = {
        **_target_config(schema, namespace),
        'archive_load_files': False,
        'source_tap_type': 'tap-postgres',
    }
    if source.iceberg:
        config.update(target_table_format='iceberg', iceberg_version=3)
    target = FastSyncTargetSnowflake(config)
    source.create([
        ('id', 'NUMERIC(10,2) PRIMARY KEY'), ('amount', 'INTEGER'), ('position', 'INTEGER'),
    ], [('NaN', 10, 1), ('1.00', 20, 2)])
    args = _args(source, config, tmp_path, namespace, replication_key='position')
    table = f'"{schema}"."{source.table_name.upper()}"'

    def assert_rows(expected):
        rows = target.query(f'SELECT "ID", "AMOUNT", "POSITION" FROM {table}')
        assert len(rows) == len(expected)
        assert {row['ID']: (row['AMOUNT'], row['POSITION']) for row in rows} == expected

    try:
        result = postgres_to_snowflake.sync_table(source.table, args)
        assert result is True, result
        assert_rows({'1': (20, 2), 'NaN': (10, 1)})
        original_columns = _fallback_columns(target, schema, source.table_name)
        assert next(column for column in original_columns if column['COLUMN_NAME'] == 'ID')['DATA_TYPE'] == 'TEXT'

        request = (source.table, {
            'column': 'position', 'start_value': '<S>1', 'end_value': '<S>2', 'drop_target_table': False,
        })
        for _ in range(2):
            result = partial_postgres.partial_sync_table(request, args)
            assert result is True, result
            assert_rows({'1': (20, 2), 'NaN': (10, 1)})
            assert _fallback_columns(target, schema, source.table_name) == original_columns

        source.execute(f'UPDATE {source.quoted_table} SET amount=%s, position=%s WHERE id=%s', (101, 3, 'NaN'))
        source.execute(f'UPDATE {source.quoted_table} SET amount=%s, position=%s WHERE id=%s', (202, 4, '1.00'))
        messages = _tap_extract(source, 'snowflake', args, dimensions=None, start=2, end=4)
        records = [message['record'] for message in messages if message['type'] == 'RECORD']
        assert {record['id'] for record in records} == {'1.00', 'NaN'}
        _load_singer(config, 'snowflake', messages)
        assert_rows({'1': (202, 4), 'NaN': (101, 3)})

        _delete_from_singer(config, source, messages, 'NaN', key_column='id')
        assert_rows({'1': (202, 4)})
    finally:
        try:
            target.query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            _remove_s3_prefix(target, config['s3_key_prefix'])


@pytest.mark.parametrize('source_export', [('yugabyte', False)], indirect=True)
def test_yugabyte_bounded_numeric_key_snowflake(source_export, tmp_path):
    """Keep YSQL numeric keys exact and NUMERIC through bulk, partial and Singer loads."""
    source = source_export
    namespace = f'ppw_decimal_key_{uuid4().hex[:12]}'
    schema = namespace.upper()
    config = {**_target_config(schema, namespace), 'archive_load_files': False}
    target = FastSyncTargetSnowflake(config)
    # The keys differ only past float precision, so any float round-trip collapses them.
    first, second = '12345678901234567891', '12345678901234567892'
    source.create([
        ('id', 'NUMERIC(20,0) PRIMARY KEY'), ('amount', 'INTEGER'), ('position', 'INTEGER'),
    ], [(first, 10, 1), (second, 20, 2)])
    args = _args(source, config, tmp_path, namespace, replication_key='position')
    table = f'"{schema}"."{source.table_name.upper()}"'

    def assert_rows(expected):
        rows = target.query(f'SELECT TO_VARCHAR("ID") AS ID, "AMOUNT", "POSITION" FROM {table}')
        assert len(rows) == len(expected)
        assert {row['ID']: (row['AMOUNT'], row['POSITION']) for row in rows} == expected

    try:
        result = yugabyte_to_snowflake.sync_table(source.table, args)
        assert result is True, result
        assert_rows({first: (10, 1), second: (20, 2)})
        original_columns = _fallback_columns(target, schema, source.table_name)
        assert next(column for column in original_columns if column['COLUMN_NAME'] == 'ID') == {
            'COLUMN_NAME': 'ID', 'DATA_TYPE': 'NUMBER', 'NUMERIC_PRECISION': 20, 'NUMERIC_SCALE': 0,
        }

        request = (source.table, {
            'column': 'position', 'start_value': '<S>1', 'end_value': '<S>2', 'drop_target_table': False,
        })
        for _ in range(2):
            result = partial_yugabyte.partial_sync_table(request, args)
            assert result is True, result
            assert_rows({first: (10, 1), second: (20, 2)})
            assert _fallback_columns(target, schema, source.table_name) == original_columns

        source.execute(f'UPDATE {source.quoted_table} SET amount=%s, position=%s WHERE id=%s', (101, 3, first))
        source.execute(f'UPDATE {source.quoted_table} SET amount=%s, position=%s WHERE id=%s', (202, 4, second))
        messages = _tap_extract(source, 'snowflake', args, dimensions=None, start=2, end=4)
        assert {message['record']['id'] for message in messages if message['type'] == 'RECORD'} == {first, second}
        _load_singer(config, 'snowflake', messages)
        assert_rows({first: (101, 3), second: (202, 4)})
        assert _fallback_columns(target, schema, source.table_name) == original_columns

        _delete_from_singer(config, source, messages, first, key_column='id')
        assert_rows({second: (202, 4)})
    finally:
        try:
            target.query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            _remove_s3_prefix(target, config['s3_key_prefix'])


@pytest.mark.parametrize('source_export', [
    ('mysql', False), ('mysql', True), ('mariadb', False), ('mariadb', True),
], indirect=True)
def test_mysql_bounded_decimal_key_snowflake(source_export, tmp_path):
    """Keep supported MySQL/MariaDB decimal keys numeric through bulk and Singer loads."""
    source = source_export
    namespace = f'ppw_decimal_key_{uuid4().hex[:12]}'
    schema = namespace.upper()
    config = {**_target_config(schema, namespace), 'archive_load_files': False}
    if source.iceberg:
        config.update(target_table_format='iceberg', iceberg_version=3)
    target = FastSyncTargetSnowflake(config)
    source.create([
        ('id', 'DECIMAL(18,2) PRIMARY KEY'), ('amount', 'INTEGER'), ('position', 'INTEGER'),
    ], [('1.00', 10, 1), ('2.50', 20, 2)])
    args = _args(source, config, tmp_path, namespace, replication_key='position')
    table = f'"{schema}"."{source.table_name.upper()}"'

    def assert_rows(expected):
        rows = target.query(f'SELECT "ID", "AMOUNT", "POSITION" FROM {table}')
        assert len(rows) == len(expected)
        assert {row['ID']: (row['AMOUNT'], row['POSITION']) for row in rows} == expected

    try:
        result = mysql_to_snowflake.sync_table(source.table, args)
        assert result is True, result
        assert_rows({Decimal('1.00'): (10, 1), Decimal('2.50'): (20, 2)})
        columns = _fallback_columns(target, schema, source.table_name)
        key = next(column for column in columns if column['COLUMN_NAME'] == 'ID')
        assert (key['DATA_TYPE'], key['NUMERIC_PRECISION'], key['NUMERIC_SCALE']) == ('NUMBER', 18, 2)

        source.execute(f'UPDATE {source.quoted_table} SET amount=%s, position=%s WHERE id=%s', (101, 3, '1.00'))
        source.execute(f'UPDATE {source.quoted_table} SET amount=%s, position=%s WHERE id=%s', (202, 4, '2.50'))
        messages = _tap_extract(source, 'snowflake', args, dimensions=None, start=2, end=4)
        records = [message['record'] for message in messages if message['type'] == 'RECORD']
        assert {record['id'] for record in records} == {'1.00', '2.50'}
        _load_singer(config, 'snowflake', messages)
        assert_rows({Decimal('1.00'): (101, 3), Decimal('2.50'): (202, 4)})
    finally:
        try:
            target.query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            _remove_s3_prefix(target, config['s3_key_prefix'])


@pytest.mark.parametrize('source_export', [
    ('mysql', False), ('mysql', True), ('mariadb', False), ('mariadb', True),
], indirect=True)
def test_mysql_extended_types_snowflake(source_export, tmp_path):
    """Preserve SET, BLOB-family and YEAR values through bulk and Singer loads."""
    source = source_export
    namespace = f'ppw_mysql_types_{uuid4().hex[:12]}'
    schema = namespace.upper()
    config = {**_target_config(schema, namespace), 'archive_load_files': False}
    if source.iceberg:
        config.update(target_table_format='iceberg', iceberg_version=3)
    target = FastSyncTargetSnowflake(config)
    binary_columns = ('tiny_payload', 'payload', 'medium_payload', 'long_payload')
    source.create(
        [
            ('id', 'INTEGER PRIMARY KEY'), ('position', 'INTEGER'),
            ('labels', "SET('first','second')"),
            ('tiny_payload', 'TINYBLOB'), ('payload', 'BLOB'),
            ('medium_payload', 'MEDIUMBLOB'), ('long_payload', 'LONGBLOB'),
            ('calendar_year', 'YEAR'),
        ],
        [
            (1, 1, 'second,first', b'\x00\xff', b'blob', b'\x00medium', b'long\xff', 2024),
            (2, 2, '', b'', b'', b'', b'', 0),
            (3, 3, None, None, None, None, None, None),
        ],
    )
    args = _args(source, config, tmp_path, namespace, replication_key='position')
    table = f'"{schema}"."{source.table_name.upper()}"'

    def rows():
        projections = ', '.join(f'HEX_ENCODE("{name.upper()}") AS "{name.upper()}"' for name in binary_columns)
        return target.query(
            f'SELECT "ID", "LABELS", {projections}, "CALENDAR_YEAR" FROM {table} ORDER BY "ID"',
        )

    try:
        result = mysql_to_snowflake.sync_table(source.table, args)
        assert result is True, result
        columns = _fallback_columns(target, schema, source.table_name)
        column_types = {column['COLUMN_NAME']: column for column in columns}
        assert column_types['LABELS']['DATA_TYPE'] == 'TEXT'
        assert all(column_types[name.upper()]['DATA_TYPE'] == 'BINARY' for name in binary_columns)
        assert column_types['CALENDAR_YEAR'] == {
            'COLUMN_NAME': 'CALENDAR_YEAR', 'DATA_TYPE': 'NUMBER',
            'NUMERIC_PRECISION': 38, 'NUMERIC_SCALE': 0,
        }
        assert rows() == [
            {
                'ID': 1, 'LABELS': 'first,second', 'TINY_PAYLOAD': '00FF', 'PAYLOAD': '626C6F62',
                'MEDIUM_PAYLOAD': '006D656469756D', 'LONG_PAYLOAD': '6C6F6E67FF', 'CALENDAR_YEAR': 2024,
            },
            {
                'ID': 2, 'LABELS': '', 'TINY_PAYLOAD': '', 'PAYLOAD': '',
                'MEDIUM_PAYLOAD': '', 'LONG_PAYLOAD': '', 'CALENDAR_YEAR': 0,
            },
            {
                'ID': 3, 'LABELS': None, 'TINY_PAYLOAD': None, 'PAYLOAD': None,
                'MEDIUM_PAYLOAD': None, 'LONG_PAYLOAD': None, 'CALENDAR_YEAR': None,
            },
        ]

        source.execute(
            f'UPDATE {source.quoted_table} SET position=%s, labels=%s, tiny_payload=%s, payload=%s, '
            'medium_payload=%s, long_payload=%s, calendar_year=%s WHERE id=%s',
            (4, '', b'', b'', b'', b'', 0, 1),
        )
        source.execute(
            f'INSERT INTO {source.quoted_table} VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
            (4, 5, None, None, None, None, None, None),
        )
        messages = _tap_extract(source, 'snowflake', args, dimensions=None, start=3, end=5)
        assert {message['record']['id'] for message in messages if message['type'] == 'RECORD'} == {1, 3, 4}
        _load_singer(config, 'snowflake', messages)
        assert rows()[0] == {
            'ID': 1, 'LABELS': '', 'TINY_PAYLOAD': '', 'PAYLOAD': '',
            'MEDIUM_PAYLOAD': '', 'LONG_PAYLOAD': '', 'CALENDAR_YEAR': 0,
        }
        assert rows()[-1] == {
            'ID': 4, 'LABELS': None, 'TINY_PAYLOAD': None, 'PAYLOAD': None,
            'MEDIUM_PAYLOAD': None, 'LONG_PAYLOAD': None, 'CALENDAR_YEAR': None,
        }
    finally:
        try:
            target.query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            _remove_s3_prefix(target, config['s3_key_prefix'])


@pytest.mark.parametrize('source_export', [('mysql', False), ('mariadb', False)], indirect=True)
def test_mysql_legacy_binary_width_partial_sync_snowflake(source_export, tmp_path):
    """Merge bounded binary columns into native targets created before the widest BINARY mapping."""
    source = source_export
    namespace = f'ppw_legacy_binary_{uuid4().hex[:12]}'
    schema = namespace.upper()
    config = {**_target_config(schema, namespace), 'archive_load_files': False}
    target = FastSyncTargetSnowflake(config)
    keys = [bytes(15) + bytes([position]) for position in (1, 2, 3)]
    source.create(
        [('id', 'BINARY(16) PRIMARY KEY'), ('token', 'VARBINARY(32)'), ('position', 'INTEGER')],
        [(keys[0], b'\x00\xff', 1), (keys[1], b'', 2), (keys[2], None, 3)],
    )
    args = _args(source, config, tmp_path, namespace, replication_key='position')
    table = f'"{schema}"."{source.table_name.upper()}"'
    request = (source.table, {
        'column': 'position', 'start_value': '<S>1', 'end_value': '<S>2', 'drop_target_table': False,
    })

    def binary_lengths():
        rows = [{key.lower(): value for key, value in row.items()} for row in target.query(
            f'SHOW COLUMNS IN TABLE {table}',
        )]
        return {
            row['column_name']: json.loads(row['data_type']).get('length')
            for row in rows if row['column_name'] in ('ID', 'TOKEN')
        }

    try:
        target.create_schema(schema)
        # FastSync before 0.94 mapped MySQL BINARY and VARBINARY to Snowflake's default BINARY width.
        target.create_table(schema, source.table, ['"ID" BINARY', '"TOKEN" BINARY', '"POSITION" NUMBER'], ['"ID"'])
        target.query(
            f'INSERT INTO {table} ("ID", "TOKEN", "POSITION") '
            "SELECT TO_BINARY(column1, 'HEX'), TO_BINARY(column2, 'HEX'), column3 "
            'FROM VALUES (%s, %s, 2), (%s, %s, 3)',
            (keys[1].hex(), 'AA', keys[2].hex(), 'BB'),
        )
        assert binary_lengths() == {'ID': 8388608, 'TOKEN': 8388608}

        result = partial_mysql.partial_sync_table(request, args)
        assert result is True, result
        assert binary_lengths() == {'ID': 8388608, 'TOKEN': 8388608}
        assert target.query(
            f'SELECT HEX_ENCODE("ID") AS "ID", HEX_ENCODE("TOKEN") AS "TOKEN", "POSITION" '
            f'FROM {table} ORDER BY "POSITION"',
        ) == [
            {'ID': keys[0].hex().upper(), 'TOKEN': '00FF', 'POSITION': 1},
            {'ID': keys[1].hex().upper(), 'TOKEN': '', 'POSITION': 2},
            {'ID': keys[2].hex().upper(), 'TOKEN': 'BB', 'POSITION': 3},
        ]
    finally:
        try:
            target.query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            _remove_s3_prefix(target, config['s3_key_prefix'])


@pytest.mark.parametrize('source_export', [
    ('postgres', False), ('mysql', False), ('mariadb', False), ('yugabyte', False),
], indirect=True)
def test_decimal_fastsync_postgres(source_export, tmp_path):
    """Each SQL source bulk-loads exact decimals and hands over to Singer PostgreSQL.

    PostgreSQL-dialect arrays, including numeric[] JSON text, NULL and empty arrays, load as the same jsonb from both.
    """
    source = source_export
    schema = f'ppw_decimal_{uuid4().hex[:12]}'
    config = {key: os.environ[f'TARGET_POSTGRES_{field}'] for key, field in {
        'host': 'HOST', 'port': 'PORT', 'user': 'USER', 'password': 'PASSWORD', 'dbname': 'DB',
    }.items()}
    config.update(default_target_schema=schema, batch_size_rows=1000, parallelism=1)
    target = FastSyncTargetPostgres(config)
    columns = [('id', 'INTEGER PRIMARY KEY'), ('amount', 'NUMERIC(38,18)')]
    rows = [(1, EXACT), (2, None)]
    arrays = list(POSTGRES_TARGET_ARRAYS)
    if source.postgres_dialect:
        columns.append(('rounded', 'NUMERIC(10,-2)'))
        columns.extend((name, data_type) for name, (data_type, _, _) in POSTGRES_TARGET_ARRAYS.items())
        literals = [literal for _, literal, _ in POSTGRES_TARGET_ARRAYS.values()]
        rows = [(1, EXACT, '12300', *literals), (2, None, None, *[None] * len(arrays))]
    source.create(columns, rows)
    args = _args(source, config, tmp_path, schema)
    full = POSTGRES_FULL.get(source.engine, mysql_to_postgres)
    target.create_schema(schema)
    table = f'{schema}."{source.table_name}"'
    # jsonb::text keeps every digit and display scale, and is NULL only for SQL NULL.
    array_sql = f'SELECT {", ".join(f"{name}::text" for name in arrays)} FROM {table} WHERE id = 1'
    texts = [text for _, _, text in POSTGRES_TARGET_ARRAYS.values()]
    try:
        result = full.sync_table(source.table, args)
        assert result is True, result
        assert [tuple(row) for row in target.query(f'SELECT id, amount FROM {table} ORDER BY id')] == [
            (1, Decimal(EXACT)), (2, None),
        ]
        if source.postgres_dialect:
            assert target.query(f'SELECT rounded FROM {table} WHERE id = 1')[0][0] == Decimal('12300')
            fastsync_arrays = tuple(target.query(array_sql)[0])
            expected_sql = 'SELECT ' + ', '.join(['%s::jsonb::text'] * len(texts))
            assert fastsync_arrays == tuple(target.query(expected_sql, texts)[0])
            source.execute(f'UPDATE {source.quoted_table} SET rounded = %s WHERE id = 1', ('12345678',))
        source.execute(f'UPDATE {source.quoted_table} SET amount = %s WHERE id = 1', (NEXT,))
        messages = _tap_extract(source, 'postgres', args)
        records = [message['record'] for message in messages if message['type'] == 'RECORD']
        assert [record['amount'] for record in records] == [NEXT]
        if source.postgres_dialect:
            assert [records[0][name] for name in arrays] == texts
        _load_singer(config, 'postgres', messages)
        assert target.query(f'SELECT amount FROM {table} WHERE id = 1')[0][0] == Decimal(NEXT)
        rows = target.query(
            'SELECT numeric_precision, numeric_scale FROM information_schema.columns '
            'WHERE table_schema = %s AND table_name = %s AND column_name = %s',
            (schema, source.table_name, 'amount'),
        )
        assert tuple(rows[0]) == (38, 18)
        if source.postgres_dialect:
            _load_singer(config, 'postgres', messages)
            assert target.query(f'SELECT rounded FROM {table} WHERE id = 1')[0][0] == Decimal('12345700')
            # Singer's update rewrote every column of row 1, so each array must match FastSync's jsonb exactly.
            assert tuple(target.query(array_sql)[0]) == fastsync_arrays
            rounded_columns = target.query(
                'SELECT a.attname, format_type(a.atttypid, a.atttypmod) '
                'FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid '
                'JOIN pg_namespace n ON n.oid = c.relnamespace '
                "WHERE n.nspname = %s AND c.relname = %s AND a.attname LIKE 'rounded%%' AND NOT a.attisdropped",
                (schema, source.table_name),
            )
            assert [tuple(row) for row in rounded_columns] == [('rounded', 'numeric(10,-2)')]
            jsonb_columns = target.query(
                'SELECT column_name FROM information_schema.columns '
                "WHERE table_schema = %s AND table_name = %s AND data_type = 'jsonb' ORDER BY ordinal_position",
                (schema, source.table_name),
            )
            assert [row[0] for row in jsonb_columns] == arrays
    finally:
        target.query(f'DROP SCHEMA IF EXISTS {schema} CASCADE')


def _fallback_columns(target, schema, table_name):
    return target.query(
        'SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS '
        'WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION', (schema, table_name.upper()),
    )


def _assert_fallback_values(target, table, keys, *, postgres=False):
    rows = target.query(f'SELECT "DECIMAL_ID", "AMOUNT" FROM {table} ORDER BY "DECIMAL_ID"')
    assert [row['DECIMAL_ID'] for row in rows] == keys
    assert all(isinstance(row['AMOUNT'], float) for row in rows)
    assert all(row['AMOUNT'] == float(Decimal(keys[0])) for row in rows)
    if postgres:
        values = target.query(
            'SELECT "HUGE", "NEGATIVE_HUGE", "TINY", "NEGATIVE_TINY" '
            f'FROM {table} ORDER BY "DECIMAL_ID"',
        )
        for row in values[:-1]:
            assert row == {'HUGE': 1.7976931348623157e308, 'NEGATIVE_HUGE': -1.7976931348623157e308,
                           'TINY': 0.0, 'NEGATIVE_TINY': 0.0}
        assert all(value is None for value in values[-1].values())


def _delete_from_singer(config, source, messages, key, key_column='decimal_id'):
    source.execute(f'DELETE FROM {source.quoted_table} WHERE {source.quoted(key_column)} = %s', (key,))
    schema = next(message for message in messages if message['type'] == 'SCHEMA')
    record = next(message for message in messages if message['type'] == 'RECORD'
                  and message['record'][key_column] == key)
    schema = {**schema, 'schema': {**schema['schema'], 'properties': {
        **schema['schema']['properties'], '_sdc_deleted_at': {'type': ['null', 'string'], 'format': 'date-time'},
    }}}
    record = {**record, 'record': {**record['record'], '_sdc_deleted_at': '2026-09-30T00:00:00Z'}}
    _load_singer(config, 'snowflake', [schema, record])


def test_decimal_fallback_snowflake(source_export, tmp_path):
    """Fallback preserves rows and decimal identities across bulk, Singer and deletes."""
    source = source_export
    namespace = f'ppw_decimal_fallback_{uuid4().hex[:12]}'
    schema = namespace.upper()
    config = {**_target_config(schema, namespace), 'archive_load_files': False}
    if source.iceberg:
        config.update(target_table_format='iceberg', iceberg_version=3)
    target = FastSyncTargetSnowflake(config)
    keys = [f'12345678901234567890123456789012345.1234567890123456789012345678{index}0' for index in (1, 2, 3)]
    canonical_keys = [key.rstrip('0') for key in keys]
    assert float(keys[0]) == float(keys[1]) == float(keys[2])
    columns = [('decimal_id', 'NUMERIC(65,30) PRIMARY KEY'), ('amount', 'NUMERIC(65,30)'), ('position', 'INTEGER')]
    # 2^24 + 2^-29 is exactly halfway between adjacent binary64 values; NUMERIC(65,30)
    # preserves the values one decimal quantum either side of that midpoint.
    midpoints = (
        '16777216.000000001862645149230957031249',
        '16777216.000000001862645149230957031250',
        '16777216.000000001862645149230957031251',
    )
    midpoint_columns = ['midpoint_below', 'midpoint', 'midpoint_above']
    columns.extend((name, 'NUMERIC(65,30)') for name in midpoint_columns)
    rows = [(keys[0], keys[0], 1, *midpoints), (keys[1], keys[1], 2, *midpoints)]
    postgres = source.engine == 'postgres'
    if postgres:
        columns.extend((name, 'NUMERIC') for name in ('huge', 'negative_huge', 'tiny', 'negative_tiny'))
        rows = [(*rows[0], '1e1000', '-1e1000', '1e-1000', '-1e-1000'), (*rows[1], None, None, None, None)]
    source.create(columns, rows)
    _add_data_diff_window(source)
    compare_columns = [
        'amount', *midpoint_columns, *(['huge', 'negative_huge', 'tiny', 'negative_tiny'] if postgres else []),
    ]
    args = _args(source, config, tmp_path, namespace, replication_key='decimal_id')
    full = postgres_to_snowflake if postgres else mysql_to_snowflake
    partial = partial_postgres if postgres else partial_mysql
    table = f'"{schema}"."{source.table_name.upper()}"'

    def assert_midpoints():
        expected = [float(Decimal(value)) for value in midpoints]
        assert expected[0] == expected[1] == 16777216.0
        assert expected[2] == math.nextafter(expected[1], math.inf)
        names = ', '.join(f'"{name.upper()}"' for name in midpoint_columns)
        for row in target.query(f'SELECT {names} FROM {table}'):
            assert [row[name.upper()] for name in midpoint_columns] == expected

    try:
        result = full.sync_table(source.table, args)
        assert result is True, result
        _assert_fallback_values(target, table, canonical_keys[:2], postgres=postgres)
        assert_midpoints()
        _assert_float_data_diff(source, config, target, compare_columns, key='position')
        original_columns = _fallback_columns(target, schema, source.table_name)
        types = {column['COLUMN_NAME']: column['DATA_TYPE'] for column in original_columns}
        assert types['AMOUNT'] == 'FLOAT'
        assert types['DECIMAL_ID'] == 'TEXT'
        request = (source.table, {
            'column': 'position', 'start_value': '<S>1', 'end_value': '<S>4', 'drop_target_table': False,
        })
        for _ in range(2):
            result = partial.partial_sync_table(request, args)
            assert result is True, result
            _assert_fallback_values(target, table, canonical_keys[:2], postgres=postgres)
            assert_midpoints()
            _assert_float_data_diff(source, config, target, compare_columns, key='position')
            assert _fallback_columns(target, schema, source.table_name) == original_columns
        if postgres:
            source.execute(
                f'UPDATE {source.quoted_table} SET huge=%s, negative_huge=%s, tiny=%s, negative_tiny=%s '
                'WHERE decimal_id=%s', ('1e1000', '-1e1000', '1e-1000', '-1e-1000', keys[1]),
            )
        placeholders = ','.join('%s' for _ in columns)
        new_row = (keys[2], keys[2], 3, *midpoints, *([None] * 4 if postgres else []))
        names = ', '.join(source.quoted(name) for name, _ in columns)
        source.execute(f'INSERT INTO {source.quoted_table} ({names}) VALUES ({placeholders})', new_row)
        messages = _tap_extract(source, 'snowflake', args, dimensions=(65, 30), start=keys[1], end=keys[2])
        assert {message['record']['decimal_id'] for message in messages if message['type'] == 'RECORD'} == set(keys[1:])
        _load_singer(config, 'snowflake', messages)
        _assert_fallback_values(target, table, canonical_keys, postgres=postgres)
        assert_midpoints()
        _assert_float_data_diff(
            source, config, target, compare_columns, key='position', mutation=('amount', 1),
        )
        _delete_from_singer(config, source, messages, keys[1])
        _assert_fallback_values(target, table, [canonical_keys[0], canonical_keys[2]], postgres=postgres)
    finally:
        try:
            target.query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            _remove_s3_prefix(target, config['s3_key_prefix'])


def _float_token(value):
    """Spell a nonfinite float like PostgreSQL so rows holding NaN compare by equality."""
    if isinstance(value, float) and not math.isfinite(value):
        return 'NaN' if math.isnan(value) else ('Infinity' if value > 0 else '-Infinity')
    return value


def _variant_json(text):
    """Decode Snowflake TO_JSON text with exact decimals: lost digits fail, a different display scale does not."""
    return None if text is None else json.loads(text, parse_float=Decimal)


def _strict_bookmark(state_path):
    """Return FastSync's only bookmark value, rejecting the NaN/Infinity JSON the taps' state loader cannot read."""
    def reject(constant):
        raise ValueError(f'FastSync state holds the non-standard JSON constant {constant}')

    state = json.loads(Path(state_path).read_text(encoding='utf-8'), parse_constant=reject)
    (bookmark,) = state['bookmarks'].values()
    return bookmark['replication_key_value']


@pytest.mark.parametrize(
    'source_export', [('postgres', False), ('postgres', True), ('yugabyte', False)], indirect=True,
    ids=_source_mode_id,
)
def test_postgres_float_nonfinite_snowflake(source_export, tmp_path):
    """FastSync and Singer load NaN and infinities into the same FLOAT columns and VARIANT arrays.

    The DOUBLE PRECISION replication key holds them too: FastSync bookmarks the finite maximum, and Singer
    reads the NaN and Infinity keys above it again without bookmarking them. Both paths load each numeric[],
    NULL and empty array as the same VARIANT; decimal-route taps carry numeric[] as array_to_json text.
    """
    source = source_export
    namespace = f'ppw_float_tokens_{uuid4().hex[:12]}'
    schema = namespace.upper()
    config = {**_target_config(schema, namespace), 'archive_load_files': False}
    if source.engine == 'postgres':
        config['source_tap_type'] = 'tap-postgres'
    if source.iceberg:
        config.update(target_table_format='iceberg', iceberg_version=3)
    target = FastSyncTargetSnowflake(config)
    rows, expected = [], {}
    for row_id, (key, value, doubles, (numerics, numerics_json)) in enumerate(
        zip(FLOAT_KEYS, FLOAT_VALUES * 2, FLOAT_ARRAYS * 2, NUMERIC_ARRAYS * 2), 1,
    ):
        rows.append((row_id, key, value, value, doubles, numerics))
        expected[row_id] = (
            _float_token(key), _float_token(value), _float_token(value),
            None if doubles is None else [_float_token(item) for item in doubles], numerics_json,
        )
    finite_doubles = (
        1.0, math.nextafter(1.0, math.inf), float(2**53), float(2**53 + 2),
        float.fromhex('0x0.0000000000001p-1022'), float.fromhex('0x1.fffffffffffffp+1023'), -0.0, None,
    )
    finite_reals = (0.1, 0.3, 16777217, 16777215, 1.17549435e-38, 1.40129846e-45, -0.0, None)
    rows = [(*row, double, real, None) for row, double, real in zip(rows, finite_doubles, finite_reals)]
    loaded_expected = {row_id: (*row[:4], _variant_json(row[4])) for row_id, row in expected.items()}
    source.create([
        ('id', 'INTEGER PRIMARY KEY'), ('position', 'DOUBLE PRECISION'), ('real_value', 'REAL'),
        ('double_value', 'DOUBLE PRECISION'), ('double_values', 'DOUBLE PRECISION[]'),
        ('numeric_values', 'NUMERIC[]'), ('finite_double', 'DOUBLE PRECISION'), ('finite_real', 'REAL'),
        ('null_double', 'DOUBLE PRECISION'),
    ], rows[:4])
    _add_data_diff_window(source)
    compare_columns = ['position', 'real_value', 'double_value', 'finite_double', 'finite_real', 'null_double']
    args = _args(source, config, tmp_path, namespace, replication_key='position')
    table = f'"{schema}"."{source.table_name.upper()}"'

    def loaded_rows():
        # Arrays stay TO_JSON text, so FastSync and Singer VARIANTs can be compared exactly.
        query = (
            'SELECT "ID", "POSITION", "REAL_VALUE", "DOUBLE_VALUE", TO_JSON("DOUBLE_VALUES") AS "DOUBLE_VALUES", '
            f'TO_JSON("NUMERIC_VALUES") AS "NUMERIC_VALUES" FROM {table}'
        )
        return {
            row['ID']: (_float_token(row['POSITION']), _float_token(row['REAL_VALUE']),
                        _float_token(row['DOUBLE_VALUE']), row['DOUBLE_VALUES'], row['NUMERIC_VALUES'])
            for row in target.query(query)
        }

    def decoded(loaded):
        return {row_id: (*row[:3], *map(_variant_json, row[3:])) for row_id, row in loaded.items()}

    def source_columns():
        # FastSync and Singer each manage the _SDC_ metadata columns; only source columns are under test.
        return [column for column in _fallback_columns(target, schema, source.table_name)
                if not column['COLUMN_NAME'].startswith('_SDC_')]

    def assert_real_values():
        # PostgreSQL/YSQL export REAL as decimal text, unlike MySQL's direct binary32 promotion.
        source_values = source.source.query(
            'SELECT CAST(CAST(finite_real AS TEXT) AS DOUBLE PRECISION) AS value '
            f'FROM {source.quoted_table} ORDER BY id',
        )
        target_values = target.query(f'SELECT "FINITE_REAL" AS VALUE FROM {table} ORDER BY "ID"')
        assert [row['VALUE'] for row in target_values] == [row['value'] for row in source_values]

    try:
        result = SNOWFLAKE_FULL[source.engine].sync_table(source.table, args)
        assert result is True, result
        assert _strict_bookmark(args.state) == 2.0
        fastsync = loaded_rows()
        assert decoded(fastsync) == {row_id: loaded_expected[row_id] for row_id in range(1, 5)}
        # Decoding also reads a JSON null VARIANT as None; NULL arrays must load as SQL NULL.
        assert 'null' not in {text for row in fastsync.values() for text in row[3:]}
        columns = source_columns()
        types = {column['COLUMN_NAME']: column['DATA_TYPE'] for column in columns}
        assert [types[name] for name in ('POSITION', 'REAL_VALUE', 'DOUBLE_VALUE')] == ['FLOAT'] * 3
        assert [types[name] for name in ('DOUBLE_VALUES', 'NUMERIC_VALUES')] == ['VARIANT'] * 2
        assert [row['FINITE_DOUBLE'] for row in target.query(
            f'SELECT "FINITE_DOUBLE" FROM {table} ORDER BY "ID"',
        )] == list(finite_doubles[:4])
        assert_real_values()
        _assert_float_data_diff(source, config, target, compare_columns)

        for row in rows[4:]:
            source.execute(
                f'INSERT INTO {source.quoted_table} '
                '(id, position, real_value, double_value, double_values, numeric_values, '
                'finite_double, finite_real, null_double) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', row,
            )
        # Rows arrive in key order, Infinity and NaN last, so ending on 8.0 shows their tokens were not bookmarked.
        messages = _tap_extract(source, 'snowflake', args, dimensions=None, start=2.0, end=8.0)
        records = [message['record'] for message in messages if message['type'] == 'RECORD']
        assert {
            record['id']: (record['position'], record['real_value'], record['double_value'], record['double_values'],
                           record['numeric_values'])
            for record in records
        } == {row_id: expected[row_id] for row_id in range(2, 9)}
        _load_singer(config, 'snowflake', messages)
        singer = loaded_rows()
        assert decoded(singer) == loaded_expected
        # Singer reloads rows 2-4 and rows 5-8 repeat rows 1-4, so each VARIANT must match FastSync's text exactly.
        assert {row_id: row[3:] for row_id, row in singer.items()} == {
            row_id: fastsync[(row_id - 1) % 4 + 1][3:] for row_id in range(1, 9)
        }
        assert source_columns() == columns
        assert [row['FINITE_DOUBLE'] for row in target.query(
            f'SELECT "FINITE_DOUBLE" FROM {table} ORDER BY "ID"',
        )] == list(finite_doubles)
        assert_real_values()
        _assert_float_data_diff(source, config, target, compare_columns, mutation=('finite_double', 1))

        if source.engine == 'postgres':
            messages = _tap_extract(
                source, 'snowflake', args, dimensions=None, replication_method='FULL_TABLE',
            )
            assert {message['record']['id'] for message in messages if message['type'] == 'RECORD'} == set(range(1, 9))
            _load_singer(config, 'snowflake', messages)
            assert decoded(loaded_rows()) == loaded_expected
            assert [row['FINITE_DOUBLE'] for row in target.query(
                f'SELECT "FINITE_DOUBLE" FROM {table} ORDER BY "ID"',
            )] == list(finite_doubles)
            assert_real_values()
            _assert_float_data_diff(source, config, target, compare_columns)

        # Exercise the ordinary floating payload through each format's PartialSync publisher.
        partial_rows = [(row[0] + 8, float(row[0] + 8), *row[2:]) for row in rows[:4]]
        for row in partial_rows:
            source.execute(
                f'INSERT INTO {source.quoted_table} '
                '(id, position, real_value, double_value, double_values, numeric_values, '
                'finite_double, finite_real, null_double) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', row,
            )
        request = (source.table, {
            'column': 'id', 'start_value': '<S>9', 'end_value': '<S>12', 'drop_target_table': False,
        })
        result = SNOWFLAKE_PARTIAL[source.engine].partial_sync_table(request, args)
        assert result is True, result
        partial = loaded_rows()
        assert {row_id: row[1:] for row_id, row in decoded(partial).items() if row_id > 8} == {
            row_id + 8: loaded_expected[row_id][1:] for row_id in range(1, 5)
        }
        assert {row_id: row[3:] for row_id, row in partial.items() if row_id > 8} == {
            row_id + 8: fastsync[row_id][3:] for row_id in range(1, 5)
        }
        assert {row_id: row for row_id, row in partial.items() if row_id <= 8} == singer
        assert source_columns() == columns
        assert_real_values()
        _assert_float_data_diff(source, config, target, compare_columns)
    finally:
        try:
            target.query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            _remove_s3_prefix(target, config['s3_key_prefix'])


def _mysql_float_mask(source, tap_id):
    """Generate transformation.json content that hides ``secret`` where the FLOAT ``amount`` equals 0.1."""
    return {'transformations': Config.generate_transformations({
        'id': tap_id, 'type': 'tap-mysql', 'db_conn': source.source.connection_config,
        'schemas': [{'source_schema': source.schema, 'tables': [{
            'table_name': source.table_name,
            'transformations': [{
                'column': 'secret', 'type': 'MASK-HIDDEN', 'when': [{'column': 'amount', 'equals': 0.1}],
            }],
        }]}],
    })}


def _round_mysql_float_conditions(args):
    """Rewrite transformation.json against the discovered catalog, as discover_tap does, and return its entries."""
    transformation_file = Path(args.state).with_name('transformation.json')
    transformation_file.write_text(json.dumps(args.transform), encoding='utf-8')
    catalog = json.loads(_catalog_path(args).read_text(encoding='utf-8'))
    assert mysql_float_conditions.round_transformation_file(str(transformation_file), catalog)
    return json.loads(transformation_file.read_text(encoding='utf-8'))['transformations']


@pytest.mark.parametrize('source_export', [
    ('mysql', False), ('mysql', True), ('mariadb', False), ('mariadb', True),
], indirect=True, ids=_source_mode_id)
def test_mysql_float_exact_snowflake(source_export, tmp_path):
    """FastSync and Singer load, bookmark and compare the exact stored FLOAT, not the server's six-digit text.

    Both paths hide the secret where the FLOAT equals the one nearest the configured 0.1; Singer matches only
    because discovery rewrote that literal against the real catalog.
    """
    source = source_export
    namespace = f'ppw_float_exact_{uuid4().hex[:12]}'
    schema = namespace.upper()
    config = {**_target_config(schema, namespace), 'archive_load_files': False}
    if source.iceberg:
        config.update(target_table_format='iceberg', iceberg_version=3)
    target = FastSyncTargetSnowflake(config)
    rows = [
        (row_id, key, amount, RAW_SECRET if amount == 0.1 else 'public')
        for row_id, (key, amount) in enumerate(zip(MYSQL_FLOAT_KEYS, (16777217, 1234567.8, 0.1, -0.0) * 2), 1)
    ]
    expected = {
        row_id: (float32(key), float32(amount), 'hidden' if secret == RAW_SECRET else secret)
        for row_id, key, amount, secret in rows
    }
    double_values = (
        1.0, math.nextafter(1.0, math.inf), float(2**53), float(2**53 + 2),
        float.fromhex('0x0.0000000000001p-1022'), 1e300, -0.0, None,
    )
    rows = [(*row, value) for row, value in zip(rows, double_values)]
    source.create([
        ('id', 'INTEGER PRIMARY KEY'), ('position', 'FLOAT'), ('amount', 'FLOAT'), ('secret', 'TEXT'),
        ('double_value', 'DOUBLE'),
    ], rows[:4])
    _add_data_diff_window(source)
    compare_columns = ['position', 'amount', 'double_value']
    args = _args(source, config, tmp_path, namespace, replication_key='position')
    args.transform = source.source.source_transformations = _mysql_float_mask(source, namespace)
    table = f'"{schema}"."{source.table_name.upper()}"'

    def loaded_rows():
        query = f'SELECT "ID", "POSITION", "AMOUNT", "SECRET" FROM {table}'
        return {row['ID']: (row['POSITION'], row['AMOUNT'], row['SECRET']) for row in target.query(query)}

    try:
        contents, exported = source.export(tmp_path / 'fastsync_floats.csv.gz')
        assert RAW_SECRET not in contents
        assert {int(row[0]): tuple(row[1:4]) for row in exported} == {
            row_id: (str(key), str(amount), secret) for row_id, (key, amount, secret) in expected.items() if row_id <= 4
        }
        result = mysql_to_snowflake.sync_table(source.table, args)
        assert result is True, result
        fastsync = loaded_rows()
        assert fastsync == {row_id: expected[row_id] for row_id in range(1, 5)}
        _assert_float_data_diff(source, config, target, compare_columns)

        for row in rows[4:]:
            source.execute(
                f'INSERT INTO {source.quoted_table} (id, position, amount, secret, double_value) '
                'VALUES (%s, %s, %s, %s, %s)', row,
            )
        # A six-digit FastSync bookmark, 1234570, would make Singer skip rows 4-8 entirely.
        messages = _tap_extract(source, 'snowflake', args, dimensions=None, start=expected[4][0], end=expected[8][0])
        records = {
            message['record']['id']: (message['record']['position'], message['record']['amount'])
            for message in messages if message['type'] == 'RECORD'
        }
        assert records == {row_id: expected[row_id][:2] for row_id in range(4, 9)}
        assert math.copysign(1.0, records[8][1]) == -1.0
        transformations = _round_mysql_float_conditions(args)
        assert transformations[0]['when'] == [{'column': 'amount', 'equals': float32(0.1)}]
        _load_singer(config, 'snowflake', messages, transformations)
        singer = loaded_rows()
        assert singer == expected
        assert [row['DOUBLE_VALUE'] for row in target.query(
            f'SELECT "DOUBLE_VALUE" FROM {table} ORDER BY "ID"',
        )] == list(double_values)
        _assert_float_data_diff(source, config, target, compare_columns, mutation=('amount', 1))
        # == ignores the sign of zero and Snowflake may normalise -0.0, so both paths must store the same sign.
        assert math.copysign(1.0, singer[8][1]) == math.copysign(1.0, fastsync[4][1])

        # The FLOAT range re-merges rows 5-8 and adds rows 9-12 through both PartialSync publishers.
        partial_rows = [
            (row_id, key, amount, RAW_SECRET if amount == 0.1 else 'public')
            for row_id, (key, amount) in enumerate(zip(
                (1234568.25, 1234568.75, 1234569.25, 1234569.75), (16777217, 1234567.8, 0.1, -0.0)), 9)
        ]
        partial_rows = [(*row, value) for row, value in zip(partial_rows, double_values[:4])]
        for row in partial_rows:
            source.execute(
                f'INSERT INTO {source.quoted_table} (id, position, amount, secret, double_value) '
                'VALUES (%s, %s, %s, %s, %s)', row,
            )
        request = (source.table, {
            'column': 'position', 'start_value': '<S>1234568', 'end_value': '<S>1234570',
            'drop_target_table': False,
        })
        result = partial_mysql.partial_sync_table(request, args)
        assert result is True, result
        partial = loaded_rows()
        assert partial == {**expected, **{
            row_id: (float32(key), float32(amount), 'hidden' if secret == RAW_SECRET else secret)
            for row_id, key, amount, secret, _ in partial_rows
        }}
        assert math.copysign(1.0, partial[12][1]) == math.copysign(1.0, fastsync[4][1])
        _assert_float_data_diff(source, config, target, compare_columns)
    finally:
        try:
            target.query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            _remove_s3_prefix(target, config['s3_key_prefix'])
