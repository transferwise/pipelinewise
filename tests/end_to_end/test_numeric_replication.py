"""Preserve decimal rows, identities and supported precision through bulk and Singer."""

import json
import os
import subprocess
from argparse import Namespace
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from pipelinewise.cli.config import Config
from pipelinewise.fastsync import mysql_to_postgres, mysql_to_snowflake, postgres_to_postgres, postgres_to_snowflake
from pipelinewise.fastsync.commons.target_postgres import FastSyncTargetPostgres
from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake
from pipelinewise.fastsync.partialsync import mysql_to_snowflake as partial_mysql
from pipelinewise.fastsync.partialsync import postgres_to_snowflake as partial_postgres
from tests.end_to_end.target_snowflake.test_source_transformation_exports import source_export as source_export
from tests.end_to_end.target_snowflake.test_source_transformation_publication import (
    _connector_python, _remove_s3_prefix, _target_config,
)


EXACT = '12345678901234567890.123456789012345678'
NEXT = '98765432109876543210.987654321098765432'


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


def _tap_extract(source, target_type, args, *, dimensions=(38, 18), start=EXACT, end=NEXT):
    """Discover and run the installed SQL tap from FastSync's saved bookmark."""
    tap_type = 'tap-postgres' if source.engine == 'postgres' else 'tap-mysql'
    tap = {'type': tap_type, 'db_conn': source.source.connection_config}
    config = Config.generate_tap_connection_config(tap, {}, f'target-{target_type}')
    config['filter_schemas' if source.engine == 'postgres' else 'filter_dbs'] = source.schema
    assert config['decimal_target'] == target_type
    config_path = Path(args.state).with_name('tap_config.json')
    catalog_path = Path(args.state).with_name('tap_catalog.json')
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
            entry['metadata'].update({'replication-method': 'INCREMENTAL', 'replication-key': replication_key})
    if dimensions is not None:
        assert stream['schema']['properties'][replication_key]['decimal'] == {
            'precision': dimensions[0], 'scale': dimensions[1],
        }
    bookmark = json.loads(Path(args.state).read_text(encoding='utf-8'))['bookmarks'][stream['tap_stream_id']]
    assert bookmark['replication_key_value'] == start
    catalog_path.write_text(json.dumps({'streams': selected}), encoding='utf-8')
    messages = [json.loads(line) for line in run('--catalog', str(catalog_path), '--state', args.state).splitlines()]
    records = [message['record'] for message in messages if message['type'] == 'RECORD']
    assert records
    if dimensions is not None:
        assert all(isinstance(record[replication_key], str) for record in records)
    final_state = [message['value'] for message in messages if message['type'] == 'STATE'][-1]
    assert final_state['bookmarks'][stream['tap_stream_id']]['replication_key_value'] == end
    return messages


def _load_singer(config, target_type, messages):
    """Pass actual tap protocol output through the installed transform and target."""
    transformed = _connector_python('transform-field', '''
import json
import sys
from transform_field import TransformField
payload = json.load(sys.stdin)
TransformField({'transformations': []}).consume(json.dumps(message) for message in payload['messages'])
''', {'messages': messages})
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
    full = postgres_to_snowflake if source.engine == 'postgres' else mysql_to_snowflake
    partial = partial_postgres if source.engine == 'postgres' else partial_mysql
    table = f'"{schema}"."{source.table_name.upper()}"'
    try:
        result = full.sync_table(source.table, args)
        assert result is True, result
        assert Decimal(target.query(
            f'SELECT TO_VARCHAR("EXACT_AMOUNT") AS AMOUNT FROM {table} WHERE "ID" = 2',
        )[0]['AMOUNT']) == Decimal(EXACT)
        alter = 'ALTER COLUMN amount TYPE' if source.engine == 'postgres' else 'MODIFY COLUMN amount'
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


@pytest.mark.parametrize('source_export', [('postgres', False), ('mysql', False), ('mariadb', False)], indirect=True)
def test_decimal_fastsync_postgres(source_export, tmp_path):
    """Each SQL source bulk-loads exact decimals and hands over to Singer PostgreSQL."""
    source = source_export
    schema = f'ppw_decimal_{uuid4().hex[:12]}'
    config = {key: os.environ[f'TARGET_POSTGRES_{field}'] for key, field in {
        'host': 'HOST', 'port': 'PORT', 'user': 'USER', 'password': 'PASSWORD', 'dbname': 'DB',
    }.items()}
    config.update(default_target_schema=schema, batch_size_rows=1000, parallelism=1)
    target = FastSyncTargetPostgres(config)
    columns = [('id', 'INTEGER PRIMARY KEY'), ('amount', 'NUMERIC(38,18)')]
    rows = [(1, EXACT), (2, None)]
    if source.engine == 'postgres':
        columns.append(('rounded', 'NUMERIC(10,-2)'))
        rows = [(1, EXACT, '12300'), (2, None, None)]
    source.create(columns, rows)
    args = _args(source, config, tmp_path, schema)
    full = postgres_to_postgres if source.engine == 'postgres' else mysql_to_postgres
    target.create_schema(schema)
    table = f'{schema}."{source.table_name}"'
    try:
        result = full.sync_table(source.table, args)
        assert result is True, result
        assert [tuple(row) for row in target.query(f'SELECT id, amount FROM {table} ORDER BY id')] == [
            (1, Decimal(EXACT)), (2, None),
        ]
        if source.engine == 'postgres':
            assert target.query(f'SELECT rounded FROM {table} WHERE id = 1')[0][0] == Decimal('12300')
            source.execute(f'UPDATE {source.quoted_table} SET rounded = %s WHERE id = 1', ('12345678',))
        source.execute(f'UPDATE {source.quoted_table} SET amount = %s WHERE id = 1', (NEXT,))
        messages = _tap_extract(source, 'postgres', args)
        assert [message['record']['amount'] for message in messages if message['type'] == 'RECORD'] == [NEXT]
        _load_singer(config, 'postgres', messages)
        assert target.query(f'SELECT amount FROM {table} WHERE id = 1')[0][0] == Decimal(NEXT)
        rows = target.query(
            'SELECT numeric_precision, numeric_scale FROM information_schema.columns '
            'WHERE table_schema = %s AND table_name = %s AND column_name = %s',
            (schema, source.table_name, 'amount'),
        )
        assert tuple(rows[0]) == (38, 18)
        if source.engine == 'postgres':
            _load_singer(config, 'postgres', messages)
            assert target.query(f'SELECT rounded FROM {table} WHERE id = 1')[0][0] == Decimal('12345700')
            rounded_columns = target.query(
                'SELECT a.attname, format_type(a.atttypid, a.atttypmod) '
                'FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid '
                'JOIN pg_namespace n ON n.oid = c.relnamespace '
                "WHERE n.nspname = %s AND c.relname = %s AND a.attname LIKE 'rounded%%' AND NOT a.attisdropped",
                (schema, source.table_name),
            )
            assert [tuple(row) for row in rounded_columns] == [('rounded', 'numeric(10,-2)')]
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
    assert all(row['AMOUNT'] == pytest.approx(float(Decimal(keys[0]))) for row in rows)
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
    rows = [(keys[0], keys[0], 1), (keys[1], keys[1], 2)]
    postgres = source.engine == 'postgres'
    if postgres:
        columns.extend((name, 'NUMERIC') for name in ('huge', 'negative_huge', 'tiny', 'negative_tiny'))
        rows = [(*rows[0], '1e1000', '-1e1000', '1e-1000', '-1e-1000'), (*rows[1], None, None, None, None)]
    source.create(columns, rows)
    args = _args(source, config, tmp_path, namespace, replication_key='decimal_id')
    full = postgres_to_snowflake if postgres else mysql_to_snowflake
    partial = partial_postgres if postgres else partial_mysql
    table = f'"{schema}"."{source.table_name.upper()}"'
    try:
        result = full.sync_table(source.table, args)
        assert result is True, result
        _assert_fallback_values(target, table, canonical_keys[:2], postgres=postgres)
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
            assert _fallback_columns(target, schema, source.table_name) == original_columns
        if postgres:
            source.execute(
                f'UPDATE {source.quoted_table} SET huge=%s, negative_huge=%s, tiny=%s, negative_tiny=%s '
                'WHERE decimal_id=%s', ('1e1000', '-1e1000', '1e-1000', '-1e-1000', keys[1]),
            )
        placeholders = ','.join('%s' for _ in columns)
        new_row = (keys[2], keys[2], 3, None, None, None, None) if postgres else (keys[2], keys[2], 3)
        source.execute(f'INSERT INTO {source.quoted_table} VALUES ({placeholders})', new_row)
        messages = _tap_extract(source, 'snowflake', args, dimensions=(65, 30), start=keys[1], end=keys[2])
        assert {message['record']['decimal_id'] for message in messages if message['type'] == 'RECORD'} == set(keys[1:])
        _load_singer(config, 'snowflake', messages)
        _assert_fallback_values(target, table, canonical_keys, postgres=postgres)
        _delete_from_singer(config, source, messages, keys[1])
        _assert_fallback_values(target, table, [canonical_keys[0], canonical_keys[2]], postgres=postgres)
    finally:
        try:
            target.query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            _remove_s3_prefix(target, config['s3_key_prefix'])
