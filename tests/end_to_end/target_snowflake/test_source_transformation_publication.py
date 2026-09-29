"""Prove privacy through real FullSync/PartialSync staging and publication."""

import csv
import gzip
import hashlib
import io
import json
import os
import subprocess
from argparse import Namespace
from pathlib import Path
from uuid import uuid4

import pytest

from pipelinewise.fastsync import mysql_to_snowflake, postgres_to_snowflake
from pipelinewise.fastsync.commons.snowflake_iceberg_versions import MANAGED_ICEBERG_V3_SPEC
from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake
from pipelinewise.fastsync.partialsync import mysql_to_snowflake as partial_mysql
from pipelinewise.fastsync.partialsync import postgres_to_snowflake as partial_postgres
from tests.end_to_end.target_snowflake.test_source_transformation_exports import (
    RAW_SECRET,
    TRANSFORMATIONS,
    source_export as source_export,
)


def _digest(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def _target_config(schema, namespace):
    fields = {
        'account': 'ACCOUNT', 'dbname': 'DBNAME', 'user': 'USER',
        'private_key': 'PRIVATE_KEY', 'warehouse': 'WAREHOUSE',
        's3_bucket': 'S3_BUCKET', 'stage': 'STAGE', 'file_format': 'FILE_FORMAT',
    }
    missing = [f'TARGET_SNOWFLAKE_{field}' for field in fields.values() if not os.environ.get(
        f'TARGET_SNOWFLAKE_{field}'
    )]
    if missing:
        pytest.skip(f'Missing Snowflake E2E configuration: {", ".join(missing)}')
    config = {key: os.environ[f'TARGET_SNOWFLAKE_{field}'] for key, field in fields.items()}
    for key, field in {
        'aws_access_key_id': 'AWS_ACCESS_KEY',
        'aws_secret_access_key': 'AWS_SECRET_ACCESS_KEY',
        'aws_session_token': 'SESSION_TOKEN', 's3_acl': 'S3_ACL', 'role': 'ROLE',
    }.items():
        if value := os.environ.get(f'TARGET_SNOWFLAKE_{field}'):
            config[key] = value
    configured_prefix = os.environ.get('TARGET_SNOWFLAKE_S3_KEY_PREFIX', '').rstrip('/')
    prefix = '/'.join(part for part in (configured_prefix, 'transformation_privacy', namespace) if part)
    return {
        **config, 'default_target_schema': schema, 'tap_id': namespace,
        'data_flattening_max_level': 0, 's3_key_prefix': f'{prefix}/staging/',
        'archive_load_files': True, 'archive_load_files_s3_prefix': f'{prefix}/archive',
    }


def _expected(rows):
    return [(row_id, _digest(secret), 'hidden', 0) for row_id, secret in rows]


def _assert_csv(contents, expected):
    decoded = gzip.decompress(contents).decode('utf-8')
    assert RAW_SECRET not in decoded
    rows = list(csv.reader(io.StringIO(decoded, newline=''), strict=True))
    assert all(len(row) == 7 for row in rows)
    assert sorted((int(row[0]), row[1], row[2], int(row[3])) for row in rows) == expected


def _assert_table(target, schema, table, expected):
    rows = target.query(
        f'SELECT "ID", "SECRET", "HIDDEN_SECRET", "NUMBER_SECRET" '
        f'FROM "{schema}"."{table.upper()}" ORDER BY "ID"'
    )
    assert [
        (row['ID'], row['SECRET'], row['HIDDEN_SECRET'], row['NUMBER_SECRET'])
        for row in rows
    ] == expected


def _remove_s3_prefix(target, prefix):
    paginator = target.s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=target.connection_config['s3_bucket'], Prefix=prefix):
        for entry in page.get('Contents', []):
            target.s3.delete_object(Bucket=target.connection_config['s3_bucket'], Key=entry['Key'])


def _connector_python(connector, program, payload):
    """Exercise each installed connector in its own runtime environment."""
    python = Path(os.environ['PIPELINEWISE_HOME']) / '.virtualenvs' / connector / 'bin' / 'python'
    result = subprocess.run(
        [str(python), '-c', program], input=json.dumps(payload), capture_output=True,
        text=True, check=False, timeout=300,
    )
    assert result.returncode == 0, f'{connector} failed:\n{result.stderr}'
    return result.stdout


def _singer_schema(source_export):
    column_type = 'data_type' if source_export.engine == 'postgres' else 'column_type'
    rows = source_export.source.query(
        'SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, '
        f'{column_type} AS column_type FROM information_schema.columns '
        'WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position',
        params=(source_export.schema, source_export.table_name),
    )
    program = '''
import json
import sys
from types import SimpleNamespace

payload = json.load(sys.stdin)
if payload['engine'] == 'postgres':
    from tap_postgres.discovery_utils import schema_for_column
else:
    from tap_mysql.discover_utils import schema_for_column
properties = {}
for column in payload['columns']:
    column.update(
        is_primary_key=column['column_name'] == 'id', sql_data_type=column['data_type'],
        is_array=False, is_enum=False, column_key='PRI' if column['column_name'] == 'id' else '',
        is_json_alias=payload['engine'] == 'mariadb' and payload['iceberg']
            and column['column_name'] == 'null_variant',
    )
    schema = schema_for_column(SimpleNamespace(**column))
    properties[column['column_name']] = schema if isinstance(schema, dict) else schema.to_dict()
print(json.dumps({'type': 'object', 'properties': properties}))
'''
    return json.loads(_connector_python(
        'tap-postgres' if source_export.engine == 'postgres' else 'tap-mysql',
        program, {'engine': source_export.engine, 'iceberg': source_export.iceberg,
                  'columns': [{key.lower(): value for key, value in dict(row).items()} for row in rows]},
    ))


def _singer_transform_and_load(config, schema, rules, records, stream):
    messages = [
        {'type': 'SCHEMA', 'stream': stream, 'schema': schema, 'key_properties': ['id']},
        *({'type': 'RECORD', 'stream': stream, 'record': record} for record in records),
    ]
    transformed = _connector_python('transform-field', '''
import json
import sys
from transform_field import TransformField

payload = json.load(sys.stdin)
TransformField(payload['config']).consume(json.dumps(message) for message in payload['messages'])
''', {'config': {'transformations': [{'tap_stream_name': stream, **rule} for rule in rules]},
      'messages': messages})
    assert all(
        RAW_SECRET not in value
        for message in map(json.loads, transformed.splitlines())
        for value in message.get('record', {}).values()
        if isinstance(value, str)
    )
    return json.loads(_connector_python('target-snowflake', '''
import json
import sys
from target_snowflake import get_snowflake_statics, persist_lines
from target_snowflake.managed_iceberg import column_type

payload = json.load(sys.stdin)
config = payload['config']
cache, file_format = get_snowflake_statics(config)
persist_lines(config, payload['lines'], cache, file_format)
print(json.dumps({name: column_type(value, config.get('iceberg_version') == 3, config.get('iceberg_version'))
                  for name, value in payload['schema']['properties'].items()}))
''', {'config': {**config, 'parallelism': 1}, 'schema': schema, 'lines': transformed.splitlines()}))


def _parity_columns_and_rules(engine):
    timestamp = 'TIMESTAMP(6)' if engine == 'postgres' else 'DATETIME(6)'
    floating = 'DOUBLE PRECISION' if engine == 'postgres' else 'DOUBLE'
    columns = [('id', 'INTEGER PRIMARY KEY')]
    rules = []
    for index, transformation in enumerate(TRANSFORMATIONS):
        field = f'value_{index}'
        data_type = timestamp if transformation == 'MASK-DATE' else (
            'INTEGER' if transformation == 'MASK-NUMBER' else 'TEXT'
        )
        columns.append((field, data_type))
        rules.append({'field_id': field, 'type': transformation})
    columns.extend([
        ('masked_float', floating), ('null_integer', 'INTEGER'), ('null_float', floating),
        ('null_timestamp', timestamp), ('null_boolean', 'BOOLEAN'), ('null_time', 'TIME(6)'),
        ('null_variant', 'JSON'),
    ])
    rules.extend([
        {'field_id': 'masked_float', 'type': 'MASK-NUMBER'},
        *({'field_id': name, 'type': 'SET-NULL'} for name, _ in columns if name.startswith('null_')),
    ])
    if engine != 'postgres':
        columns.append(('null_binary', 'VARBINARY(16)'))
        rules.append({'field_id': 'null_binary', 'type': 'SET-NULL'})
    return columns, rules


def _parity_record(columns, rules, row_id, value):
    record = {'id': row_id}
    for rule in rules:
        field, transformation = rule['field_id'], rule['type']
        record[field] = '2024-08-19 23:59:58.123456' if transformation == 'MASK-DATE' else (
            12345 if transformation == 'MASK-NUMBER' else value
        )
    for name, data_type in columns:
        if name.startswith('null_'):
            record[name] = {
                'null_integer': 12345, 'null_float': 12.345, 'null_boolean': True,
                'null_time': '12:34:56.123456', 'null_timestamp': '2024-08-19 23:59:58.123456',
                'null_variant': '{"secret": "source only"}', 'null_binary': 'AB',
            }[name]
        if value is None and data_type != 'INTEGER PRIMARY KEY':
            record[name] = None
    return record


def _assert_parity_tables(target, schema, table, columns, engine):
    projection = ', '.join(f'"{name.upper()}"' for name, _ in columns)
    if engine == 'postgres':
        projection += (
            ', CASE WHEN "NULL_VARIANT" IS NULL THEN \'SQL_NULL\' '
            'WHEN IS_NULL_VALUE("NULL_VARIANT") THEN \'JSON_NULL\' ELSE \'VALUE\' END AS "NULL_KIND"'
        )
    fastsync = target.query(f'SELECT {projection} FROM "{schema}"."{table.upper()}" ORDER BY "ID"')
    singer = target.query(f'SELECT {projection} FROM "{schema}"."{table.upper()}_SINGER" ORDER BY "ID"')
    assert len(fastsync) == len(singer)
    # PostgreSQL's Singer object/array schema historically serializes None as JSON null.
    if engine == 'postgres':
        assert all(row['NULL_KIND'] == 'SQL_NULL' for row in fastsync)
        assert all(row['NULL_KIND'] == 'JSON_NULL' for row in singer)
    differences = [
        (left['ID'], name, left[name], right[name])
        for left, right in zip(fastsync, singer)
        for name in left if left[name] != right[name]
        and not (engine == 'postgres' and name in {'NULL_VARIANT', 'NULL_KIND'})
    ]
    assert not differences, differences
    metadata = {}
    for table_name in (table.upper(), table.upper() + '_SINGER'):
        metadata[table_name] = target.query(
            'SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, '
            'NUMERIC_SCALE, DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS '
            'WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s '
            "AND LEFT(COLUMN_NAME, 5) <> '_SDC_' ORDER BY COLUMN_NAME",
            {'schema': schema, 'table': table_name},
        )
    assert metadata[table.upper()] == metadata[table.upper() + '_SINGER']


def test_singer_and_fastsync_preserve_mapped_types_and_values(source_export, tmp_path):
    """Compare real Singer loads with FullSync and PartialSync for native/v3."""
    namespace = f'ppw_parity_{uuid4().hex[:12]}'
    schema = namespace.upper()
    config = {**_target_config(schema, namespace), 'archive_load_files': False}
    if source_export.iceberg:
        config.update(target_table_format='iceberg', iceberg_version=3)
    target = FastSyncTargetSnowflake(config)
    columns, rules = _parity_columns_and_rules(source_export.engine)
    # Singer skips UTF-8 bytes while FastSync skips characters; keep skipped prefixes ASCII.
    values = (None, '', 'a', '0123456789' + RAW_SECRET)
    records = [_parity_record(columns, rules, index, value) for index, value in enumerate(values, 1)]
    source_export.create(columns, [tuple(record[name] for name, _ in columns) for record in records])
    source_export.configure(rules)
    singer_schema = _singer_schema(source_export)
    mapped_types = source_export.source.map_column_types_to_target(source_export.table)
    stream = f'{source_export.schema}-{source_export.table_name}_singer'
    runtime = tmp_path / 'target' / namespace
    runtime.mkdir(parents=True)
    args = Namespace(
        tap={**source_export.source.connection_config, 'tap_id': namespace}, target=config,
        transform=source_export.source.source_transformations, temp_dir=str(tmp_path),
        state=str(runtime / 'state.json'), end_value='4',
        properties={'streams': [{'stream': source_export.table_name, 'metadata': [{
            'breadcrumb': [], 'metadata': {
                'database-name': source_export.source.connection_config['dbname'],
                'schema-name': source_export.schema, 'replication-method': 'FULL_TABLE',
            },
        }]}]},
    )
    full_route = postgres_to_snowflake if source_export.engine == 'postgres' else mysql_to_snowflake
    partial_route = partial_postgres if source_export.engine == 'postgres' else partial_mysql
    try:
        singer_types = _singer_transform_and_load(config, singer_schema, rules, records, stream)
        fastsync_types = {
            column.split(' ', 1)[0].strip('"').lower(): column.split(' ', 1)[1].upper()
            for column in mapped_types['columns']
        }
        if source_export.iceberg:
            fastsync_types = {
                name: MANAGED_ICEBERG_V3_SPEC.canonical_type(data_type)
                for name, data_type in fastsync_types.items()
            }
        assert fastsync_types == {name: data_type.upper() for name, data_type in singer_types.items()}
        result = full_route.sync_table(source_export.table, args)
        assert result is True, result
        _assert_parity_tables(target, schema, source_export.table_name, columns, source_export.engine)

        replacement = _parity_record(columns, rules, 2, '9876543210' + RAW_SECRET)
        assignments = ', '.join(f'{source_export.quoted(name)} = %s' for name, _ in columns if name != 'id')
        source_export.execute(
            f'UPDATE {source_export.quoted_table} SET {assignments} WHERE id = 2',
            tuple(replacement[name] for name, _ in columns if name != 'id'),
        )
        _singer_transform_and_load(config, singer_schema, rules, [replacement], stream)
        result = partial_route.partial_sync_table((source_export.table, {
            'column': 'id', 'start_value': '<S>2', 'end_value': '<S>3', 'drop_target_table': False,
        }), args)
        assert result is True, result
        _assert_parity_tables(target, schema, source_export.table_name, columns, source_export.engine)
    finally:
        try:
            target.query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            _remove_s3_prefix(target, config['s3_key_prefix'])
            _remove_s3_prefix(target, config['archive_load_files_s3_prefix'] + '/')


def test_existing_singer_semantic_differences_remain_explicit(source_export, tmp_path):
    """Keep legacy FastSync boundaries visible where Singer already differs."""
    columns = [
        ('id', 'INTEGER PRIMARY KEY'), ('unicode_secret', 'TEXT'), ('gate', 'INTEGER'),
        ('equals_secret', 'TEXT'), ('regex_gate', 'TEXT'), ('regex_secret', 'TEXT'),
    ]
    record = dict(zip((name for name, _ in columns), (1, '雪AB', 0, 'public', 'xAx', 'public')))
    rules = [
        {'field_id': 'unicode_secret', 'type': 'HASH-SKIP-FIRST-1'},
        {'field_id': 'equals_secret', 'type': 'MASK-HIDDEN', 'when': [{'column': 'gate', 'equals': 0}]},
        {'field_id': 'regex_secret', 'type': 'MASK-HIDDEN', 'when': [{'column': 'regex_gate', 'regex_match': 'A'}]},
    ]
    source_export.create(columns, [tuple(record.values())])
    source_export.configure(rules)
    _, rows = source_export.export(tmp_path / 'existing_semantics.csv.gz')
    actual = dict(zip((name for name, _ in columns), rows[0]))
    singer = json.loads(_connector_python('transform-field', '''
import json
import sys
from transform_field.transform import do_transform

payload = json.load(sys.stdin)
record = payload['record']
for rule in payload['rules']:
    record[rule['field_id']] = do_transform(record, rule['field_id'], rule['type'], rule.get('when'))
print(json.dumps(record))
''', {'record': record, 'rules': rules}))
    assert actual['unicode_secret'] == '雪' + _digest('AB')
    assert singer['unicode_secret'] == '雪' + hashlib.sha256('雪AB'.encode('utf-8')[1:]).hexdigest()
    assert (actual['equals_secret'], singer['equals_secret']) == ('hidden', 'public')
    assert (actual['regex_secret'], singer['regex_secret']) == ('public', 'hidden')


def test_ambiguous_regex_is_rejected_before_export(source_export, tmp_path):
    """A valid Snowflake class must not silently become a MySQL intersection."""
    target = FastSyncTargetSnowflake(_target_config('UNUSED', 'regex_reference'))
    assert target.query(
        "SELECT REGEXP_LIKE(column1, '[a&&b]') AS MATCHED FROM VALUES ('a'), ('b'), ('&')"
    ) == [{'MATCHED': True}] * 3
    source_export.create(
        [('id', 'INTEGER PRIMARY KEY'), ('gate', 'TEXT'), ('secret', 'TEXT')],
        [(1, 'a', RAW_SECRET)],
    )
    source_export.configure([{
        'field_id': 'secret', 'type': 'MASK-HIDDEN',
        'when': [{'column': 'gate', 'regex_match': '[a&&b]'}],
    }])

    with pytest.raises(ValueError, match='character-class set operators'):
        source_export.export(tmp_path / 'rejected.csv.gz')

    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('source_export', [('postgres', False), ('postgres', True)], indirect=True,
                         ids=['postgres-native', 'postgres-v3'])
def test_bit_varying_conditions_match_snowflake(source_export, tmp_path):
    """Compare source conditions with the legacy staged NUMBER representation."""
    values = [(1, '101'), (2, '00101'), (3, '111'), (4, None), (5, '0')]
    source_export.create(
        [('id', 'INTEGER PRIMARY KEY'), ('bits', 'BIT VARYING(8)'), ('secret', 'TEXT')],
        [(row_id, bits, RAW_SECRET if bits in ('101', '00101', None) else 'public') for row_id, bits in values],
    )
    source_export.configure([
        {'field_id': 'secret', 'type': 'MASK-HIDDEN', 'when': [{'column': 'bits', 'equals': 101}]},
        {'field_id': 'secret', 'type': 'MASK-HIDDEN', 'when': [{'column': 'bits', 'equals': None}]},
    ])

    contents, rows = source_export.export(tmp_path / 'bit_conditions.csv.gz')

    assert RAW_SECRET not in contents
    target = FastSyncTargetSnowflake(_target_config('UNUSED', 'bit_reference'))
    references = target.query(
        "SELECT ID, BITS, CASE WHEN BITS = 101 OR BITS IS NULL THEN 'hidden' ELSE 'public' END AS SECRET "
        'FROM (SELECT column1 AS ID, TO_NUMBER(column2) AS BITS FROM VALUES '
        "(1, '101'), (2, '00101'), (3, '111'), (4, NULL), (5, '0')) ORDER BY ID"
    )
    expected = [(row['ID'], row['BITS'], row['SECRET']) for row in references]
    assert sorted((int(row[0]), int(row[1]) if row[1] else None, row[2]) for row in rows) == expected


def test_transformations_are_private_through_publication(source_export, tmp_path, monkeypatch):
    """Inspect local CSV, S3, Snowflake staging, archive and both publications."""
    namespace = f'ppw_privacy_{uuid4().hex[:12]}'
    schema = namespace.upper()
    config = _target_config(schema, namespace)
    if source_export.iceberg:
        config.update(target_table_format='iceberg', iceberg_version=3)
    target = FastSyncTargetSnowflake(config)
    original_upload = FastSyncTargetSnowflake.upload_to_s3
    original_copy = FastSyncTargetSnowflake.copy_to_table
    observed = {'uploads': 0, 'stages': 0, 'expected': []}

    def inspect_upload(self, file, tmp_dir=None):
        with open(file, 'rb') as exported_file:
            contents = exported_file.read()
        _assert_csv(contents, observed['expected'])
        key = original_upload(self, file, tmp_dir)
        body = self.s3.get_object(Bucket=config['s3_bucket'], Key=key)['Body']
        try:
            _assert_csv(body.read(), observed['expected'])
        finally:
            body.close()
        observed['uploads'] += 1
        return key

    def inspect_copy(self, s3_key, target_schema, table_name, size_bytes, is_temporary,
                     skip_csv_header=False, staging_table_name=None):
        count = original_copy(
            self, s3_key, target_schema, table_name, size_bytes, is_temporary,
            skip_csv_header, staging_table_name,
        )
        staging_table = staging_table_name or f'{source_export.table_name}_temp'
        _assert_table(self, schema, staging_table, observed['expected'])
        observed['stages'] += 1
        return count

    def reject_post_load_transform(*_args, **_kwargs):
        raise AssertionError('Source transformations must never run after loading Snowflake staging')

    monkeypatch.setattr(FastSyncTargetSnowflake, 'upload_to_s3', inspect_upload)
    monkeypatch.setattr(FastSyncTargetSnowflake, 'copy_to_table', inspect_copy)
    monkeypatch.setattr(FastSyncTargetSnowflake, 'obfuscate_columns', reject_post_load_transform)
    full_rows = [(row_id, f'{RAW_SECRET}/full/{row_id}') for row_id in range(1, 5)]
    source_export.create(
        [
            ('id', 'INTEGER PRIMARY KEY'), ('secret', 'TEXT'),
            ('hidden_secret', 'TEXT'), ('number_secret', 'INTEGER'),
        ],
        [(row_id, secret, RAW_SECRET, 98765) for row_id, secret in full_rows],
    )
    source_export.configure([
        {'field_id': 'secret', 'type': 'HASH'},
        {'field_id': 'hidden_secret', 'type': 'MASK-HIDDEN'},
        {'field_id': 'number_secret', 'type': 'MASK-NUMBER'},
    ])
    runtime = tmp_path / 'target' / namespace
    runtime.mkdir(parents=True)
    args = Namespace(
        tap={**source_export.source.connection_config, 'tap_id': namespace},
        target=config, transform=source_export.source.source_transformations,
        temp_dir=str(tmp_path), state=str(runtime / 'state.json'),
        properties={'streams': [{
            'stream': source_export.table_name,
            'metadata': [{'breadcrumb': [], 'metadata': {
                'database-name': source_export.source.connection_config['dbname'],
                'schema-name': source_export.schema,
                'replication-method': 'FULL_TABLE',
            }}],
        }]},
        end_value='4',
    )
    full_route = postgres_to_snowflake if source_export.engine == 'postgres' else mysql_to_snowflake
    partial_route = partial_postgres if source_export.engine == 'postgres' else partial_mysql

    try:
        observed['expected'] = _expected(full_rows)
        result = full_route.sync_table(source_export.table, args)
        assert result is True, result
        _assert_table(target, schema, source_export.table_name, observed['expected'])
        format_rows = target.query(
            'SELECT IS_ICEBERG FROM INFORMATION_SCHEMA.TABLES '
            f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{source_export.table_name.upper()}'"
        )
        assert format_rows == [{'IS_ICEBERG': 'YES' if source_export.iceberg else 'NO'}]
        archived = target.s3.list_objects_v2(
            Bucket=config['s3_bucket'], Prefix=config['archive_load_files_s3_prefix'] + '/',
        ).get('Contents', [])
        assert len(archived) == 1
        body = target.s3.get_object(Bucket=config['s3_bucket'], Key=archived[0]['Key'])['Body']
        try:
            _assert_csv(body.read(), observed['expected'])
        finally:
            body.close()

        partial_rows = [(row_id, f'{RAW_SECRET}/partial/{row_id}') for row_id in (2, 3)]
        for row_id, secret in [(1, f'{RAW_SECRET}/outside-range'), *partial_rows]:
            source_export.execute(
                f'UPDATE {source_export.quoted_table} SET secret = %s WHERE id = %s',
                (secret, row_id),
            )
        source_export.execute(f'DELETE FROM {source_export.quoted_table} WHERE id = 4')
        observed['expected'] = _expected(partial_rows)
        result = partial_route.partial_sync_table((source_export.table, {
            'column': 'id', 'start_value': '<S>2', 'end_value': '<S>4', 'drop_target_table': False,
        }), args)
        assert result is True, result
        _assert_table(target, schema, source_export.table_name, _expected([full_rows[0], *partial_rows]))
        assert observed['uploads'] == observed['stages'] == 2
    finally:
        try:
            target.query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            _remove_s3_prefix(target, config['s3_key_prefix'])
            _remove_s3_prefix(target, config['archive_load_files_s3_prefix'] + '/')
