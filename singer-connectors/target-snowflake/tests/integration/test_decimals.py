"""Real native and managed-Iceberg decimal loads and column evolution."""

import json
import math
import os
from decimal import Decimal
from uuid import uuid4

import pytest

import target_snowflake
from singer.decimal_support import decimal_schema
from target_snowflake.db_sync import DbSync
from target_snowflake.upload_clients.s3_upload_client import S3UploadClient
from tests.integration import utils


@pytest.fixture(params=['native', 'iceberg'])
def snowflake_decimal_target(request):
    config = utils.get_db_config()
    run_id = uuid4().hex[:12].upper()
    config.update({
        'default_target_schema': f'PW_DECIMAL_{run_id}',
        'target_table_format': request.param,
        'file_format': config.get('file_format') or os.environ.get('TARGET_SNOWFLAKE_FILE_FORMAT'),
        'client_side_encryption_master_key': '',
        's3_key_prefix': f'decimal-integration/{run_id}/',
        'disable_table_cache': False,
        'validate_records': True,
        'parallelism': 1,
    })
    if request.param == 'iceberg':
        config['iceberg_version'] = 3
    database = DbSync(config)
    try:
        yield config, database
    finally:
        database.query(f'DROP SCHEMA IF EXISTS "{config["default_target_schema"]}" CASCADE')
        if config.get('s3_bucket'):
            client = S3UploadClient(config).s3_client
            for page in client.get_paginator('list_objects_v2').paginate(
                Bucket=config['s3_bucket'], Prefix=config['s3_key_prefix'],
            ):
                objects = [{'Key': item['Key']} for item in page.get('Contents', [])]
                if objects:
                    client.delete_objects(Bucket=config['s3_bucket'], Delete={'Objects': objects})


def load(config, amount_schema, records=(), id_schema=None, extra_properties=None, key_properties=('id',),
         record_update_mode=None):
    properties = {'id': id_schema or {'type': ['integer']}, 'amount': amount_schema, **(extra_properties or {})}
    messages = [{
        'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': list(key_properties),
        'schema': {'type': 'object', 'properties': properties},
    }]
    if record_update_mode:
        messages[0]['schema']['x-pipelinewise-record-update-mode'] = record_update_mode
    messages.extend({'type': 'RECORD', 'stream': 'public-items', 'record': record} for record in records)
    cache, file_format = target_snowflake.get_snowflake_statics(config)
    target_snowflake.persist_lines(config, [json.dumps(message) for message in messages], cache, file_format)


def test_opt_in_exact_decimal_load_versions_history_and_retries_without_reversion(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    config['version_legacy_float_columns'] = True
    schema = config['default_target_schema']
    amount = '12345678901234567890.123456789'
    load(config, {'type': ['null', 'number']}, [{'id': 1, 'amount': 1.25}, {'id': 2, 'amount': 2.5}])
    load(config, decimal_schema(29, 9), [{'id': 1, 'amount': amount}, {'id': 3, 'amount': None}])
    columns = database.get_table_columns([schema])
    archives = [column['COLUMN_NAME'] for column in columns if column['COLUMN_NAME'].startswith('AMOUNT_')]
    assert len(archives) == 1
    column = next(column for column in columns if column['COLUMN_NAME'] == 'AMOUNT')
    assert (column['DATA_TYPE'], column['NUMERIC_PRECISION'], column['NUMERIC_SCALE']) == ('NUMBER', 29, 9)
    rows = database.query(f'SELECT ID, AMOUNT, "{archives[0]}" AS PREVIOUS FROM "{schema}".ITEMS ORDER BY ID')
    assert rows == [
        {'ID': 1, 'AMOUNT': Decimal(amount), 'PREVIOUS': 1.25},
        {'ID': 2, 'AMOUNT': None, 'PREVIOUS': 2.5},
        {'ID': 3, 'AMOUNT': None, 'PREVIOUS': None},
    ]
    load(config, decimal_schema(29, 9))
    load(config, decimal_schema(30, 10), [{'id': 1, 'amount': amount + '0'}])
    load(config, decimal_schema(30, 10))
    columns = database.get_table_columns([schema])
    assert sum(column['COLUMN_NAME'].startswith('AMOUNT_') for column in columns) == 2
    column = next(column for column in columns if column['COLUMN_NAME'] == 'AMOUNT')
    assert (column['DATA_TYPE'], column['NUMERIC_PRECISION'], column['NUMERIC_SCALE']) == ('NUMBER', 30, 10)
    assert database.query(f'SELECT AMOUNT FROM "{schema}".ITEMS WHERE ID = 1')[0]['AMOUNT'] == Decimal(amount)


def test_decimal_key_change_retains_existing_type_and_adds_other_columns(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    load(config, decimal_schema(29, 9), [{'id': 1, 'amount': '1.25'}])
    schema = config['default_target_schema']
    before = next(column for column in database.get_table_columns([schema]) if column['COLUMN_NAME'] == 'ID')
    load(config, decimal_schema(29, 9), [{'id': '1.0', 'amount': '2.25', 'added': 'new'}],
         id_schema=decimal_schema(29, 9), extra_properties={'added': {'type': ['string']}})
    columns = database.get_table_columns([schema])
    assert next(column for column in columns if column['COLUMN_NAME'] == 'ID') == before
    assert any(column['COLUMN_NAME'] == 'ADDED' for column in columns)
    assert database.query(f'SELECT ID, AMOUNT, ADDED FROM "{schema}".ITEMS') == [
        {'ID': Decimal(1), 'AMOUNT': Decimal('2.25'), 'ADDED': 'new'},
    ]


def test_decimal_primary_keys_coalesce_equal_numeric_values(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    load(config, decimal_schema(10, 2), [
        {'id': '9007199254740992.00', 'amount': '2.00'},
        {'id': '9007199254740992.000', 'amount': '3.00'},
        {'id': '9007199254740992.01', 'amount': '4.00'},
    ], id_schema=decimal_schema(29, 9))
    load(config, decimal_schema(10, 2), [
        {'id': '9007199254740992.00', 'amount': '5.00'},
        {'id': '9007199254740992.01', 'amount': '6.00'},
    ], id_schema=decimal_schema(29, 9))
    rows = database.query(f'SELECT ID, AMOUNT FROM "{config["default_target_schema"]}".ITEMS ORDER BY ID')
    assert rows == [
        {'ID': Decimal('9007199254740992.00'), 'AMOUNT': Decimal('5')},
        {'ID': Decimal('9007199254740992.01'), 'AMOUNT': Decimal('6')},
    ]


def test_postgres_bounded_numeric_key_keeps_nan_and_canonical_identity(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    config['source_tap_type'] = 'tap-postgres'
    schema = decimal_schema(10, 2)
    load(config, schema, [
        {'id': 'NaN', 'amount': '1.00'},
        {'id': '1.00', 'amount': '2.00'},
        {'id': '2.50', 'amount': '3.00'},
    ], id_schema=schema)
    load(config, schema, [
        {'id': 'NaN', 'amount': '4.00'},
        {'id': '1.0', 'amount': '5.00'},
    ], id_schema=schema)

    table = f'"{config["default_target_schema"]}".ITEMS'
    columns = database.get_table_columns([config['default_target_schema']])
    key = next(column for column in columns if column['COLUMN_NAME'] == 'ID')
    assert key['DATA_TYPE'] in ('TEXT', 'VARCHAR')
    assert database.query(f'SELECT ID, AMOUNT FROM {table} ORDER BY ID') == [
        {'ID': '1', 'AMOUNT': Decimal('5.00')},
        {'ID': '2.5', 'AMOUNT': Decimal('3.00')},
        {'ID': 'NaN', 'AMOUNT': Decimal('4.00')},
    ]


def test_decimal_precision_reversal_in_one_process_refreshes_startup_cache(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    load(config, decimal_schema(10, 2), [{'id': 1, 'amount': '1.25'}])
    cache, file_format = target_snowflake.get_snowflake_statics(config)
    messages = []
    for precision, amount in ((10, '2.25'), (11, '3.25'), (10, '4.25')):
        messages.extend([
            {'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': ['id'],
             'schema': {'type': 'object', 'properties': {
                 'id': {'type': ['integer']}, 'amount': decimal_schema(precision, 2),
             }}},
            {'type': 'RECORD', 'stream': 'public-items', 'record': {'id': 1, 'amount': amount}},
        ])
    target_snowflake.persist_lines(config, [json.dumps(message) for message in messages], cache, file_format)
    schema = config['default_target_schema']
    columns = database.get_table_columns([schema])
    archives = [column['COLUMN_NAME'] for column in columns if column['COLUMN_NAME'].startswith('AMOUNT_')]
    assert len(archives) == 2
    amount_column = next(column for column in columns if column['COLUMN_NAME'] == 'AMOUNT')
    assert (amount_column['NUMERIC_PRECISION'], amount_column['NUMERIC_SCALE']) == (10, 2)
    archived_values = ', '.join(f'"{name}"' for name in sorted(archives))
    rows = database.query(f'SELECT AMOUNT, {archived_values} FROM "{schema}".ITEMS WHERE ID = 1')
    assert rows == [{'AMOUNT': Decimal('4.25'), **dict(zip(sorted(archives), [Decimal('2.25'), Decimal('3.25')]))}]


def test_decimal_archive_bounds_preserve_exact_text_and_ignore_nulls(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    archive_root = f'{config["s3_key_prefix"]}archive'
    config.update(archive_load_files=True, archive_load_files_s3_prefix=archive_root, tap_id='decimal_archive')
    minimum, maximum = '2.000000000000000001', '10.000000000000000002'
    messages = [{
        'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': ['id'], 'bookmark_properties': ['amount'],
        'schema': {'type': 'object', 'properties': {
            'id': {'type': ['integer']}, 'amount': decimal_schema(38, 18),
        }},
    }]
    messages.extend(
        {'type': 'RECORD', 'stream': 'public-items', 'record': {'id': index, 'amount': amount}}
        for index, amount in enumerate((minimum, None, maximum), start=1)
    )
    cache, file_format = target_snowflake.get_snowflake_statics(config)
    target_snowflake.persist_lines(config, [json.dumps(message) for message in messages], cache, file_format)

    client = S3UploadClient(config).s3_client
    archived = client.list_objects_v2(Bucket=config['s3_bucket'], Prefix=f'{archive_root}/decimal_archive/')['Contents']
    assert len(archived) == 1
    metadata = client.head_object(Bucket=config['s3_bucket'], Key=archived[0]['Key'])['Metadata']
    assert (metadata['incremental-key'], metadata['incremental-key-min'], metadata['incremental-key-max']) == (
        'amount', minimum, maximum,
    )
    rows = database.query(f'SELECT ID, AMOUNT FROM "{config["default_target_schema"]}".ITEMS ORDER BY ID')
    assert rows == [
        {'ID': 1, 'AMOUNT': Decimal(minimum)}, {'ID': 2, 'AMOUNT': None}, {'ID': 3, 'AMOUNT': Decimal(maximum)},
    ]


@pytest.mark.parametrize('has_key', [True, False])
def test_float_fallback_retains_overflow_rows_nulls_and_stable_column(snowflake_decimal_target, has_key):
    config, database = snowflake_decimal_target
    config['primary_key_required'] = has_key
    keys = ('id',) if has_key else ()
    table = f'"{config["default_target_schema"]}".ITEMS'
    load(config, decimal_schema(10, 2), [{'id': 1, 'amount': '1.25'}], key_properties=keys)
    load(config, decimal_schema(None, None), [
        {'id': 2, 'amount': '1e1000'}, {'id': 3, 'amount': '-1e1000'},
        {'id': 4, 'amount': None}, {'id': 5, 'amount': '12345678901234567890.123456789'},
        {'id': 6, 'amount': '1e-1000'}, {'id': 7, 'amount': '-1e-1000'},
        {'id': 8, 'amount': 'NaN'}, {'id': 9, 'amount': 'Infinity'}, {'id': 10, 'amount': '-Infinity'},
    ], key_properties=keys)
    load(config, decimal_schema(65, 30), key_properties=keys)
    load(config, decimal_schema(None, None), key_properties=keys)
    columns = database.get_table_columns([config['default_target_schema']])
    assert sum(column['COLUMN_NAME'].startswith('AMOUNT_') for column in columns) == 1
    rows = database.query(f'SELECT ID, AMOUNT FROM {table} ORDER BY ID')
    assert len(rows) == 10
    assert rows[1]['AMOUNT'] == float('1.7976931348623157e308')
    assert rows[2]['AMOUNT'] == -float('1.7976931348623157e308')
    assert rows[3]['AMOUNT'] is None
    assert rows[4]['AMOUNT'] == float('12345678901234567890.123456789')
    assert rows[5]['AMOUNT'] == rows[6]['AMOUNT'] == 0
    assert math.isnan(rows[7]['AMOUNT'])
    assert rows[8]['AMOUNT'] == float('inf')
    assert rows[9]['AMOUNT'] == -float('inf')


def test_float_fallback_keys_preserve_distinct_rows_across_batches(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    keys = ['9007199254740992', '9007199254740992.01']
    load(config, decimal_schema(10, 2), [
        {'id': keys[0] + '.00', 'amount': '1.00'}, {'id': keys[1] + '00', 'amount': '2.00'},
    ], id_schema=decimal_schema(None, None))
    load(config, decimal_schema(10, 2), [
        {'id': '9.007199254740992E15', 'amount': '3.00'},
        {'id': '9.00719925474099201E15', 'amount': '4.00'},
    ], id_schema=decimal_schema(None, None))
    rows = database.query(f'SELECT ID, AMOUNT FROM "{config["default_target_schema"]}".ITEMS ORDER BY ID')
    assert rows == [{'ID': keys[0], 'AMOUNT': Decimal(3)}, {'ID': keys[1], 'AMOUNT': Decimal(4)}]


def test_bounded_nan_retains_rows_in_copy_and_merge(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    config['primary_key_required'] = False
    load(config, decimal_schema(10, 2), [
        {'id': 1, 'amount': 'NaN'}, {'id': 2, 'amount': '1.25'},
    ], key_properties=())
    config['primary_key_required'] = True
    load(config, decimal_schema(10, 2), [
        {'id': 1, 'amount': '2.25'}, {'id': 2, 'amount': 'NaN'}, {'id': 3, 'amount': 'NaN'},
    ])
    rows = database.query(f'SELECT ID, AMOUNT FROM "{config["default_target_schema"]}".ITEMS ORDER BY ID')
    assert rows == [{'ID': 1, 'AMOUNT': Decimal('2.25')}, {'ID': 2, 'AMOUNT': None}, {'ID': 3, 'AMOUNT': None}]


def current_primary_keys(config, database):
    rows = database.query(f'SHOW PRIMARY KEYS IN TABLE "{config["default_target_schema"]}".ITEMS')
    return {row['column_name'].upper() for row in rows}


def drop_items(config, database):
    table_kind = 'ICEBERG TABLE' if config['target_table_format'] == 'iceberg' else 'TABLE'
    database.query(f'DROP {table_kind} "{config["default_target_schema"]}".ITEMS')


def test_mysql_extended_composite_key_upgrade_preserves_historical_updates_and_deletes(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    config['source_tap_type'] = 'tap-mysql'
    amount_schema = decimal_schema(10, 2)
    table = f'"{config["default_target_schema"]}".ITEMS'
    load(config, amount_schema, [{'id': 1, 'amount': '1.00'}, {'id': 2, 'amount': '2.00'}])
    properties = {
        'calendar_year': {'type': ['null', 'integer'], 'format': 'singer.year'},
        'tags': {'type': ['null', 'string']},
        'payload': {'type': ['null', 'string'], 'format': 'binary'},
    }
    keys = ('id', *properties)
    # Each load starts a target process, including the second schema after the columns already exist.
    load(config, amount_schema, [{'id': 1, 'amount': '3.00', 'calendar_year': 2026,
                                 'tags': 'a,b', 'payload': '00ff'}], extra_properties=properties,
         key_properties=keys)
    assert current_primary_keys(config, database) == {'ID'}
    assert database.query(f'SELECT ID, AMOUNT, CALENDAR_YEAR, TAGS, HEX_ENCODE(PAYLOAD) AS PAYLOAD '
                          f'FROM {table} ORDER BY ID') == [
        {'ID': 1, 'AMOUNT': Decimal('3.00'), 'CALENDAR_YEAR': 2026, 'TAGS': 'a,b', 'PAYLOAD': '00FF'},
        {'ID': 2, 'AMOUNT': Decimal('2.00'), 'CALENDAR_YEAR': None, 'TAGS': None, 'PAYLOAD': None},
    ]
    load(config, amount_schema, [
        {'id': 1, 'amount': '4.00', 'calendar_year': 2025, 'tags': 'b', 'payload': 'abcd'},
        {'id': 2, 'calendar_year': 2026, 'tags': 'a', 'payload': 'ff00',
         '_sdc_deleted_at': '2026-10-05T00:00:00Z'},
    ], extra_properties=properties, key_properties=keys)
    assert database.query(f'SELECT ID, AMOUNT, CALENDAR_YEAR FROM {table}') == [
        {'ID': 1, 'AMOUNT': Decimal('4.00'), 'CALENDAR_YEAR': 2025},
    ]
    assert current_primary_keys(config, database) == {'ID'}

    drop_items(config, database)
    load(config, amount_schema, [
        {'id': 7, 'amount': '1.00', 'calendar_year': 2025, 'tags': 'a', 'payload': '00ff'},
        {'id': 7, 'amount': '2.00', 'calendar_year': 2026, 'tags': 'a', 'payload': '00ff'},
    ], extra_properties=properties, key_properties=keys)
    assert current_primary_keys(config, database) == {name.upper() for name in keys}
    assert database.query(f'SELECT ID, AMOUNT, CALENDAR_YEAR FROM {table} ORDER BY CALENDAR_YEAR') == [
        {'ID': 7, 'AMOUNT': Decimal('1.00'), 'CALENDAR_YEAR': 2025},
        {'ID': 7, 'AMOUNT': Decimal('2.00'), 'CALENDAR_YEAR': 2026},
    ]


def test_legacy_text_binary_key_matches_uppercase_fastsync_hex(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    config['source_tap_type'] = 'tap-mysql'
    amount_schema = decimal_schema(10, 2)
    table = f'"{config["default_target_schema"]}".ITEMS'
    load(config, amount_schema, [{'id': '00FFABCD', 'amount': '1.00'}, {'id': '11AABB', 'amount': '2.00'}],
         id_schema={'type': ['string']})
    binary_schema = {'type': ['string'], 'format': 'binary'}
    load(config, amount_schema, [{'id': '00ffabcd', 'amount': '3.00'}], id_schema=binary_schema)
    load(config, amount_schema, [{'id': '11aabb', '_sdc_deleted_at': '2026-10-05T00:00:00Z'}],
         id_schema=binary_schema)
    assert database.query(f'SELECT ID, AMOUNT FROM {table}') == [{'ID': '00FFABCD', 'AMOUNT': Decimal('3.00')}]
    columns = database.get_table_columns([config['default_target_schema']])
    assert next(column for column in columns if column['COLUMN_NAME'] == 'ID')['DATA_TYPE'] in ('TEXT', 'VARCHAR')

    drop_items(config, database)
    load(config, amount_schema, [{'id': '00ffabcd', 'amount': '4.00'}], id_schema=binary_schema)
    assert database.query(f'SELECT HEX_ENCODE(ID) AS ID, AMOUNT FROM {table}') == [
        {'ID': '00FFABCD', 'AMOUNT': Decimal('4.00')},
    ]
    columns = database.get_table_columns([config['default_target_schema']])
    assert next(column for column in columns if column['COLUMN_NAME'] == 'ID')['DATA_TYPE'] == 'BINARY'


def test_legacy_float_decimal_keys_coalesce_collisions_and_keep_last_patch_and_delete(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    config['source_tap_type'] = 'tap-postgres'
    amount_schema = decimal_schema(10, 2)
    table = f'"{config["default_target_schema"]}".ITEMS'
    properties = {'tenant': {'type': ['integer']}, 'description': {'type': ['null', 'string']}}
    keys = ('id', 'tenant')
    # Iceberg rejects FLOAT identifier fields; legacy tables can still use logical Singer keys.
    legacy_keys = () if config['target_table_format'] == 'iceberg' else keys
    config['primary_key_required'] = bool(legacy_keys)
    load(config, amount_schema, [
        {'id': 9007199254740992, 'tenant': 1, 'amount': '1.00', 'description': 'historical'},
    ], id_schema={'type': ['number']}, extra_properties=properties, key_properties=legacy_keys)
    config['primary_key_required'] = True
    load(config, amount_schema, [
        {'id': '9007199254740992', 'tenant': 1, 'amount': '2.00', 'description': 'kept by PATCH'},
        {'id': '9007199254740993', 'tenant': 1, 'amount': '3.00'},
        {'id': '9007199254740992', 'tenant': 2, 'amount': '4.00', 'description': 'other tenant'},
    ], id_schema=decimal_schema(None, None), extra_properties=properties, key_properties=keys,
         record_update_mode='PATCH')
    assert database.query(f'SELECT ID, TENANT, AMOUNT, DESCRIPTION FROM {table} ORDER BY TENANT') == [
        {'ID': float(9007199254740992), 'TENANT': 1, 'AMOUNT': Decimal('3.00'), 'DESCRIPTION': 'kept by PATCH'},
        {'ID': float(9007199254740992), 'TENANT': 2, 'AMOUNT': Decimal('4.00'), 'DESCRIPTION': 'other tenant'},
    ]
    load(config, amount_schema, [
        {'id': '9007199254740992', 'tenant': 1, 'amount': '5.00'},
        {'id': '9007199254740993', 'tenant': 1, '_sdc_deleted_at': '2026-10-05T00:00:00Z'},
    ], id_schema=decimal_schema(None, None), extra_properties=properties, key_properties=keys,
         record_update_mode='PATCH')
    assert database.query(f'SELECT TENANT, AMOUNT FROM {table}') == [{'TENANT': 2, 'AMOUNT': Decimal('4.00')}]

    extremes = [
        ('1e1000', '2e1000', float('1.7976931348623157e308')),
        ('-1e1000', '-2e1000', -float('1.7976931348623157e308')),
        ('1e-1000', '-1e-1000', 0.0),
        ('-0', '0.000', 0.0),
        ('NaN', 'NaN', float('nan')),
    ]
    records = [
        {'id': key, 'tenant': index, 'amount': amount}
        for index, (first, last, _expected) in enumerate(extremes, start=3)
        for key, amount in ((first, '1.00'), (last, '2.00'))
    ]
    load(config, amount_schema, records, id_schema=decimal_schema(None, None), extra_properties=properties,
         key_properties=keys)
    rows = database.query(f'SELECT ID, TENANT, AMOUNT FROM {table} WHERE TENANT >= 3 ORDER BY TENANT')
    assert len(rows) == len(extremes)
    for row, (_first, _last, expected) in zip(rows, extremes):
        assert math.isnan(row['ID']) if math.isnan(expected) else row['ID'] == expected
        assert row['AMOUNT'] == Decimal('2.00')
