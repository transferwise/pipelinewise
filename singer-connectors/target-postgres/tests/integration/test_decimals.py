"""Real PostgreSQL decimal loads, retries and retained column history."""

import json
import os
from decimal import Decimal
from uuid import uuid4

import pytest

import target_postgres
from singer.decimal_support import decimal_schema
from target_postgres.db_sync import DbSync


@pytest.fixture
def postgres_decimal_target():
    config = {
        name: os.environ.get(f'TARGET_POSTGRES_{name.upper()}')
        for name in ('host', 'port', 'user', 'password', 'dbname')
    }
    config['dbname'] = config['dbname'] or os.environ.get('TARGET_POSTGRES_DB')
    missing = [name for name, value in config.items() if not value]
    if missing:
        pytest.fail(f'Missing PostgreSQL integration settings: {", ".join(missing)}')
    config.update(default_target_schema=f'pw_decimal_{uuid4().hex[:12]}', validate_records=True, parallelism=1)
    database = DbSync(config)
    try:
        yield config, database
    finally:
        database.query(f'DROP SCHEMA IF EXISTS {config["default_target_schema"]} CASCADE')


def load(config, amount_schema, records=(), id_schema=None, extra_properties=None):
    properties = {'id': id_schema or {'type': ['integer']}, 'amount': amount_schema, **(extra_properties or {})}
    messages = [{
        'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': ['id'],
        'schema': {'type': 'object', 'properties': properties},
    }]
    messages.extend({'type': 'RECORD', 'stream': 'public-items', 'record': record} for record in records)
    target_postgres.persist_lines(config, [json.dumps(message) for message in messages])


def test_exact_decimal_load_versions_history_and_retries_without_reversion(postgres_decimal_target):
    config, database = postgres_decimal_target
    schema = config['default_target_schema']
    amount = '12345678901234567890.123456789'
    load(config, {'type': ['null', 'number']}, [{'id': 1, 'amount': 1.25}, {'id': 2, 'amount': 2.5}])
    load(config, decimal_schema(29, 9), [{'id': 1, 'amount': amount}, {'id': 3, 'amount': None}])
    columns = database.query(
        'SELECT column_name, numeric_precision, numeric_scale FROM information_schema.columns '
        'WHERE table_schema = %s AND table_name = %s', (schema, 'items'),
    )
    archives = [column['column_name'] for column in columns if column['column_name'].startswith('amount_')]
    assert len(archives) == 1
    assert dict(next(column for column in columns if column['column_name'] == 'amount')) == {
        'column_name': 'amount', 'numeric_precision': 29, 'numeric_scale': 9,
    }
    rows = database.query(f'SELECT id, amount, "{archives[0]}" AS previous FROM {schema}.items ORDER BY id')
    assert [dict(row) for row in rows] == [
        {'id': 1, 'amount': Decimal(amount), 'previous': 1.25},
        {'id': 2, 'amount': None, 'previous': 2.5},
        {'id': 3, 'amount': None, 'previous': None},
    ]
    load(config, decimal_schema(29, 9))
    load(config, decimal_schema(30, 10), [{'id': 1, 'amount': amount + '0'}])
    load(config, decimal_schema(30, 10))
    columns = database.query(
        'SELECT column_name, numeric_precision, numeric_scale FROM information_schema.columns '
        'WHERE table_schema = %s AND table_name = %s', (schema, 'items'),
    )
    assert sum(column['column_name'].startswith('amount_') for column in columns) == 2
    assert dict(next(column for column in columns if column['column_name'] == 'amount')) == {
        'column_name': 'amount', 'numeric_precision': 30, 'numeric_scale': 10,
    }
    assert database.query(f'SELECT amount FROM {schema}.items WHERE id = 1')[0]['amount'] == Decimal(amount)


def test_decimal_key_change_fails_before_adding_other_columns(postgres_decimal_target):
    config, database = postgres_decimal_target
    load(config, decimal_schema(29, 9), [{'id': 1, 'amount': '1.25'}])
    schema = config['default_target_schema']
    before = database.query(
        'SELECT column_name FROM information_schema.columns WHERE table_schema = %s ORDER BY ordinal_position',
        (schema,),
    )
    with pytest.raises(ValueError, match='primary-key'):
        load(config, decimal_schema(29, 9), id_schema=decimal_schema(29, 9),
             extra_properties={'added': {'type': ['string']}})
    after = database.query(
        'SELECT column_name FROM information_schema.columns WHERE table_schema = %s ORDER BY ordinal_position',
        (schema,),
    )
    assert after == before


@pytest.mark.parametrize(('precision', 'scale', 'amount'), [
    (65, 30, '12345678901234567890123456789012345.123456789012345678901234567890'),
    (None, None, '1234567890123456789012345678901234567890.123456789012345678901234567890'),
    (4, -2, '123400'),
    (2, 4, '0.0099'),
])
def test_postgres_preserves_its_wider_numeric_domain(postgres_decimal_target, precision, scale, amount):
    config, database = postgres_decimal_target
    load(config, decimal_schema(precision, scale), [{'id': 1, 'amount': amount}])
    load(config, decimal_schema(precision, scale), [{'id': 1, 'amount': amount}])
    load(config, decimal_schema(precision, scale))
    assert database.query(f'SELECT amount FROM {config["default_target_schema"]}.items')[0]['amount'] == Decimal(amount)
    columns = database.query(
        'SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s',
        (config['default_target_schema'], 'items'),
    )
    assert not any(column['column_name'].startswith('amount_') for column in columns)


def test_decimal_primary_keys_coalesce_equal_numeric_values(postgres_decimal_target):
    config, database = postgres_decimal_target
    load(config, decimal_schema(10, 2), [
        {'id': '9007199254740992.00', 'amount': '2.00'},
        {'id': '9007199254740992.000', 'amount': '3.00'},
        {'id': '9007199254740992.01', 'amount': '4.00'},
    ], id_schema=decimal_schema(None, None))
    load(config, decimal_schema(10, 2), [
        {'id': '9007199254740992.00', 'amount': '5.00'},
        {'id': '9007199254740992.01', 'amount': '6.00'},
    ], id_schema=decimal_schema(None, None))
    rows = database.query(f'SELECT id, amount FROM {config["default_target_schema"]}.items ORDER BY id')
    assert [dict(row) for row in rows] == [
        {'id': Decimal('9007199254740992.00'), 'amount': Decimal('5')},
        {'id': Decimal('9007199254740992.01'), 'amount': Decimal('6')},
    ]


def test_bounded_numeric_nan_is_preserved_on_repeat_load(postgres_decimal_target):
    config, database = postgres_decimal_target
    for _ in range(2):
        load(config, decimal_schema(10, 2), [{'id': 1, 'amount': 'NaN'}, {'id': 2, 'amount': '1.25'}])
    rows = database.query(f'SELECT id, amount FROM {config["default_target_schema"]}.items ORDER BY id')
    assert len(rows) == 2
    assert rows[0]['amount'].is_nan()
    assert rows[1]['amount'] == Decimal('1.25')
