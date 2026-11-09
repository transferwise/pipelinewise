"""Real PostgreSQL decimal loads, retries and retained column history."""

import json
import math
import os
from decimal import Decimal
from uuid import uuid4

import pytest

import target_postgres
from singer.decimal_support import decimal_float_value, decimal_schema
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


def load(config, amount_schema, records=(), id_schema=None, extra_properties=None, key_properties=('id',),
         definitions=None):
    properties = {'id': id_schema or {'type': ['integer']}, 'amount': amount_schema, **(extra_properties or {})}
    messages = [{
        'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': list(key_properties),
        'schema': {'type': 'object', 'properties': properties},
    }]
    if definitions:
        messages[0]['schema']['definitions'] = definitions
    messages.extend({'type': 'RECORD', 'stream': 'public-items', 'record': record} for record in records)
    target_postgres.persist_lines(config, [json.dumps(message) for message in messages])


@pytest.mark.parametrize(('legacy_type', 'limit'), [
    ('double precision', '1.7976931348623157e308'),
    ('real', '3.4028234663852886e38'),
])
def test_legacy_floating_decimal_is_retained_and_saturates_overflow(
        postgres_decimal_target, legacy_type, limit):
    config, database = postgres_decimal_target
    schema = config['default_target_schema']
    load(config, {'type': ['null', 'number']}, [{'id': 1, 'amount': 1.25}, {'id': 2, 'amount': 2.5}])
    if legacy_type == 'real':
        database.query(f'ALTER TABLE {schema}.items ALTER COLUMN amount TYPE REAL')
    load(config, decimal_schema(None, None), [
        {'id': 1, 'amount': '1e999'},
        {'id': 2, 'amount': '-1e999'},
        {'id': 3, 'amount': None},
        {'id': 4, 'amount': '1e-400'},
    ])
    columns = database.query(
        'SELECT column_name, data_type FROM information_schema.columns '
        'WHERE table_schema = %s AND table_name = %s', (schema, 'items'),
    )
    assert not any(column['column_name'].startswith('amount_') for column in columns)
    assert dict(next(column for column in columns if column['column_name'] == 'amount')) == {
        'column_name': 'amount', 'data_type': legacy_type,
    }
    rows = database.query(f'SELECT id, amount FROM {schema}.items ORDER BY id')
    assert [row['id'] for row in rows] == [1, 2, 3, 4]
    assert rows[0]['amount'] == pytest.approx(float(limit), rel=1e-7)
    assert rows[1]['amount'] == pytest.approx(-float(limit), rel=1e-7)
    assert rows[2]['amount'] is None
    assert rows[3]['amount'] == 0.0


def test_exact_decimal_dimension_change_versions_once(postgres_decimal_target):
    config, database = postgres_decimal_target
    schema = config['default_target_schema']
    amount = '12345678901234567890.123456789'
    load(config, decimal_schema(29, 9), [{'id': 1, 'amount': amount}])
    load(config, decimal_schema(30, 10), [{'id': 1, 'amount': amount + '0'}])
    load(config, decimal_schema(30, 10))
    columns = database.query(
        'SELECT column_name, numeric_precision, numeric_scale FROM information_schema.columns '
        'WHERE table_schema = %s AND table_name = %s', (schema, 'items'),
    )
    assert sum(column['column_name'].startswith('amount_') for column in columns) == 1
    assert dict(next(column for column in columns if column['column_name'] == 'amount')) == {
        'column_name': 'amount', 'numeric_precision': 30, 'numeric_scale': 10,
    }
    assert database.query(f'SELECT amount FROM {schema}.items WHERE id = 1')[0]['amount'] == Decimal(amount)


def test_legacy_double_precision_decimal_key_is_retained(postgres_decimal_target):
    config, database = postgres_decimal_target
    load(config, decimal_schema(29, 9), [{'id': 1.25, 'amount': '1.25'}], id_schema={'type': ['number']})
    schema = config['default_target_schema']
    load(
        config,
        decimal_schema(29, 9),
        [{'id': '1.25', 'amount': '2.50'}],
        id_schema=decimal_schema(29, 9),
        extra_properties={'added': {'type': ['string']}},
    )
    columns = database.query(
        'SELECT column_name, data_type FROM information_schema.columns '
        'WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position',
        (schema, 'items'),
    )
    assert dict(next(column for column in columns if column['column_name'] == 'id')) == {
        'column_name': 'id', 'data_type': 'double precision',
    }
    assert not any(column['column_name'].startswith('id_') for column in columns)
    assert any(column['column_name'] == 'added' for column in columns)
    assert [dict(row) for row in database.query(f'SELECT id, amount FROM {schema}.items')] == [
        {'id': 1.25, 'amount': Decimal('2.50')},
    ]


@pytest.mark.parametrize(('legacy_type', 'first', 'second'), [
    ('double precision', '9007199254740992', '9007199254740993'),
    ('real', '16777216', '16777217'),
    ('double precision', '1e999', '2e999'),
    ('real', '1e999', '2e999'),
    ('double precision', '0', '-1e-999'),
    ('real', '0', '-1e-999'),
])
def test_retained_float_composite_keys_coalesce_before_staging(
        postgres_decimal_target, legacy_type, first, second):
    config, database = postgres_decimal_target
    schema = config['default_target_schema']
    extra_properties = {'tenant': {'type': ['integer']}}
    load(config, decimal_schema(10, 2), [{
        'tenant': 1, 'id': float(decimal_float_value(first, legacy_type)), 'amount': '1.00',
    }], id_schema={'type': ['number']}, extra_properties=extra_properties, key_properties=('tenant', 'id'))
    if legacy_type == 'real':
        database.query(f'ALTER TABLE {schema}.items ALTER COLUMN id TYPE REAL')

    records = [
        {'tenant': tenant, 'id': value, 'amount': amount}
        for tenant in (1, 2)
        for value, amount in ((first, '2.00'), (second, '3.00'))
    ]
    load(config, decimal_schema(10, 2), records, id_schema=decimal_schema(None, None),
         extra_properties=extra_properties, key_properties=('tenant', 'id'))

    rows = database.query(f'SELECT tenant, amount FROM {schema}.items ORDER BY tenant')
    assert [dict(row) for row in rows] == [
        {'tenant': 1, 'amount': Decimal('3.00')}, {'tenant': 2, 'amount': Decimal('3.00')},
    ]


@pytest.mark.parametrize(('legacy_type', 'first', 'second'), [
    ('double precision', '9007199254740992', '9007199254740993'),
    ('real', '16777216', '16777217'),
])
def test_retained_float_key_keeps_last_delete_and_last_insert(
        postgres_decimal_target, legacy_type, first, second):
    config, database = postgres_decimal_target
    schema = config['default_target_schema']
    load(config, decimal_schema(10, 2), [{'id': float(first), 'amount': '1.00'}],
         id_schema={'type': ['number']})
    if legacy_type == 'real':
        database.query(f'ALTER TABLE {schema}.items ALTER COLUMN id TYPE REAL')
    deleted = {'id': first, 'amount': '2.00', '_sdc_deleted_at': '2026-10-05T00:00:00Z'}
    inserted = {'id': second, 'amount': '3.00'}

    load(config, decimal_schema(10, 2), [inserted, deleted], id_schema=decimal_schema(None, None))
    assert database.query(f'SELECT id FROM {schema}.items') == []

    load(config, decimal_schema(10, 2), [deleted, inserted], id_schema=decimal_schema(None, None))
    assert [dict(row) for row in database.query(f'SELECT amount FROM {schema}.items')] == [
        {'amount': Decimal('3.00')},
    ]


@pytest.mark.parametrize('value', [
    '1.000000059604644775390624999999999999',
    '1.000000059604644775390625',
    '1.000000059604644775390625000000000001',
    '1.000000178813934326171875',
])
def test_real_decimal_conversion_matches_postgres_at_midpoints(postgres_decimal_target, value):
    _, database = postgres_decimal_target
    stored = database.query('SELECT CAST(CAST(%s AS REAL) AS DOUBLE PRECISION) AS value', (value,))[0]['value']
    assert float(decimal_float_value(value, 'real')) == stored


@pytest.mark.parametrize('added_key_schema', [
    {'type': ['integer'], 'format': 'singer.year', 'minimum': 0, 'maximum': 2155},
    {'type': ['string']},
    {'type': ['string'], 'format': 'binary'},
])
def test_mysql_expanded_key_updates_and_deletes_legacy_rows_across_restarts(
        postgres_decimal_target, added_key_schema):
    config, database = postgres_decimal_target
    config['source_tap_type'] = 'tap-mysql'
    schema = config['default_target_schema']
    load(config, decimal_schema(10, 2), [{'id': 1, 'amount': '1.00'}, {'id': 2, 'amount': '2.00'}])
    extra_properties = {'new_key': added_key_schema}
    value = 2026 if 'integer' in added_key_schema['type'] else 'abcd'

    load(config, decimal_schema(10, 2), [{'id': 1, 'new_key': value, 'amount': '3.00'}],
         extra_properties=extra_properties, key_properties=('id', 'new_key'))
    rows = database.query(f'SELECT id, amount FROM {schema}.items ORDER BY id')
    assert [dict(row) for row in rows] == [
        {'id': Decimal(1), 'amount': Decimal('3.00')}, {'id': Decimal(2), 'amount': Decimal('2.00')},
    ]

    # A fresh Singer process must retain the old key even after the new column was added.
    load(config, decimal_schema(10, 2), [
        {'id': 1, 'new_key': value, 'amount': '4.00'},
        {'id': 2, 'new_key': value, 'amount': '5.00', '_sdc_deleted_at': '2026-10-05T00:00:00Z'},
    ], extra_properties=extra_properties, key_properties=('id', 'new_key'))
    assert [dict(row) for row in database.query(f'SELECT id, amount FROM {schema}.items')] == [
        {'id': Decimal(1), 'amount': Decimal('4.00')},
    ]


def test_mysql_binary_key_updates_and_deletes_legacy_fastsync_hex(postgres_decimal_target):
    config, database = postgres_decimal_target
    config['source_tap_type'] = 'tap-mysql'
    schema = config['default_target_schema']
    load(config, decimal_schema(10, 2), [{'id': '00FF', 'amount': '1.00'}, {'id': 'ABCD', 'amount': '2.00'}],
         id_schema={'type': ['string']})
    binary_schema = {'type': ['string'], 'format': 'binary'}
    load(config, decimal_schema(10, 2), [{'id': '00ff', 'amount': '3.00'}], id_schema=binary_schema)
    assert [dict(row) for row in database.query(f'SELECT id, amount FROM {schema}.items ORDER BY id')] == [
        {'id': '00FF', 'amount': Decimal('3.00')}, {'id': 'ABCD', 'amount': Decimal('2.00')},
    ]
    load(config, decimal_schema(10, 2), [{
        'id': 'abcd', 'amount': '2.00', '_sdc_deleted_at': '2026-10-05T00:00:00Z',
    }], id_schema=binary_schema)
    assert [dict(row) for row in database.query(f'SELECT id FROM {schema}.items')] == [{'id': '00FF'}]


def test_mysql_legacy_varchar_year_key_keeps_live_identity_for_updates_and_deletes(postgres_decimal_target):
    config, database = postgres_decimal_target
    config['source_tap_type'] = 'tap-mysql'
    schema = config['default_target_schema']
    load(config, decimal_schema(10, 2), [
        {'id': 1, 'year': '2024', 'amount': '1.00'}, {'id': 2, 'year': '2024', 'amount': '2.00'},
    ], extra_properties={'year': {'type': ['string']}}, key_properties=('id', 'year'))
    year_schema = {'type': ['integer'], 'format': 'singer.year', 'minimum': 0, 'maximum': 2155}

    load(config, decimal_schema(10, 2), [
        {'id': 1, 'year': 2024, 'amount': '3.00'},
        {'id': 2, 'year': 2024, 'amount': '2.00', '_sdc_deleted_at': '2026-10-05T00:00:00Z'},
    ], extra_properties={'year': year_schema}, key_properties=('id', 'year'))
    assert [dict(row) for row in database.query(f'SELECT id, year, amount FROM {schema}.items')] == [
        {'id': Decimal(1), 'year': '2024', 'amount': Decimal('3.00')},
    ]
    columns = database.query(
        'SELECT column_name, data_type FROM information_schema.columns '
        'WHERE table_schema = %s AND table_name = %s', (schema, 'items'),
    )
    assert dict(next(column for column in columns if column['column_name'] == 'year')) == {
        'column_name': 'year', 'data_type': 'character varying',
    }
    assert not any(column['column_name'].startswith('year_') for column in columns)


def test_mysql_set_composite_keys_keep_distinct_csv_values(postgres_decimal_target):
    config, database = postgres_decimal_target
    config['source_tap_type'] = 'tap-mysql'
    schema = config['default_target_schema']
    load(config, decimal_schema(10, 2), [
        {'id': 'a,b', 'second': 'c', 'amount': '1.00'},
        {'id': 'a', 'second': 'b,c', 'amount': '2.00'},
    ], id_schema={'type': ['string']}, extra_properties={'second': {'type': ['string']}},
         key_properties=('id', 'second'))
    assert [dict(row) for row in database.query(f'SELECT id, second FROM {schema}.items ORDER BY id')] == [
        {'id': 'a', 'second': 'b,c'}, {'id': 'a,b', 'second': 'c'},
    ]


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


def test_float_tokens_load_nonfinite_values_into_existing_double_precision_key(postgres_decimal_target):
    config, database = postgres_decimal_target
    schema = config['default_target_schema']
    token_schema, key_schema = {'type': ['null', 'number', 'string']}, {'type': ['number', 'string']}
    load(config, {'type': ['null', 'number']}, [{'id': 0.5, 'amount': 0.5}], id_schema={'type': ['number']})
    load(config, token_schema, [
        {'id': 'NaN', 'amount': 'NaN'}, {'id': 'Infinity', 'amount': 'Infinity'},
        {'id': '-Infinity', 'amount': '-Infinity'}, {'id': 0.5, 'amount': 1.5}, {'id': 2.5, 'amount': None},
    ], id_schema=key_schema)
    load(config, token_schema, [{'id': 'NaN', 'amount': -0.0}], id_schema=key_schema)

    rows = [
        tuple('NaN' if isinstance(value, float) and math.isnan(value) else value for value in row)
        for row in database.query(f'SELECT id, amount FROM {schema}.items ORDER BY id')
    ]
    assert rows == [
        (-math.inf, -math.inf), (0.5, 1.5), (2.5, None), (math.inf, math.inf), ('NaN', 0.0),
    ]
    assert math.copysign(1.0, rows[-1][1]) == -1.0
    columns = database.query(
        'SELECT column_name, data_type FROM information_schema.columns '
        'WHERE table_schema = %s AND table_name = %s', (schema, 'items'),
    )
    types = {column['column_name']: column['data_type'] for column in columns}
    assert (types['id'], types['amount']) == ('double precision', 'double precision')
    assert not any(name.startswith(('id_', 'amount_')) for name in types)


NUMERIC_ARRAY_ITEMS = {'$ref': '#/definitions/sdc_recursive_decimal_100_38_array'}
ARRAY_DEFINITIONS = {
    'sdc_recursive_decimal_100_38_array': {
        'type': ['null', 'number', 'array'], 'multipleOf': 1e-38, 'exclusiveMaximum': True, 'maximum': 10 ** 62,
        'exclusiveMinimum': True, 'minimum': -10 ** 62, 'items': NUMERIC_ARRAY_ITEMS,
    },
    'sdc_recursive_number_array': {
        'type': ['null', 'number', 'string', 'array'], 'items': {'$ref': '#/definitions/sdc_recursive_number_array'},
    },
}
BEYOND_DOUBLE = '1' + '0' * 400
NUMERIC_ARRAY_TEXTS = {
    1: '[1,1.10,12345678901234567890.123,0.00000012,"NaN","Infinity","-Infinity",null]',
    2: '[[1,2],[3,4]]',
    3: '[5,6]',
    4: '[]',
    5: None,
    6: '[123456789012345678901234567890123456789.5,123456789012345678901234567890123456789]',
    7: f'[{BEYOND_DOUBLE},-{BEYOND_DOUBLE},0.{"0" * 399}1]',
}


@pytest.mark.parametrize('key_properties', [('id',), ()])
def test_numeric_array_json_text_loads_into_jsonb_exactly(postgres_decimal_target, key_properties):
    config, database = postgres_decimal_target
    config['primary_key_required'] = bool(key_properties)
    schema = config['default_target_schema']
    extra = {'floats': {'type': ['null', 'array'], 'items': {'$ref': '#/definitions/sdc_recursive_number_array'}}}
    # The unmarked schema created existing tables; the JSON text schema must reuse their jsonb column.
    load(config, {'type': ['null', 'array'], 'items': NUMERIC_ARRAY_ITEMS}, [{'id': 0, 'amount': [], 'floats': [1.5]}],
         extra_properties=extra, key_properties=key_properties, definitions=ARRAY_DEFINITIONS)
    floats = {1: [1.5, None], 2: []}
    load(config, {'type': ['null', 'string', 'array'], 'format': 'singer.json', 'items': NUMERIC_ARRAY_ITEMS}, [
        {'id': row_id, 'amount': text, 'floats': floats.get(row_id)} for row_id, text in NUMERIC_ARRAY_TEXTS.items()
    ], extra_properties=extra, key_properties=key_properties, definitions=ARRAY_DEFINITIONS)

    texts = ', '.join(['(%s, %s)'] * len(NUMERIC_ARRAY_TEXTS))
    rows = database.query(
        'SELECT t.id, t.amount::text AS loaded, t.amount::text = v.text::jsonb::text AS same, '
        't.floats::text AS floats '
        f'FROM {schema}.items t LEFT JOIN (VALUES {texts}) AS v(id, text) ON t.id = v.id ORDER BY t.id',
        [value for row_id, text in NUMERIC_ARRAY_TEXTS.items() for value in (row_id, text)],
    )

    # jsonb keeps every digit and display scale; amount::text is NULL only for SQL NULL, never for '[]'.
    assert [tuple(row) for row in rows] == [
        (0, '[]', None, '[1.5]'),
        (1, '[1, 1.10, 12345678901234567890.123, 0.00000012, "NaN", "Infinity", "-Infinity", null]', True,
         '[1.5, null]'),
        (2, '[[1, 2], [3, 4]]', True, '[]'),
        (3, '[5, 6]', True, None),
        (4, '[]', True, None),
        (5, None, None, None),
        (6, '[123456789012345678901234567890123456789.5, 123456789012345678901234567890123456789]', True, None),
        (7, f'[{BEYOND_DOUBLE}, -{BEYOND_DOUBLE}, 0.{"0" * 399}1]', True, None),
    ]
    columns = database.query(
        'SELECT column_name, data_type FROM information_schema.columns '
        'WHERE table_schema = %s AND table_name = %s', (schema, 'items'),
    )
    types = {column['column_name']: column['data_type'] for column in columns}
    assert (types['amount'], types['floats']) == ('jsonb', 'jsonb')
    assert not any(name.startswith(('amount_', 'floats_')) for name in types)
