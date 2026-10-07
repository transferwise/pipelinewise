"""Exercise target startup, schema guards, and acknowledgements against Snowflake."""

import io
import json
import sys
from decimal import Decimal

import pytest
from snowflake.connector.errors import ProgrammingError

import target_snowflake
from singer.decimal_support import decimal_schema
from target_snowflake.exceptions import TableFormatDiscoveryException, TableFormatMismatchException
from tests.integration.test_decimals import load, snowflake_decimal_target as snowflake_decimal_target


def run_cli(config, messages, tmp_path, monkeypatch):
    """Use the real CLI parser and UTF-8 stdin without spawning an unmeasured process."""
    config_path = tmp_path / 'config.json'
    config_path.write_text(json.dumps(config))
    payload = '\n'.join(json.dumps(message, ensure_ascii=False) for message in messages).encode('utf-8')
    monkeypatch.setattr(sys, 'argv', ['target-snowflake', '--config', str(config_path)])
    monkeypatch.setattr(sys, 'stdin', io.TextIOWrapper(io.BytesIO(payload), encoding='utf-8'))
    target_snowflake.main()


def schema_message(amount_schema, keys=('id',), extra_properties=None):
    """Build the schema shared by the runtime contract tests."""
    return {
        'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': list(keys),
        'schema': {'type': 'object', 'properties': {
            'id': {'type': ['integer']}, 'amount': amount_schema, **(extra_properties or {}),
        }},
    }


def test_cli_loads_utf8_and_acknowledges_committed_state(snowflake_decimal_target, tmp_path, monkeypatch, capsys):
    config, database = snowflake_decimal_target
    state = {'bookmarks': {'public-items': {'last_id': 1}}}
    run_cli(config, [
        schema_message(decimal_schema(10, 2), extra_properties={'label': {'type': ['string']}}),
        {'type': 'RECORD', 'stream': 'public-items', 'record': {'id': 1, 'amount': '1.25', 'label': 'Καλημέρα 日本語'}},
        {'type': 'ACTIVATE_VERSION'},
        {'type': 'STATE', 'value': state},
    ], tmp_path, monkeypatch)
    assert [json.loads(line) for line in capsys.readouterr().out.splitlines()] == [state]
    assert database.query(f'SELECT ID, AMOUNT, LABEL FROM "{config["default_target_schema"]}".ITEMS') == [
        {'ID': 1, 'AMOUNT': Decimal('1.25'), 'LABEL': 'Καλημέρα 日本語'},
    ]


@pytest.mark.parametrize('snowflake_decimal_target', ['native'], indirect=True)
@pytest.mark.parametrize('format_kind', ['missing', 'json', 'incompatible_csv', 'invalid_identifier'])
def test_cli_rejects_invalid_file_format_before_loading(
    snowflake_decimal_target, format_kind, tmp_path, monkeypatch, capsys,
):
    config, database = snowflake_decimal_target
    schema = config['default_target_schema']
    database.query(f'CREATE SCHEMA "{schema}"')
    config['file_format'] = f'"{schema}"."TEST_FORMAT"'
    if format_kind == 'json':
        database.query(f'CREATE FILE FORMAT {config["file_format"]} TYPE=JSON')
    elif format_kind == 'incompatible_csv':
        database.query(f'CREATE FILE FORMAT {config["file_format"]} TYPE=CSV')
    elif format_kind == 'invalid_identifier':
        config['file_format'] = 'invalid;identifier'
    with pytest.raises(SystemExit) as error:
        run_cli(config, [schema_message(decimal_schema(10, 2))], tmp_path, monkeypatch)
    assert error.value.code == 1
    assert not capsys.readouterr().out
    assert database.get_tables([schema]) == []


def test_rejects_table_format_change_without_altering_rows(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    load(config, decimal_schema(10, 2), [{'id': 1, 'amount': '1.25'}])
    changed_config = dict(config)
    if config['target_table_format'] == 'native':
        changed_config.update(target_table_format='iceberg', iceberg_version=3)
    else:
        changed_config['target_table_format'] = 'native'
        changed_config.pop('iceberg_version')
    with pytest.raises(TableFormatMismatchException, match='requires'):
        load(changed_config, decimal_schema(10, 2), [{'id': 2, 'amount': '2.25'}])
    assert database.query(f'SELECT ID, AMOUNT FROM "{config["default_target_schema"]}".ITEMS') == [
        {'ID': 1, 'AMOUNT': Decimal('1.25')},
    ]


@pytest.mark.parametrize('snowflake_decimal_target', ['iceberg'], indirect=True)
def test_rejects_managed_v2_before_schema_changes(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    table = f'"{config["default_target_schema"]}".ITEMS'
    database.query(f'CREATE SCHEMA "{config["default_target_schema"]}"')
    database.query(
        f'CREATE ICEBERG TABLE {table} (ID NUMBER(19,0), AMOUNT NUMBER(10,2)) '
        "CATALOG='SNOWFLAKE' ICEBERG_VERSION=2"
    )
    database.query(f'INSERT INTO {table} VALUES (1, 1.25)')
    with pytest.raises(TableFormatDiscoveryException, match='unsupported ICEBERG_VERSION 2'):
        load(config, decimal_schema(10, 2), extra_properties={'added': {'type': ['string']}})
    assert database.query(f'SELECT * FROM {table}') == [{'ID': 1, 'AMOUNT': Decimal('1.25')}]


@pytest.mark.parametrize('snowflake_decimal_target', ['iceberg'], indirect=True)
def test_rejects_managed_merge_on_read_before_schema_changes(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    load(config, decimal_schema(10, 2), [{'id': 1, 'amount': '1.25'}])
    table = f'"{config["default_target_schema"]}".ITEMS'
    database.query(f"ALTER ICEBERG TABLE {table} SET ICEBERG_MERGE_ON_READ_BEHAVIOR='ENABLED'")
    with pytest.raises(TableFormatDiscoveryException, match='PipelineWise requires'):
        load(config, decimal_schema(10, 2), extra_properties={'added': {'type': ['string']}})
    assert database.query(f'SELECT ID, AMOUNT FROM {table}') == [{'ID': 1, 'AMOUNT': Decimal('1.25')}]
    columns = database.get_table_columns([config['default_target_schema']])
    assert not any(column['COLUMN_NAME'] == 'ADDED' for column in columns)


@pytest.mark.parametrize('snowflake_decimal_target', ['iceberg'], indirect=True)
@pytest.mark.parametrize('starts_as_variant', [False, True])
def test_rejects_managed_text_variant_changes_without_versioning(snowflake_decimal_target, starts_as_variant):
    config, database = snowflake_decimal_target
    variant, text = {'type': ['object']}, {'type': ['string']}
    load(config, variant if starts_as_variant else text, [
        {'id': 1, 'amount': {'kept': True} if starts_as_variant else 'original'},
    ])
    columns = database.get_table_columns([config['default_target_schema']])
    with pytest.raises(TableFormatMismatchException, match='migrate the column explicitly'):
        load(config, text if starts_as_variant else variant)
    assert database.get_table_columns([config['default_target_schema']]) == columns
    value = database.query(f'SELECT AMOUNT FROM "{config["default_target_schema"]}".ITEMS')[0]['AMOUNT']
    assert (json.loads(value) if starts_as_variant else value) == ({'kept': True} if starts_as_variant else 'original')


@pytest.mark.parametrize('has_key', [False, True])
def test_failed_copy_or_merge_does_not_acknowledge_state(snowflake_decimal_target, has_key, capsys):
    config, database = snowflake_decimal_target
    config.update(primary_key_required=has_key, validate_records=False)
    keys = ('id',) if has_key else ()
    amount_schema = {'type': ['null', 'number']}
    load(config, amount_schema, [{'id': 1, 'amount': 1.25}], key_properties=keys)
    capsys.readouterr()
    messages = [
        schema_message(amount_schema, keys),
        {'type': 'RECORD', 'stream': 'public-items', 'record': {'id': 2, 'amount': 'not-a-number'}},
        {'type': 'STATE', 'value': {'bookmarks': {'public-items': {'last_id': 2}}}},
    ]
    cache, file_format = target_snowflake.get_snowflake_statics(config)
    with pytest.raises(ProgrammingError):
        target_snowflake.persist_lines(config, map(json.dumps, messages), cache, file_format)
    assert not capsys.readouterr().out
    assert database.query(f'SELECT ID, AMOUNT FROM "{config["default_target_schema"]}".ITEMS') == [
        {'ID': 1, 'AMOUNT': 1.25},
    ]


def test_sparse_patch_batches_preserve_omitted_values_and_apply_null(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    extra = {'label': {'type': ['null', 'string']}}
    load(config, decimal_schema(10, 2), [
        {'id': 1, 'amount': '1.25', 'label': 'first'}, {'id': 2, 'amount': '2.25', 'label': 'second'},
    ], extra_properties=extra)
    load(config, decimal_schema(10, 2), [
        {'id': 1, 'amount': '3.25'}, {'id': 2, 'label': None},
    ], extra_properties=extra, record_update_mode='PATCH')
    assert database.query(f'SELECT ID, AMOUNT, LABEL FROM "{config["default_target_schema"]}".ITEMS ORDER BY ID') == [
        {'ID': 1, 'AMOUNT': Decimal('3.25'), 'LABEL': 'first'},
        {'ID': 2, 'AMOUNT': Decimal('2.25'), 'LABEL': None},
    ]


def test_polymorphic_and_untyped_source_fields_load_after_flattening(snowflake_decimal_target):
    config, database = snowflake_decimal_target
    config['data_flattening_max_level'] = 1
    long_name = 'long_source_column_' * 12
    extra = {
        'nested': {'anyOf': [{'type': 'integer'}, {'type': 'object', 'properties': {'label': {'type': 'string'}}}]},
        'array_value': {'anyOf': [{'type': 'array'}]},
        'untyped': {},
        'legacy_string': {'oneOf': [{'type': 'string'}]},
        'fallback': {'oneOf': [{'type': 'integer'}]},
        'mixed_integer_string': {'type': ['integer', 'string']},
        long_name: {'type': 'object', 'properties': {long_name: {'type': 'string'}}},
    }
    load(config, decimal_schema(10, 2), [{
        'id': 1, 'amount': '1.25', 'nested': {'label': 'nested text'}, 'array_value': [1, 'two'],
        'untyped': 'no declaration', 'legacy_string': 'legacy', 'fallback': 42,
        'mixed_integer_string': 123, long_name: {long_name: 'long column value'},
    }], extra_properties=extra)
    row = database.query(f'SELECT * FROM "{config["default_target_schema"]}".ITEMS')[0]
    assert row['NESTED__LABEL'] == 'nested text'
    assert json.loads(row['ARRAY_VALUE']) == [1, 'two']
    assert row['UNTYPED'] == 'no declaration'
    assert row['LEGACY_STRING'] == 'legacy'
    assert row['FALLBACK'] == '42'
    assert row['MIXED_INTEGER_STRING'] == '123'
    assert 'long column value' in row.values()
    assert all(len(name) <= 255 for name in row)
