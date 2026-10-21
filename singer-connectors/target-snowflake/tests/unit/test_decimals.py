"""Exact SQL decimal transport and column evolution."""

import json
import re
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

import target_snowflake
from target_snowflake.db_sync import DbSync, validate_config
from target_snowflake.exceptions import TableFormatDiscoveryException
from target_snowflake.file_formats.csv import create_copy_sql, create_merge_sql, record_to_csv_line
from target_snowflake.managed_iceberg import column_type, plan_column_changes


def decimal_schema(precision=29, scale=9):
    return {
        'type': ['null', 'string'],
        'format': 'singer.decimal',
        'decimal': {'precision': precision, 'scale': scale},
    }


def binary_schema():
    return {'type': ['null', 'string'], 'format': 'binary'}


def year_schema():
    return {'type': ['null', 'integer'], 'format': 'singer.year'}


@pytest.mark.parametrize('value', [False, True])
def test_decimal_float_versioning_config_accepts_exact_booleans(value):
    assert not any(
        'version_legacy_float_columns' in error
        for error in validate_config({'version_legacy_float_columns': value})
    )


@pytest.mark.parametrize('value', [None, 0, 1, 'false'])
def test_decimal_float_versioning_config_rejects_non_booleans(value):
    assert 'version_legacy_float_columns must be true or false' in validate_config({
        'version_legacy_float_columns': value,
    })


@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_decimal_mapping_and_unmarked_api_fields(iceberg_version):
    arguments = {'is_iceberg_table': iceberg_version == 3, 'iceberg_version': iceberg_version}
    assert column_type(decimal_schema(), **arguments) == 'NUMERIC(29,9)'
    assert column_type({'type': ['number']}, **arguments) == ('double' if iceberg_version else 'float')
    assert column_type({'type': ['string'], 'format': 'singer.decimal'}, **arguments) == 'varchar(134217728)'


@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_binary_mapping_uses_snowflake_maximum_width(iceberg_version):
    arguments = {'is_iceberg_table': iceberg_version == 3, 'iceberg_version': iceberg_version}

    assert column_type(binary_schema(), **arguments).upper() == 'BINARY(67108864)'


@pytest.mark.parametrize(('precision', 'scale', 'expected'), [
    (None, None, 'FLOAT'), (39, 2, 'FLOAT'), (10, -1, 'NUMERIC(11,0)'), (2, 3, 'NUMERIC(3,3)'),
])
@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_decimal_mapping_falls_back_to_supported_domain(precision, scale, expected, iceberg_version):
    if iceberg_version and expected == 'FLOAT':
        expected = 'DOUBLE'
    assert column_type(decimal_schema(precision, scale), iceberg_version == 3, iceberg_version) == expected


@pytest.mark.parametrize('iceberg_version', [None, 3])
@pytest.mark.parametrize('existing', ['NUMBER(29,8)', 'NUMBER(28,9)', 'NUMBER(30,9)'])
def test_decimal_type_or_dimension_change_versions_column(existing, iceberg_version):
    plan = plan_column_changes(
        {'amount': decimal_schema()}, {'AMOUNT': existing}, iceberg_version == 3, iceberg_version,
    )
    assert plan.additions == ()
    assert plan.replacements == (('"AMOUNT"', '"AMOUNT" NUMERIC(29,9)'),)


@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_legacy_decimal_float_requires_explicit_versioning(iceberg_version):
    arguments = {'is_iceberg_table': iceberg_version == 3, 'iceberg_version': iceberg_version}
    existing = 'DOUBLE' if iceberg_version else 'FLOAT'

    assert plan_column_changes(
        {'amount': decimal_schema()}, {'AMOUNT': existing}, **arguments,
    ).replacements == ()
    assert plan_column_changes(
        {'amount': decimal_schema()}, {'AMOUNT': existing}, **arguments,
        version_legacy_float_columns=True,
    ).replacements == (('"AMOUNT"', '"AMOUNT" NUMERIC(29,9)'),)


@pytest.mark.parametrize('iceberg_version', [None, 3])
@pytest.mark.parametrize('version_legacy_float_columns', [False, True])
def test_existing_float_postgres_decimal_key_is_always_retained(iceberg_version, version_legacy_float_columns):
    arguments = {'is_iceberg_table': iceberg_version == 3, 'iceberg_version': iceberg_version}
    existing = 'DOUBLE' if iceberg_version else 'FLOAT'
    plan = plan_column_changes(
        {'id': decimal_schema(10, 2)},
        {'ID': existing},
        **arguments,
        key_properties=['id'],
        source='postgres',
        version_legacy_float_columns=version_legacy_float_columns,
    )

    assert plan.replacements == ()
    assert plan.retained_decimal_types == (('ID', 'FLOAT'),)


@pytest.mark.parametrize('iceberg_version', [None, 3])
@pytest.mark.parametrize('version_legacy_float_columns', [False, True])
def test_existing_float_postgres_decimal_key_uses_float_load_projection(
    iceberg_version, version_legacy_float_columns,
):
    sync = object.__new__(DbSync)
    sync.connection_config = {
        'source_tap_type': 'tap-postgres',
        'version_legacy_float_columns': version_legacy_float_columns,
    }
    sync.stream_schema_message = {'stream': 'public-items', 'key_properties': ['id']}
    sync.flatten_schema = {'id': decimal_schema(10, 2)}
    sync.schema_name = 'PUBLIC'
    sync.table_cache = [{
        'SCHEMA_NAME': 'PUBLIC',
        'TABLE_NAME': 'ITEMS',
        'COLUMN_NAME': 'ID',
        'DATA_TYPE': 'DOUBLE' if iceberg_version else 'FLOAT',
    }]
    sync.query = Mock()
    sync.get_table_columns = Mock(return_value=sync.table_cache)
    sync.add_column = Mock()
    sync.version_column = Mock()

    sync.update_columns(iceberg_version == 3, iceberg_version)

    assert sync.decimal_load_type('id', sync.flatten_schema['id']) == 'FLOAT'
    sync.query.assert_not_called()
    sync.add_column.assert_not_called()
    sync.version_column.assert_not_called()


@pytest.mark.parametrize('existing', ['NUMBER(29,9)', 'NUMERIC(29, 9)', 'DECIMAL(29,9)', 'FIXED(29,9)'])
def test_decimal_aliases_do_not_repeat_versioning(existing):
    plan = plan_column_changes({'amount': decimal_schema()}, {'AMOUNT': existing})
    assert plan.additions == plan.replacements == ()


@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_year_number_alias_does_not_repeat_versioning(iceberg_version):
    plan = plan_column_changes(
        {'calendar_year': year_schema()},
        {'CALENDAR_YEAR': 'NUMBER(38,0)'},
        is_iceberg_table=iceberg_version == 3,
        iceberg_version=iceberg_version,
    )

    assert plan.additions == plan.replacements == ()


def test_decimal_metadata_must_include_dimensions():
    with pytest.raises(TableFormatDiscoveryException, match='precision and scale'):
        plan_column_changes({'amount': decimal_schema()}, {'AMOUNT': 'NUMBER'})


def test_every_existing_key_type_is_retained_instead_of_versioned():
    plan = plan_column_changes({'id': {'type': ['string']}}, {'ID': 'NUMBER'}, key_properties=['id'])
    assert plan.replacements == ()
    assert plan.retained_column_types == (('ID', 'NUMBER'),)


@pytest.mark.parametrize(('schema', 'existing'), [
    (year_schema(), 'TEXT'),
    (binary_schema(), 'TEXT'),
    (decimal_schema(10, 2), 'NUMBER(10,2)'),
])
def test_newly_supported_existing_key_columns_keep_their_target_type(schema, existing):
    plan = plan_column_changes(
        {'id': schema},
        {'ID': existing},
        key_properties=['id'],
        source='postgres' if schema.get('format') == 'singer.decimal' else None,
    )

    assert plan.additions == plan.replacements == ()
    assert plan.retained_column_types == (('ID', existing),)


@pytest.mark.parametrize(('schema', 'existing'), [
    ({'type': ['null', 'object']}, 'TEXT(134217728)'),
    (decimal_schema(10, 2), 'NUMBER'),
])
def test_iceberg_keys_are_retained_when_type_comparison_would_fail(schema, existing):
    plan = plan_column_changes(
        {'id': schema},
        {'ID': existing},
        is_iceberg_table=True,
        iceberg_version=3,
        key_properties=['id'],
        source='postgres' if schema.get('format') == 'singer.decimal' else None,
    )

    assert plan.additions == plan.replacements == ()
    assert plan.retained_column_types == (('ID', existing),)


@pytest.mark.parametrize(('column', 'expected'), [
    ({'DATA_TYPE': 'TEXT', 'TYPE_LENGTH': 134217728}, 'TEXT(134217728)'),
    ({'DATA_TYPE': 'BINARY', 'TYPE_LENGTH': 67108864}, 'BINARY(67108864)'),
])
def test_existing_column_type_preserves_retained_key_width(column, expected):
    assert DbSync._existing_column_type(column) == expected


def test_table_column_discovery_reads_string_and_binary_length():
    sync = object.__new__(DbSync)
    sync.connection_config = {'dbname': 'DATABASE'}
    sync.query = Mock(return_value=[{'COLUMN_NAME': 'ID'}])

    assert sync.get_table_columns(['PUBLIC']) == [{'COLUMN_NAME': 'ID'}]

    queries = sync.query.call_args.args[0]
    assert 'PARSE_JSON("data_type"):length::integer AS type_length' in queries[1]


def test_retained_year_and_binary_keys_use_existing_text_type_in_merge_projection():
    sync = object.__new__(DbSync)
    sync.connection_config = {}
    sync.stream_schema_message = {
        'stream': 'public-items',
        'key_properties': ['id', 'calendar_year', 'payload'],
    }
    sync.flatten_schema = {
        'id': {'type': ['integer']},
        'calendar_year': year_schema(),
        'payload': binary_schema(),
    }
    retained_text_type = 'TEXT(134217728)'
    sync._retained_column_types = {
        'CALENDAR_YEAR': retained_text_type,
        'PAYLOAD': retained_text_type,
    }
    sync.schema_name = 'PUBLIC'
    sync.logger = Mock()
    sync._load_file_merge = Mock(return_value=(0, 0))

    sync.load_file('rows.csv', 1, 10)

    columns = sync._load_file_merge.call_args.kwargs['columns_with_trans']
    assert columns[1]['trans'] == columns[2]['trans'] == ''
    assert columns[1]['retained_type'] == columns[2]['retained_type'] == retained_text_type
    sql = create_merge_sql('ITEMS', 'STAGE', 'rows.csv', 'FORMAT', columns, 'condition')
    assert 'CAST(($2) AS TEXT(134217728)) "CALENDAR_YEAR"' in sql
    assert 'CAST(($3) AS TEXT(134217728)) "PAYLOAD"' in sql


@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_legacy_decimal_float_key_does_not_block_other_column_changes(iceberg_version):
    sync = object.__new__(DbSync)
    sync.connection_config = {'version_legacy_float_columns': True}
    sync.stream_schema_message = {'stream': 'public-items', 'key_properties': ['id']}
    sync.flatten_schema = {
        'new_column': {'type': ['string']},
        'id': decimal_schema(),
        'amount': decimal_schema(),
    }
    sync.schema_name = 'PUBLIC'
    sync.table_cache = [
        {'SCHEMA_NAME': 'PUBLIC', 'TABLE_NAME': 'ITEMS', 'COLUMN_NAME': 'ID', 'DATA_TYPE': 'FLOAT'},
        {'SCHEMA_NAME': 'PUBLIC', 'TABLE_NAME': 'ITEMS', 'COLUMN_NAME': 'AMOUNT', 'DATA_TYPE': 'FLOAT'},
    ]
    sync.query = Mock()
    sync.get_table_columns = Mock(return_value=sync.table_cache)
    sync.add_column = Mock()
    sync.version_column = Mock()

    sync.update_columns(iceberg_version == 3, iceberg_version)

    sync.query.assert_not_called()
    assert [entry.args for entry in sync.add_column.call_args_list] == [
        ('"NEW_COLUMN" varchar(134217728)', 'public-items', iceberg_version == 3),
        ('"AMOUNT" NUMERIC(29,9)', 'public-items', iceberg_version == 3),
    ]
    sync.version_column.assert_called_once()
    assert sync.version_column.call_args.args[:3] == (
        '"AMOUNT"', 'public-items', iceberg_version == 3,
    )
    assert sync._retained_decimal_types == {'ID': 'FLOAT'}


@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_reimport_of_matching_decimal_metadata_keeps_column(iceberg_version):
    sync = object.__new__(DbSync)
    sync.connection_config = {}
    sync.stream_schema_message = {'stream': 'public-items', 'key_properties': ['amount']}
    sync.flatten_schema = {'amount': decimal_schema()}
    sync.schema_name = 'PUBLIC'
    sync.table_cache = [{
        'SCHEMA_NAME': 'PUBLIC', 'TABLE_NAME': 'ITEMS', 'COLUMN_NAME': 'AMOUNT',
        'DATA_TYPE': 'NUMBER', 'NUMERIC_PRECISION': 29, 'NUMERIC_SCALE': 9,
    }]
    sync.query = Mock()
    sync.get_table_columns = Mock(return_value=sync.table_cache)

    sync.update_columns(iceberg_version == 3, iceberg_version)

    sync.query.assert_not_called()


def run_cached_schema_sequence(
    initial_type, schemas, iceberg_version, *, version_legacy_float_columns=False,
):
    """Keep the startup cache while applying DDL to a separate simulated table."""
    physical_columns = {'AMOUNT': initial_type}
    versioned_columns = []

    def current_columns(**_kwargs):
        columns = []
        for name, data_type in physical_columns.items():
            row = {'SCHEMA_NAME': 'PUBLIC', 'TABLE_NAME': 'ITEMS', 'COLUMN_NAME': name, 'DATA_TYPE': data_type}
            if match := re.fullmatch(r'(?:NUMBER|NUMERIC)\((\d+),(\d+)\)', data_type):
                row.update(DATA_TYPE='NUMBER', NUMERIC_PRECISION=int(match[1]), NUMERIC_SCALE=int(match[2]))
            columns.append(row)
        return columns

    def create_sync(config, message, cache, _file_format):
        sync = object.__new__(DbSync)
        sync.connection_config = config
        sync.stream_schema_message = message
        sync.flatten_schema = {
            name: schema for name, schema in message['schema']['properties'].items()
            if not name.startswith('_sdc_')
        }
        sync.schema_name = 'PUBLIC'
        sync.table_cache = cache
        sync.logger = Mock()
        sync.create_schema_if_not_exists = Mock()
        sync.get_table_columns = current_columns

        def version_column(column_name, *_args):
            name = column_name.strip('"')
            archived_name = f'{name}_ARCHIVE_{len(versioned_columns)}'
            physical_columns[archived_name] = physical_columns.pop(name)
            versioned_columns.append(archived_name)
            return archived_name

        def add_column(definition, *_args):
            name, data_type = definition.split(' ', 1)
            physical_columns[name.strip('"')] = data_type.upper()

        sync.version_column = version_column
        sync.add_column = add_column
        sync.sync_table = lambda: sync.update_columns(iceberg_version == 3, iceberg_version)
        return sync

    messages = [
        {'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': [], 'schema': {'properties': properties}}
        for properties in schemas
    ]
    startup_cache = current_columns()
    with patch('target_snowflake.DbSync', side_effect=create_sync):
        target_snowflake.persist_lines(
            {
                'primary_key_required': False,
                'version_legacy_float_columns': version_legacy_float_columns,
            },
            [json.dumps(message) for message in messages], startup_cache,
        )
    return physical_columns, versioned_columns


@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_persist_lines_versions_precision_reversal_with_startup_cache(iceberg_version):
    columns, archives = run_cached_schema_sequence('NUMBER(10,2)', [
        {'amount': decimal_schema(precision, 2)} for precision in (10, 11, 10)
    ], iceberg_version)
    assert columns['AMOUNT'] == 'NUMERIC(10,2)'
    assert [columns[name] for name in archives] == ['NUMBER(10,2)', 'NUMERIC(11,2)']


@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_persist_lines_unrelated_addition_does_not_reversion_decimal(iceberg_version):
    floating_type = 'DOUBLE' if iceberg_version else 'FLOAT'
    columns, archives = run_cached_schema_sequence(floating_type, [
        {'amount': decimal_schema(10, 2)},
        {'amount': decimal_schema(10, 2), 'added': {'type': ['string']}},
    ], iceberg_version)
    assert columns['AMOUNT'] == floating_type
    assert archives == []
    assert 'ADDED' in columns


@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_persist_lines_repeated_year_schema_does_not_version_column(iceberg_version):
    columns, archives = run_cached_schema_sequence('NUMBER(38,0)', [
        {'amount': year_schema()},
        {'amount': year_schema(), 'added': {'type': ['string']}},
    ], iceberg_version)

    assert columns['AMOUNT'] == 'NUMBER(38,0)'
    assert archives == []
    assert 'ADDED' in columns


@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_persist_lines_opt_in_versions_legacy_decimal_float_once(iceberg_version):
    floating_type = 'DOUBLE' if iceberg_version else 'FLOAT'
    columns, archives = run_cached_schema_sequence(floating_type, [
        {'amount': decimal_schema(10, 2)},
        {'amount': decimal_schema(10, 2), 'added': {'type': ['string']}},
    ], iceberg_version, version_legacy_float_columns=True)
    assert columns['AMOUNT'] == 'NUMERIC(10,2)'
    assert [columns[name] for name in archives] == [floating_type]
    assert 'ADDED' in columns


@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_persist_lines_decimal_then_float_does_not_reuse_pre_decimal_cache(iceberg_version):
    floating_type = 'DOUBLE' if iceberg_version else 'FLOAT'
    floating_schema = {'type': ['number']}
    columns, archives = run_cached_schema_sequence(floating_type, [
        {'amount': floating_schema},
        {'amount': decimal_schema(10, 2)},
        {'amount': floating_schema},
        {'amount': floating_schema, 'added': {'type': ['string']}},
    ], iceberg_version, version_legacy_float_columns=True)
    assert columns['AMOUNT'] == floating_type
    assert [columns[name] for name in archives] == [floating_type, 'NUMERIC(10,2)']


def test_decimal_evolution_cache_invalidation_keeps_unmarked_stream_cache():
    startup_cache = [{'SCHEMA_NAME': 'PUBLIC', 'TABLE_NAME': 'OTHER', 'COLUMN_NAME': 'ID', 'DATA_TYPE': 'NUMBER'}]
    messages = [
        {'type': 'SCHEMA', 'stream': stream, 'key_properties': ['id'], 'schema': {'properties': {'id': schema}}}
        for stream, schema in [('public-items', decimal_schema()), ('public-other', {'type': ['integer']})]
    ]
    with patch('target_snowflake.DbSync') as sync:
        target_snowflake.persist_lines({}, [json.dumps(message) for message in messages], startup_cache)
    assert sync.call_args_list[0].args[2] is None
    assert sync.call_args_list[1].args[2] is startup_cache


def test_versioned_name_avoids_collision_and_identifier_overflow():
    sync = object.__new__(DbSync)
    sync.schema_name = 'PUBLIC'
    sync.query = Mock()
    sync.logger = Mock()
    base = 'A' * 255
    existing = 'A' * 232 + '_20260929_100000_123456'
    with patch('target_snowflake.db_sync.datetime') as clock:
        clock.now.return_value = datetime(2026, 9, 29, 10, 0, 0, 123456, tzinfo=timezone.utc)
        archived = sync.version_column(f'"{base}"', 'public-items', True, {existing})

    assert archived == 'A' * 232 + '_20260929_100000_123457'
    assert len(archived) == 255
    assert f'TO "{archived}"' in sync.query.call_args.args[0]


def test_csv_preserves_large_decimal_and_sql_null():
    schema = {'amount': decimal_schema(38, 9)}
    assert record_to_csv_line({'amount': '12345678901234567890123456789.123456789'}, schema) == (
        '"12345678901234567890123456789.123456789"'
    )
    assert record_to_csv_line({'amount': None}, schema) == ''
    assert record_to_csv_line({'amount': '0.000000000'}, schema) == '"0.000000000"'


def test_decimal_key_identity_is_exact_and_independent_of_text_scale():
    sync = object.__new__(DbSync)
    sync.stream_schema_message = {'key_properties': ['id']}
    sync.flatten_schema = {'id': decimal_schema()}
    sync.data_flattening_max_level = 0
    assert sync.record_primary_key_string({'id': '1.0'}) == sync.record_primary_key_string({'id': '1.00'})
    assert sync.record_primary_key_string({'id': '9007199254740992.00'}) != (
        sync.record_primary_key_string({'id': '9007199254740992.01'})
    )


def test_decimal_merge_projection_casts_before_primary_key_comparison():
    sql = create_merge_sql('ITEMS', 'STAGE', 'records.csv', 'FORMAT', [
        {'name': '"ID"', 'trans': '', 'decimal_type': 'NUMERIC(29,9)', 'decimal_key': True},
        {'name': '"VALUE"', 'trans': ''},
    ], 's."ID" = t."ID"')
    assert 'SELECT CAST(($1) AS NUMERIC(29,9)) "ID", ($2) "VALUE"' in sql


@pytest.mark.parametrize('value', [1.25, '1.1234567891', '100000000000000000000'])
def test_invalid_decimal_record_rejected_even_without_generic_validation(value):
    messages = [
        {'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': ['id'],
         'schema': {'properties': {'id': {'type': ['integer']}, 'amount': decimal_schema()}}},
        {'type': 'RECORD', 'stream': 'public-items', 'record': {'id': 1, 'amount': value}},
    ]
    with patch('target_snowflake.DbSync'), patch('target_snowflake.flush_streams') as flush:
        with pytest.raises(ValueError):
            target_snowflake.persist_lines({'validate_records': False}, [json.dumps(message) for message in messages])
    flush.assert_not_called()


def test_exact_decimal_string_reaches_target_buffer():
    amount = '12345678901234567890.123456789'
    messages = [
        {'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': ['id'],
         'schema': {'properties': {'id': {'type': ['integer']}, 'amount': decimal_schema()}}},
        {'type': 'RECORD', 'stream': 'public-items', 'record': {'id': 1, 'amount': amount}},
    ]
    with patch('target_snowflake.DbSync') as sync, patch('target_snowflake.flush_streams') as flush:
        sync.return_value.record_primary_key_string.return_value = '1'
        flush.return_value = None
        target_snowflake.persist_lines({'validate_records': True}, [json.dumps(message) for message in messages])
    assert flush.call_args.args[0]['public-items']['1']['amount'] == amount


@pytest.mark.parametrize('values', [
    ['10', '2'], ['10.123456789012345678', '2.123456789012345678'],
])
@pytest.mark.parametrize('marked', [True, False])
def test_archive_bookmark_extrema_use_decimal_order_and_keep_exact_text(values, marked):
    schema = decimal_schema(38, 18) if marked else {'type': ['string']}
    messages = [
        {'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': ['id'], 'bookmark_properties': ['amount'],
         'schema': {'properties': {'id': {'type': ['integer']}, 'amount': schema}}},
        *({'type': 'RECORD', 'stream': 'public-items', 'record': {'id': index, 'amount': value}}
          for index, value in enumerate(values)),
    ]
    with patch('target_snowflake.DbSync') as sync, patch('target_snowflake.flush_streams') as flush:
        sync.return_value.record_primary_key_string.side_effect = lambda record: str(record['id'])
        flush.return_value = None
        target_snowflake.persist_lines({'archive_load_files': True}, [json.dumps(message) for message in messages])
    metadata = flush.call_args.args[6]['public-items']
    assert (metadata['min'], metadata['max']) == tuple(reversed(values) if marked else values)


@pytest.mark.parametrize(('values', 'expected'), [
    (['2', None, '10'], ('2', '10')),
    ([None, '2.000000000000000001', '10.000000000000000002', None],
     ('2.000000000000000001', '10.000000000000000002')),
    ([None, None], (None, None)),
    (['NaN', '2', 'NaN', '-1'], ('-1', 'NaN')),
])
def test_archive_decimal_bookmark_extrema_ignore_nulls(values, expected):
    messages = [
        {'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': ['id'], 'bookmark_properties': ['amount'],
         'schema': {'properties': {'id': {'type': ['integer']}, 'amount': decimal_schema(38, 18)}}},
        *({'type': 'RECORD', 'stream': 'public-items', 'record': {'id': index, 'amount': value}}
          for index, value in enumerate(values)),
    ]
    with patch('target_snowflake.DbSync') as sync, patch('target_snowflake.flush_streams') as flush:
        sync.return_value.record_primary_key_string.side_effect = lambda record: str(record['id'])
        flush.return_value = None
        target_snowflake.persist_lines({'archive_load_files': True}, [json.dumps(message) for message in messages])
    metadata = flush.call_args.args[6]['public-items']
    assert (metadata['min'], metadata['max']) == expected
    assert len(flush.call_args.args[0]['public-items']) == len(values)


@pytest.mark.parametrize('nested_schema', [
    {'type': ['object'], 'properties': {'amount': decimal_schema()}},
    {'type': ['array'], 'items': decimal_schema()},
])
def test_decimal_validation_cache_tracks_nested_fields_and_schema_removal(nested_schema):
    messages = []
    for schema in ({'type': ['string']}, nested_schema, {'type': ['string']}):
        messages.extend([
            {'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': ['id'],
             'schema': {'properties': {'id': {'type': ['integer']}, 'value': schema}}},
            {'type': 'RECORD', 'stream': 'public-items', 'record': {'id': 1, 'value': None}},
        ])
    with patch('target_snowflake.DbSync'), patch('target_snowflake.flush_streams', return_value=None), \
            patch('target_snowflake.validate_decimal_record') as validate:
        target_snowflake.persist_lines({}, [json.dumps(message) for message in messages])
    assert validate.call_count == 1
    assert validate.call_args.args[1]['properties']['value'] == nested_schema


@pytest.mark.parametrize('existing', ['FLOAT', 'DOUBLE', 'DOUBLE PRECISION', 'REAL'])
@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_float_fallback_aliases_do_not_repeat_versioning(existing, iceberg_version):
    plan = plan_column_changes(
        {'amount': decimal_schema(65, 30)}, {'AMOUNT': existing}, iceberg_version == 3, iceberg_version,
    )
    assert plan.additions == plan.replacements == ()


@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_persist_lines_fallback_and_exact_transitions_are_stable(iceberg_version):
    columns, archives = run_cached_schema_sequence('FLOAT', [
        {'amount': decimal_schema(65, 30)},
        {'amount': decimal_schema(None, None)},
        {'amount': decimal_schema(29, 9)},
        {'amount': decimal_schema(65, 30)},
        {'amount': decimal_schema(65, 30), 'added': {'type': ['string']}},
    ], iceberg_version, version_legacy_float_columns=True)
    assert columns['AMOUNT'] == ('DOUBLE' if iceberg_version else 'FLOAT')
    assert [columns[name] for name in archives] == ['FLOAT', 'NUMERIC(29,9)']


@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_unbounded_decimal_keys_use_lossless_text_and_remain_stable(iceberg_version):
    schema = decimal_schema(None, None)
    arguments = {'is_iceberg_table': iceberg_version == 3, 'iceberg_version': iceberg_version}
    assert column_type(schema, **arguments, is_key=True) == 'VARCHAR(134217728)'
    plan = plan_column_changes({'id': schema}, {}, **arguments, key_properties=['id'])
    assert plan.additions == ('"ID" VARCHAR(134217728)',)
    assert plan_column_changes(
        {'id': schema}, {'ID': 'TEXT'}, **arguments, key_properties=['id'],
    ).replacements == ()
    assert record_to_csv_line({'id': '9007199254740992.0100'}, {'id': schema}, key_properties=['id']) == (
        '"9007199254740992.01"'
    )
    assert record_to_csv_line({'id': '9.00719925474099201E15'}, {'id': schema}, key_properties=['id']) == (
        '"9007199254740992.01"'
    )


def test_float_fallback_copy_and_merge_saturate_overflow_without_losing_nulls():
    columns = [{'name': '"AMOUNT"', 'trans': '', 'decimal_type': 'FLOAT'}]
    copy_sql = create_copy_sql('ITEMS', 'STAGE', 'rows.csv', 'FORMAT', columns)
    merge_sql = create_merge_sql('ITEMS', 'STAGE', 'rows.csv', 'FORMAT', columns, 's.ID=t.ID')
    for sql in (copy_sql, merge_sql):
        assert 'TRY_TO_DOUBLE(($1))' in sql
        assert 'IS NULL THEN NULL' in sql
        assert '1.7976931348623157e308' in sql


def test_bounded_nan_payload_maps_to_null_without_hiding_nan_keys():
    columns = [
        {'name': '"ID"', 'trans': '', 'decimal_type': 'NUMERIC(10,2)', 'decimal_key': True},
        {'name': '"AMOUNT"', 'trans': '', 'decimal_type': 'NUMERIC(10,2)', 'decimal_key': False},
    ]
    sql = create_merge_sql('ITEMS', 'STAGE', 'rows.csv', 'FORMAT', columns, 's.ID=t.ID')
    assert 'CAST(($1) AS NUMERIC(10,2))' in sql
    assert "CAST(NULLIF(($2), 'NaN') AS NUMERIC(10,2))" in sql
    assert "NULLIF(($2), 'NaN')" in create_copy_sql('ITEMS', 'STAGE', 'rows.csv', 'FORMAT', columns)


def test_fixed_point_nan_key_refused_and_text_nan_key_preserved():
    sync = object.__new__(DbSync)
    sync.stream_schema_message = {'stream': 'public-items', 'key_properties': ['id']}
    sync.flatten_schema = {'id': decimal_schema()}
    sync.data_flattening_max_level = 0
    with pytest.raises(ValueError, match='NaN primary key id'):
        sync.record_primary_key_string({'id': 'NaN'})
    sync.flatten_schema = {'id': decimal_schema(None, None)}
    del sync._decimal_fields
    assert sync.record_primary_key_string({'id': 'NaN'}) == '["NaN"]'


@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_postgres_bounded_numeric_key_uses_text_from_creation_and_retains_existing_number(iceberg_version):
    schema = decimal_schema(10, 2)
    arguments = {'is_iceberg_table': iceberg_version == 3, 'iceberg_version': iceberg_version}
    assert column_type(schema, **arguments, is_key=True, source='postgres') == 'VARCHAR(134217728)'
    assert column_type(schema, **arguments, is_key=True, source='mysql') == 'NUMERIC(10,2)'
    assert plan_column_changes(
        {'id': schema}, {}, **arguments, key_properties=['id'], source='postgres',
    ).additions == ('"ID" VARCHAR(134217728)',)
    assert plan_column_changes(
        {'id': schema}, {'ID': 'TEXT'}, **arguments, key_properties=['id'], source='postgres',
    ).replacements == ()
    retained = plan_column_changes(
        {'id': schema}, {'ID': 'NUMBER(10,2)'}, **arguments, key_properties=['id'], source='postgres',
    )
    assert retained.replacements == ()
    assert retained.retained_column_types == (('ID', 'NUMBER(10,2)'),)

    sync = object.__new__(DbSync)
    sync.connection_config = {'source_tap_type': 'tap-postgres'}
    sync.stream_schema_message = {'stream': 'public-items', 'key_properties': ['id']}
    sync.flatten_schema = {'id': schema}
    sync.data_flattening_max_level = 0
    assert sync.record_primary_key_string({'id': 'NaN'}) == '["NaN"]'
    assert sync.record_primary_key_string({'id': '1.00'}) == sync.record_primary_key_string({'id': '1.0'})
    columns = [{'name': '"ID"', 'trans': '', 'decimal_type': 'VARCHAR(134217728)', 'decimal_key': True}]
    assert 'CAST(($1) AS VARCHAR(134217728)) "ID"' in create_merge_sql(
        'ITEMS', 'STAGE', 'rows.csv', 'FORMAT', columns, 's."ID"=t."ID"',
    )


def test_bounded_nan_warning_once_per_column_keeps_keyless_rows():
    sync = object.__new__(DbSync)
    sync.stream_schema_message = {'stream': 'public-items', 'key_properties': []}
    sync.flatten_schema = {'amount': decimal_schema()}
    sync.data_flattening_max_level = 0
    sync.logger = Mock()
    assert sync.record_primary_key_string({'amount': 'NaN'}) is None
    assert sync.record_primary_key_string({'amount': 'NaN'}) is None
    assert sync.logger.warning.call_count == 1
