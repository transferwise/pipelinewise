"""Preserve live target identities when upgraded source schemas expose more types."""

import gzip
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

import target_snowflake
from singer.decimal_support import decimal_schema
from target_snowflake.db_sync import DbSync, RECORD_UPDATE_MODE_PATCH
from target_snowflake.file_formats import csv


EXTENDED_KEYS = ['id', 'calendar_year', 'tags', 'payload']
EXTENDED_SCHEMA = {
    'id': {'type': ['integer']},
    'calendar_year': {'type': ['integer'], 'format': 'singer.year'},
    'tags': {'type': ['string']},
    'payload': {'type': ['string'], 'format': 'binary'},
}


def key_sync(properties, keys, *, source='tap-mysql', retained_types=None, update_mode=None):
    """Exercise key and staging methods without opening a Snowflake connection."""
    sync = object.__new__(DbSync)
    sync.connection_config = {'source_tap_type': source, 'file_format': 'FORMAT'}
    sync.stream_schema_message = {
        'stream': 'public-items', 'key_properties': list(keys),
        'schema': {'type': 'object', 'properties': properties},
    }
    sync.flatten_schema = properties
    sync.data_flattening_max_level = 0
    sync.schema_name = 'PUBLIC'
    sync.table_cache = None
    sync.grantees = []
    sync.grant_select_on_all_tables_in_schema = False
    sync.logger = Mock()
    sync.record_update_mode = update_mode
    sync._retained_column_types = retained_types or {}
    sync.file_format = Mock(formatter=csv)
    return sync


@pytest.mark.parametrize('table_format', ['native', 'managed_iceberg_v3'])
def test_existing_mysql_composite_key_keeps_established_members_across_restarts(table_format):
    for _restart in range(2):
        sync = key_sync(EXTENDED_SCHEMA, EXTENDED_KEYS)
        if table_format == 'managed_iceberg_v3':
            sync.connection_config.update(target_table_format='iceberg', iceberg_version=3)
        sync.discover_table_format = Mock(return_value=table_format)
        sync._get_current_pks = Mock(return_value={'ID'})
        observed_keys = []
        sync.update_columns = Mock(side_effect=lambda *_args: observed_keys.append(sync.effective_key_properties()))
        sync.query = Mock()

        sync.sync_table()

        assert observed_keys == [['id']]
        assert sync.effective_key_properties() == ['id']
        assert sync.stream_schema_message['key_properties'] == EXTENDED_KEYS
        assert sync.primary_key_merge_condition() == 's."ID" = t."ID"'
        assert sync.record_primary_key_string({'id': 7}) == '["7"]'
        assert sync.record_primary_key_string({'id': 7, 'calendar_year': 2026, 'tags': 'a,b', 'payload': '00ff'}) == (
            sync.record_primary_key_string({'id': 7, 'calendar_year': 2025, 'tags': 'b', 'payload': 'abcd'})
        )
        queries = [statement for call in sync.query.call_args_list for statement in call.args[0]]
        assert not any('primary key' in statement.lower() for statement in queries)


@pytest.mark.parametrize('table_format', ['native', 'managed_iceberg_v3'])
def test_new_mysql_table_adopts_complete_key_after_fullsync(table_format):
    sync = key_sync(EXTENDED_SCHEMA, EXTENDED_KEYS)
    sync._effective_key_properties = ['id']
    if table_format == 'managed_iceberg_v3':
        sync.connection_config.update(target_table_format='iceberg', iceberg_version=3)
    sync.discover_table_format = Mock(return_value='missing')
    sync.query = Mock()
    sync.grant_privilege = Mock()
    sync._verify_created_table_format = Mock()
    sync._get_current_pks = Mock(return_value=set(EXTENDED_KEYS))
    sync._refresh_table_pks = Mock()

    sync.sync_table()

    assert sync.effective_key_properties() == EXTENDED_KEYS
    assert sync.primary_key_merge_condition() == (
        's."ID" = t."ID" AND s."CALENDAR_YEAR" = t."CALENDAR_YEAR" '
        'AND s."TAGS" = t."TAGS" AND s."PAYLOAD" = t."PAYLOAD"'
    )
    first = {'id': 7, 'calendar_year': 2025, 'tags': 'a,b', 'payload': '00ff'}
    second = {**first, 'calendar_year': 2026}
    assert sync.record_primary_key_string(first) != sync.record_primary_key_string(second)
    assert 'PRIMARY KEY("ID", "CALENDAR_YEAR", "TAGS", "PAYLOAD")' in sync.query.call_args_list[0].args[0]


@pytest.mark.parametrize('current_keys', [set(), set(EXTENDED_KEYS), {'UNRELATED'}])
def test_only_existing_nonempty_subsets_are_retained(current_keys):
    sync = key_sync(EXTENDED_SCHEMA, EXTENDED_KEYS)
    sync._get_current_pks = Mock(return_value=current_keys)
    sync._retain_existing_primary_keys()
    assert sync.effective_key_properties() == EXTENDED_KEYS


def test_other_sources_do_not_reuse_mysql_key_membership_policy():
    sync = key_sync(EXTENDED_SCHEMA, EXTENDED_KEYS, source='tap-postgres')
    sync._get_current_pks = Mock(return_value={'ID'})
    sync._retain_existing_primary_keys()
    sync._get_current_pks.assert_not_called()
    assert sync.effective_key_properties() == EXTENDED_KEYS


@pytest.mark.parametrize('existing_type', ['TEXT(134217728)', 'VARCHAR(134217728)', 'CHAR(8)'])
def test_retained_text_binary_key_uses_historical_uppercase_hex(existing_type):
    properties = {'payload': {'type': ['string'], 'format': 'binary'}}
    sync = key_sync(properties, ['payload'], retained_types={'PAYLOAD': existing_type})

    assert sync.record_primary_key_string({'payload': '00ffabcd'}) == '["00FFABCD"]'
    assert sync.record_primary_key_string({'payload': '00ffabcd'}) == sync.record_primary_key_string({
        'payload': '00FFABCD',
    })
    assert csv.record_to_csv_line(
        {'payload': '00ffabcd'}, properties, value_transformer=sync.load_column_value,
    ) == '"00FFABCD"'
    assert sync.load_column_value('payload', None) is None


@pytest.mark.parametrize('existing_type', [None, 'BINARY(67108864)'])
def test_new_binary_column_preserves_original_hex_text_for_binary_parser(existing_type):
    properties = {'payload': {'type': ['string'], 'format': 'binary'}}
    retained_types = {'PAYLOAD': existing_type} if existing_type else {}
    sync = key_sync(properties, ['payload'], retained_types=retained_types)
    assert sync.load_column_value('payload', '00ffabcd') == '00ffabcd'
    assert csv.record_to_csv_line(
        {'payload': '00ffabcd'}, properties, value_transformer=sync.load_column_value,
    ) == '"00ffabcd"'


@pytest.mark.parametrize(('first', 'last', 'expected'), [
    ('9007199254740992', '9007199254740993', '9007199254740992.0'),
    ('1e1000', '2e1000', '1.7976931348623157e+308'),
    ('-1e1000', '-2e1000', '-1.7976931348623157e+308'),
    ('1e-1000', '-1e-1000', '0'),
    ('-0', '0.000', '0'),
    ('NaN', 'NaN', 'NaN'),
])
@pytest.mark.parametrize('existing_type', ['FLOAT', 'DOUBLE', 'DOUBLE PRECISION', 'REAL'])
def test_retained_float_decimal_identity_matches_staged_rounding(first, last, expected, existing_type):
    properties = {'id': decimal_schema(None, None)}
    sync = key_sync(properties, ['id'], source='tap-postgres', retained_types={'ID': existing_type})
    assert sync.record_primary_key_string({'id': first}) == sync.record_primary_key_string({'id': last})
    assert sync.load_column_value('id', first) == sync.load_column_value('id', last) == expected
    assert csv.record_to_csv_line(
        {'id': last}, properties, value_transformer=sync.load_column_value,
    ) == f'"{expected}"'


def test_new_exact_numeric_and_text_keys_keep_distinct_source_identities():
    for schema, source in [(decimal_schema(29, 9), 'tap-mysql'), (decimal_schema(None, None), 'tap-postgres')]:
        sync = key_sync({'id': schema}, ['id'], source=source)
        assert sync.record_primary_key_string({'id': '9007199254740992'}) != (
            sync.record_primary_key_string({'id': '9007199254740993'})
        )


@pytest.mark.parametrize('update_mode', [None, RECORD_UPDATE_MODE_PATCH])
def test_retained_float_collision_keeps_last_event_and_composite_tenants(update_mode):
    properties = {
        'id': decimal_schema(None, None), 'tenant': {'type': ['integer']},
        'amount': {'type': ['integer']}, 'description': {'type': ['string']},
    }
    sync = key_sync(properties, ['id', 'tenant'], source='tap-postgres', retained_types={'ID': 'FLOAT'},
                    update_mode=update_mode)
    sync.create_schema_if_not_exists = Mock()
    sync.sync_table = Mock()
    records = [
        {'id': '9007199254740992', 'tenant': 1, 'amount': 1, 'description': 'kept by PATCH'},
        {'id': '9007199254740992', 'tenant': 2, 'amount': 3, 'description': 'other tenant'},
        {'id': '9007199254740993', 'tenant': 1, 'amount': 2},
    ]
    messages = [
        {'type': 'SCHEMA', **sync.stream_schema_message},
        *({'type': 'RECORD', 'stream': 'public-items', 'record': record} for record in records),
    ]
    with patch('target_snowflake.DbSync', return_value=sync), \
            patch('target_snowflake.flush_streams', return_value=None) as flush:
        target_snowflake.persist_lines({}, [json.dumps(message) for message in messages])
    buffered = flush.call_args.args[0]['public-items']
    assert len(buffered) == 2
    first_tenant = buffered[sync.record_primary_key_string(records[0])]
    assert first_tenant['id'] == '9007199254740993'
    assert first_tenant['amount'] == 2
    if update_mode == RECORD_UPDATE_MODE_PATCH:
        assert first_tenant['description'] == 'kept by PATCH'
    else:
        assert 'description' not in first_tenant
    assert buffered[sync.record_primary_key_string(records[1])]['amount'] == 3


def test_retained_float_collision_keeps_last_delete_event():
    sync = key_sync({'id': decimal_schema(None, None)}, ['id'], source='tap-postgres',
                    retained_types={'ID': 'FLOAT'}, update_mode=RECORD_UPDATE_MODE_PATCH)
    records = {}
    first = {'id': '9007199254740992'}
    last = {'id': '9007199254740993', '_sdc_deleted_at': '2026-10-05T00:00:00Z'}
    for record in (first, last):
        target_snowflake.store_record(records, sync.record_primary_key_string(record), record, sync)
    assert list(records.values()) == [last]


@pytest.mark.parametrize('compression', [False, True])
def test_flush_serializes_retained_keys_with_same_transform_as_buffer(compression, tmp_path):
    properties = {'id': decimal_schema(None, None), 'payload': {'type': ['string'], 'format': 'binary'}}
    sync = key_sync(properties, ['id', 'payload'], source='tap-mysql',
                    retained_types={'ID': 'FLOAT', 'PAYLOAD': 'TEXT(134217728)'})
    staged_lines = []

    def inspect_staged_file(filename, *_args, **_kwargs):
        if compression:
            with gzip.open(filename, 'rt', encoding='utf-8') as infile:
                staged_lines.append(infile.read())
        else:
            staged_lines.append(Path(filename).read_text(encoding='utf-8'))
        return 'rows.csv'

    sync.put_to_stage = Mock(side_effect=inspect_staged_file)
    sync.load_file = Mock()
    sync.delete_from_stage = Mock()
    record = {'id': '9007199254740993', 'payload': '00ff'}
    target_snowflake.flush_record_group(
        'public-items', {sync.record_primary_key_string(record): record}, sync, None,
        str(tmp_path), not compression, None,
    )
    assert staged_lines == ['"9007199254740992.0","00FF"\n']
    assert not list(tmp_path.iterdir())
