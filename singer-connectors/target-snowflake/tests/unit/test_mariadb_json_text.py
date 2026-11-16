"""Keep string transformations of legacy MariaDB JSON out of VARIANT columns."""

import csv
import io
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import target_snowflake
from target_snowflake.db_sync import DbSync
from target_snowflake.exceptions import TableFormatMismatchException
from target_snowflake.file_format import FileFormatTypes
from target_snowflake.managed_iceberg import column_clause, plan_column_changes


JSON_TEXT_SCHEMA = {
    'type': ['null', 'object', 'array', 'string', 'number', 'boolean'],
    'format': 'mariadb-json-text',
}


def stream_schema(keyed: bool) -> dict:
    """Represent the SCHEMA emitted by transform-field for a string rule."""
    return {
        'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': ['payload'] if keyed else [],
        'schema': {'type': 'object', 'properties': {'payload': JSON_TEXT_SCHEMA}},
    }


def config_for(iceberg_version: int | None, temp_dir: str) -> dict:
    config = {
        'account': 'account', 'dbname': 'DB', 'user': 'user', 'private_key': 'unused',
        'warehouse': 'WH', 'default_target_schema': 'PUBLIC', 'file_format': 'FORMAT',
        'source_tap_type': 'tap-mysql', 'primary_key_required': False,
        'parallelism': 1, 'no_compression': True, 'temp_dir': temp_dir,
    }
    if iceberg_version is not None:
        config.update(target_table_format='iceberg', iceberg_version=iceberg_version)
    return config


def column_metadata(data_type: str) -> list[dict]:
    return [{
        'SCHEMA_NAME': 'PUBLIC', 'TABLE_NAME': 'ITEMS', 'COLUMN_NAME': 'PAYLOAD',
        'DATA_TYPE': data_type, 'TYPE_LENGTH': 134217728 if data_type == 'TEXT' else None,
    }]


@pytest.mark.parametrize('iceberg_version', [None, 3])
@pytest.mark.parametrize('keyed', [False, True])
@pytest.mark.parametrize('current_type', [None, 'VARIANT', 'NUMBER', 'BINARY', 'ARRAY', 'OBJECT'])
def test_transformed_json_schema_rejects_unsupported_destination_before_ddl_or_upload(
    iceberg_version, keyed, current_type, tmp_path,
):
    table_format = 'managed_iceberg_v3' if iceberg_version else 'native'
    columns = column_metadata(current_type) if current_type else []
    messages = [stream_schema(keyed), {
        'type': 'RECORD', 'stream': 'public-items', 'record': {'payload': 'private transformed value'},
    }]
    with patch.object(DbSync, 'discover_table_format', return_value=table_format), \
            patch.object(DbSync, 'get_table_columns', return_value=columns) as live_columns, \
            patch.object(DbSync, '_get_current_pks', return_value={'PAYLOAD'} if keyed else set()), \
            patch.object(DbSync, 'query') as query, \
            patch.object(DbSync, 'put_to_stage') as upload, \
            patch.object(target_snowflake, 'emit_state') as emit_state, \
            pytest.raises(TableFormatMismatchException) as error:
        target_snowflake.persist_lines(
            config_for(iceberg_version, str(tmp_path)), [json.dumps(message) for message in messages],
            table_cache=column_metadata('TEXT'), file_format_type=FileFormatTypes.CSV,
        )
    assert 'Column PAYLOAD requires an existing Snowflake TEXT column' in str(error.value)
    assert 'before FullSync' in str(error.value)
    assert 'private transformed value' not in str(error.value)
    live_columns.assert_called_once_with(table_schemas=['PUBLIC'])
    query.assert_not_called()
    upload.assert_not_called()
    emit_state.assert_not_called()
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('iceberg_version', [None, 3])
@pytest.mark.parametrize('keyed', [False, True])
def test_transformed_json_schema_rejects_missing_table_without_creating_schema(iceberg_version, keyed, tmp_path):
    with patch.object(DbSync, 'discover_table_format', return_value='missing'), \
            patch.object(DbSync, 'query') as query, \
            patch.object(DbSync, 'put_to_stage') as upload, \
            pytest.raises(TableFormatMismatchException, match='existing Snowflake TEXT column'):
        target_snowflake.persist_lines(
            config_for(iceberg_version, str(tmp_path)), [json.dumps(stream_schema(keyed))],
            file_format_type=FileFormatTypes.CSV,
        )
    query.assert_not_called()
    upload.assert_not_called()


@pytest.mark.parametrize('iceberg_version', [None, 3])
@pytest.mark.parametrize('keyed', [False, True])
def test_transformed_json_schema_stages_and_loads_text_without_json_parsing(iceberg_version, keyed, tmp_path):
    table_format = 'managed_iceberg_v3' if iceberg_version else 'native'
    values = ['deadbeef0123456789', '***', 'plain text', '', None, '"JSON-looking text"', '初,\n\t\\']
    if keyed:
        values.remove(None)
    messages = [stream_schema(keyed), *[
        {'type': 'RECORD', 'stream': 'public-items', 'record': {'payload': value}} for value in values
    ], {'type': 'STATE', 'value': {'bookmarks': {'public-items': {'position': 7}}}}]
    staged = []

    def inspect_upload(_sync, filename, *_args, **_kwargs):
        staged.append(Path(filename).read_text(encoding='utf-8'))
        return 'rows.csv'

    connection = MagicMock()
    cursor = connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = []
    with patch.object(DbSync, 'discover_table_format', return_value=table_format), \
            patch.object(DbSync, 'get_table_columns', return_value=column_metadata('TEXT')), \
            patch.object(DbSync, '_get_current_pks', return_value={'PAYLOAD'} if keyed else set()), \
            patch.object(DbSync, 'query'), \
            patch.object(DbSync, 'open_connection', connection), \
            patch.object(DbSync, 'put_to_stage', autospec=True, side_effect=inspect_upload), \
            patch.object(DbSync, 'delete_from_stage'), \
            patch.object(target_snowflake, 'emit_state') as emit_state:
        target_snowflake.persist_lines(
            config_for(iceberg_version, str(tmp_path)), [json.dumps(message) for message in messages],
            file_format_type=FileFormatTypes.CSV,
        )

    fields = list(csv.reader(io.StringIO(staged[0]), escapechar='\\', doublequote=False))
    # Metadata columns sort ahead of payload; their values do not affect its text representation.
    assert [row[-1] for row in fields] == [value if value is not None else '' for value in values]
    sql = cursor.execute.call_args.args[0]
    assert sql.startswith('MERGE INTO' if keyed else 'COPY INTO')
    assert 'CAST(($4) AS TEXT(134217728))' in sql
    assert 'parse_json' not in sql.lower()
    emit_state.assert_called_once_with(messages[-1]['value'])
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('iceberg_version', [None, 3])
@pytest.mark.parametrize('current_type', ['TEXT', 'VARCHAR(64)', 'STRING', 'CHARACTER VARYING(128)'])
def test_transformed_json_retains_actual_string_type(iceberg_version, current_type):
    plan = plan_column_changes(
        {'payload': JSON_TEXT_SCHEMA}, {'PAYLOAD': current_type},
        is_iceberg_table=iceberg_version == 3, iceberg_version=iceberg_version, key_properties=['payload'],
    )
    assert plan.additions == plan.replacements == ()
    assert plan.retained_column_types == (('PAYLOAD', current_type),)


@pytest.mark.parametrize('iceberg_version', [None, 3])
def test_transformed_json_cannot_generate_create_or_add_column_definition(iceberg_version):
    with pytest.raises(TableFormatMismatchException, match='Column PAYLOAD requires an existing Snowflake TEXT column'):
        column_clause('payload', JSON_TEXT_SCHEMA, iceberg_version == 3, iceberg_version)
