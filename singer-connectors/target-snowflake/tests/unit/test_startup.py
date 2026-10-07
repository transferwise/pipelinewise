"""Validate the named CSV format before consuming Singer input."""

import io
import json
from unittest.mock import patch

import pytest

import target_snowflake
from target_snowflake.file_format import FileFormatTypes
from target_snowflake.file_formats.csv import REQUIRED_FILE_FORMAT_OPTIONS


@pytest.mark.parametrize('hard_delete', [True, False, None, 'false'])
def test_startup_ignores_legacy_hard_delete_setting(tmp_path, hard_delete):
    config_path = tmp_path / 'config.json'
    config_path.write_text(json.dumps({'hard_delete': hard_delete}), encoding='utf-8')
    input_bytes = io.BytesIO(b'{"type":"STATE","value":{"position":1}}\n')
    singer_input = io.TextIOWrapper(input_bytes, encoding='utf-8')

    with patch('sys.argv', ['target-snowflake', '--config', str(config_path)]), \
            patch('sys.stdin', singer_input), \
            patch.object(target_snowflake, 'get_snowflake_statics', return_value=([], FileFormatTypes.CSV)), \
            patch.object(target_snowflake, 'emit_state') as emit_state:
        target_snowflake.main()

        emit_state.assert_called_once_with({'position': 1})


@pytest.mark.parametrize('compatible', [False, True])
@pytest.mark.parametrize('disable_table_cache', [False, True])
def test_startup_validates_csv_before_consuming_records(tmp_path, compatible, disable_table_cache):
    config = {
        'account': 'test-account',
        'dbname': 'TEST_DB',
        'user': 'test-user',
        'private_key': 'unused-test-key',
        'warehouse': 'TEST_WH',
        'default_target_schema': 'TEST_SCHEMA',
        'file_format': 'TEST_DB.TEST_SCHEMA.TEST_FORMAT',
        'disable_table_cache': disable_table_cache,
    }
    config_path = tmp_path / 'config.json'
    config_path.write_text(json.dumps(config), encoding='utf-8')
    options = dict(REQUIRED_FILE_FORMAT_OPTIONS)
    if not compatible:
        options['NULL_IF'] = ['\\N']
    metadata = [{
        'name': 'TEST_FORMAT',
        'database_name': 'TEST_DB',
        'schema_name': 'TEST_SCHEMA',
        'type': 'CSV',
        'format_options': json.dumps(options),
    }]
    input_bytes = io.BytesIO(b'{"type":"RECORD","stream":"scripts","record":{"id":1}}\n')
    singer_input = io.TextIOWrapper(input_bytes, encoding='utf-8')

    with patch('sys.argv', ['target-snowflake', '--config', str(config_path)]), \
            patch('sys.stdin', singer_input), \
            patch.object(target_snowflake.LOGGER, 'error') as log_error, \
            patch.object(target_snowflake.DbSync, 'query', return_value=metadata) as query, \
            patch.object(target_snowflake.DbSync, 'get_table_columns', return_value=[]) as columns, \
            patch.object(target_snowflake, 'persist_lines') as persist:
        if compatible:
            target_snowflake.main()
            if disable_table_cache:
                columns.assert_not_called()
            else:
                columns.assert_called_once()
            persist.assert_called_once()
            assert persist.call_args.args[0] == config
            assert persist.call_args.args[2:] == ([], FileFormatTypes.CSV)
            log_error.assert_not_called()
        else:
            with pytest.raises(SystemExit) as error:
                target_snowflake.main()
            assert error.value.code == 1
            log_error.assert_called_once()
            message = str(log_error.call_args.args[1])
            assert 'Named CSV file format TEST_DB.TEST_SCHEMA.TEST_FORMAT is incompatible' in message
            assert 'NULL_IF' in message
            columns.assert_not_called()
            persist.assert_not_called()
            assert input_bytes.tell() == 0
        query.assert_called_once()


@pytest.mark.parametrize('disable_table_cache', [False, True])
@pytest.mark.parametrize('format_exists', [False, True], ids=['missing', 'parquet'])
def test_startup_rejects_invalid_format_before_consuming_records(tmp_path, disable_table_cache, format_exists):
    config = {
        'account': 'test-account',
        'dbname': 'TEST_DB',
        'user': 'test-user',
        'private_key': 'unused-test-key',
        'warehouse': 'TEST_WH',
        'default_target_schema': 'TEST_SCHEMA',
        'file_format': 'TEST_DB.TEST_SCHEMA.TEST_FORMAT',
        'disable_table_cache': disable_table_cache,
    }
    config_path = tmp_path / 'config.json'
    config_path.write_text(json.dumps(config), encoding='utf-8')
    metadata = [{
        'name': 'TEST_FORMAT',
        'database_name': 'TEST_DB',
        'schema_name': 'TEST_SCHEMA',
        'type': 'PARQUET',
    }] if format_exists else []
    expected_message = (
        "Named file format TEST_DB.TEST_SCHEMA.TEST_FORMAT has unsupported type 'PARQUET'; "
        'target-snowflake supports only CSV staging'
        if format_exists else 'Named file format not found: TEST_DB.TEST_SCHEMA.TEST_FORMAT'
    )
    input_bytes = io.BytesIO(b'{"type":"RECORD","stream":"scripts","record":{"id":1}}\n')
    singer_input = io.TextIOWrapper(input_bytes, encoding='utf-8')

    with patch('sys.argv', ['target-snowflake', '--config', str(config_path)]), \
            patch('sys.stdin', singer_input), \
            patch.object(target_snowflake.LOGGER, 'error') as log_error, \
            patch.object(target_snowflake.DbSync, 'query', return_value=metadata) as query, \
            patch.object(target_snowflake.DbSync, 'get_table_columns', return_value=[]) as columns, \
            patch.object(target_snowflake, 'persist_lines') as persist, \
            pytest.raises(SystemExit) as error:
        target_snowflake.main()

    assert error.value.code == 1
    log_error.assert_called_once()
    assert log_error.call_args.args[0] == '%s'
    assert str(log_error.call_args.args[1]) == expected_message
    query.assert_called_once()
    columns.assert_not_called()
    persist.assert_not_called()
    assert input_bytes.tell() == 0


def test_startup_preserves_unexpected_errors_without_consuming_records():
    input_bytes = io.BytesIO(b'{"type":"STATE","value":{"position":1}}\n')
    singer_input = io.TextIOWrapper(input_bytes, encoding='utf-8')

    with patch('sys.argv', ['target-snowflake']), \
            patch('sys.stdin', singer_input), \
            patch.object(target_snowflake, 'get_snowflake_statics', side_effect=RuntimeError('connection failed')), \
            patch.object(target_snowflake, 'persist_lines') as persist, \
            pytest.raises(RuntimeError, match='connection failed'):
        target_snowflake.main()

    persist.assert_not_called()
    assert input_bytes.tell() == 0


@pytest.mark.parametrize(('message', 'error'), [
    ({}, "missing required key 'type'"),
    ({'type': 'RECORD', 'record': {}}, "missing required key 'stream'"),
    ({'type': 'RECORD', 'stream': 'public-items', 'record': {}}, 'before a corresponding schema'),
    ({'type': 'SCHEMA', 'schema': {}}, "missing required key 'stream'"),
    ({'type': 'SCHEMA', 'stream': 'public-items', 'schema': {'type': 'object'}}, 'key_properties field is required'),
    ({'type': 'SCHEMA', 'stream': 'public-items', 'schema': {'type': 'object'}, 'key_properties': []},
     'key_properties field is required'),
    ({'type': 'UNRECOGNIZED'}, 'Unknown message type'),
    ('{broken-json', 'Expecting property name'),
])
def test_malformed_protocol_stops_before_database_sync_or_acknowledgement(message, error):
    first_line = message if isinstance(message, str) else json.dumps(message)
    with patch.object(target_snowflake, 'DbSync') as database, \
            patch.object(target_snowflake, 'emit_state') as emit, \
            pytest.raises(Exception, match=error):
        target_snowflake.persist_lines({}, [first_line, json.dumps({'type': 'STATE', 'value': {'last_id': 1}})])
    database.assert_not_called()
    emit.assert_not_called()


@pytest.mark.parametrize('invalid_settings', [
    {'warehouse': None},
    {'s3_bucket': None},
    {'stage': None},
    {'stage': 'unqualified_stage'},
    {'default_target_schema': None},
    {'s3_bucket': None, 'stage': None, 'archive_load_files': True},
    {'version_legacy_float_columns': 'true'},
    {'iceberg_create': True},
    {'target_table_format': 'unsupported'},
    {'target_table_format': 'iceberg', 'iceberg_version': 2},
    {'iceberg_version': 3},
])
def test_invalid_cli_settings_do_not_query_snowflake_or_read_input(tmp_path, invalid_settings):
    config = {
        'account': 'test-account', 'dbname': 'TEST_DB', 'user': 'test-user', 'private_key': 'unused.pem',
        'warehouse': 'TEST_WH', 'file_format': 'TEST_DB.TEST_SCHEMA.TEST_FORMAT',
        'default_target_schema': 'TEST_SCHEMA', 's3_bucket': 'test-bucket', 'stage': 'TEST_SCHEMA.TEST_STAGE',
        **invalid_settings,
    }
    config_path = tmp_path / 'config.json'
    config_path.write_text(json.dumps(config), encoding='utf-8')
    input_bytes = io.BytesIO(b'{"type":"STATE","value":{"last_id":1}}\n')
    singer_input = io.TextIOWrapper(input_bytes, encoding='utf-8')
    with patch('sys.argv', ['target-snowflake', '--config', str(config_path)]), \
            patch('sys.stdin', singer_input), \
            patch.object(target_snowflake.DbSync, 'open_connection') as connection, \
            patch.object(target_snowflake, 'persist_lines') as persist, \
            pytest.raises(SystemExit) as error:
        target_snowflake.main()
    assert error.value.code == 1
    connection.assert_not_called()
    persist.assert_not_called()
    assert input_bytes.tell() == 0


def test_activation_message_does_not_prevent_idle_state_acknowledgement():
    state = {'bookmarks': {'public-items': {'last_id': 1}}}
    with patch.object(target_snowflake, 'DbSync') as database, \
            patch.object(target_snowflake, 'emit_state') as emit:
        target_snowflake.persist_lines({}, [
            json.dumps({'type': 'ACTIVATE_VERSION'}), json.dumps({'type': 'STATE', 'value': state}),
        ])
    database.assert_not_called()
    emit.assert_called_once_with(state)
