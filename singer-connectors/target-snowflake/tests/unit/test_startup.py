"""Validate the named CSV format before consuming Singer input."""

import io
import json
from unittest.mock import patch

import pytest

import target_snowflake
from target_snowflake.exceptions import InvalidFileFormatException
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
def test_startup_validates_csv_before_consuming_records(tmp_path, compatible):
    config = {
        'account': 'test-account',
        'dbname': 'TEST_DB',
        'user': 'test-user',
        'private_key': 'unused-test-key',
        'warehouse': 'TEST_WH',
        'default_target_schema': 'TEST_SCHEMA',
        'file_format': 'TEST_DB.TEST_SCHEMA.TEST_FORMAT',
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
            patch.object(target_snowflake.DbSync, 'query', return_value=metadata) as query, \
            patch.object(target_snowflake.DbSync, 'get_table_columns', return_value=[]) as columns, \
            patch.object(target_snowflake, 'persist_lines') as persist:
        if compatible:
            target_snowflake.main()
            columns.assert_called_once()
            persist.assert_called_once()
            assert persist.call_args.args[0] == config
            assert persist.call_args.args[2:] == ([], FileFormatTypes.CSV)
        else:
            with pytest.raises(InvalidFileFormatException, match='NULL_IF'):
                target_snowflake.main()
            columns.assert_not_called()
            persist.assert_not_called()
            assert input_bytes.tell() == 0
        query.assert_called_once()
