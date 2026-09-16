"""Keep load SQL bound to the same quoted file-format name as discovery."""

import pytest

from target_snowflake.file_formats import csv, parquet


@pytest.mark.parametrize('formatter', [csv, parquet])
@pytest.mark.parametrize('operation', ['copy', 'merge'])
def test_load_sql_preserves_file_format_identifier(formatter, operation):
    file_format = "\"DB\".\"A\\B\".\"it's_format\""
    expected_literal = "'\"DB\".\"A\\\\B\".\"it''s_format\"'"
    arguments = {
        'table_name': 'TEST_TABLE',
        'stage_name': 'TEST_STAGE',
        's3_key': 'test-file',
        'file_format_name': file_format,
        'columns': [{'name': 'ID', 'trans': '', 'json_element_name': 'ID'}],
    }
    if operation == 'merge':
        arguments['pk_merge_condition'] = 's.ID = t.ID'
    sql = getattr(formatter, f'create_{operation}_sql')(**arguments)

    clause = 'format_name=' if operation == 'copy' else 'FILE_FORMAT => '
    assert clause + expected_literal in sql
