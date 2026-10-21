"""Protect Snowflake execution failures from exposing source row values."""

import logging
import traceback
from unittest.mock import MagicMock

import pytest
from snowflake.connector.errors import ProgrammingError

from target_snowflake.db_sync import DbSync
from target_snowflake.file_format import FileFormatTypes


@pytest.fixture
def db_sync():
    config = {
        'account': 'dummy-account',
        'dbname': 'SAFE_DB',
        'user': 'dummy-user',
        'private_key': 'dummy-key',
        'warehouse': 'SAFE_WAREHOUSE',
        'default_target_schema': 'SAFE_SCHEMA',
        'file_format': 'SAFE_FILE_FORMAT',
    }
    schema = {
        'stream': 'public-safe_table',
        'key_properties': ['id'],
        'schema': {'properties': {'id': {'type': ['integer']}, 'value': {'type': ['string']}}},
    }
    sync = DbSync(config, schema, file_format_type=FileFormatTypes.CSV)
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = connection.cursor.return_value
    cursor.__enter__.return_value = cursor
    cursor.rowcount = 0
    sync.open_connection = MagicMock(return_value=connection)
    return sync, connection, cursor


def _data_error(message, errno, sqlstate):
    error = ProgrammingError(msg=message, errno=errno, sqlstate=sqlstate, sfqid='safe-query-id')
    error.query = 'SELECT PRIVATE_ROW_CANARY'
    error.__cause__ = RuntimeError('PRIVATE_ROW_CANARY in chained error')
    error.__context__ = RuntimeError('PRIVATE_ROW_CANARY in previous error')
    error.add_note('PRIVATE_ROW_CANARY in diagnostic note')
    return error


def _assert_safe_error(error, original, errno, sqlstate):
    assert error is original
    assert type(error) is ProgrammingError
    assert error.errno == errno
    assert error.sqlstate == sqlstate
    assert error.sfqid == 'safe-query-id'
    for value in (str(error), repr(error), repr(error.args), error.raw_msg, getattr(error, 'query', None)):
        assert 'PRIVATE_ROW_CANARY' not in str(value)
    assert 'PRIVATE_ROW_CANARY' not in ''.join(traceback.format_exception(error))


@pytest.mark.parametrize('operation', ['query', 'merge', 'copy', 'put', 'remove'])
@pytest.mark.parametrize(
    ('message', 'errno', 'sqlstate'),
    [
        ('PRIVATE_ROW_CANARY: Duplicate row detected. Row Values: [PRIVATE_ROW_CANARY]', 100090, '42P18'),
        ('PRIVATE_ROW_CANARY: Duplicate row detected.\nRow Values:\n[PRIVATE_ROW_CANARY]', 100090, '42P18'),
        ("Numeric value 'PRIVATE_ROW_CANARY' is not recognized", 100038, '22018'),
        ('PRIVATE_ROW_CANARY: Row Values: [PRIVATE_ROW_CANARY]', 12345, 'XX000'),
    ],
)
def test_direct_execution_boundaries_preserve_error_without_row_values(db_sync, operation, message, errno, sqlstate):
    sync, connection, cursor = db_sync
    original = _data_error(message, errno, sqlstate)
    cursor.execute.side_effect = original

    with pytest.raises(ProgrammingError) as raised:
        if operation == 'query':
            sync.query('SELECT * FROM "SAFE_TABLE"')
        elif operation == 'merge':
            sync._load_file_merge('safe-file.csv.gz', 'public-safe_table', [])
        elif operation == 'copy':
            sync._load_file_copy('safe-file.csv.gz', 'public-safe_table', [])
        elif operation == 'put':
            sync.put_to_stage('/tmp/safe-file.csv.gz', 'public-safe_table', 1)
        else:
            sync.delete_from_stage('public-safe_table', 'safe-file.csv.gz')

    _assert_safe_error(raised.value, original, errno, sqlstate)
    cursor.execute.assert_called_once()
    cursor.fetchall.assert_not_called()
    connection.commit.assert_not_called()


@pytest.mark.parametrize('has_primary_key', [True, False])
def test_load_failure_logs_table_context_without_success_or_row_values(db_sync, caplog, has_primary_key):
    sync, _, cursor = db_sync
    if not has_primary_key:
        sync.stream_schema_message['key_properties'] = []
    original = _data_error(
        'PRIVATE_ROW_CANARY: Duplicate row detected.\nRow Values:\n[PRIVATE_ROW_CANARY]', 100090, '42P18',
    )
    cursor.execute.side_effect = original

    with caplog.at_level(logging.INFO, logger=sync.logger.name), pytest.raises(ProgrammingError) as raised:
        sync.load_file('safe-file.csv.gz', count=1, size_bytes=42)

    _assert_safe_error(raised.value, original, 100090, '42P18')
    operation = 'MERGE' if has_primary_key else 'COPY'
    assert f'Error while executing {operation} query for table' in caplog.text
    assert 'SAFE_SCHEMA."SAFE_TABLE"' in caplog.text
    assert 'public-safe_table' in caplog.text
    assert 'PRIVATE_ROW_CANARY' not in caplog.text
    assert '"inserts"' not in caplog.text
    assert '"updates"' not in caplog.text
    cursor.fetchall.assert_not_called()


def test_missing_table_query_error_remains_available_for_recovery(db_sync):
    sync, _, cursor = db_sync
    original = ProgrammingError(
        msg='Table SAFE_TABLE does not exist or not authorized', errno=2043, sqlstate='42S02', sfqid='safe-query-id',
    )
    original_text = str(original)
    cursor.execute.side_effect = original

    with pytest.raises(ProgrammingError) as raised:
        sync.query('SELECT * FROM "SAFE_TABLE"')

    assert raised.value is original
    assert raised.value.errno == 2043
    assert raised.value.sqlstate == '42S02'
    assert str(raised.value) == original_text
    assert raised.value.raw_msg == 'Table SAFE_TABLE does not exist or not authorized'


def test_statement_list_stops_after_failure_without_executing_following_statements(db_sync):
    sync, connection, cursor = db_sync
    original = _data_error('Row Values: [PRIVATE_ROW_CANARY]', 100090, '42P18')
    cursor.execute.side_effect = [None, original]

    with pytest.raises(ProgrammingError) as raised:
        sync.query(['MERGE INTO "SAFE_TABLE"', 'DELETE FROM "SAFE_TABLE"'])

    _assert_safe_error(raised.value, original, 100090, '42P18')
    assert cursor.execute.call_count == 2
    assert cursor.execute.call_args_list[0].args == ('START TRANSACTION',)
    assert cursor.execute.call_args_list[1].args == ('MERGE INTO "SAFE_TABLE"',)
    cursor.fetchall.assert_not_called()
    connection.commit.assert_not_called()
