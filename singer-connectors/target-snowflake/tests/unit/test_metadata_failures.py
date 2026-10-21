"""Distinguish missing tables from failed discovery and preserve load errors."""

from unittest.mock import Mock

import pytest
from snowflake.connector.errors import ProgrammingError

from target_snowflake.db_sync import DbSync
from target_snowflake.file_format import FileFormatTypes


@pytest.fixture
def database_sync():
    """Construct a normal stream without opening a Snowflake connection."""
    return DbSync({
        'account': 'test-account', 'dbname': 'test-db', 'user': 'test-user', 'private_key': 'unused.pem',
        'warehouse': 'test-warehouse', 'file_format': 'test-format', 'default_target_schema': 'TEST_SCHEMA',
    }, {
        'stream': 'public-items', 'key_properties': ['id'],
        'schema': {'type': 'object', 'properties': {'id': {'type': ['integer']}, 'amount': {'type': ['number']}}},
    }, file_format_type=FileFormatTypes.CSV)


@pytest.mark.parametrize(('method', 'missing_code', 'missing_message', 'expected'), [
    ('get_tables', 2043, '\nSchema does not exist', []),
    ('get_table_columns', 2003, '\nSchema does not exist or not authorized', []),
    ('_get_current_pks', 2043, '\nTable does not exist', set()),
])
def test_missing_metadata_is_empty_but_other_database_errors_propagate(
    database_sync, method, missing_code, missing_message, expected,
):
    args = () if method == '_get_current_pks' else (['TEST_SCHEMA'],)
    database_sync.query = Mock(side_effect=ProgrammingError(msg=missing_message, errno=missing_code, sqlstate='02000'))
    assert getattr(database_sync, method)(*args) == expected
    denied = ProgrammingError(msg='Insufficient privileges', errno=3001, sqlstate='42501')
    database_sync.query.side_effect = denied
    with pytest.raises(ProgrammingError) as error:
        getattr(database_sync, method)(*args)
    assert error.value is denied


@pytest.mark.parametrize('method', ['get_tables', 'get_table_columns'])
def test_discovery_requires_an_explicit_schema(database_sync, method):
    database_sync.query = Mock()
    with pytest.raises(Exception, match='List of table schemas empty'):
        getattr(database_sync, method)([])
    database_sync.query.assert_not_called()


@pytest.mark.parametrize('has_key', [False, True])
def test_load_file_propagates_database_error_without_trying_another_loading_mode(database_sync, has_key):
    database_sync.stream_schema_message['key_properties'] = ['id'] if has_key else []
    failure = ProgrammingError(msg='Failed to load staged records', errno=100038, sqlstate='22018')
    database_sync._load_file_copy = Mock(side_effect=failure)
    database_sync._load_file_merge = Mock(side_effect=failure)
    with pytest.raises(ProgrammingError) as error:
        database_sync.load_file('staged/file.csv', count=1, size_bytes=100)
    assert error.value is failure
    selected = database_sync._load_file_merge if has_key else database_sync._load_file_copy
    alternative = database_sync._load_file_copy if has_key else database_sync._load_file_merge
    selected.assert_called_once()
    alternative.assert_not_called()


@pytest.mark.parametrize(('method', 'response', 'expected'), [
    ('_load_file_merge', [{'number of rows inserted': 2, 'number of rows updated': 3}], (2, 3)),
    ('_load_file_merge', [{'number of rows inserted': 2}], (2, 0)),
    ('_load_file_merge', [], (0, 0)),
    ('_load_file_copy', [{'rows_loaded': 4}], 4),
    ('_load_file_copy', [], 0),
])
def test_load_counts_accept_snowflake_copy_and_merge_responses(database_sync, method, response, expected):
    cursor = Mock()
    cursor.__enter__ = Mock(return_value=cursor)
    cursor.__exit__ = Mock(return_value=False)
    cursor.fetchall.return_value = response
    connection = Mock()
    connection.__enter__ = Mock(return_value=connection)
    connection.__exit__ = Mock(return_value=False)
    connection.cursor.return_value = cursor
    database_sync.open_connection = Mock(return_value=connection)
    assert getattr(database_sync, method)('file.csv', 'public-items', [{
        'name': '"ID"', 'trans': '', 'json_element_name': '"id"',
    }]) == expected
    cursor.execute.assert_called_once()
    cursor.__exit__.assert_called_once()
    connection.__exit__.assert_called_once()
