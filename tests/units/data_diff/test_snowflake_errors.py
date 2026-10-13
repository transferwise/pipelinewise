"""Protect Snowflake data-diff diagnostics before they are persisted or alerted."""

from unittest.mock import MagicMock, patch

import pytest
from snowflake.connector.errors import ProgrammingError

from pipelinewise.data_diff.adapters import SnowflakeAdapter
from pipelinewise.data_diff.engine import connect_target


@pytest.mark.parametrize('operation', ['connect', 'columns', 'metrics'])
def test_snowflake_data_diff_boundaries_sanitize_data_errors(operation):
    error = ProgrammingError(
        msg='Numeric value SYNTHETIC_CANARY is not recognized',
        errno=100038,
        sqlstate='22018',
        sfqid='01234567-89ab-cdef-0123-456789abcdef',
    )
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.execute.side_effect = error
    adapter = SnowflakeAdapter(connection, 30)

    with pytest.raises(ProgrammingError) as caught:
        if operation == 'connect':
            with patch('snowflake.connector.connect', side_effect=error):
                connect_target(
                    {'target_type': 'target-snowflake', 'statement_timeout_seconds': 30, 'check_id': 'check-id'},
                    {'user': 'dummy-user', 'account': 'dummy-account', 'dbname': 'dummy-db',
                     'warehouse': 'dummy-warehouse', 'password': 'dummy-password'},
                )
        elif operation == 'columns':
            adapter.resolve_columns('DUMMY_SCHEMA', 'DUMMY_TABLE', ['value'])
        else:
            adapter.execute_metrics('SELECT COUNT(*) AS row_count', (), ['row_count'])

    assert caught.value is error
    assert (error.errno, error.sqlstate) == (100038, '22018')
    assert 'SYNTHETIC_CANARY' not in str(error) + repr(error) + error.raw_msg
    assert '01234567-89ab-cdef-0123-456789abcdef' in str(error)
