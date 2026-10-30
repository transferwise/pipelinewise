"""Snowflake sessions used for native-to-Iceberg conversion."""

from unittest.mock import patch

import pytest

from pipelinewise.fastsync.commons.snowflake_iceberg import SnowflakeConversionQueryAdapter
from pipelinewise.fastsync.commons.snowflake_iceberg_validation import manual_recovery_identity


CONFIG = {
    'user': 'user',
    'private_key': '/key.pem',
    'account': 'account',
    'dbname': 'database',
    'warehouse': 'TARGET_WH',
    'role': 'loader',
}


@pytest.mark.parametrize(('warehouse', 'expected'), [(None, 'TARGET_WH'), ('CONVERSION_WH', 'CONVERSION_WH')])
@patch('pipelinewise.fastsync.commons.snowflake_iceberg.pem2der', return_value=b'key')
@patch('pipelinewise.fastsync.commons.snowflake_iceberg.snowflake.connector.connect')
def test_conversion_sessions_abort_detached_queries(mock_connect, _mock_pem2der, warehouse, expected):
    """Every conversion session stops orphaned statements and can use its own warehouse."""
    config = dict(CONFIG)
    adapter = SnowflakeConversionQueryAdapter(config, warehouse)

    adapter.open_connection({'attempt_id': 'one'})

    mock_connect.assert_called_once_with(
        user='user',
        private_key=b'key',
        account='account',
        database='database',
        warehouse=expected,
        role='loader',
        authenticator='SNOWFLAKE_JWT',
        autocommit=True,
        session_parameters={
            'QUOTED_IDENTIFIERS_IGNORE_CASE': 'FALSE',
            'QUERY_TAG': '{"attempt_id":"one"}',
            'ABORT_DETACHED_QUERY': 'TRUE',
        },
    )
    assert config == CONFIG


def test_conversion_warehouse_is_outside_the_recovery_identity():
    """A manual retry on the target's warehouse can resume an automatic attempt."""
    adapter = SnowflakeConversionQueryAdapter(dict(CONFIG), 'CONVERSION_WH')

    assert manual_recovery_identity(adapter.connection_config) == manual_recovery_identity(CONFIG)
