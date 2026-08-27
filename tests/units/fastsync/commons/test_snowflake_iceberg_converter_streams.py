"""Dependent-stream preflight tests for manual Iceberg conversion."""

import pytest

from pipelinewise.fastsync.commons.snowflake_iceberg_converter import (
    NativeToIcebergConversionError,
    SnowflakeNativeToIcebergConverter,
)
from tests.units.fastsync.commons.test_snowflake_iceberg_converter import (
    FakeSnowflake,
)


def _converter(tmp_path, snowflake):
    return SnowflakeNativeToIcebergConverter(snowflake, str(tmp_path))


@pytest.mark.parametrize(
    'stream_row',
    [
        {
            'database_name': 'OTHER_DATABASE',
            'schema_name': 'STREAMS',
            'name': 'CROSS_SCHEMA_STREAM',
            'table_name': 'DATABASE.SCHEMA.TABLE',
            'source_type': 'Table',
            'base_tables': 'DATABASE.SCHEMA.TABLE',
        },
        {
            'database_name': 'OTHER_DATABASE',
            'schema_name': 'STREAMS',
            'name': 'VIEW_STREAM',
            'table_name': 'DATABASE.OTHER_SCHEMA.TRACKED_VIEW',
            'source_type': 'View',
            'base_tables': (
                'DATABASE.OTHER_SCHEMA.OTHER_TABLE, DATABASE.SCHEMA.TABLE'
            ),
        },
    ],
)
def test_rejects_cross_schema_streams(tmp_path, stream_row):
    """Account enumeration catches direct and view-base dependencies."""
    snowflake = FakeSnowflake()
    snowflake.stream_rows = [stream_row]

    with pytest.raises(NativeToIcebergConversionError, match='dependent streams'):
        _converter(tmp_path, snowflake).convert('database.schema.table')

    stream_queries = [
        sql for sql, _, _ in snowflake.queries
        if sql.startswith('SHOW STREAMS')
    ]
    assert stream_queries == ['SHOW STREAMS IN ACCOUNT']
    assert not any(sql.startswith('CREATE') for sql, _, _ in snowflake.queries)


def test_rejects_malformed_stream_metadata(tmp_path):
    """Unreadable base-table metadata cannot hide a relevant view stream."""
    snowflake = FakeSnowflake()
    snowflake.stream_rows = [{
        'table_name': 'DATABASE.OTHER_SCHEMA.TRACKED_VIEW',
        'source_type': 'View',
        'base_tables': 'DATABASE.SCHEMA.TABLE, NOT_QUALIFIED',
    }]

    with pytest.raises(
        NativeToIcebergConversionError,
        match='stream dependency metadata',
    ):
        _converter(tmp_path, snowflake).convert('database.schema.table')

    assert not any(sql.startswith('CREATE') for sql, _, _ in snowflake.queries)


def test_ignores_exactly_unrelated_streams(tmp_path):
    """Similar fully qualified names do not create false dependencies."""
    snowflake = FakeSnowflake()
    snowflake.stream_rows = [{
        'table_name': 'DATABASE.SCHEMA.TABLE_SUFFIX',
        'source_type': 'View',
        'base_tables': 'DATABASE.SCHEMA.OTHER_TABLE',
    }]

    _converter(tmp_path, snowflake).convert('database.schema.table')

    assert snowflake.formats == {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}
