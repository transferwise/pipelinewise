"""Tests for the managed-Iceberg physical VARCHAR width contract."""

import pytest

from pipelinewise.fastsync.commons.snowflake_iceberg import IcebergColumn, IcebergTableSpec
from pipelinewise.fastsync.commons.snowflake_iceberg_validation import (
    NativeToIcebergConversionError,
    SnowflakeTableName,
    assert_iceberg_table_spec,
)


def test_converter_rejects_narrow_varchar():
    """Conversion cannot adopt a companion with a narrow physical string."""
    table = SnowflakeTableName('DATABASE', 'SCHEMA', 'TABLE_ICEBERG')
    expected = IcebergTableSpec(table, (IcebergColumn('BODY', 'VARCHAR'),))
    queries = []

    def query(sql, params=None, phase=None):
        queries.append((sql, params, phase))
        if 'INFORMATION_SCHEMA"."COLUMNS' in sql:
            return [{
                'COLUMN_NAME': 'BODY',
                'DATA_TYPE': 'TEXT',
                'CHARACTER_MAXIMUM_LENGTH': 16777216,
                'IS_NULLABLE': 'YES',
            }]
        return []

    with pytest.raises(NativeToIcebergConversionError) as error:
        assert_iceberg_table_spec(query, table, expected)

    assert 'ALTER ICEBERG TABLE' in str(error.value)
    assert 'recreate the table' in str(error.value)
    assert 'CHARACTER_MAXIMUM_LENGTH' in queries[0][0]
