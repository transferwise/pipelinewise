"""Exact metadata validation for manual Snowflake Iceberg conversion."""

from decimal import Decimal

import pytest

from pipelinewise.fastsync.commons.snowflake_iceberg import SnowflakeObjectName
from pipelinewise.fastsync.commons.snowflake_iceberg_validation import (
    NativeToIcebergConversionError,
    assert_managed_v3,
)


@pytest.mark.parametrize(
    'metadata_version',
    (True, 3.0, 3.5, Decimal('3'), Decimal('3.5')),
)
def test_v3_metadata_requires_exact_integer(metadata_version):
    """Converter metadata validation never truncates numeric version values."""
    queries = []

    def query(sql, **_kwargs):
        queries.append(sql)
        if sql.startswith('SHOW ICEBERG TABLES'):
            return [{'name': 'TABLE', 'catalog_name': 'SNOWFLAKE'}]
        if sql.startswith("SHOW PARAMETERS LIKE 'ICEBERG_VERSION'"):
            return [{'key': 'ICEBERG_VERSION', 'value': metadata_version}]
        raise AssertionError(f'Unexpected query after invalid version: {sql}')

    with pytest.raises(
        NativeToIcebergConversionError,
        match='not managed Iceberg version 3',
    ):
        assert_managed_v3(
            query,
            SnowflakeObjectName('DATABASE', 'SCHEMA', 'TABLE'),
        )

    assert len(queries) == 2
