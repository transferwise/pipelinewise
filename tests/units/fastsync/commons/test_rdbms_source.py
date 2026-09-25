"""Tests for explicit RDBMS source lifecycle adapters."""
from argparse import Namespace
from unittest import mock

import pytest

from pipelinewise.fastsync.commons.rdbms_source import RdbmsSnowflakeSource


@pytest.mark.parametrize(
    ('tap_config', 'expected'),
    [({}, 'auto'), ({'engine': 'mysql'}, 'mysql'), ({'engine': 'mariadb'}, 'mariadb')],
)
def test_mysql_recovery_engine_identity(tap_config, expected):
    """Recovery records auto mode without connecting to detect the server."""
    adapter = RdbmsSnowflakeSource.mysql(mock.Mock(), mock.Mock())

    assert adapter.source_engine(Namespace(tap=tap_config)) == expected


@pytest.mark.parametrize('iceberg_version', [None, 2])
def test_postgres_hstore_projection(iceberg_version):
    """PostgreSQL exports hstore as JSON only for managed Iceberg."""
    source = mock.Mock()
    factory = mock.Mock(return_value=source)
    mapper = mock.Mock()
    adapter = RdbmsSnowflakeSource.postgres(factory, mapper)
    args = Namespace(tap={'dbname': 'source'}, transform={'transformations': []})

    assert adapter.create(args, iceberg_version) is source

    factory.assert_called_once_with(args.tap, mapper)
    assert source.hstore_as_json is (iceberg_version is not None)
    assert source.source_transformations is args.transform
    # The route's validated version is passed in; the adapter never reads target config itself.
    assert source.target_iceberg_version == iceberg_version


@pytest.mark.parametrize('iceberg_version', [None, 2])
def test_mysql_source_transformations(iceberg_version):
    """Both Snowflake formats pass transformation config to the source export."""
    factory = mock.Mock()
    mapper = mock.Mock()
    adapter = RdbmsSnowflakeSource.mysql(factory, mapper)
    args = Namespace(tap={'dbname': 'source'}, transform={'transformations': []})

    source = adapter.create(args, iceberg_version)

    factory.assert_called_once_with(args.tap, mapper)
    assert source.source_transformations is args.transform
    # The route's validated version is passed in; the adapter never reads target config itself.
    assert source.target_iceberg_version == iceberg_version
