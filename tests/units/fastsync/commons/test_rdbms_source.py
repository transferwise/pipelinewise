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


@pytest.mark.parametrize('iceberg_requested', [False, True])
def test_postgres_hstore_projection(iceberg_requested):
    """PostgreSQL exports hstore as JSON only for managed Iceberg."""
    source = mock.Mock()
    factory = mock.Mock(return_value=source)
    mapper = mock.Mock()
    adapter = RdbmsSnowflakeSource.postgres(factory, mapper)
    args = Namespace(tap={'dbname': 'source'})

    assert adapter.create(args, iceberg_requested) is source

    factory.assert_called_once_with(args.tap, mapper)
    assert source.hstore_as_json is iceberg_requested
