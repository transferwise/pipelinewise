"""The metadata report uses the same native source configuration and mapping as FastSync."""

from argparse import Namespace
from unittest import mock

import pymysql
import pytest

from pipelinewise.fastsync import mysql_to_snowflake, postgres_to_snowflake
from pipelinewise.fastsync.commons.rdbms_source import MySqlSnowflakeSource, PostgresSnowflakeSource
from pipelinewise.fastsync.commons.tap_mysql import FastSyncTapMySql
from pipelinewise.fastsync.partialsync import compatibility_report as report


POSTGRES_COLUMNS = [
    ('hstore', 'hstore', '"hstore"', None),
    ('bit_one', 'bit', '"bit_one"', 1),
    ('bit_many', 'bit', '"bit_many"', 8),
    ('varbit', 'bit varying', '"varbit"', 8),
    ('date', 'date', '"date"', None),
]
MYSQL_COLUMNS = [
    {'column_name': 'json', 'data_type': 'json', 'column_type': 'json'},
    {'column_name': 'json_alias', 'data_type': 'longtext', 'column_type': 'longtext'},
    {'column_name': 'binary', 'data_type': 'varbinary', 'column_type': 'varbinary(32)'},
    {'column_name': 'boolean', 'data_type': 'tinyint', 'column_type': 'tinyint(1)'},
    {'column_name': 'number', 'data_type': 'tinyint', 'column_type': 'tinyint(4)'},
]


def runtime_source(tap_type, config, connection):
    """Exercise runtime factory and engine binding without a database connection."""
    module = postgres_to_snowflake if tap_type == 'tap-postgres' else mysql_to_snowflake
    source = module._source_adapter().create(
        Namespace(tap=config, target={'target_table_format': 'native'}, transform=None),
        iceberg_version=None,
    )
    if tap_type == 'tap-mysql':
        with mock.patch.object(pymysql, 'connect', return_value=connection), \
                mock.patch.object(source, 'run_session_sqls'):
            source.open_connections()
    return source


@pytest.mark.parametrize(('tap_type', 'engine', 'server_version'), [
    ('tap-postgres', None, None),
    ('tap-mysql', None, '8.4.0'),
    ('tap-mysql', None, '11.4.10-MariaDB-log'),
    ('tap-mysql', 'mysql', '8.4.0'),
    ('tap-mysql', 'mariadb', '11.4.10-MariaDB-log'),
    ('tap-mysql', 'mysql', '11.4.10-MariaDB-log'),
    ('tap-mysql', 'mariadb', '8.4.0'),
])
def test_report_and_runtime_share_native_configuration_query_and_mapping(tap_type, engine, server_version):
    config = {'host': 'source', 'port': 3306, 'user': 'reader', 'password': 'secret',
              'session_sqls': ['SELECT custom_session_setting']}
    if engine is not None:
        config['engine'] = engine
    original_config = config.copy()
    connection = mock.MagicMock()
    connection.get_server_info.return_value = server_version
    runtime = runtime_source(tap_type, config, connection)
    metadata = POSTGRES_COLUMNS if tap_type == 'tap-postgres' else MYSQL_COLUMNS
    with mock.patch.object(runtime, 'query', return_value=metadata) as runtime_query, \
            mock.patch.object(runtime, 'get_primary_keys', return_value=['ID']):
        expected = runtime.map_column_types_to_target('source.records')['columns']
    runtime_sql = runtime_query.call_args.args[0]
    runtime_params = runtime_query.call_args.kwargs['params']

    connection.reset_mock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = metadata
    source = report.native_source(connection, tap_type, config, {'target_table_format': 'native'})
    with mock.patch.object(source, 'query', side_effect=AssertionError('Runtime query must not execute')) as query, \
            mock.patch.object(source, 'get_primary_keys') as primary_keys:
        actual = report.mapped_source_columns(connection, source, 'source.records')
    assert actual == expected
    assert source.connection_config == runtime.connection_config
    assert config == original_config
    assert source.target_iceberg_version is None
    cursor.execute.assert_called_once_with(runtime_sql, runtime_params)
    query.assert_not_called()
    primary_keys.assert_not_called()
    if tap_type == 'tap-postgres':
        assert source.hstore_as_json is runtime.hstore_as_json is False
        assert actual == ['"HSTORE" VARCHAR(134217728)', '"BIT_ONE" BOOLEAN', '"BIT_MANY" BOOLEAN',
                          '"VARBIT" NUMBER', '"DATE" TIMESTAMP_NTZ']
        connection.cursor.assert_called_once_with()
    else:
        assert source.source_engine == runtime.source_engine
        assert source.uses_mariadb_json_aliases is runtime.uses_mariadb_json_aliases is False
        assert actual == ['"JSON" VARIANT', '"JSON_ALIAS" VARCHAR(134217728)', '"BINARY" BINARY',
                          '"BOOLEAN" BOOLEAN', '"NUMBER" NUMBER']
        assert 'information_schema.check_constraints' not in runtime_sql
        connection.cursor.assert_called_once_with(pymysql.cursors.DictCursor)


def test_report_obeys_postgres_runtime_factory_mapping_flags():
    original_create = PostgresSnowflakeSource.create

    def configure_mapping(adapter, args, iceberg_version):
        source = original_create(adapter, args, iceberg_version)
        source.hstore_as_json = True
        return source

    connection = mock.MagicMock()
    connection.cursor.return_value.__enter__.return_value.fetchall.return_value = POSTGRES_COLUMNS
    with mock.patch.object(PostgresSnowflakeSource, 'create', configure_mapping):
        runtime = runtime_source('tap-postgres', {}, connection)
        source = report.native_source(connection, 'tap-postgres', {}, {})
    assert report.mapped_source_columns(connection, source, 'source.records') == runtime.map_table_columns(
        POSTGRES_COLUMNS,
    )
    assert source.hstore_as_json is True
    assert runtime.map_table_columns(POSTGRES_COLUMNS)[0] == '"HSTORE" VARIANT'
    assert 'hstore_to_json' in connection.cursor.return_value.__enter__.return_value.execute.call_args.args[0]


def test_report_obeys_mysql_runtime_factory_flags_and_detected_mariadb_flavour():
    original_create = MySqlSnowflakeSource.create

    def configure_mapping(adapter, args, iceberg_version):
        source = original_create(adapter, args, iceberg_version)
        source.set_mariadb_json_aliases_enabled(True)
        return source

    connection = mock.MagicMock()
    connection.get_server_info.return_value = '11.4.10-MariaDB-log'
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [{'column_name': 'payload', 'data_type': 'json', 'column_type': 'longtext'}]
    with mock.patch.object(MySqlSnowflakeSource, 'create', configure_mapping):
        source = report.native_source(connection, 'tap-mysql', {}, {})
    assert source.is_mariadb
    assert source.uses_mariadb_json_aliases
    assert report.mapped_source_columns(connection, source, 'source.records') == ['"PAYLOAD" VARIANT']
    assert 'information_schema.check_constraints' in cursor.execute.call_args.args[0]


@pytest.mark.parametrize('tap_type', ['tap-postgres', 'tap-mysql'])
def test_report_binds_quoted_and_percent_metadata_identifiers(tap_type):
    connection = mock.MagicMock()
    connection.get_server_info.return_value = '11.4.10-MariaDB-log'
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = POSTGRES_COLUMNS if tap_type == 'tap-postgres' else MYSQL_COLUMNS
    source = report.native_source(connection, tap_type, {}, {})
    schema, table = "schema'_%", "table'_%"
    report.mapped_source_columns(connection, source, f'{schema}.{table}')
    sql, params = cursor.execute.call_args.args
    assert schema not in sql
    assert table not in sql
    assert params == (schema, table)
    assert 'table_schema = %s' in sql
    assert 'table_name = %s' in sql
    if tap_type == 'tap-mysql':
        assert '%%Y-%%m-01' in sql
        assert '"%Y-%m-01"' in sql % ("'schema'", "'table'")


@pytest.mark.parametrize('error_code', [2006, 2013])
def test_report_metadata_failure_never_reconnects_or_executes_custom_sql(error_code):
    connection = mock.MagicMock()
    connection.get_server_info.return_value = '11.4.10-MariaDB-log'
    cursor = connection.cursor.return_value.__enter__.return_value
    error = pymysql.err.OperationalError(error_code, 'connection unavailable')
    cursor.execute.side_effect = error
    with mock.patch.object(FastSyncTapMySql, 'query') as query, \
            mock.patch.object(FastSyncTapMySql, 'open_connections') as reconnect, \
            mock.patch.object(FastSyncTapMySql, 'run_session_sqls') as session_sqls:
        source = report.native_source(connection, 'tap-mysql', {'session_sqls': ['DROP TABLE private_data']}, {})
        with pytest.raises(pymysql.err.OperationalError) as failure:
            report.mapped_source_columns(connection, source, 'source.records')
    assert failure.value is error
    cursor.execute.assert_called_once()
    query.assert_not_called()
    reconnect.assert_not_called()
    session_sqls.assert_not_called()
