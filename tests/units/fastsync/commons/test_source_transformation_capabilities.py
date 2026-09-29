"""Source regex checks and preflight must agree with the actual exporter."""

import inspect
from unittest.mock import MagicMock, Mock

import pymysql
import pytest

from pipelinewise.fastsync.commons import tap_mysql, tap_postgres
from pipelinewise.fastsync.commons.source_transformations import UnsupportedSourceTransformation
from pipelinewise.fastsync.mysql_to_snowflake import tap_type_to_target_type as mysql_mapper
from pipelinewise.fastsync.postgres_to_snowflake import tap_type_to_target_type as postgres_mapper


def _mysql_source():
    source = tap_mysql.FastSyncTapMySql({}, mysql_mapper)
    source.conn_unbuffered = MagicMock()
    source.conn_unbuffered.cursor.return_value.__enter__.return_value.fetchone.return_value = (1,)
    return source


def test_regex_support_probe_uses_only_literals_and_is_cached():
    source = _mysql_source()
    source._validate_source_regex_support()
    source._validate_source_regex_support()
    cursor = source.conn_unbuffered.cursor.return_value.__enter__.return_value
    cursor.execute.assert_called_once_with(
        'SELECT CONVERT(%s USING utf8mb4) COLLATE utf8mb4_bin REGEXP CONVERT(%s USING utf8mb4)',
        ('a', r'(?-x)\A(?:a)\z'),
    )


@pytest.mark.parametrize('code', [1064, 1139, 1305])
def test_unsupported_regex_engine_has_actionable_error(code):
    source = _mysql_source()
    cursor = source.conn_unbuffered.cursor.return_value.__enter__.return_value
    cursor.execute.side_effect = pymysql.err.ProgrammingError(code, 'Unsupported regex')
    with pytest.raises(UnsupportedSourceTransformation, match='MySQL ICU or MariaDB PCRE'):
        source._validate_source_regex_support()
    assert not source._source_regex_verified


@pytest.mark.parametrize('result', [None, (0,), ('1',)])
def test_regex_engine_must_match_probe(result):
    source = _mysql_source()
    source.conn_unbuffered.cursor.return_value.__enter__.return_value.fetchone.return_value = result
    with pytest.raises(UnsupportedSourceTransformation, match='capability check failed'):
        source._validate_source_regex_support()


@pytest.mark.parametrize('code', [2006, 2013, 1205])
def test_regex_probe_preserves_connection_and_transient_errors(code):
    source = _mysql_source()
    cursor = source.conn_unbuffered.cursor.return_value.__enter__.return_value
    cursor.execute.side_effect = pymysql.err.OperationalError(code, 'Connection or transient failure')
    with pytest.raises(pymysql.err.OperationalError) as raised:
        source._validate_source_regex_support()
    assert raised.value.args[0] == code


def test_reopening_export_connection_resets_capability_cache(monkeypatch):
    source = _mysql_source()
    source._source_regex_verified = True
    source.get_connection_parameters = Mock(return_value=({}, False))
    source.run_session_sqls = Mock()
    connection = MagicMock()
    connection.get_server_info.return_value = '8.4.0'
    monkeypatch.setattr(tap_mysql.pymysql, 'connect', Mock(return_value=connection))
    source.open_connections()
    assert not source._source_regex_verified


@pytest.mark.parametrize('condition,stream,expected', [
    ({'regex_match': '.+'}, 'source-orders', True),
    ({'equals': 'a'}, 'source-orders', False),
    ({'regex_match': '.+'}, 'source-other', False),
])
def test_only_matching_regex_rules_need_capability_probe(condition, stream, expected):
    source = _mysql_source()
    source._validate_source_regex_support = Mock()
    source.source_transformations = {'transformations': [{
        'tap_stream_name': stream, 'field_id': 'secret', 'type': 'HASH',
        'when': [{'column': 'secret', **condition}],
    }]}
    columns = [{'column_name': 'secret', 'data_type': 'text', 'column_type': 'text', 'safe_sql_value': '`secret`'}]
    source._compile_source_projection('source.orders', columns)
    assert source._validate_source_regex_support.call_count == int(expected)


@pytest.mark.parametrize('engine', ['postgres', 'mysql', 'mariadb'])
def test_preflight_and_export_use_identical_metadata_options(monkeypatch, engine):
    module = tap_postgres if engine == 'postgres' else tap_mysql
    source = (
        tap_postgres.FastSyncTapPostgres({}, postgres_mapper) if engine == 'postgres'
        else tap_mysql.FastSyncTapMySql({'engine': engine}, mysql_mapper)
    )
    signature = inspect.signature(source.get_table_columns)
    column = {'column_name': 'id', 'data_type': 'integer', 'column_type': 'int', 'safe_sql_value': 'id'}
    source.source_transformations = {'transformations': []}
    source.get_table_columns = Mock(return_value=[column])
    source.curr = Mock()
    source.conn_unbuffered = MagicMock()
    source.conn_unbuffered.cursor.return_value.__enter__.return_value.fetchmany.return_value = []
    monkeypatch.setattr(module.split_gzip, 'open', MagicMock())

    source.validate_source_transformations('source.orders')
    source.copy_table('source.orders', '/unused-test-export.csv.gz')

    options = []
    for call in source.get_table_columns.call_args_list:
        bound = signature.bind(*call.args, **call.kwargs)
        bound.apply_defaults()
        options.append(bound.arguments)
    assert options == [
        {'table_name': 'source.orders', 'max_num': None, 'date_type': 'date', 'metadata_query': None},
        {'table_name': 'source.orders', 'max_num': None, 'date_type': 'date', 'metadata_query': None},
    ]
