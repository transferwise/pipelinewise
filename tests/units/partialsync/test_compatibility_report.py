"""The deployment report shares native guards and never enters a sync lifecycle."""

from contextlib import contextmanager
import json
from unittest import mock

import pytest

from pipelinewise.fastsync.partialsync import compatibility_report as report
from pipelinewise.fastsync.partialsync import utils


def catalog(*tables):
    return {'streams': [{
        'tap_stream_id': f'db-{table}', 'table_name': table,
        'metadata': [{'breadcrumb': [], 'metadata': {'schema-name': 'source', 'selected': True}}],
    } for table in tables]}


def selection(*tables):
    return [{'tap_stream_id': f'db-{table}', 'sync_start_from': {'column': 'id', 'value': '1'}} for table in tables]


def column(name, **metadata):
    return {'column_name': name, 'data_type': json.dumps(metadata)}


def test_default_selection_uses_bounded_selected_streams_without_splitting_ids():
    properties = catalog('orders-with-hyphens', 'other', 'disabled')
    properties['streams'][2]['metadata'][0]['metadata']['selected'] = False
    assert report.selected_tables(properties, selection('orders-with-hyphens', 'disabled')) == [
        'source.orders-with-hyphens',
    ]


def test_explicit_tables_can_include_selected_unbounded_streams():
    assert report.selected_tables(catalog('orders', 'other'), selection('orders'), ['source.other']) == ['source.other']
    with pytest.raises(ValueError, match='must be selected'):
        report.selected_tables(catalog('orders'), selection('orders'), ['source.unselected'])


def test_case_different_stream_ids_do_not_omit_one_of_the_selected_tables():
    properties = catalog('MixedCase', 'other')
    configured = selection('MixedCase', 'other')
    configured[0]['tap_stream_id'] = configured[0]['tap_stream_id'].upper()
    assert report.selected_tables(properties, configured) == ['source.MixedCase', 'source.other']


def test_mysql_selection_uses_database_metadata():
    properties = catalog('orders')
    metadata = properties['streams'][0]['metadata'][0]['metadata']
    metadata['database-name'] = metadata.pop('schema-name')
    assert report.selected_tables(properties, selection('orders')) == ['source.orders']


@pytest.mark.parametrize('table', ['has.dot', ''])
def test_ambiguous_identifiers_are_rejected(table):
    with pytest.raises(ValueError, match='unambiguous'):
        report.selected_tables(catalog(table), selection(table))


def test_report_collects_all_incompatible_columns_and_safe_changes():
    result = utils.report_source_target_columns(
        {'schema': 'TARGET', 'table': 'ORDERS'},
        ['"NUMBER" NUMBER', '"TIMESTAMP" TIMESTAMP_NTZ', '"TEXT" VARCHAR(134217728)',
         '"ADDED" BOOLEAN', '"OK" FLOAT'],
        [column('NUMBER', type='FIXED', precision=10, scale=0),
         column('TIMESTAMP', type='TIMESTAMP_TZ', scale=9),
         column('TEXT', type='TEXT', length=1024), column('OK', type='REAL')],
    )
    assert [row['status'] for row in result] == [
        'incompatible', 'incompatible', 'would_widen', 'would_add', 'compatible',
    ]
    assert result[0]['target_type'] == 'NUMBER(10,0)'
    assert 'FullSync' in result[0]['reason']


def test_report_continues_after_invalid_column_metadata():
    rows = [column('BAD', type='FIXED'), column('OK', type='BOOLEAN')]
    result = utils.report_source_target_columns(
        {'schema': 'T', 'table': 'T'}, ['"BAD" NUMBER', '"OK" BOOLEAN'], rows,
    )
    assert [row['status'] for row in result] == ['incompatible', 'compatible']


@pytest.mark.parametrize(('tap_type', 'metadata', 'expected'), [
    ('tap-postgres', [('text', 'text', 'text', None), ('hstore', 'hstore', 'hstore', None),
                      ('id', 'integer', 'id', None)],
     ['"TEXT" VARCHAR(134217728)', '"HSTORE" VARCHAR(134217728)', '"ID" NUMBER']),
    ('tap-mysql', [{'column_name': 'text', 'data_type': 'longtext', 'column_type': 'longtext'},
                   {'column_name': 'id', 'data_type': 'tinyint', 'column_type': 'tinyint(1)'}],
     ['"TEXT" VARCHAR(134217728)', '"ID" BOOLEAN']),
])
def test_source_mapping_uses_only_parameterized_metadata_reads(tap_type, metadata, expected):
    connection = mock.MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = metadata
    source = report.native_source(connection, tap_type, {'engine': 'mysql'}, {})
    assert report.mapped_source_columns(connection, source, 'schema.orders') == expected
    sql, parameters = cursor.execute.call_args.args
    assert 'FROM information_schema.columns' in sql
    assert parameters == ('schema', 'orders')
    assert connection.mock_calls == [
        mock.call.cursor(*((report.pymysql.cursors.DictCursor,) if tap_type == 'tap-mysql' else ())),
        mock.call.cursor().__enter__(), mock.call.cursor().__enter__().execute(sql, parameters),
        mock.call.cursor().__enter__().fetchall(), mock.call.cursor().__exit__(None, None, None),
    ]


def test_empty_source_metadata_does_not_claim_a_compatible_schema():
    connection = mock.MagicMock()
    connection.cursor.return_value.__enter__.return_value.fetchall.return_value = []
    source = report.native_source(connection, 'tap-mysql', {'engine': 'mysql'}, {})
    with pytest.raises(ValueError, match='missing or inaccessible'):
        report.mapped_source_columns(connection, source, 'schema.orders')


def test_mysql_report_connection_is_read_only_autocommit_and_ignores_custom_sql():
    config = {'host': 'primary', 'replica_host': 'replica', 'port': 3306, 'user': 'u', 'password': 'secret',
              'session_sqls': ['DROP TABLE dangerous'], 'engine': 'mariadb'}
    with mock.patch.object(report.pymysql, 'connect') as connect, \
            mock.patch.object(report.FastSyncTapMySql, 'open_connections') as sync_open:
        with report.source_connection('tap-mysql', config) as connection:
            assert connection is connect.return_value
        assert connect.call_args.kwargs['host'] == 'replica'
        assert connect.call_args.kwargs['autocommit'] is True
        assert connect.call_args.kwargs['init_command'] == 'SET SESSION TRANSACTION READ ONLY'
        assert connect.return_value.mock_calls == [mock.call.close()]
        sync_open.assert_not_called()


def test_postgres_report_connection_is_read_only_and_closes_on_failure():
    with mock.patch.object(report.FastSyncTapPostgres, 'get_connection') as connect:
        with pytest.raises(RuntimeError):
            with report.source_connection('tap-postgres', {'session_sqls': ['DROP TABLE dangerous']}):
                raise RuntimeError('probe failed')
        connection = connect.return_value
        connection.set_session.assert_called_once_with(readonly=True, autocommit=True)
        connection.cursor.return_value.__enter__.return_value.execute.assert_called_once_with(
            'SET statement_timeout = 30000',
        )
        connection.close.assert_called_once_with()


@pytest.mark.parametrize(('tap_type', 'target_type', 'target', 'reason'), [
    ('tap-mongodb', 'target-snowflake', {}, 'Only tap-postgres/tap-mysql'),
    ('tap-postgres', 'target-postgres', {}, 'Only tap-postgres/tap-mysql'),
    ('tap-postgres', 'target-snowflake', {'target_table_format': 'iceberg'}, 'Configured Iceberg'),
])
def test_unsupported_routes_skip_without_any_connection(tap_type, target_type, target, reason):
    with mock.patch.object(report, 'source_connection') as source:
        result = report.build_report(
            tap_type, {}, target, catalog('orders'), selection('orders'), target_type=target_type,
        )
        assert result[0]['status'] == 'skipped'
        assert reason in result[0]['reason']
        source.assert_not_called()


def test_empty_selection_does_not_open_connections():
    with mock.patch.object(report, 'source_connection') as source:
        result = report.build_report('tap-postgres', {}, {}, catalog('orders'), [])
        assert result[0]['status'] == 'skipped'
        source.assert_not_called()


def test_default_replacement_report_skips_without_opening_connections():
    configured = selection('orders')
    configured[0]['tap_stream_id'] = configured[0]['tap_stream_id'].upper()
    configured[0]['sync_start_from']['drop_target_table'] = True
    with mock.patch.object(report, 'source_connection') as source:
        result = report.build_report('tap-postgres', {}, {}, catalog('orders'), configured)
    assert result[0]['status'] == 'skipped'
    assert result[0]['source_table'] == 'source.orders'
    assert 'replaces this target' in result[0]['reason']
    source.assert_not_called()


@pytest.mark.parametrize('explicit', [False, True])
def test_replacement_skip_does_not_hide_other_tables_and_explicit_tables_check_merge_types(explicit):
    configured = selection('replaced', 'merged')
    configured[0]['sync_start_from']['drop_target_table'] = True
    requested = ['source.replaced', 'source.merged'] if explicit else None
    with mock.patch.object(report, 'source_connection') as source, \
            mock.patch.object(report, '_table_report') as check:
        check.side_effect = lambda _connection, _tap_type, _target, table: {
            'source_table': table, 'status': 'compatible',
        }
        result = report.build_report(
            'tap-postgres', {}, {}, catalog('replaced', 'merged'), configured, tables=requested,
        )
    expected = {'source.merged': 'compatible', 'source.replaced': 'compatible' if explicit else 'skipped'}
    assert {row['source_table']: row['status'] for row in result} == expected
    assert [call.args[3] for call in check.call_args_list] == (
        ['source.merged', 'source.replaced'] if explicit else ['source.merged']
    )
    source.assert_called_once_with('tap-postgres', {})


@pytest.mark.parametrize(('table_format', 'status'), [('missing', 'missing_target'), ('iceberg_v3', 'skipped')])
def test_missing_and_non_native_tables_never_read_source_columns(table_format, status):
    target = report.MetadataSnowflakeClient({'dbname': 'db', 'default_target_schema': 'mapped'})
    with mock.patch.object(report, 'SnowflakeTableInspector') as inspector, \
            mock.patch.object(report, 'mapped_source_columns') as source:
        inspector.return_value.discover_table_format.return_value = table_format
        result = report._table_report(mock.Mock(), 'tap-postgres', target, 'source.orders')
        assert result['status'] == status
        assert result['target_table'] == '"DB"."MAPPED"."ORDERS"'
        source.assert_not_called()


def test_native_target_uses_only_quoted_metadata_query_and_existing_guard():
    target = report.MetadataSnowflakeClient({'dbname': 'db', 'schema_mapping': {'source': {'target_schema': 'mapped'}}})
    with mock.patch.object(report, 'SnowflakeTableInspector') as inspector, \
            mock.patch.object(report, 'mapped_source_columns', return_value=['"ID" NUMBER']), \
            mock.patch.object(target, 'query', return_value=[column('ID', type='REAL')]) as query:
        inspector.return_value.discover_table_format.return_value = 'native'
        result = report._table_report(mock.Mock(), 'tap-postgres', target, 'source.orders')
        assert result['status'] == 'incompatible'
        query.assert_called_once_with('SHOW COLUMNS IN TABLE "DB"."MAPPED"."ORDERS"')


def test_table_errors_are_redacted_and_do_not_hide_remaining_tables():
    @contextmanager
    def source(*_):
        yield mock.Mock()

    with mock.patch.object(report, 'source_connection', side_effect=source), \
            mock.patch.object(report, '_table_report', side_effect=[
                RuntimeError('secret password'), {'source_table': 'source.second', 'status': 'compatible'},
            ]):
        result = report.build_report('tap-postgres', {}, {}, catalog('first', 'second'), selection('first', 'second'))
    assert [row['status'] for row in result] == ['error', 'compatible']
    assert 'secret' not in json.dumps(result)


def test_source_connection_failure_is_redacted():
    with mock.patch.object(report, 'source_connection', side_effect=RuntimeError('secret password')):
        result = report.build_report('tap-mysql', {}, {}, catalog('orders'), selection('orders'))
    assert result[0]['status'] == 'error'
    assert 'secret' not in json.dumps(result)


@pytest.mark.parametrize('operation', ['target_format', 'source_columns', 'target_columns', 'compare_columns'])
def test_table_failure_identifies_operation_and_database_code(operation, capsys):
    target = report.MetadataSnowflakeClient({'dbname': 'db', 'default_target_schema': 'mapped'})
    error = report.pymysql.OperationalError(2013, 'secret endpoint and password')
    with mock.patch.object(report, 'SnowflakeTableInspector') as inspector, \
            mock.patch.object(report, 'mapped_source_columns', return_value=['"ID" NUMBER']) as source, \
            mock.patch.object(target, 'query', return_value=[column('ID', type='REAL')]) as columns, \
            mock.patch.object(report, 'report_source_target_columns', return_value=[]) as compare:
        inspector.return_value.discover_table_format.return_value = 'native'
        steps = {'target_format': inspector.return_value.discover_table_format, 'source_columns': source,
                 'target_columns': columns, 'compare_columns': compare}
        steps[operation].side_effect = error
        result = report._table_report(mock.Mock(), mock.Mock(), target, 'source.orders')
    assert result['operation'] == operation
    assert result['error_kind'] == 'metadata'
    assert result['errno'] == 2013
    assert 'secret' not in json.dumps(result) + capsys.readouterr().err


def test_snowflake_metadata_queries_are_bounded_and_s3_free():
    target = report.MetadataSnowflakeClient({})
    with mock.patch.object(target, 'query_with_timeout', return_value=[]) as query:
        assert target.query('SHOW TABLES') == []
        query.assert_called_once_with('SHOW TABLES', None, 30)
    assert target.create_query_tag() == '{"ppw_component": "partialsync_compatibility_report"}'


@pytest.mark.parametrize(('status', 'expected'), [('compatible', 0), ('skipped', 0), ('incompatible', 1), ('error', 1)])
def test_cli_exit_status_and_json_output(tmp_path, capsys, status, expected):
    args = ['--tap-type', 'tap-postgres', '--tap-dir', str(tmp_path), '--target', str(tmp_path / 'target.json')]
    for name in ('config', 'target', 'properties', 'selection', 'inheritable_config'):
        path = tmp_path / f'{name}.json'
        path.write_text('{"selection": []}' if name == 'selection' else '{}')
    with mock.patch.object(report, 'build_report', return_value=[{'status': status}]):
        assert report.main(args) == expected
        assert json.loads(capsys.readouterr().out) == [{'status': status}]


def test_cli_bad_input_does_not_echo_file_contents_or_paths(tmp_path, capsys):
    path = tmp_path / 'config.json'
    path.write_text('secret password not json')
    args = ['--tap-type', 'tap-postgres', '--tap-dir', str(tmp_path), '--target', str(path)]
    assert report.main(args) == 2
    output = capsys.readouterr()
    assert output.out == ''
    assert 'secret' not in output.err


@pytest.mark.parametrize('exception', [TypeError, KeyError, AttributeError, ValueError])
def test_cli_execution_bug_is_not_mislabeled_as_invalid_input(tmp_path, capsys, exception):
    for name in ('config', 'target', 'properties', 'selection', 'inheritable_config'):
        (tmp_path / f'{name}.json').write_text('{"selection": []}' if name == 'selection' else '{}')
    previous_logging = report.logging.root.manager.disable
    with mock.patch.object(report, 'build_report', side_effect=exception('secret password')):
        assert report.main([
            '--tap-type', 'tap-postgres', '--tap-dir', str(tmp_path), '--target', str(tmp_path / 'target.json'),
        ]) == 1
    assert report.logging.root.manager.disable == previous_logging
    output = capsys.readouterr()
    result = json.loads(output.out)[0]
    assert result['error_kind'] == 'internal'
    assert result['operation'] == 'build_report'
    assert result['error_type'] == exception.__name__
    assert 'traceback_locations' in json.loads(output.err)
    assert 'Invalid report input' not in output.err
    assert 'secret' not in output.err + output.out


@pytest.mark.parametrize(('name', 'contents'), [
    ('selection', {}), ('selection', {'selection': [None]}),
    ('selection', {'selection': [{'tap_stream_id': 1}]}),
    ('selection', {'selection': [{'tap_stream_id': 's', 'sync_start_from': 'bad'}]}),
    ('properties', {'streams': {}}), ('properties', {'streams': [None]}),
    ('properties', {'streams': [{'metadata': 'bad'}]}),
    ('properties', {'streams': [{'metadata': [{'metadata': None}]}]}),
])
def test_cli_validates_input_shapes_before_metadata_connections(tmp_path, capsys, name, contents):
    files = {'config': {}, 'target': {}, 'properties': catalog('orders'),
             'selection': {'selection': selection('orders')}, 'inheritable_config': {}}
    files[name] = contents
    for filename, value in files.items():
        (tmp_path / f'{filename}.json').write_text(json.dumps(value))
    previous_logging = report.logging.root.manager.disable
    with mock.patch.object(report, 'source_connection') as source, \
            mock.patch.object(report, 'MetadataSnowflakeClient') as target:
        assert report.main([
            '--tap-type', 'tap-postgres', '--tap-dir', str(tmp_path), '--target', str(tmp_path / 'target.json'),
        ]) == 2
    source.assert_not_called()
    target.assert_not_called()
    assert report.logging.root.manager.disable == previous_logging
    assert 'Invalid report input' in capsys.readouterr().err


@pytest.mark.parametrize(('tap_type', 'target_type', 'target_config'), [
    ('tap-mongodb', 'target-snowflake', {}),
    ('tap-postgres', 'target-postgres', {}),
    ('tap-postgres', 'target-snowflake', {'target_table_format': 'iceberg'}),
])
def test_cli_skips_unsupported_routes_before_validating_relational_catalog(
    tmp_path, capsys, tap_type, target_type, target_config,
):
    properties = {'streams': [{'tap_stream_id': 'orders', 'stream': 'orders',
                              'metadata': [{'breadcrumb': [], 'metadata': {'selected': True}}]}]}
    files = {'config': {}, 'target': target_config, 'properties': properties,
             'selection': {'selection': []}, 'inheritable_config': {}}
    for filename, value in files.items():
        (tmp_path / f'{filename}.json').write_text(json.dumps(value))
    with mock.patch.object(report, 'source_connection') as source, \
            mock.patch.object(report, 'MetadataSnowflakeClient') as target:
        assert report.main([
            '--tap-type', tap_type, '--target-type', target_type,
            '--tap-dir', str(tmp_path), '--target', str(tmp_path / 'target.json'),
        ]) == 0
    source.assert_not_called()
    target.assert_not_called()
    output = capsys.readouterr()
    assert json.loads(output.out)[0]['status'] == 'skipped'
    assert output.err == ''


def test_cli_consumes_generated_selection_wrapper_and_target_overrides(tmp_path, capsys):
    files = {'config': {'host': 'host'}, 'target': {'dbname': 'DB', 'default_target_schema': 'default'},
             'properties': catalog('orders'), 'selection': {'selection': selection('orders')},
             'inheritable_config': {'default_target_schema': 'mapped', 'target_table_format': 'iceberg'}}
    for name, contents in files.items():
        (tmp_path / f'{name}.json').write_text(json.dumps(contents))
    with mock.patch.object(report, 'build_report', return_value=[{'status': 'skipped'}]) as build:
        assert report.main([
            '--tap-type', 'tap-postgres', '--tap-dir', str(tmp_path), '--target', str(tmp_path / 'target.json'),
        ]) == 0
    assert build.call_args.kwargs['target'] == {
        'dbname': 'DB', 'default_target_schema': 'mapped', 'target_table_format': 'iceberg',
    }
    assert build.call_args.kwargs['selection'] == selection('orders')
    assert json.loads(capsys.readouterr().out) == [{'status': 'skipped'}]
