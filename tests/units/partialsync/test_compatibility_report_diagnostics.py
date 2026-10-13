"""Report diagnostics keep useful codes and locations without exposing secrets."""

import json
from importlib.machinery import ModuleSpec

import psycopg2
import pymysql
import pytest
from snowflake.connector.errors import ProgrammingError

from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import TableFormatDiscoveryError
from pipelinewise.fastsync.commons.tap_postgres import UnsupportedPostgresVersionError
from pipelinewise.fastsync.partialsync.report_diagnostics import ReportMetadataError, report_error


@pytest.mark.parametrize(('error', 'codes'), [
    (pymysql.OperationalError(2006, 'secret endpoint password'), {'errno': 2006}),
    (pymysql.OperationalError(1142, 'secret endpoint password'), {'errno': 1142}),
    (psycopg2.OperationalError('secret endpoint password'), {}),
    (ProgrammingError(msg='secret endpoint password', errno=2003, sqlstate='42S02',
                      sfqid='019b1425-0123-abcd-0000-123456789abc'),
     {'errno': 2003, 'sqlstate': '42S02', 'query_id': '019b1425-0123-abcd-0000-123456789abc'}),
    (ReportMetadataError('secret metadata'), {}),
    (TableFormatDiscoveryError('secret metadata'), {}),
    (OSError('secret endpoint'), {}),
])
def test_expected_errors_preserve_safe_diagnostics_only(error, codes, capsys):
    result = report_error(error, 'source_columns')
    output = capsys.readouterr()
    assert output.out == ''
    assert json.loads(output.err) == result
    assert result['operation'] == 'source_columns'
    assert result['error_kind'] == 'metadata'
    assert result['error_type'] == type(error).__name__
    assert {key: value for key, value in result.items() if key in ('errno', 'sqlstate', 'query_id')} == codes
    assert 'secret' not in json.dumps(result) + output.err
    assert 'traceback_locations' not in result


@pytest.mark.parametrize('error_type', [TypeError, KeyError, AttributeError, ValueError])
def test_internal_errors_are_not_reported_as_invalid_input(error_type, capsys):
    error = error_type('secret endpoint password')
    result = report_error(error, 'compare_columns')
    output = capsys.readouterr()
    assert result['status'] == 'error'
    assert result['error_kind'] == 'internal'
    assert result['error_type'] == error_type.__name__
    assert json.loads(output.err)['traceback_locations'] == []
    assert 'secret' not in json.dumps(result) + output.err
    assert 'Invalid report input' not in output.err


@pytest.mark.parametrize(('errno', 'sqlstate', 'query_id'), [
    (True, '42S0\n', 'secret'),
    ('2006', 'secret', 'https://secret.example'),
    (-1, '123456', '019b1425-0123-abcd-0000-123456789abc\n'),
    (1000000, None, None),
])
def test_unvalidated_driver_attributes_cannot_leak(errno, sqlstate, query_id, capsys):
    error = ProgrammingError(msg='secret', errno=2003)
    error.errno, error.sqlstate, error.sfqid = errno, sqlstate, query_id
    result = report_error(error, 'target_columns')
    output = capsys.readouterr()
    assert {'errno', 'sqlstate', 'query_id'}.isdisjoint(result)
    assert 'secret' not in json.dumps(result) + output.err


def test_postgres_sqlstate_is_preserved_without_driver_diagnostics(capsys):
    class PermissionError(psycopg2.ProgrammingError):
        pgcode = '42501'

    result = report_error(PermissionError('secret relation and user'), 'source_columns')
    assert result['sqlstate'] == '42501'
    assert 'secret' not in capsys.readouterr().err


def test_unsupported_postgres_is_a_prerequisite_failure_with_a_safe_reason(capsys):
    error = UnsupportedPostgresVersionError('secret endpoint or credentials')
    result = report_error(error, 'source_connection')
    assert isinstance(error, RuntimeError)
    assert result['error_kind'] == 'metadata'
    assert result['error_type'] == 'UnsupportedPostgresVersionError'
    assert result['reason'] == 'PostgreSQL 11.2 or later is required'
    output = capsys.readouterr()
    assert json.loads(output.err) == result
    assert 'secret' not in json.dumps(result) + output.err


def test_traceback_excludes_source_locals_paths_and_chained_exception(capsys):
    # A compiled frame models production code without relying on another module's error behavior.
    namespace = {'__name__': 'pipelinewise.fastsync.partialsync.compatibility_report'}
    code = compile(
        "def fail():\n    password = 'secret password'\n    raise KeyError(password) from ValueError('secret cause')\n",
        '/private/secret-user/secret-host/config.py', 'exec',
    )
    exec(code, namespace)
    try:
        namespace['fail']()
    except KeyError as error:
        result = report_error(error, 'compare_columns')
    output = capsys.readouterr()
    locations = json.loads(output.err)['traceback_locations']
    assert locations == [{'module': namespace['__name__'], 'function': 'fail', 'line': 3}]
    assert 'traceback_locations' not in result
    assert 'secret' not in json.dumps(result) + output.err
    assert 'ValueError' not in output.err


def test_unknown_operation_does_not_echo_caller_input(capsys):
    result = report_error(TypeError('secret'), 'secret endpoint')
    assert result['operation'] == 'build_report'
    assert 'secret' not in capsys.readouterr().err


@pytest.mark.parametrize('module_name', [
    'pipelinewise.fastsync.partialsync.compatibility_report', 'secret.invalid-module', None,
])
def test_module_cli_frames_use_only_validated_spec_names(module_name, capsys):
    namespace = {'__name__': '__main__', '__spec__': ModuleSpec(module_name, None) if module_name else None}
    code = compile("def fail():\n    raise TypeError('secret password')\n", '/secret/config.py', 'exec')
    exec(code, namespace)
    try:
        namespace['fail']()
    except TypeError as error:
        report_error(error, 'build_report')
    output = capsys.readouterr()
    expected = []
    if module_name and module_name.startswith('pipelinewise.'):
        expected.append({'module': module_name, 'function': 'fail', 'line': 2})
    assert json.loads(output.err)['traceback_locations'] == expected
    assert 'secret' not in output.err
