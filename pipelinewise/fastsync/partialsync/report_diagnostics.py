"""Shareable metadata-report errors without driver messages or source values."""

import json
import re
import sys
import traceback

import psycopg2
import pymysql
from snowflake.connector.errors import Error as SnowflakeError

from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import TableFormatDiscoveryError
from pipelinewise.fastsync.commons.tap_postgres import UnsupportedPostgresVersionError


class ReportMetadataError(ValueError):
    """Required metadata is absent or cannot be interpreted safely."""


_OPERATIONS = frozenset({
    'source_connection', 'target_connection', 'target_format', 'source_columns',
    'target_columns', 'compare_columns', 'table_report', 'build_report',
})
_EXPECTED_ERRORS = (psycopg2.Error, pymysql.MySQLError, SnowflakeError, TableFormatDiscoveryError,
                    ReportMetadataError, UnsupportedPostgresVersionError, OSError)
_IDENTIFIER = re.compile(r'[A-Za-z_][A-Za-z_0-9]{0,99}')
_QUERY_ID = re.compile(r'[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}')


def _database_codes(error):
    """Include only constrained database identifiers, never free-form messages."""
    codes = {}
    if isinstance(error, (pymysql.MySQLError, SnowflakeError)):
        errno = error.args[0] if isinstance(error, pymysql.MySQLError) and error.args else getattr(error, 'errno', None)
        if type(errno) is int and 0 < errno < 1000000:
            codes['errno'] = errno
    if isinstance(error, (psycopg2.Error, SnowflakeError)):
        attribute = 'pgcode' if isinstance(error, psycopg2.Error) else 'sqlstate'
        sqlstate = getattr(error, attribute, None)
        if isinstance(sqlstate, str) and re.fullmatch(r'[0-9A-Z]{5}', sqlstate):
            codes['sqlstate'] = sqlstate
    if isinstance(error, SnowflakeError):
        query_id = getattr(error, 'sfqid', None)
        if isinstance(query_id, str) and _QUERY_ID.fullmatch(query_id):
            codes['query_id'] = query_id
    return codes


def _traceback_locations(error):
    """Keep bounded code locations, excluding paths, source lines, and locals."""
    locations = []
    for frame, line in traceback.walk_tb(error.__traceback__):
        module = frame.f_globals.get('__name__', '')
        if module == '__main__':
            module = getattr(frame.f_globals.get('__spec__'), 'name', '')
        if not isinstance(module, str) or not module.startswith('pipelinewise.'):
            continue
        if not all(_IDENTIFIER.fullmatch(part) for part in module.split('.')):
            continue
        function = frame.f_code.co_name
        locations.append({'module': module, 'function': function if _IDENTIFIER.fullmatch(function) else '<code>',
                          'line': line})
    return locations[-10:]


def report_error(error, operation):
    """Return safe JSON fields and write a sanitized diagnostic to stderr.

    Expected database and metadata errors need only their stage and error codes.
    Internal errors also include PipelineWise traceback locations on stderr.
    """
    expected = isinstance(error, _EXPECTED_ERRORS)
    error_type = type(error).__name__
    reason = ('Metadata operation failed; check database access, connectivity and configuration'
              if expected else 'Unexpected report error; provide the sanitized stderr diagnostic to maintainers')
    if isinstance(error, UnsupportedPostgresVersionError):
        reason = 'PostgreSQL 11.2 or later is required'
    result = {
        'status': 'error',
        'operation': operation if operation in _OPERATIONS else 'build_report',
        'error_type': error_type if _IDENTIFIER.fullmatch(error_type) else 'Exception',
        'error_kind': 'metadata' if expected else 'internal',
        'reason': reason,
        **_database_codes(error),
    }
    diagnostic = dict(result)
    if not expected:
        diagnostic['traceback_locations'] = _traceback_locations(error)
    print(json.dumps(diagnostic), file=sys.stderr)
    return result
