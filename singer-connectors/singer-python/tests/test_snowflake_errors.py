"""Sanitize SDK diagnostics without adding an SDK dependency to Singer."""

import logging
import traceback

import pytest

from singer.snowflake_errors import sanitize_snowflake_error, sanitize_snowflake_errors


class Error(Exception):
    """Model the SDK message fields; real SDK boundaries are tested by its users."""

    __module__ = 'snowflake.connector.errors'

    def __init__(self, message, code=100090, state='42P18', query_id='dummy-query-id'):
        super().__init__(message)
        self.msg = self.raw_msg = message
        self.errno, self.sqlstate, self.sfqid = code, state, query_id
        self.query = message

    def __str__(self):
        return self.msg


@pytest.mark.parametrize('code,state,message', [
    (100090, '42P18', 'Duplicate key CANARY. Row Values: [CANARY]'),
    (100090, '42P18', 'Duplicate key CANARY.\r\nRow Values:\r\n[CANARY]'),
    (100038, '22018', 'Numeric value CANARY is not recognized'),
    (100039, '22003', 'Numeric value CANARY is out of range'),
    (123, 'XX000', 'Server failure. rOw VaLuEs: [CANARY]'),
])
def test_data_error_has_no_value_details_in_any_message_field(code, state, message):
    error = Error(message, code, state)
    error.__cause__ = ValueError('CANARY')
    error.__context__ = RuntimeError('CANARY')
    error.add_note('CANARY')
    sanitize_snowflake_error(error)

    assert type(error) is Error
    assert (error.errno, error.sqlstate, error.sfqid) == (code, state, 'dummy-query-id')
    assert error.query is None
    assert '[REDACTED]' in str(error) and 'dummy-query-id' in str(error)
    assert 'CANARY' not in str(error) + repr(error) + error.raw_msg + str(error.args)
    assert 'CANARY' not in ''.join(traceback.format_exception(error))
    assert error.__cause__ is error.__context__ is None
    sanitize_snowflake_error(error)
    assert error.msg == error.raw_msg == error.args[0]


def test_other_sdk_errors_and_application_errors_keep_their_diagnostics():
    for error in (
        Error('Object does not exist', 2043, '02000'),
        Error('Object "Row Values" does not exist', 2043, '02000'),
        ValueError('Row Values: application context'),
    ):
        message = str(error)
        sanitize_snowflake_error(error)
        assert str(error) == message


def test_marker_in_formatted_sdk_message_is_also_redacted():
    error = Error('Original server diagnostic', 123, 'XX000')
    error.msg += '\nRow Values: [CANARY]'
    sanitize_snowflake_error(error)
    assert 'CANARY' not in str(error) + repr(error)


def test_untrusted_metadata_is_not_interpolated_into_diagnostics():
    error = Error('Row Values: [CANARY]', 'CANARY', 'CANARY', 'CANARY\nsource-value')
    sanitize_snowflake_error(error)
    assert 'CANARY' not in str(error)
    assert error.errno == 'CANARY' and error.sqlstate == 'CANARY'


def test_boundary_keeps_return_values_and_non_sdk_failure_behavior():
    @sanitize_snowflake_errors
    def success(value):
        return value

    @sanitize_snowflake_errors
    def failure(error):
        raise error

    sentinel = object()
    assert success(sentinel) is sentinel
    for error in (Error('Row Values: [CANARY]'), ValueError('application CANARY')):
        with pytest.raises(type(error)) as caught:
            failure(error)
        assert caught.value is error
    assert str(error) == 'application CANARY'


def test_boundary_disables_sdk_response_debug_logging_without_hiding_app_logs(monkeypatch, caplog):
    parent = logging.getLogger('snowflake.connector')
    child = logging.getLogger('snowflake.connector.cursor')
    monkeypatch.setattr(parent, 'level', logging.DEBUG)
    monkeypatch.setattr(child, 'level', logging.DEBUG)

    @sanitize_snowflake_errors
    def execute():
        child.debug('Raw Snowflake response CANARY')
        logging.getLogger('singer.app').debug('Application diagnostic')

    with caplog.at_level(logging.DEBUG):
        execute()
    assert 'CANARY' not in caplog.text
    assert 'Application diagnostic' in caplog.text
