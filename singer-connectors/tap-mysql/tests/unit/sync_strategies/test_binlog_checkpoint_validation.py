"""Validate resume boundaries and report every selected stream needing repair."""

import copy
from unittest.mock import patch

import pytest
from pymysql.err import InternalError, OperationalError, ProgrammingError

from tap_mysql.sync_strategies import binlog


LOG_FILE = 'mysql-bin.000004'
ROTATED_HEADERS = [
    (LOG_FILE, 4, 'Format_desc', 1, 256, ''),
    (LOG_FILE, 256, 'Gtid_list', 1, 299, ''),
    (LOG_FILE, 299, 'Binlog_checkpoint', 1, 342, ''),
]


@pytest.fixture
def cursor():
    with patch.object(binlog, 'connect_with_backoff') as connect:
        yield connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value


@pytest.mark.parametrize('require_transaction_boundary', [False, True])
def test_rotated_mariadb_headers_preserve_the_next_transaction(cursor, require_transaction_boundary):
    cursor.fetchall.return_value = ROTATED_HEADERS + [(LOG_FILE, 342, 'Gtid', 1, 384, '')]

    binlog.verify_binlog_checkpoint(None, LOG_FILE, 4, require_transaction_boundary)

    cursor.execute.assert_called_once_with('SHOW BINLOG EVENTS IN %s FROM %s LIMIT 16', (LOG_FILE, 4))


@pytest.mark.parametrize('require_transaction_boundary', [False, True])
def test_rotated_mariadb_headers_without_transactions_are_resumable(cursor, require_transaction_boundary):
    cursor.fetchall.side_effect = [ROTATED_HEADERS, [(LOG_FILE, 342)]]

    binlog.verify_binlog_checkpoint(None, LOG_FILE, 4, require_transaction_boundary)

    assert cursor.execute.call_count == 2
    cursor.execute.assert_called_with('SHOW BINARY LOGS')


@pytest.mark.parametrize('require_transaction_boundary', [False, True])
@pytest.mark.parametrize('file_size', [341, 343])
def test_neutral_headers_require_exact_eof(cursor, require_transaction_boundary, file_size):
    cursor.fetchall.side_effect = [ROTATED_HEADERS, [(LOG_FILE, file_size)]]

    with pytest.raises(ValueError, match='safe transaction boundary.*full resync'):
        binlog.verify_binlog_checkpoint(None, LOG_FILE, 4, require_transaction_boundary)


@pytest.mark.parametrize('require_transaction_boundary', [False, True])
@pytest.mark.parametrize('event_type, end_pos', [('Unknown_event', 360), ('Binlog_checkpoint', 0)])
def test_unknown_or_incomplete_events_are_not_neutral(cursor, require_transaction_boundary, event_type, end_pos):
    cursor.fetchall.return_value = ROTATED_HEADERS + [
        (LOG_FILE, 342, event_type, 1, end_pos, ''),
        (LOG_FILE, 360, 'Gtid', 1, 402, ''),
    ]

    with pytest.raises(ValueError, match='safe transaction boundary.*full resync'):
        binlog.verify_binlog_checkpoint(None, LOG_FILE, 4, require_transaction_boundary)

    assert cursor.execute.call_count == 1


@pytest.mark.parametrize('require_transaction_boundary', [False, True])
def test_checkpoint_probe_does_not_scan_past_its_bounded_window(cursor, require_transaction_boundary):
    cursor.fetchall.side_effect = [
        [(LOG_FILE, pos, 'Binlog_checkpoint', 1, pos + 1, '') for pos in range(4, 20)],
        [(LOG_FILE, 300)],
    ]

    with pytest.raises(ValueError, match='safe transaction boundary'):
        binlog.verify_binlog_checkpoint(None, LOG_FILE, 4, require_transaction_boundary)

    assert cursor.execute.call_count == 2


@pytest.mark.parametrize('error_type', [InternalError, OperationalError])
def test_decode_errors_identify_the_checkpoint_and_preserve_the_server_cause(cursor, error_type):
    original = error_type(1220, 'Error reading Log_event: Event too big')
    cursor.execute.side_effect = original

    with pytest.raises(ValueError, match=r'mysql-bin\.000004:700.*binlog integrity.*full resync') as error:
        binlog.verify_binlog_checkpoint(None, LOG_FILE, 700)

    assert error.value.__cause__ is original
    assert 'Event too big' in str(error.value)


@pytest.mark.parametrize('original', [
    OperationalError(2013, 'Connection lost'),
    InternalError(1236, 'Binlog no longer exists'),
    ProgrammingError(1064, 'Invalid syntax'),
])
def test_unrelated_checkpoint_sql_errors_are_unchanged(cursor, original):
    cursor.execute.side_effect = original

    with pytest.raises(type(original)) as error:
        binlog.verify_binlog_checkpoint(None, LOG_FILE, 4)

    assert error.value is original


def test_all_missing_and_legacy_gtid_streams_are_reported_together():
    bookmarks = {
        'db-missing_second': {'log_file': LOG_FILE, 'log_pos': 4},
        'db-legacy_second': {'gtid': '0-1-10', 'gtid_complete': False},
        'db-complete': {'gtid': '0-1-10', 'gtid_complete': True},
        'db-legacy_first': {'gtid': '0-1-10'},
        'db-unselected': {'gtid': '0-1-10'},
    }
    selected = {name: {} for name in bookmarks if name != 'db-unselected'}
    selected['db-missing_first'] = {}
    state = {'bookmarks': bookmarks}
    original = copy.deepcopy(state)

    with pytest.raises(ValueError) as error:
        binlog.calculate_gtid_bookmark(None, selected, state, 'mariadb')

    message = str(error.value)
    assert 'missing GTID bookmarks: db-missing_first, db-missing_second' in message
    assert 'legacy GTID bookmark cannot prove complete transaction history' in message
    assert 'affected streams: db-legacy_first, db-legacy_second' in message
    assert 'full resync of the affected streams' in message
    assert 'db-complete' not in message
    assert 'db-unselected' not in message
    assert state == original


def test_missing_gtid_report_does_not_label_complete_streams_legacy():
    state = {'bookmarks': {'db-complete': {'gtid': '0-1-10', 'gtid_complete': True}}}

    with pytest.raises(ValueError, match='missing GTID bookmarks: db-missing') as error:
        binlog.calculate_gtid_bookmark(None, {'db-complete': {}, 'db-missing': {}}, state, 'mariadb')

    assert 'legacy' not in str(error.value)


def test_mariadb_without_gtids_can_still_infer_from_valid_rotated_file_coordinates(cursor):
    state = {'bookmarks': {
        'db-first': {'log_file': LOG_FILE, 'log_pos': 4},
        'db-second': {'log_file': LOG_FILE, 'log_pos': 342},
    }}
    cursor.fetchall.side_effect = [[(LOG_FILE, 342)], ROTATED_HEADERS, [(LOG_FILE, 342)]]
    cursor.fetchone.return_value = ('0-1-10',)
    streams = {stream: {'selected': True} for stream in state['bookmarks']}

    assert binlog.calculate_gtid_bookmark(None, streams, state, 'mariadb') == '0-1-10'

    cursor.execute.assert_called_with("select BINLOG_GTID_POS('mysql-bin.000004', 4);")
