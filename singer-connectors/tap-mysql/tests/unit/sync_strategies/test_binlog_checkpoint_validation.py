"""Validate resume boundaries and report every selected stream needing repair."""

import copy
from unittest.mock import call, patch

import pytest
from pymysql.err import InternalError, OperationalError, ProgrammingError

from tap_mysql.sync_strategies import binlog


LOG_FILE = 'mysql-bin.000004'
ROTATED_HEADERS = [
    (LOG_FILE.encode(), 4, b'Format_desc', 1, 0, b'\xa0'),
    (LOG_FILE.encode(), 256, b'Gtid_list', 1, 0, b''),
    (LOG_FILE.encode(), 299, b'Binlog_checkpoint', 1, 0, b''),
]


@pytest.fixture
def cursor():
    with patch.object(binlog, 'connect_with_backoff') as connect:
        yield connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value


@pytest.mark.parametrize('require_transaction_boundary', [False, True])
def test_rotated_mariadb_headers_preserve_the_next_transaction(cursor, require_transaction_boundary):
    cursor.fetchall.return_value = ROTATED_HEADERS + [(LOG_FILE.encode(), 342, b'Gtid', 1, 384, b'')]

    binlog.verify_binlog_checkpoint(None, LOG_FILE, 4, require_transaction_boundary)

    assert cursor.execute.call_args_list == [
        call('SET character_set_results = binary'),
        call('SHOW BINLOG EVENTS IN %s FROM %s LIMIT 16', (LOG_FILE, 4)),
    ]


@pytest.mark.parametrize('require_transaction_boundary', [False, True])
def test_rotated_mariadb_headers_without_transactions_are_resumable(cursor, require_transaction_boundary):
    cursor.fetchall.return_value = ROTATED_HEADERS

    binlog.verify_binlog_checkpoint(None, LOG_FILE, 4, require_transaction_boundary)

    assert cursor.execute.call_count == 2


@pytest.mark.parametrize('require_transaction_boundary', [False, True])
@pytest.mark.parametrize('file_size', [341, 343])
def test_empty_checkpoint_results_require_exact_eof(cursor, require_transaction_boundary, file_size):
    cursor.fetchall.side_effect = [[], [(LOG_FILE.encode(), file_size)]]

    with pytest.raises(ValueError, match='safe transaction boundary.*full resync'):
        binlog.verify_binlog_checkpoint(None, LOG_FILE, 342, require_transaction_boundary)


@pytest.mark.parametrize('require_transaction_boundary', [False, True])
def test_empty_checkpoint_result_accepts_binary_log_name_at_exact_eof(cursor, require_transaction_boundary):
    cursor.fetchall.side_effect = [[], [(LOG_FILE.encode(), 342)]]

    binlog.verify_binlog_checkpoint(None, LOG_FILE, 342, require_transaction_boundary)


def test_transaction_recovery_prefers_gtid_and_handles_binary_info(cursor):
    cursor.fetchall.return_value = [
        (LOG_FILE.encode(), 4, b'Format_desc', 1, 256, b''),
        (LOG_FILE.encode(), 50, b'Xid', 1, 60, b''),
        (LOG_FILE.encode(), 60, b'Gtid', 1, 70, b'GTID 0-1-2'),
        (LOG_FILE.encode(), 70, b'Annotate_rows', 1, 0, b'INSERT \xa0'),
        (LOG_FILE.encode(), 80, b'Table_map', 1, 0, b'table_id: 1'),
        (LOG_FILE.encode(), 100, b'Write_rows_v1', 1, 0, b'table_id: 1'),
    ]

    assert binlog.find_binlog_transaction_start(None, LOG_FILE, 100) == 60
    assert cursor.execute.call_args_list == [
        call('SET character_set_results = binary'),
        call('SHOW BINLOG EVENTS IN %s FROM %s LIMIT 10001', (LOG_FILE, 4)),
    ]


@pytest.mark.parametrize('boundary_event, expected', [
    ((LOG_FILE, 60, 'Query', 1, 70, 'BEGIN'), 60),
    ((LOG_FILE, 60, 'Xid', 1, 70, ''), 60),
])
def test_transaction_recovery_uses_begin_or_previous_terminal_boundary(cursor, boundary_event, expected):
    cursor.fetchall.return_value = [
        (LOG_FILE, 4, 'Format_desc', 1, 50, ''),
        boundary_event,
        (LOG_FILE, 80, 'Table_map', 1, 0, ''),
        (LOG_FILE, 100, 'Update_rows_v1', 1, 0, ''),
    ]

    assert binlog.find_binlog_transaction_start(None, LOG_FILE, 100) == expected


def test_transaction_recovery_retains_boundary_across_bounded_pages(cursor, monkeypatch):
    monkeypatch.setattr(binlog, 'CHECKPOINT_SCAN_PAGE_SIZE', 2)
    monkeypatch.setattr(binlog, 'CHECKPOINT_SCAN_LOG_INTERVAL', 2)
    cursor.fetchall.side_effect = [
        [
            (LOG_FILE, 4, 'Format_desc', 1, 50, ''),
            (LOG_FILE, 50, 'Anonymous_Gtid', 1, 60, ''),
            (LOG_FILE, 70, 'Rows_query', 1, 0, ''),
        ],
        [
            (LOG_FILE, 70, 'Rows_query', 1, 0, ''),
            (LOG_FILE, 80, 'Table_map', 1, 0, ''),
            (LOG_FILE, 100, 'Write_rows', 1, 0, ''),
        ],
        [(LOG_FILE, 100, 'Write_rows', 1, 0, '')],
    ]

    with patch.object(binlog.LOGGER, 'info') as progress:
        assert binlog.find_binlog_transaction_start(None, LOG_FILE, 100) == 50
    assert cursor.execute.call_args_list[1:] == [
        call('SHOW BINLOG EVENTS IN %s FROM %s LIMIT 3', (LOG_FILE, 4)),
        call('SHOW BINLOG EVENTS IN %s FROM %s LIMIT 3', (LOG_FILE, 70)),
        call('SHOW BINLOG EVENTS IN %s FROM %s LIMIT 3', (LOG_FILE, 100)),
    ]
    assert any('Scanned %s binlog event(s)' in logged.args[0]
               for logged in progress.call_args_list)


def test_transaction_recovery_fails_closed_without_a_proven_boundary(cursor):
    cursor.fetchall.return_value = [
        (LOG_FILE, 4, 'Format_desc', 1, 50, ''),
        (LOG_FILE, 80, 'Table_map', 1, 0, ''),
        (LOG_FILE, 100, 'Delete_rows_v1', 1, 0, ''),
    ]

    with pytest.raises(ValueError, match='Cannot prove a transaction boundary.*full resync'):
        binlog.find_binlog_transaction_start(None, LOG_FILE, 100)


@pytest.mark.parametrize('require_transaction_boundary', [False, True])
def test_unknown_events_are_not_neutral(cursor, require_transaction_boundary):
    cursor.fetchall.return_value = ROTATED_HEADERS + [
        (LOG_FILE, 342, 'Unknown_event', 1, 0, ''),
        (LOG_FILE, 360, 'Gtid', 1, 402, ''),
    ]

    with pytest.raises(ValueError, match='safe transaction boundary.*full resync'):
        binlog.verify_binlog_checkpoint(None, LOG_FILE, 4, require_transaction_boundary)

    assert cursor.execute.call_count == 2


@pytest.mark.parametrize('require_transaction_boundary', [False, True])
def test_checkpoint_probe_does_not_scan_past_its_bounded_window(cursor, require_transaction_boundary):
    cursor.fetchall.side_effect = [
        [(LOG_FILE, pos, 'Binlog_checkpoint', 1, pos + 1, '') for pos in range(4, 20)],
        [(LOG_FILE, 300)],
    ]

    with pytest.raises(ValueError, match='safe transaction boundary'):
        binlog.verify_binlog_checkpoint(None, LOG_FILE, 4, require_transaction_boundary)

    assert cursor.execute.call_count == 3


@pytest.mark.parametrize('error_type', [InternalError, OperationalError])
def test_decode_errors_identify_the_checkpoint_and_preserve_the_server_cause(cursor, error_type):
    original = error_type(1220, 'Error reading Log_event: Event too big')
    cursor.execute.side_effect = [None, original]

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
    cursor.execute.side_effect = [None, original]

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
