import datetime
import io
import json

from decimal import Decimal
from unittest import TestCase
from unittest.mock import MagicMock, Mock, PropertyMock, call, patch

from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres
from pipelinewise.fastsync.commons import tap_postgres
from pipelinewise.fastsync.commons.partial_sync_boundary import (
    PartialSyncBoundary,
)

MAX_PRICE_SQL = 'SELECT MAX(price) AS key_value FROM schema."table1"'
FINITE_MAX_PRICE_SQL = (
    'SELECT MAX("price") AS key_value FROM "schema"."table1" '
    "WHERE \"price\" > '-Infinity' AND \"price\" < 'Infinity'"
)
EMPTY_BOOKMARK_WARNING = 'No replication value found for table schema.table1, returning empty bookmark'


class TestFastSyncTapPostgres(TestCase):
    """
    Unit tests for fastsync tap postgres
    """

    def setUp(self) -> None:
        """Initialise test FastSyncTapPostgres object"""
        self.postgres = FastSyncTapPostgres(
            connection_config={'dbname': 'test_database', 'tap_id': 'test_tap'},
            tap_type_to_target_type={},
        )
        self.postgres.executed_queries_primary_host = []
        self.postgres.executed_queries = []

        def primary_host_query_mock(query, _=None):
            self.postgres.executed_queries_primary_host.append(query)
            return [{'slot_name': 'slot', 'lsn': '0/2A'}]

        self.postgres.primary_host_query = primary_host_query_mock

    def test_copy_table_mogrifies_only_the_structured_boundary(self):
        """COPY keeps projection percent signs outside placeholder parsing."""
        table_columns = [{
            0: 'rate%s',
            1: 'text',
            2: 'to_char("event_date", \'%Y-%m-01\')',
            3: None,
            'safe_sql_value': 'to_char("event_date", \'%Y-%m-01\')',
        }]
        self.postgres.curr = MagicMock()
        self.postgres.curr.connection.encoding = 'UTF8'
        self.postgres.curr.mogrify.return_value = (
            b' WHERE "rate%s" >= \'x\\\'\' OR 1=1 --\''
        )
        boundary = PartialSyncBoundary('rate%s', "x' OR 1=1 --")

        with patch.object(
            self.postgres, 'get_table_columns', return_value=table_columns
        ), patch.object(
            tap_postgres.split_gzip, 'open', return_value=io.BytesIO()
        ):
            self.postgres.copy_table(
                'public.my_table', 'unused.csv', boundary=boundary
            )

        self.postgres.curr.mogrify.assert_called_once_with(
            ' WHERE "rate%%s" >= %s',
            ("x' OR 1=1 --",),
        )
        export_sql = self.postgres.curr.copy_expert.call_args.args[0]
        self.assertIn('to_char("event_date", \'%Y-%m-01\')', export_sql)
        self.assertIn(
            ' WHERE "rate%s" >= \'x\\\'\' OR 1=1 --\'', export_sql
        )

    def test_generate_repl_slot_name(self):
        """Validate if the replication slot name generated correctly"""
        # Provide only database name
        assert (
            self.postgres.generate_replication_slot_name('some_db')
            == 'pipelinewise_some_db'
        )

        # Provide database name and tap_id
        assert (
            self.postgres.generate_replication_slot_name('some_db', 'some_tap')
            == 'pipelinewise_some_db_some_tap'
        )

        # Provide database name, tap_id and prefix
        assert (
            self.postgres.generate_replication_slot_name(
                'some_db', 'some_tap', prefix='custom_prefix'
            )
            == 'custom_prefix_some_db_some_tap'
        )

        # Replication slot name should be lowercase
        assert (
            self.postgres.generate_replication_slot_name('SoMe_DB', 'SoMe_TaP')
            == 'pipelinewise_some_db_some_tap'
        )

        # Invalid characters should be replaced by underscores
        assert (
            self.postgres.generate_replication_slot_name('some-db', 'some-tap')
            == 'pipelinewise_some_db_some_tap'
        )

        assert (
            self.postgres.generate_replication_slot_name('some.db', 'some.tap')
            == 'pipelinewise_some_db_some_tap'
        )

    def test_close_connection_is_idempotent(self):
        """Close each opened connection once and clear its cursor reference."""
        connection = Mock()
        primary_connection = Mock()
        self.postgres.conn = connection
        self.postgres.curr = Mock()
        self.postgres.primary_host_conn = primary_connection

        self.postgres.close_connection()
        self.postgres.close_connection()

        connection.close.assert_called_once_with()
        primary_connection.close.assert_called_once_with()
        self.assertIsNone(self.postgres.conn)
        self.assertIsNone(self.postgres.curr)
        self.assertIsNone(self.postgres.primary_host_conn)

    def test_close_connection_silences_driver_failure(self):
        """Cleanup failures must not replace the original sync exception."""
        connection = Mock()
        connection.close.side_effect = RuntimeError('close failed')
        self.postgres.conn = connection

        self.postgres.close_connection(silent=True)

        connection.close.assert_called_once_with()
        self.assertIsNone(self.postgres.conn)

    def test_fetch_current_log_pos_closes_primary_connection_on_success(self):
        """The dedicated primary connection is released before reading the source LSN."""
        primary_connection = Mock()

        with patch.object(
            self.postgres, 'get_connection', return_value=primary_connection
        ), patch.object(
            self.postgres, 'create_replication_slot', return_value=42
        ) as create_replication_slot, patch.object(
            self.postgres, 'query', return_value=[{'current_lsn': '0/2A'}]
        ) as query:
            bookmark = self.postgres.fetch_current_log_pos()

        self.assertEqual({'lsn': 42, 'version': 1}, bookmark)
        create_replication_slot.assert_called_once_with()
        query.assert_called_once_with('SELECT pg_current_wal_lsn() AS current_lsn')
        primary_connection.close.assert_called_once_with()
        self.assertIsNone(self.postgres.primary_host_conn)

    PRIMARY_HISTORY = [{'system_identifier': '7300000000000000001', 'wal_file': '000000020000000000000001'}]

    @staticmethod
    def _replica_row(current_lsn='0/2A', timeline_id=2, system_identifier='7300000000000000001'):
        return [{'system_identifier': system_identifier, 'timeline_id': timeline_id, 'current_lsn': current_lsn}]

    def _fetch_from_replica(self, replica_rows, slot_start=42, monotonic=None):
        self.postgres.connection_config['replica_host'] = 'replica'
        self.postgres.primary_host_query = Mock(return_value=self.PRIMARY_HISTORY)
        monotonic_patch = patch.object(
            tap_postgres.time, 'monotonic', side_effect=monotonic
        ) if monotonic else patch.object(tap_postgres.time, 'monotonic', return_value=0)
        with patch.object(self.postgres, 'get_connection', return_value=Mock()), patch.object(
            self.postgres, 'create_replication_slot', return_value=slot_start
        ), patch.object(
            self.postgres, 'query', side_effect=replica_rows
        ) as query, patch.object(tap_postgres.time, 'sleep') as sleep, monotonic_patch:
            bookmark = self.postgres.fetch_current_log_pos()
        return bookmark, query, sleep

    def test_fetch_current_log_pos_reads_replay_position_and_history_on_replica(self):
        """A replica bookmark comes with the replica's system identifier and timeline in one query."""
        bookmark, query, _ = self._fetch_from_replica([self._replica_row()])

        self.assertEqual({'lsn': 42, 'version': 1}, bookmark)
        query.assert_called_once_with(tap_postgres.REPLICA_POSITION_SQL)
        self.postgres.primary_host_query.assert_called_once_with(tap_postgres.PRIMARY_HISTORY_SQL)

    def test_fetch_current_log_pos_rejects_primary_bookmark_before_slot_start(self):
        """A bookmark before the slot start would let the snapshot miss commits the slot never streams."""
        with patch.object(self.postgres, 'get_connection', return_value=Mock()), patch.object(
            self.postgres, 'create_replication_slot', return_value=43
        ), patch.object(
            self.postgres, 'query', return_value=[{'current_lsn': '0/2A'}]
        ), self.assertRaisesRegex(
            tap_postgres.ReplicationSlotVerificationError, 'Bookmark 0/2A is before the replication slot start 0/2B'
        ):
            self.postgres.fetch_current_log_pos()

    def test_fetch_current_log_pos_waits_for_replica_to_replay_past_slot_start(self):
        """A lagging replica is polled until its replay position reaches the slot start."""
        bookmark, _, sleep = self._fetch_from_replica([self._replica_row('0/29'), self._replica_row('0/2B')])

        self.assertEqual({'lsn': 43, 'version': 1}, bookmark)
        sleep.assert_called_once_with(tap_postgres.SLOT_READY_POLL_SECONDS)

    def test_fetch_current_log_pos_waits_for_replica_checkpoint_on_the_new_timeline(self):
        """A standby's checkpoint timeline trails a timeline switch until its next restartpoint."""
        bookmark, _, sleep = self._fetch_from_replica(
            [self._replica_row('0/2B', timeline_id=1), self._replica_row('0/2B')])

        self.assertEqual({'lsn': 43, 'version': 1}, bookmark)
        sleep.assert_called_once_with(tap_postgres.SLOT_READY_POLL_SECONDS)

    def test_fetch_current_log_pos_rejects_replica_that_never_reaches_slot_start(self):
        """A replica stuck behind the slot start fails after the timeout instead of taking an early bookmark."""
        with self.assertRaisesRegex(tap_postgres.ReplicationSlotVerificationError, 'did not catch up'):
            self._fetch_from_replica(
                [self._replica_row('0/29')] * 2, monotonic=[0, 0, tap_postgres.SLOT_READY_TIMEOUT_SECONDS])

    def test_fetch_current_log_pos_rejects_replica_of_an_older_timeline_after_the_timeout(self):
        """A standby of the old primary after a switchover never reaches the new timeline."""
        with self.assertRaisesRegex(tap_postgres.ReplicationSlotVerificationError, 'timeline 1 is behind host'):
            self._fetch_from_replica(
                [self._replica_row('0/FF', timeline_id=1)] * 2,
                monotonic=[0, 0, tap_postgres.SLOT_READY_TIMEOUT_SECONDS])

    def test_fetch_current_log_pos_rejects_replica_of_another_server(self):
        """A standby of another cluster, or ahead on another branch, can never supply the bookmark."""
        for row in (self._replica_row(system_identifier='7399999999999999999'), self._replica_row(timeline_id=3)):
            with self.subTest(row=row), self.assertRaisesRegex(
                tap_postgres.ReplicationSlotVerificationError, 'replica_host is not a standby of host'
            ):
                self._fetch_from_replica([row])

    def test_fetch_current_log_pos_rejects_replica_without_replay_position(self):
        """A replica_host that is not a standby has no replay position and cannot supply a bookmark."""
        with self.assertRaisesRegex(tap_postgres.ReplicationSlotVerificationError, 'not a streaming standby'):
            self._fetch_from_replica([self._replica_row(None)])

    def test_fetch_current_log_pos_closes_primary_connection_on_failure(self):
        """A replication-slot failure cannot leak the primary connection."""
        primary_connection = Mock()

        with patch.object(
            self.postgres, 'get_connection', return_value=primary_connection
        ), patch.object(
            self.postgres,
            'create_replication_slot',
            side_effect=RuntimeError('replication slot failed'),
        ), self.assertRaisesRegex(RuntimeError, 'replication slot failed'):
            self.postgres.fetch_current_log_pos()

        primary_connection.close.assert_called_once_with()
        self.assertIsNone(self.postgres.primary_host_conn)

    def test_create_replication_slot_1(self):
        """
        Validate if replication slot creation SQL commands generated correctly in case no v15 slots exists
        """

        def execute_mock(query):
            print('Mocked execute called')
            self.postgres.executed_queries_primary_host.append(query)

        # mock cursor with execute method
        cursor_mock = MagicMock().return_value
        cursor_mock.__enter__.return_value.execute.side_effect = execute_mock
        type(cursor_mock.__enter__.return_value).rowcount = PropertyMock(return_value=0)

        # mock PG connection instance with ability to open cursor
        pg_con = Mock()
        pg_con.cursor.return_value = cursor_mock

        self.postgres.primary_host_conn = pg_con

        self.assertEqual(42, self.postgres.create_replication_slot())
        assert self.postgres.executed_queries_primary_host == [
            "SELECT * FROM pg_replication_slots WHERE slot_name = 'pipelinewise_test_database';",
            "SELECT * FROM pg_create_logical_replication_slot('pipelinewise_test_database_test_tap', 'wal2json')",
        ]

    def test_create_replication_slot_2(self):
        """
        Validate if replication slot creation SQL commands generated correctly in case a v15 slots exists
        """

        def execute_mock(query):
            print('Mocked execute called')
            self.postgres.executed_queries_primary_host.append(query)

        # mock cursor with execute method
        cursor_mock = MagicMock().return_value
        cursor_mock.__enter__.return_value.execute.side_effect = execute_mock
        type(cursor_mock.__enter__.return_value).rowcount = PropertyMock(return_value=1)

        # mock PG connection instance with ability to open cursor
        pg_con = Mock()
        pg_con.cursor.return_value = cursor_mock

        self.postgres.primary_host_conn = pg_con

        self.postgres.create_replication_slot()
        assert self.postgres.executed_queries_primary_host == [
            "SELECT * FROM pg_replication_slots WHERE slot_name = 'pipelinewise_test_database';",
            "SELECT * FROM pg_create_logical_replication_slot('pipelinewise_test_database', 'wal2json')",
        ]

    def _existing_slot(self, slots, monotonic=None):
        """Run create_replication_slot against a slot that already exists, returning the result."""
        cursor_mock = MagicMock().return_value
        type(cursor_mock.__enter__.return_value).rowcount = PropertyMock(return_value=0)
        pg_con = Mock()
        pg_con.cursor.return_value = cursor_mock
        self.postgres.primary_host_conn = pg_con

        duplicate = Exception('replication slot "pipelinewise_test_database_test_tap" already exists')
        duplicate.pgcode = '42710'
        self.postgres.primary_host_query = Mock(side_effect=duplicate)
        with patch.object(tap_postgres, 'read_slot', side_effect=slots) as read_slot, patch.object(
            tap_postgres.time, 'sleep'
        ) as sleep, patch.object(
            tap_postgres.time, 'monotonic', side_effect=monotonic or [0] * (len(slots) + 1)
        ):
            start_lsn = self.postgres.create_replication_slot()
        read_slot.assert_called_with(pg_con, 'pipelinewise_test_database_test_tap')
        return start_lsn, sleep

    @staticmethod
    def _slot(**overrides):
        slot = {
            'slot_name': 'pipelinewise_test_database_test_tap', 'database': 'test_database', 'plugin': 'wal2json',
            'slot_type': 'logical', 'temporary': False, 'active_pid': None, 'confirmed_flush_lsn': '0/2A',
        }
        slot.update(overrides)
        return slot

    def test_create_replication_slot_accepts_finished_slot_of_this_tap(self):
        """An existing finished slot on this database and plugin starts at its confirmed_flush_lsn."""
        start_lsn, sleep = self._existing_slot([self._slot()])

        self.assertEqual(42, start_lsn)
        sleep.assert_not_called()

    def test_create_replication_slot_waits_for_slot_still_being_created(self):
        """A slot another worker is still building has no start point yet, so it is polled until finished."""
        start_lsn, sleep = self._existing_slot([
            self._slot(active_pid=123, confirmed_flush_lsn=None),
            self._slot(active_pid=123, confirmed_flush_lsn='0/2B'),
            self._slot(confirmed_flush_lsn='0/2B'),
        ])

        self.assertEqual(43, start_lsn)
        self.assertEqual(2, sleep.call_count)

    def test_create_replication_slot_rejects_slot_that_never_finishes(self):
        """A slot that stays in creation or in use fails after the timeout."""
        with self.assertRaisesRegex(tap_postgres.ReplicationSlotVerificationError, 'is not ready'):
            self._existing_slot(
                [self._slot(active_pid=123, confirmed_flush_lsn=None)],
                monotonic=[0, tap_postgres.SLOT_READY_TIMEOUT_SECONDS],
            )

    def test_create_replication_slot_rejects_foreign_slots(self):
        """A slot on another database, with another plugin, physical or temporary is never reused."""
        for overrides in (
            {'database': 'other_database'},
            {'plugin': 'pgoutput'},
            {'slot_type': 'physical'},
            {'temporary': True},
        ):
            with self.subTest(**overrides), self.assertRaisesRegex(
                tap_postgres.ReplicationSlotVerificationError, 'does not belong to this tap'
            ):
                self._existing_slot([self._slot(**overrides)])

    def test_create_replication_slot_rejects_slot_that_disappears(self):
        """A slot dropped after the duplicate error, for example a cancelled creation, is an error."""
        with self.assertRaisesRegex(tap_postgres.ReplicationSlotVerificationError, 'is gone'):
            self._existing_slot([None])

    def test_create_replication_slot_propagates_other_errors(self):
        """Errors other than the duplicate-object SQLSTATE are raised unchanged."""
        cursor_mock = MagicMock().return_value
        type(cursor_mock.__enter__.return_value).rowcount = PropertyMock(return_value=0)
        pg_con = Mock()
        pg_con.cursor.return_value = cursor_mock
        self.postgres.primary_host_conn = pg_con
        error = Exception('permission denied')
        error.pgcode = '42501'
        self.postgres.primary_host_query = Mock(side_effect=error)

        with self.assertRaisesRegex(Exception, 'permission denied'):
            self.postgres.create_replication_slot()

    def test_read_slot_returns_the_named_slot_or_none(self):
        """The slot row is read by exact name, so reset_state and FastSync see the same slot."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [self._slot(), None]

        self.assertEqual(self._slot(), tap_postgres.read_slot(connection, 'pipelinewise_test_database_test_tap'))
        self.assertIsNone(tap_postgres.read_slot(connection, 'missing'))
        cursor.execute.assert_called_with(tap_postgres.SLOT_STATUS_SQL, ('missing',))

    def test_lsn_conversions_round_trip(self):
        """LSN text and integer forms convert both ways."""
        self.assertEqual((0x16 << 32) + 0xB374D848, tap_postgres.lsn_to_int('16/B374D848'))
        self.assertEqual('16/B374D848', tap_postgres.int_to_lsn((0x16 << 32) + 0xB374D848))

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_to_primary(self, connect_mock):
        """
        Check that get connection uses the right credentials to connect to primary
        """
        connect_mock.return_value.server_version = 140000
        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
        }

        self.assertEqual(
            FastSyncTapPostgres.get_connection(creds, prioritize_primary=True),
            connect_mock.return_value,
        )

        connect_mock.assert_called_once_with(
            f"host='{creds['host']}' port='{creds['port']}' user='{creds['user']}' password='{creds['password']}' "
            f"dbname='{creds['dbname']}'"
        )

        self.assertTrue(connect_mock.return_value.autocommit)

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_rejects_postgres_before_14(self, connect_mock):
        """Every FastSync source connection enforces the PostgreSQL floor."""
        connect_mock.return_value.server_version = 130016
        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
        }

        with self.assertRaisesRegex(
            RuntimeError,
            'PostgreSQL 14 or later.*server_version_num 130016',
        ) as error:
            FastSyncTapPostgres.get_connection(creds, prioritize_primary=True)

        self.assertIsInstance(error.exception, tap_postgres.UnsupportedPostgresVersionError)
        connect_mock.return_value.close.assert_called_once_with()

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_allows_unsupported_version_for_config_removal(
        self, connect_mock
    ):
        """Removed-tap cleanup can still connect to an obsolete source."""
        connect_mock.return_value.server_version = 130016
        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
        }

        connection = FastSyncTapPostgres.get_connection(
            creds,
            prioritize_primary=True,
            allow_unsupported_version_for_config_removal=True,
        )

        self.assertIs(connect_mock.return_value, connection)
        connect_mock.return_value.close.assert_not_called()
        self.assertTrue(connection.autocommit)

    def test_drop_slot_forwards_only_the_config_removal_bypass(self):
        """Slot cleanup forwards the explicit removed-config exception."""
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value.rowcount = 0
        creds = {
            'dbname': 'my_db',
            'tap_id': 'my_tap',
        }

        with patch.object(
            FastSyncTapPostgres, 'get_connection', return_value=connection
        ) as get_connection:
            FastSyncTapPostgres.drop_slot(
                creds,
                allow_unsupported_version_for_config_removal=True,
            )

        get_connection.assert_called_once_with(
            creds,
            prioritize_primary=True,
            allow_unsupported_version_for_config_removal=True,
        )
        connection.close.assert_called_once_with()

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_drop_slot_validates_version_by_default(self, connect_mock):
        """FastSync resync slot cleanup must retain source-version validation."""
        connect_mock.return_value.server_version = 130016
        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
            'tap_id': 'my_tap',
        }

        with self.assertRaisesRegex(RuntimeError, 'PostgreSQL 14 or later'):
            FastSyncTapPostgres.drop_slot(creds)

        connect_mock.return_value.close.assert_called_once_with()
        connect_mock.return_value.cursor.assert_not_called()

    def test_reset_slot_drops_only_current_slot_after_state_invalidation(self):
        """Only the preflighted tap-specific slot is dropped, after state is durable."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [('pipelinewise_my_db_my_tap', 'my_db', 'wal2json', False)]
        creds = {'dbname': 'my_db', 'tap_id': 'my_tap'}
        before_reset = MagicMock(return_value='state.backup')
        calls = MagicMock()
        calls.attach_mock(cursor.execute, 'execute')
        calls.attach_mock(before_reset, 'before_reset')

        with patch.object(
            FastSyncTapPostgres, 'get_connection', return_value=connection
        ) as get_connection:
            FastSyncTapPostgres.reset_slot(creds, before_reset=before_reset)

        get_connection.assert_called_once_with(creds, prioritize_primary=True)
        assert calls.mock_calls == [
            call.execute(
                'SELECT slot_name, database, plugin, active FROM pg_replication_slots '
                'WHERE slot_name IN (%s, %s)',
                ('pipelinewise_my_db', 'pipelinewise_my_db_my_tap'),
            ),
            call.before_reset(),
            call.execute('SELECT pg_drop_replication_slot(%s)', ('pipelinewise_my_db_my_tap',)),
            call.execute(
                'SELECT * FROM pg_create_logical_replication_slot(%s, %s)',
                ('pipelinewise_my_db_my_tap', 'wal2json'),
            ),
        ]
        connection.close.assert_called_once_with()

    def test_reset_slot_rejects_legacy_active_and_incompatible_slots_before_state_changes(self):
        """Legacy lookup takes precedence even when an inactive tap-specific slot also exists."""
        current = ('pipelinewise_my_db_my_tap', 'my_db', 'wal2json', False)
        cases = [
            ([('pipelinewise_my_db', 'my_db', 'wal2json', active)] + modern, 'legacy')
            for active in (False, True) for modern in ([], [current])
        ] + [
            ([(current[0], database, plugin, active)], 'must be inactive')
            for database, plugin, active in (
                ('my_db', 'wal2json', True), ('other_db', 'wal2json', False), ('my_db', 'pgoutput', False),
            )
        ]
        for rows, message in cases:
            with self.subTest(rows=rows):
                connection = MagicMock()
                cursor = connection.cursor.return_value.__enter__.return_value
                cursor.fetchall.return_value = rows
                before_reset = MagicMock()
                with patch.object(FastSyncTapPostgres, 'get_connection', return_value=connection):
                    with self.assertRaisesRegex(RuntimeError, message):
                        FastSyncTapPostgres.reset_slot(
                            {'dbname': 'my_db', 'tap_id': 'my_tap'}, before_reset=before_reset,
                        )
                before_reset.assert_not_called()
                self.assertEqual(cursor.execute.call_count, 1)
                connection.close.assert_called_once_with()

    def test_reset_slot_creates_missing_slot_without_dropping_any_slot(self):
        """A missing slot still requires state invalidation before creation."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = []
        before_reset = MagicMock(return_value=None)
        with patch.object(FastSyncTapPostgres, 'get_connection', return_value=connection):
            FastSyncTapPostgres.reset_slot({'dbname': 'my_db', 'tap_id': 'my_tap'}, before_reset=before_reset)
        before_reset.assert_called_once_with()
        self.assertEqual(cursor.execute.call_count, 2)
        cursor.execute.assert_called_with(
            'SELECT * FROM pg_create_logical_replication_slot(%s, %s)', ('pipelinewise_my_db_my_tap', 'wal2json'),
        )
        connection.close.assert_called_once_with()

    def test_reset_slot_name_length_boundary(self):
        """Accept exactly 63 characters; reject a longer name before SQL or state changes."""
        slot_prefix = FastSyncTapPostgres.generate_replication_slot_name('my_db') + '_'
        for length in (63, 64):
            with self.subTest(length=length):
                config = {'dbname': 'my_db', 'tap_id': 't' * (length - len(slot_prefix))}
                connection = MagicMock()
                cursor = connection.cursor.return_value.__enter__.return_value
                cursor.fetchall.return_value = []
                before_reset = MagicMock(return_value=None)
                with patch.object(FastSyncTapPostgres, 'get_connection', return_value=connection):
                    if length == 64:
                        with self.assertRaisesRegex(RuntimeError, 'at most 63 characters'):
                            FastSyncTapPostgres.reset_slot(config, before_reset=before_reset)
                        cursor.execute.assert_not_called()
                        before_reset.assert_not_called()
                    else:
                        FastSyncTapPostgres.reset_slot(config, before_reset=before_reset)
                        before_reset.assert_called_once_with()
                        cursor.execute.assert_called_with(
                            'SELECT * FROM pg_create_logical_replication_slot(%s, %s)',
                            (slot_prefix + config['tap_id'], 'wal2json'),
                        )
                connection.close.assert_called_once_with()

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_to_sec(self, connect_mock):
        """
        Check that get connection uses the right credentials to connect to secondary if present
        """
        connect_mock.return_value.server_version = 140000
        creds = {
            'host': 'my_primary_host',
            'replica_host': 'my_replica_host',
            'user': 'my_primary_user',
            'replica_user': 'my_replica_user',
            'password': 'my_primary_user',
            'replica_password': 'my_replica_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
            'replica_port': 'my_replica_port',
        }

        self.assertEqual(
            FastSyncTapPostgres.get_connection(creds, prioritize_primary=False),
            connect_mock.return_value,
        )

        connect_mock.assert_called_once_with(
            f"host='{creds['replica_host']}' port='{creds['replica_port']}' user='{creds['replica_user']}' password"
            f"='{creds['replica_password']}' "
            f"dbname='{creds['dbname']}'"
        )

        self.assertTrue(connect_mock.return_value.autocommit)

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_fallback(self, connect_mock):
        """
        Check that get connection uses the primary server credentials as a fallback
        """
        connect_mock.return_value.server_version = 140000
        creds = {
            'host': 'my_primary_host',
            'replica_host': 'my_replica_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
        }

        self.assertEqual(
            FastSyncTapPostgres.get_connection(creds, prioritize_primary=False),
            connect_mock.return_value,
        )

        connect_mock.assert_called_once_with(
            f"host='{creds['replica_host']}' port='{creds['port']}' user='{creds['user']}' password"
            f"='{creds['password']}' dbname='{creds['dbname']}'"
        )

        self.assertTrue(connect_mock.return_value.autocommit)

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_ssl(self, connect_mock):
        """
        Check that get connection uses ssl when present
        """
        connect_mock.return_value.server_version = 140000
        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
            'ssl': 'true',
        }

        self.assertEqual(
            FastSyncTapPostgres.get_connection(creds, prioritize_primary=False),
            connect_mock.return_value,
        )

        connect_mock.assert_called_once_with(
            f"host='{creds['host']}' port='{creds['port']}' user='{creds['user']}' password"
            f"='{creds['password']}' dbname='{creds['dbname']}' sslmode='require'"
        )

        self.assertTrue(connect_mock.return_value.autocommit)

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_drop_slot_v15(self, connect_mock):
        """
        Check that dropping slots works fine for v15 slots
        """

        def execute_mock(query):
            print('Mocked execute called')
            self.postgres.executed_queries_primary_host.append(query)

        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
            'ssl': 'true',
            'tap_id': 'tap_test',
        }

        # mock cursor with execute method
        cursor_mock = MagicMock().return_value
        cursor_mock.__enter__.return_value.execute.side_effect = execute_mock
        type(cursor_mock.__enter__.return_value).rowcount = PropertyMock(
            side_effect=[1, 2]
        )

        # mock PG connection instance with ability to open cursor
        pg_con = Mock()
        pg_con.server_version = 140000
        pg_con.cursor.return_value = cursor_mock

        connect_mock.return_value = pg_con

        self.postgres.drop_slot(creds)

        assert self.postgres.executed_queries_primary_host == [
            "SELECT * FROM pg_replication_slots WHERE slot_name = 'pipelinewise_my_db';",
            'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE '
            "slot_name = 'pipelinewise_my_db';",
        ]

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_drop_slot_v16(self, connect_mock):
        """
        Check that dropping slots works fine for v16 slots
        """

        def execute_mock(query):
            print('Mocked execute called')
            self.postgres.executed_queries_primary_host.append(query)

        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
            'ssl': 'true',
            'tap_id': 'tap_test',
        }

        # mock cursor with execute method
        cursor_mock = MagicMock().return_value
        cursor_mock.__enter__.return_value.execute.side_effect = execute_mock
        type(cursor_mock.__enter__.return_value).rowcount = PropertyMock(
            side_effect=[0, 1]
        )

        # mock PG connection instance with ability to open cursor
        pg_con = Mock()
        pg_con.server_version = 140000
        pg_con.cursor.return_value = cursor_mock

        connect_mock.return_value = pg_con

        self.postgres.drop_slot(creds)

        assert self.postgres.executed_queries_primary_host == [
            "SELECT * FROM pg_replication_slots WHERE slot_name = 'pipelinewise_my_db';",
            'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE '
            "slot_name = 'pipelinewise_my_db_tap_test';",
        ]

    def test_fetch_current_incremental_key_pos_empty_result_expect_exception(self):
        """
        test fetch_current_incremental_key_pos where result is empty, it should raise an exception
        """
        with patch.object(self.postgres, 'query') as query_mock:
            query_mock.return_value = None

            with self.assertRaises(Exception) as context:
                self.postgres.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertEqual('Cannot get replication key value for table: schema.table1', str(context.exception))

    def test_primary_keys_preserve_declared_order(self):
        """Composite keys follow index order rather than physical column order."""
        with patch.object(
            self.postgres,
            'query',
            return_value=[('second_key',), ('first_key',)],
        ) as query_mock:
            keys = self.postgres.get_primary_keys('public.composite_key')

        self.assertEqual(keys, ['"SECOND_KEY"', '"FIRST_KEY"'])
        query_mock.assert_called_once()
        self.assertEqual(query_mock.call_args.args[1], ('public', 'composite_key'))
        self.assertIn('WITH ORDINALITY', query_mock.call_args.args[0])
        self.assertIn(
            'ORDER BY key_column.key_ordinality', query_mock.call_args.args[0]
        )

    def test_hstore_is_exported_as_json(self):
        """FastSync and Singer must share object semantics for hstore."""
        self.postgres.hstore_as_json = True
        with patch.object(self.postgres, 'query', return_value=[]) as query_mock:
            self.postgres.get_table_columns('public.hstore_table', max_num='1')

        query = query_mock.call_args.args[0]
        self.assertIn("WHEN udt_name = 'hstore' THEN 'hstore'", query)
        self.assertIn(
            "WHEN udt_name = 'hstore' THEN 'hstore_to_json(\"'",
            query,
        )

    def test_hstore_export_is_unchanged_for_native_routes(self):
        """Native routes retain the existing textual hstore export."""
        with patch.object(self.postgres, 'query', return_value=[]) as query_mock:
            self.postgres.get_table_columns('public.hstore_table', max_num='1')

        query = query_mock.call_args.args[0]
        self.assertNotIn('hstore_to_json', query)

    def test_fetch_current_incremental_key_pos_empty_key_value_return_empty_state(self):
        """
        test fetch_current_incremental_key_pos where result has empty value is empty, it should return an empty state
        """
        with patch.object(self.postgres, 'query') as query_mock:
            query_mock.return_value = [{}]

            state = self.postgres.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertFalse(state)

    def test_fetch_current_incremental_key_pos_non_empty_key_value_return_state(self):
        """
        test fetch_current_incremental_key_pos where result exists, it should return a non empty state with key value
        """
        with patch.object(self.postgres, 'query') as query_mock:
            query_mock.return_value = [{'key_value': 123}]

            state = self.postgres.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertDictEqual({
                'replication_key': 'id',
                'replication_key_value': 123,
                'version': 1,
            }, state)

    def test_fetch_current_incremental_key_pos_datetime_key_value_return_state(self):
        """
        test fetch_current_incremental_key_pos where result is datetime, it should return a state with iso formatted
         datetime key value
        """
        with patch.object(self.postgres, 'query') as query_mock:
            query_mock.return_value = [{'key_value': datetime.datetime(2020, 1, 24, 7, 12, 6)}]

            state = self.postgres.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertDictEqual({
                'replication_key': 'id',
                'replication_key_value': '2020-01-24T07:12:06',
                'version': 1,
            }, state)

    def test_fetch_current_incremental_key_pos_date_key_value_return_state(self):
        """
        test fetch_current_incremental_key_pos where result is date, it should return a state with iso formatted
         datetime key value
        """
        with patch.object(self.postgres, 'query') as query_mock:
            query_mock.return_value = [{'key_value': datetime.date(2020, 1, 24)}]

            state = self.postgres.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertDictEqual({
                'replication_key': 'id',
                'replication_key_value': '2020-01-24T00:00:00',
                'version': 1,
            }, state)

    def test_fetch_current_incremental_key_pos_decimal_key_value_return_state(self):
        """
        Decimal bookmarks preserve every digit as a JSON string.
        """
        with patch.object(self.postgres, 'query') as query_mock:
            query_mock.return_value = [{'key_value': Decimal('12345678901234567890.123456789')}]

            state = self.postgres.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertDictEqual({
                'replication_key': 'id',
                'replication_key_value': '12345678901234567890.123456789',
                'version': 1,
            }, state)

    def _fetch_price_bookmark(self, *maxima):
        """Run the handover on schema.table1.price, answering each MAX query in turn."""
        rows = [[{'key_value': value}] for value in maxima]
        with patch.object(self.postgres, 'query', side_effect=rows) as query_mock:
            state = self.postgres.fetch_current_incremental_key_pos('schema.table1', 'price')
        return state, query_mock.call_args_list

    def test_fetch_current_incremental_key_pos_nan_max_bookmarks_finite_maximum(self):
        """A NaN maximum bookmarks the largest finite key, which strict JSON state can hold."""
        state, queries = self._fetch_price_bookmark(float('nan'), 2.5)

        self.assertDictEqual({'replication_key': 'price', 'replication_key_value': 2.5, 'version': 1}, state)
        self.assertEqual(state, json.loads(json.dumps(state, allow_nan=False)))
        self.assertEqual([call(MAX_PRICE_SQL), call(FINITE_MAX_PRICE_SQL)], queries)

    def test_fetch_current_incremental_key_pos_infinite_max_bookmarks_finite_maximum(self):
        """An Infinity maximum bookmarks the largest finite key, which strict JSON state can hold."""
        state, queries = self._fetch_price_bookmark(float('inf'), -3.25)

        self.assertDictEqual({'replication_key': 'price', 'replication_key_value': -3.25, 'version': 1}, state)
        self.assertEqual(state, json.loads(json.dumps(state, allow_nan=False)))
        self.assertEqual([call(MAX_PRICE_SQL), call(FINITE_MAX_PRICE_SQL)], queries)

    def test_fetch_current_incremental_key_pos_negative_infinite_max_returns_empty_state(self):
        """A -Infinity maximum means no finite key exists, so Singer starts without a bookmark."""
        with self.assertLogs(tap_postgres.LOGGER, 'WARNING') as logs:
            state, queries = self._fetch_price_bookmark(float('-inf'), None)

        self.assertEqual({}, state)
        self.assertEqual([call(MAX_PRICE_SQL), call(FINITE_MAX_PRICE_SQL)], queries)
        self.assertEqual([EMPTY_BOOKMARK_WARNING], [record.getMessage() for record in logs.records])

    def test_fetch_current_incremental_key_pos_only_nonfinite_keys_return_empty_state(self):
        """NaN, Infinity and -Infinity keys alone return the existing empty-bookmark warning."""
        with self.assertLogs(tap_postgres.LOGGER, 'WARNING') as logs:
            state, queries = self._fetch_price_bookmark(float('nan'), None)

        self.assertEqual({}, state)
        self.assertEqual([call(MAX_PRICE_SQL), call(FINITE_MAX_PRICE_SQL)], queries)
        self.assertEqual([EMPTY_BOOKMARK_WARNING], [record.getMessage() for record in logs.records])

    def test_fetch_current_incremental_key_pos_finite_float_max_runs_one_query(self):
        """A finite float maximum keeps the existing single MAX query and number bookmark."""
        state, queries = self._fetch_price_bookmark(2.5)

        self.assertDictEqual({'replication_key': 'price', 'replication_key_value': 2.5, 'version': 1}, state)
        self.assertEqual([call(MAX_PRICE_SQL)], queries)

    def test_fetch_current_incremental_key_pos_empty_float_table_returns_empty_state(self):
        """An empty table keeps the existing single MAX query and empty-bookmark warning."""
        with self.assertLogs(tap_postgres.LOGGER, 'WARNING') as logs:
            state, queries = self._fetch_price_bookmark(None)

        self.assertEqual({}, state)
        self.assertEqual([call(MAX_PRICE_SQL)], queries)
        self.assertEqual([EMPTY_BOOKMARK_WARNING], [record.getMessage() for record in logs.records])

    def test_fetch_current_incremental_key_pos_numeric_nan_max_is_unchanged(self):
        """Only float maxima take the finite fallback; numeric NaN keeps its existing text bookmark."""
        state, queries = self._fetch_price_bookmark(Decimal('NaN'))

        self.assertDictEqual({'replication_key': 'price', 'replication_key_value': 'NaN', 'version': 1}, state)
        self.assertEqual([call(MAX_PRICE_SQL)], queries)

    def test_fetch_current_incremental_key_pos_finite_maximum_quotes_identifiers(self):
        """The finite fallback quotes each identifier and binds no parameters, so % stays literal."""
        rows = [[{'key_value': float('inf')}], [{'key_value': 7.0}]]
        with patch.object(self.postgres, 'query', side_effect=rows) as query_mock:
            state = self.postgres.fetch_current_incremental_key_pos('public.Margin %', 'unit_price')

        self.assertEqual(7.0, state['replication_key_value'])
        self.assertEqual(
            call(
                'SELECT MAX("unit_price") AS key_value FROM "public"."Margin %" '
                "WHERE \"unit_price\" > '-Infinity' AND \"unit_price\" < 'Infinity'"
            ),
            query_mock.call_args_list[1],
        )
