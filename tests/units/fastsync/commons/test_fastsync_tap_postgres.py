import base64
import datetime
import io
import json

from decimal import Decimal
from unittest import TestCase
from unittest.mock import MagicMock, Mock, call, patch

from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres
from pipelinewise.fastsync.commons import tap_postgres
from pipelinewise.fastsync.commons.partial_sync_boundary import (
    PartialSyncBoundary,
)


def _managed_publication_comment(state='ready', original_comment=None):
    payload = json.dumps({
        'state': state,
        'original_comment': original_comment,
    }, separators=(',', ':')).encode()
    return (
        tap_postgres.PUBLICATION_FENCE_COMMENT_PREFIX
        + base64.urlsafe_b64encode(payload).decode()
    )


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

    def test_validate_replication_slot_identity_requires_canonical_tap_id(self):
        """Canonical tap IDs are injective and fit PostgreSQL slot identifiers."""
        valid = 't' * 50
        assert FastSyncTapPostgres.validate_replication_slot_identity('source_db', valid) == (
            f'pipelinewise_{valid}',
            'pipelinewise_source_db',
            f'pipelinewise_source_db_{valid}',
        )
        for invalid in ('', 'UPPER', 'has-hyphen', 'has.dot', 't' * 51):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                RuntimeError, 'lowercase ASCII letters.*at most 50 characters'
            ):
                FastSyncTapPostgres.validate_replication_slot_identity('source_db', invalid)

    def test_validate_replication_slot_identity_rejects_legacy_collision(self):
        """A tap cannot migrate when its canonical name aliases the legacy database slot."""
        with self.assertRaisesRegex(RuntimeError, 'collides with a historical wal2json slot'):
            FastSyncTapPostgres.validate_replication_slot_identity('same_name', 'same_name')

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
            self.postgres, 'create_replication_slot'
        ) as create_replication_slot, patch.object(
            self.postgres, 'query', return_value=[{'current_lsn': '0/2A'}]
        ) as query:
            bookmark = self.postgres.fetch_current_log_pos()

        self.assertEqual({'lsn': 42, 'version': 1}, bookmark)
        create_replication_slot.assert_called_once_with()
        query.assert_called_once_with('SELECT pg_current_wal_lsn() AS current_lsn')
        primary_connection.close.assert_called_once_with()
        self.assertIsNone(self.postgres.primary_host_conn)

    def test_fetch_current_log_pos_uses_current_wal_function_on_replica(self):
        """Supported replicas use the current WAL function name."""
        self.postgres.connection_config['replica_host'] = 'replica'
        primary_connection = Mock()

        with patch.object(
            self.postgres, 'get_connection', return_value=primary_connection
        ), patch.object(
            self.postgres, 'create_replication_slot'
        ), patch.object(
            self.postgres, 'primary_host_query', return_value=[{'current_lsn': '0/2A'}]
        ), patch.object(
            self.postgres, 'query', return_value=[{'current_lsn': '0/2A'}]
        ) as query:
            bookmark = self.postgres.fetch_current_log_pos()

        self.assertEqual({'lsn': 42, 'version': 1}, bookmark)
        query.assert_called_once_with('SELECT pg_last_wal_replay_lsn() AS current_lsn')

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

    def test_replica_bookmark_waits_for_primary_slot_and_publication_boundary(self):
        """A lagging snapshot must not start before pgoutput can publish its changes."""
        self.postgres.connection_config['replica_host'] = 'replica'
        primary_connection = Mock()

        with patch.object(
            self.postgres, 'get_connection', return_value=primary_connection
        ), patch.object(
            self.postgres, 'create_replication_slot'
        ), patch.object(
            self.postgres, 'primary_host_query', return_value=[{'current_lsn': '0/40'}]
        ), patch.object(
            self.postgres, 'query', side_effect=[
                [{'current_lsn': '0/20'}],
                [{'current_lsn': '0/50'}],
            ]
        ) as query, patch.object(tap_postgres, 'sleep') as sleep:
            bookmark = self.postgres.fetch_current_log_pos()

        self.assertEqual({'lsn': 80, 'version': 1}, bookmark)
        self.assertEqual(query.call_count, 2)
        sleep.assert_called_once_with(1)
        primary_connection.close.assert_called_once_with()

    def test_replica_replay_timeout_prevents_snapshot_bookmark(self):
        """A stopped replica cannot establish a bookmark behind the publication."""
        self.postgres.connection_config['replica_host'] = 'replica'
        primary_connection = Mock()

        with patch.object(
            self.postgres, 'get_connection', return_value=primary_connection
        ), patch.object(
            self.postgres, 'create_replication_slot'
        ), patch.object(
            self.postgres, 'primary_host_query', return_value=[{'current_lsn': '0/40'}]
        ), patch.object(
            self.postgres, 'query', return_value=[{'current_lsn': '0/20'}]
        ), patch.object(
            tap_postgres, 'monotonic', side_effect=[0, 301]
        ), patch.object(tap_postgres, 'sleep') as sleep, self.assertRaisesRegex(
            RuntimeError, 'No snapshot was exported.*Resolve replica lag'
        ):
            self.postgres.fetch_current_log_pos()

        primary_connection.close.assert_called_once_with()
        self.assertIsNone(self.postgres.primary_host_conn)
        sleep.assert_not_called()

    def test_replica_replay_waits_when_no_wal_has_replayed(self):
        """A starting replica must produce a safe replay position before export."""
        with patch.object(
            self.postgres, 'query', side_effect=[
                [{'current_lsn': None}],
                [{'current_lsn': '0/40'}],
            ]
        ), patch.object(tap_postgres, 'sleep') as sleep:
            self.assertEqual(self.postgres._wait_for_replica_replay(64), 64)

        sleep.assert_called_once_with(1)

    def test_create_replication_slot_creates_fresh_pgoutput_slot_by_tap_id(self):
        """A fresh FullSync reserves WAL with the canonical pgoutput slot."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = []
        self.postgres.primary_host_conn = connection

        self.postgres.create_replication_slot()

        assert cursor.execute.call_args_list == [
            call(
                'SELECT slot_name, database, plugin, active FROM pg_replication_slots '
                'WHERE slot_name IN (%s, %s, %s)',
                (
                    'pipelinewise_test_tap',
                    'pipelinewise_test_database',
                    'pipelinewise_test_database_test_tap',
                ),
            ),
            call(
                'SELECT * FROM pg_create_logical_replication_slot(%s, %s)',
                ('pipelinewise_test_tap', 'pgoutput'),
            ),
        ]

    def test_create_replication_slot_copies_wal2json_without_dropping_source(self):
        """Migration keeps the tap-specific historical wal2json source slot."""
        current = ('pipelinewise_test_database_test_tap', 'test_database', 'wal2json', False)
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [current]
        self.postgres.primary_host_conn = connection

        self.postgres.create_replication_slot()

        cursor.execute.assert_called_with(
            'SELECT * FROM pg_copy_logical_replication_slot(%s, %s, %s, %s)',
            (current[0], 'pipelinewise_test_tap', False, 'pgoutput'),
        )
        self.assertFalse(any('pg_drop_replication_slot' in call_.args[0]
                             for call_ in cursor.execute.call_args_list))

    def test_create_replication_slot_rejects_shared_database_wide_source(self):
        """A database-wide slot has no provable single-tap owner."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ('pipelinewise_test_database', 'test_database', 'wal2json', False),
        ]
        self.postgres.primary_host_conn = connection

        with self.assertRaisesRegex(RuntimeError, 'database-wide PostgreSQL slot.*may be shared'):
            self.postgres.create_replication_slot()

        self.assertEqual(cursor.execute.call_count, 1)

    def test_create_replication_slot_prefers_owned_source_over_database_wide_slot(self):
        """An unrelated shared slot does not block migration of the tap-owned slot."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ('pipelinewise_test_database', 'test_database', 'wal2json', False),
            ('pipelinewise_test_database_test_tap', 'test_database', 'wal2json', False),
        ]
        self.postgres.primary_host_conn = connection

        self.postgres.create_replication_slot()

        cursor.execute.assert_called_with(
            'SELECT * FROM pg_copy_logical_replication_slot(%s, %s, %s, %s)',
            ('pipelinewise_test_database_test_tap', 'pipelinewise_test_tap', False, 'pgoutput'),
        )

    def test_create_replication_slot_accepts_existing_pgoutput_slot(self):
        """A valid canonical slot makes setup idempotent, even if it is active."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ('pipelinewise_test_tap', 'test_database', 'pgoutput', True),
            ('pipelinewise_test_database', 'test_database', 'wal2json', False),
        ]
        self.postgres.primary_host_conn = connection

        self.postgres.create_replication_slot()

        self.assertEqual(cursor.execute.call_count, 1)

    def test_create_replication_slot_rejects_incompatible_or_active_source(self):
        """Migration validates plugin, database, and inactivity before copying."""
        cases = [
            [('pipelinewise_test_tap', 'test_database', 'wal2json', False)],
            [('pipelinewise_test_tap', 'other_database', 'pgoutput', False)],
            [('pipelinewise_test_database_test_tap', 'test_database', 'wal2json', True)],
            [('pipelinewise_test_database_test_tap', 'test_database', 'pgoutput', False)],
        ]
        for rows in cases:
            with self.subTest(rows=rows):
                connection = MagicMock()
                cursor = connection.cursor.return_value.__enter__.return_value
                cursor.fetchall.return_value = rows
                self.postgres.primary_host_conn = connection

                with self.assertRaisesRegex(RuntimeError, 'No source changes were made'):
                    self.postgres.create_replication_slot()

                self.assertEqual(cursor.execute.call_count, 1)

    def test_create_replication_slot_validates_concurrent_duplicate(self):
        """A concurrent creator is accepted only when it produced the expected slot."""
        class DuplicateSlot(tap_postgres.psycopg2.Error):
            pgcode = '42710'

        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.side_effect = [
            [],
            [('pipelinewise_test_tap', 'test_database', 'pgoutput', False)],
        ]
        cursor.execute.side_effect = [None, DuplicateSlot('already exists'), None]
        self.postgres.primary_host_conn = connection

        self.postgres.create_replication_slot()

        self.assertEqual(cursor.execute.call_count, 3)

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
        connect_mock.return_value.server_version = 139999
        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
        }

        with self.assertRaisesRegex(
            RuntimeError,
            'PostgreSQL 14 or later.*server_version_num 139999',
        ) as error:
            FastSyncTapPostgres.get_connection(creds, prioritize_primary=True)

        self.assertIsInstance(error.exception, tap_postgres.UnsupportedPostgresVersionError)
        connect_mock.return_value.close.assert_called_once_with()

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_allows_unsupported_version_for_config_removal(
        self, connect_mock
    ):
        """Removed-tap cleanup can still connect to an obsolete source."""
        connect_mock.return_value.server_version = 139999
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
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = []
        cursor.fetchone.return_value = None
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
        connect_mock.return_value.server_version = 139999
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

    def test_reset_slot_drops_only_pgoutput_slot_after_state_invalidation(self):
        """Only the canonical pgoutput slot is replaced, after state is durable."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [('pipelinewise_my_tap', 'my_db', 'pgoutput', False)]
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
                'WHERE slot_name IN (%s, %s, %s)',
                ('pipelinewise_my_tap', 'pipelinewise_my_db', 'pipelinewise_my_db_my_tap'),
            ),
            call.before_reset(),
            call.execute('SELECT pg_drop_replication_slot(%s)', ('pipelinewise_my_tap',)),
            call.execute(
                'SELECT * FROM pg_create_logical_replication_slot(%s, %s)',
                ('pipelinewise_my_tap', 'pgoutput'),
            ),
        ]
        connection.close.assert_called_once_with()

    def test_reset_slot_copies_historical_slot_after_state_invalidation(self):
        """First reset migrates one old boundary and retains its source slot."""
        legacy = ('pipelinewise_my_db_my_tap', 'my_db', 'wal2json', False)
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [legacy]
        before_reset = MagicMock(return_value='state.backup')
        calls = MagicMock()
        calls.attach_mock(cursor.execute, 'execute')
        calls.attach_mock(before_reset, 'before_reset')

        with patch.object(FastSyncTapPostgres, 'get_connection', return_value=connection):
            FastSyncTapPostgres.reset_slot(
                {'dbname': 'my_db', 'tap_id': 'my_tap'}, before_reset=before_reset
            )

        assert calls.mock_calls == [
            call.execute(
                'SELECT slot_name, database, plugin, active FROM pg_replication_slots '
                'WHERE slot_name IN (%s, %s, %s)',
                ('pipelinewise_my_tap', 'pipelinewise_my_db', 'pipelinewise_my_db_my_tap'),
            ),
            call.before_reset(),
            call.execute(
                'SELECT * FROM pg_copy_logical_replication_slot(%s, %s, %s, %s)',
                ('pipelinewise_my_db_my_tap', 'pipelinewise_my_tap', False, 'pgoutput'),
            ),
        ]
        self.assertFalse(any('pg_drop_replication_slot' in call_.args[0]
                             for call_ in cursor.execute.call_args_list))
        connection.close.assert_called_once_with()

    def test_reset_slot_rejects_shared_database_wide_slot_before_state_invalidation(self):
        """Reset cannot claim a database-wide slot when no tap-owned slot exists."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ('pipelinewise_my_db', 'my_db', 'wal2json', False),
        ]
        cursor.fetchone.return_value = None
        before_reset = MagicMock()

        with patch.object(FastSyncTapPostgres, 'get_connection', return_value=connection):
            with self.assertRaisesRegex(RuntimeError, 'database-wide PostgreSQL slot.*may be shared'):
                FastSyncTapPostgres.reset_slot(
                    {'dbname': 'my_db', 'tap_id': 'my_tap'}, before_reset=before_reset
                )

        before_reset.assert_not_called()
        self.assertEqual(cursor.execute.call_count, 1)
        connection.close.assert_called_once_with()

    def test_reset_slot_rejects_active_and_incompatible_destination_before_state_changes(self):
        """Reset validates the canonical or selected migration source before state changes."""
        destination = 'pipelinewise_my_tap'
        cases = [
            ([(destination, database, plugin, active)], 'must belong')
            for database, plugin, active in (
                ('my_db', 'pgoutput', True),
                ('other_db', 'pgoutput', False),
                ('my_db', 'wal2json', False),
            )
        ] + [
            ([(source, database, plugin, active)], 'must belong')
            for source, database, plugin, active in (
                ('pipelinewise_my_db_my_tap', 'my_db', 'wal2json', True),
                ('pipelinewise_my_db_my_tap', 'other_db', 'wal2json', False),
                ('pipelinewise_my_db_my_tap', 'my_db', 'pgoutput', False),
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
            'SELECT * FROM pg_create_logical_replication_slot(%s, %s)',
            ('pipelinewise_my_tap', 'pgoutput'),
        )
        connection.close.assert_called_once_with()

    def test_reset_slot_name_length_boundary(self):
        """Accept exactly 63 characters; reject a longer name before SQL or state changes."""
        slot_prefix = 'pipelinewise_'
        for length in (63, 64):
            with self.subTest(length=length):
                config = {'dbname': 'my_db', 'tap_id': 't' * (length - len(slot_prefix))}
                connection = MagicMock()
                cursor = connection.cursor.return_value.__enter__.return_value
                cursor.fetchall.return_value = []
                before_reset = MagicMock(return_value=None)
                with patch.object(FastSyncTapPostgres, 'get_connection', return_value=connection):
                    if length == 64:
                        with self.assertRaisesRegex(RuntimeError, 'at most 50 characters'):
                            FastSyncTapPostgres.reset_slot(config, before_reset=before_reset)
                        cursor.execute.assert_not_called()
                        before_reset.assert_not_called()
                    else:
                        FastSyncTapPostgres.reset_slot(config, before_reset=before_reset)
                        before_reset.assert_called_once_with()
                        cursor.execute.assert_called_with(
                            'SELECT * FROM pg_create_logical_replication_slot(%s, %s)',
                            (slot_prefix + config['tap_id'], 'pgoutput'),
                        )
                connection.close.assert_called_once_with()

    def test_reset_slot_rejects_legacy_name_collision_before_sql_or_state(self):
        """A colliding historical identity fails before inspecting or invalidating anything."""
        connection = MagicMock()
        before_reset = MagicMock()
        with patch.object(FastSyncTapPostgres, 'get_connection', return_value=connection):
            with self.assertRaisesRegex(RuntimeError, 'collides with a historical wal2json slot'):
                FastSyncTapPostgres.reset_slot(
                    {'dbname': 'same_name', 'tap_id': 'same_name'},
                    before_reset=before_reset,
                )
        connection.cursor.return_value.__enter__.return_value.execute.assert_not_called()
        before_reset.assert_not_called()
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
    def test_drop_slot_removes_owned_slots_and_preserves_database_wide_slot(self, connect_mock):
        """Deleted-tap cleanup does not claim a potentially shared slot."""
        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
            'ssl': 'true',
            'tap_id': 'tap_test',
        }
        connection = MagicMock()
        connection.server_version = 140000
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ('pipelinewise_tap_test', 'my_db', 'pgoutput', False),
            ('pipelinewise_my_db_tap_test', 'my_db', 'wal2json', False),
            ('pipelinewise_my_db', 'my_db', 'wal2json', False),
        ]
        cursor.fetchone.return_value = None
        connect_mock.return_value = connection

        self.postgres.drop_slot(creds)

        assert cursor.execute.call_args_list == [
            call(
                'SELECT slot_name, database, plugin, active FROM pg_replication_slots '
                'WHERE slot_name IN (%s, %s, %s)',
                ('pipelinewise_tap_test', 'pipelinewise_my_db_tap_test', 'pipelinewise_my_db'),
            ),
            call(
                'SELECT publication.pubname, owner.rolname, actor.rolsuper, current_user, '
                "pg_catalog.obj_description(publication.oid, 'pg_publication') "
                'FROM pg_catalog.pg_publication AS publication '
                'JOIN pg_catalog.pg_roles AS owner ON owner.oid = publication.pubowner '
                'JOIN pg_catalog.pg_roles AS actor ON actor.rolname = current_user '
                'WHERE publication.pubname = %s',
                ('pw_pub_tap_test',),
            ),
            call('SELECT pg_drop_replication_slot(%s)', ('pipelinewise_tap_test',)),
            call('SELECT pg_drop_replication_slot(%s)', ('pipelinewise_my_db_tap_test',)),
        ]
        connection.close.assert_called_once_with()

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_drop_slot_rejects_any_unsafe_candidate_before_dropping(self, connect_mock):
        """Cleanup preflights every candidate so it cannot leave a partial known result."""
        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
            'ssl': 'true',
            'tap_id': 'tap_test',
        }
        for unsafe in (
            ('pipelinewise_tap_test', 'my_db', 'pgoutput', True),
            ('pipelinewise_my_db_tap_test', 'other_db', 'wal2json', False),
        ):
            with self.subTest(unsafe=unsafe):
                connection = MagicMock()
                connection.server_version = 140000
                cursor = connection.cursor.return_value.__enter__.return_value
                cursor.fetchall.return_value = [
                    ('pipelinewise_tap_test', 'my_db', 'pgoutput', False),
                    unsafe,
                ]
                connect_mock.return_value = connection

                with self.assertRaisesRegex(RuntimeError, 'No source changes were made'):
                    self.postgres.drop_slot(creds)

                self.assertEqual(cursor.execute.call_count, 1)
                connection.close.assert_called_once_with()

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_drop_slot_preserves_shared_wal2json_name_collision(self, connect_mock):
        """Even an ambiguous old tap ID cannot prove ownership of a shared slot."""
        connection = MagicMock(server_version=140000)
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ('pipelinewise_same_name', 'same_name', 'wal2json', False),
        ]
        cursor.fetchone.return_value = None
        connect_mock.return_value = connection

        FastSyncTapPostgres.drop_slot({
            'host': 'host',
            'port': 5432,
            'user': 'user',
            'password': 'password',
            'dbname': 'same_name',
            'tap_id': 'same_name',
        })

        self.assertEqual(
            sum('pg_drop_replication_slot' in item.args[0] for item in cursor.execute.call_args_list),
            0,
        )

    def test_drop_slot_preserves_canonical_slot_for_legacy_invalid_tap_id(self):
        """A normalized legacy ID cannot claim another tap's canonical slot."""
        connection = MagicMock(server_version=140000)
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ('pipelinewise_foo_bar', 'my_db', 'pgoutput', True),
            ('pipelinewise_my_db_foo_bar', 'my_db', 'wal2json', False),
        ]
        cursor.fetchone.return_value = None

        with patch.object(
                FastSyncTapPostgres,
                'get_connection',
                return_value=connection), patch.object(
                tap_postgres.LOGGER,
                'warning') as warning:
            FastSyncTapPostgres.drop_slot({
                'dbname': 'my_db',
                'tap_id': 'foo-bar',
            })

        warning.assert_any_call(
            'Leaving canonical PostgreSQL slot "%s" and publication derived from '
            'legacy tap ID %r unchanged. The tap ID does not satisfy the canonical '
            'naming rules and may collide with another tap; complete manual review.',
            'pipelinewise_foo_bar',
            'foo-bar',
        )
        self.assertEqual(
            [
                item.args[1][0]
                for item in cursor.execute.call_args_list
                if 'pg_drop_replication_slot' in item.args[0]
            ],
            ['pipelinewise_my_db_foo_bar'],
        )
        self.assertFalse(any(
            'pg_publication' in repr(item.args[0])
            for item in cursor.execute.call_args_list
        ))
        connection.close.assert_called_once_with()

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_drop_slot_removes_owned_publication(self, connect_mock):
        """Deleted-tap cleanup removes the publication that affects source DML."""
        connection = MagicMock(server_version=140000)
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ('pipelinewise_tap_test', 'my_db', 'pgoutput', False),
        ]
        cursor.fetchone.return_value = (
            'pw_pub_tap_test', 'my_user', False, 'my_user',
            _managed_publication_comment(),
        )
        connect_mock.return_value = connection

        FastSyncTapPostgres.drop_slot({
            'host': 'host',
            'port': 5432,
            'user': 'my_user',
            'password': 'password',
            'dbname': 'my_db',
            'tap_id': 'tap_test',
        })

        self.assertTrue(any(
            'DROP PUBLICATION' in repr(item.args[0])
            for item in cursor.execute.call_args_list
        ))

    def test_managed_publication_provenance_accepts_fence_states_and_original_comment(self):
        """Both emitted fence states prove ownership, including an adopted DBA comment."""
        for state, original_comment in (
                ('pending', None),
                ('ready', 'DBA-owned publication')):
            with self.subTest(state=state, original_comment=original_comment):
                self.assertTrue(FastSyncTapPostgres._is_managed_publication_comment(
                    _managed_publication_comment(state, original_comment)))

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_drop_slot_rejects_foreign_publication_before_source_changes(self, connect_mock):
        """Cleanup cannot partially mutate slots when it cannot drop the publication."""
        connection = MagicMock(server_version=140000)
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ('pipelinewise_tap_test', 'my_db', 'pgoutput', False),
        ]
        cursor.fetchone.return_value = (
            'pw_pub_tap_test', 'dba', False, 'my_user',
            _managed_publication_comment(),
        )
        connect_mock.return_value = connection

        with self.assertRaisesRegex(RuntimeError, 'publication.*owned by.*dba'):
            FastSyncTapPostgres.drop_slot({
                'host': 'host',
                'port': 5432,
                'user': 'my_user',
                'password': 'password',
                'dbname': 'my_db',
                'tap_id': 'tap_test',
            })

        self.assertFalse(any(
            'pg_drop_replication_slot' in repr(item.args[0])
            or 'DROP PUBLICATION' in repr(item.args[0])
            for item in cursor.execute.call_args_list
        ))

    def test_drop_slot_rejects_publication_without_managed_provenance(self):
        """Cleanup preserves an ambiguous publication and all slots for manual review."""
        comments = (
            None,
            'DBA-owned publication',
            tap_postgres.PUBLICATION_FENCE_COMMENT_PREFIX + 'not-base64',
            _managed_publication_comment() + '!',
            tap_postgres.PUBLICATION_FENCE_COMMENT_PREFIX
            + base64.urlsafe_b64encode(json.dumps({
                'state': 'unknown',
                'original_comment': None,
            }).encode()).decode(),
        )
        for comment in comments:
            with self.subTest(comment=comment):
                connection = MagicMock(server_version=140000)
                cursor = connection.cursor.return_value.__enter__.return_value
                cursor.fetchall.return_value = [
                    ('pipelinewise_tap_test', 'my_db', 'pgoutput', False),
                ]
                cursor.fetchone.return_value = (
                    'pw_pub_tap_test', 'my_user', False, 'my_user', comment,
                )
                with patch.object(
                        FastSyncTapPostgres,
                        'get_connection',
                        return_value=connection), self.assertRaisesRegex(
                        RuntimeError, 'managed metadata.*manual review'):
                    FastSyncTapPostgres.drop_slot({
                        'dbname': 'my_db',
                        'tap_id': 'tap_test',
                    })

                self.assertFalse(any(
                    'pg_drop_replication_slot' in repr(item.args[0])
                    or 'DROP PUBLICATION' in repr(item.args[0])
                    for item in cursor.execute.call_args_list
                ))
                connection.close.assert_called_once_with()

    def test_advance_migrated_replication_slot_advances_both_bridge_slots(self):
        """A target-durable bridge boundary releases WAL on both retained slots."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ('pipelinewise_my_tap', 'my_db', 'pgoutput', False, '0/64'),
            ('pipelinewise_my_db_my_tap', 'my_db', 'wal2json', False, '0/65'),
        ]
        cursor.fetchone.side_effect = [
            ('pipelinewise_my_tap', '0/6E'),
            ('pipelinewise_my_db_my_tap', '0/6E'),
        ]
        marker = {
            'version': 1,
            'phase': 'bridge',
            'source_slot': 'pipelinewise_my_db_my_tap',
            'destination_slot': 'pipelinewise_my_tap',
            'copy_lsn': 99,
            'bridge_lsn': 110,
        }
        with patch.object(FastSyncTapPostgres, 'get_connection', return_value=connection):
            updated = FastSyncTapPostgres.advance_migrated_replication_slot(
                {'dbname': 'my_db', 'tap_id': 'my_tap'}, marker
            )

        assert updated == {**marker, 'phase': 'pgoutput'}
        assert cursor.execute.call_args_list == [
            call(
                'SELECT slot_name, database, plugin, active, confirmed_flush_lsn::text '
                'FROM pg_replication_slots WHERE slot_name = ANY(%s)',
                (['pipelinewise_my_tap', 'pipelinewise_my_db_my_tap'],),
            ),
            call(
                'SELECT slot_name, end_lsn::text '
                'FROM pg_replication_slot_advance(%s, %s::pg_lsn)',
                ('pipelinewise_my_tap', '0/6E'),
            ),
            call(
                'SELECT slot_name, end_lsn::text '
                'FROM pg_replication_slot_advance(%s, %s::pg_lsn)',
                ('pipelinewise_my_db_my_tap', '0/6E'),
            ),
        ]
        connection.close.assert_called_once_with()

    def test_advance_canonical_replication_slot_is_idempotent(self):
        """A retry performs no advance when confirmed WAL is already beyond durable state."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ('pipelinewise_my_tap', 'my_db', 'pgoutput', False, '0/C8'),
        ]
        with patch.object(FastSyncTapPostgres, 'get_connection', return_value=connection):
            FastSyncTapPostgres.advance_canonical_replication_slot(
                {'dbname': 'my_db', 'tap_id': 'my_tap'}, 100
            )
        self.assertEqual(cursor.execute.call_count, 1)
        connection.close.assert_called_once_with()

    def test_advance_canonical_replication_slot_validates_returned_boundary(self):
        """An incomplete server advance cannot be treated as durable progress."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ('pipelinewise_my_tap', 'my_db', 'pgoutput', False, '0/64'),
        ]
        cursor.fetchone.return_value = ('pipelinewise_my_tap', '0/6D')
        with patch.object(
            FastSyncTapPostgres, 'get_connection', return_value=connection
        ), self.assertRaisesRegex(RuntimeError, 'did not advance'):
            FastSyncTapPostgres.advance_canonical_replication_slot(
                {'dbname': 'my_db', 'tap_id': 'my_tap'}, 110
            )
        connection.close.assert_called_once_with()

    def test_retire_migrated_replication_slot_advances_then_drops_source(self):
        """Retirement proves canonical durability before deleting the inactive source."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ('pipelinewise_my_tap', 'my_db', 'pgoutput', False),
            ('pipelinewise_my_db_my_tap', 'my_db', 'wal2json', False),
        ]
        marker = {
            'version': 1,
            'phase': 'retire',
            'source_slot': 'pipelinewise_my_db_my_tap',
            'destination_slot': 'pipelinewise_my_tap',
            'copy_lsn': 90,
            'bridge_lsn': 100,
            'retire_lsn': 110,
        }
        with patch.object(
            FastSyncTapPostgres, '_advance_replication_slots'
        ) as advance, patch.object(
            FastSyncTapPostgres, 'get_connection', return_value=connection
        ):
            FastSyncTapPostgres.retire_migrated_replication_slot(
                {'dbname': 'my_db', 'tap_id': 'my_tap'}, marker
            )

        advance.assert_called_once_with(
            {'dbname': 'my_db', 'tap_id': 'my_tap'},
            {'pipelinewise_my_tap': 'pgoutput'},
            110,
        )
        assert cursor.execute.call_args_list[-1] == call(
            'SELECT pg_drop_replication_slot(%s)', ('pipelinewise_my_db_my_tap',)
        )
        connection.close.assert_called_once_with()

    def test_retire_migrated_replication_slot_accepts_already_missing_source(self):
        """A retry after an uncertain successful drop is idempotent."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ('pipelinewise_my_tap', 'my_db', 'pgoutput', False),
        ]
        marker = {
            'version': 1,
            'phase': 'retire',
            'source_slot': 'pipelinewise_my_db_my_tap',
            'destination_slot': 'pipelinewise_my_tap',
            'copy_lsn': 90,
            'bridge_lsn': 100,
            'retire_lsn': 110,
        }
        with patch.object(
            FastSyncTapPostgres, '_advance_replication_slots'
        ), patch.object(FastSyncTapPostgres, 'get_connection', return_value=connection):
            FastSyncTapPostgres.retire_migrated_replication_slot(
                {'dbname': 'my_db', 'tap_id': 'my_tap'}, marker
            )
        self.assertEqual(cursor.execute.call_count, 1)
        connection.close.assert_called_once_with()

    def test_retire_migrated_replication_slot_rejects_invalid_marker_before_connecting(self):
        """Malformed or foreign durable state cannot select a source slot for deletion."""
        valid = {
            'version': 1,
            'phase': 'retire',
            'source_slot': 'pipelinewise_my_db_my_tap',
            'destination_slot': 'pipelinewise_my_tap',
            'copy_lsn': 90,
            'bridge_lsn': 100,
            'retire_lsn': 110,
        }
        invalid_markers = [
            {**valid, 'version': 2},
            {**valid, 'phase': 'unknown'},
            {**valid, 'retire_lsn': 100},
            {**valid, 'source_slot': 'pipelinewise_other_db'},
            {**valid, 'destination_slot': 'pipelinewise_other_tap'},
        ]
        for marker in invalid_markers:
            with self.subTest(marker=marker), patch.object(
                FastSyncTapPostgres, 'get_connection'
            ) as get_connection, self.assertRaisesRegex(RuntimeError, 'No source or state changes'):
                FastSyncTapPostgres.retire_migrated_replication_slot(
                    {'dbname': 'my_db', 'tap_id': 'my_tap'}, marker
                )
            get_connection.assert_not_called()

    def test_retire_migrated_replication_slot_requires_safe_catalog_rows(self):
        """Destination continuity and source inactivity are preconditions to deletion."""
        marker = {
            'version': 1,
            'phase': 'retire',
            'source_slot': 'pipelinewise_my_db_my_tap',
            'destination_slot': 'pipelinewise_my_tap',
            'copy_lsn': 90,
            'bridge_lsn': 100,
            'retire_lsn': 110,
        }
        cases = [
            [('pipelinewise_my_db_my_tap', 'my_db', 'wal2json', False)],
            [
                ('pipelinewise_my_tap', 'my_db', 'pgoutput', False),
                ('pipelinewise_my_db_my_tap', 'my_db', 'wal2json', True),
            ],
            [
                ('pipelinewise_my_tap', 'other_db', 'pgoutput', False),
                ('pipelinewise_my_db_my_tap', 'my_db', 'wal2json', False),
            ],
        ]
        for rows in cases:
            with self.subTest(rows=rows):
                connection = MagicMock()
                cursor = connection.cursor.return_value.__enter__.return_value
                cursor.fetchall.return_value = rows
                with patch.object(
                    FastSyncTapPostgres, '_advance_replication_slots'
                ), patch.object(
                    FastSyncTapPostgres, 'get_connection', return_value=connection
                ):
                    with self.assertRaisesRegex(RuntimeError, 'No source'):
                        FastSyncTapPostgres.retire_migrated_replication_slot(
                            {'dbname': 'my_db', 'tap_id': 'my_tap'}, marker
                        )
                self.assertEqual(cursor.execute.call_count, 1)
                connection.close.assert_called_once_with()

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
        test fetch_current_incremental_key_pos where result is decimal, it should return a state with float key value
        """
        with patch.object(self.postgres, 'query') as query_mock:
            query_mock.return_value = [{'key_value': Decimal(4.222222222)}]

            state = self.postgres.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertDictEqual({
                'replication_key': 'id',
                'replication_key_value': 4.222222222,
                'version': 1,
            }, state)
