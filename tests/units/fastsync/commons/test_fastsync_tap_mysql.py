import csv
import io

import pymysql

from unittest import TestCase
from unittest.mock import patch, call, MagicMock, Mock

from pipelinewise.fastsync.commons import tap_mysql
from pipelinewise.fastsync.commons.partial_sync_boundary import (
    PartialSyncBoundary,
)
from pipelinewise.fastsync.commons.tap_mysql import (
    MARIADB_ENGINE,
    MARIADB_MAX_STATEMENT_TIME_SQL,
    MYSQL_MAX_EXECUTION_TIME_SQL,
    FastSyncTapMySql,
)


MYSQL_GTID_SET = (
    '11111111-1111-1111-1111-111111111111:1-3:8-10,'
    '22222222-2222-2222-2222-222222222222:1-55'
)


class FastSyncTapMySqlMock(FastSyncTapMySql):
    """
    Mocked FastSyncTapMySql class
    """

    def __init__(self, connection_config, tap_type_to_target_type=None):
        super().__init__(connection_config, tap_type_to_target_type)

        self.executed_queries_unbuffered = []
        self.executed_queries = []

    def _run_session_sql(self, conn, sql):
        self.query(sql, conn)

    def query(self, query, conn=None, params=None, return_as_cursor=False, n_retry=1):
        if query.startswith('INVALID-SQL'):
            raise pymysql.err.InternalError

        if conn == self.conn_unbuffered:
            self.executed_queries.append(query)
        else:
            self.executed_queries_unbuffered.append(query)

        return []


class TestFastSyncTapMySql(TestCase):
    """
    Unit tests for fastsync tap mysql
    """

    def setUp(self) -> None:
        """Initialise test FastSyncTapPostgres object"""
        self.connection_config = {
            'host': 'foo.com',
            'port': 3306,
            'user': 'my_user',
            'password': 'secret',
            'dbname': 'my_db',
        }
        self.mysql = None

    def test_copy_table_mogrifies_only_the_structured_boundary(self):
        """Driver binding cannot reinterpret percent signs in export SQL."""
        self.mysql = FastSyncTapMySql(
            self.connection_config, lambda value, *_args: value
        )
        table_columns = [{
            'column_name': 'rate%s',
            'safe_sql_value': 'DATE_FORMAT(`event_date`, "%Y-%m-01")',
        }]
        cursor = MagicMock()
        cursor.mogrify.return_value = (
            " WHERE `rate%s` >= 'x\\\\'' OR 1=1 --'"
        )
        cursor.fetchmany.return_value = []
        self.mysql.conn_unbuffered = MagicMock()
        self.mysql.conn_unbuffered.cursor.return_value.__enter__.return_value = (
            cursor
        )
        boundary = PartialSyncBoundary('rate%s', "x\\' OR 1=1 --")

        with patch.object(
            self.mysql, 'get_table_columns', return_value=table_columns
        ), patch.object(tap_mysql.split_gzip, 'open', return_value=io.StringIO()):
            self.mysql.copy_table(
                'my_db.my_table', 'unused.csv', boundary=boundary
            )

        cursor.mogrify.assert_called_once_with(
            ' WHERE `rate%%s` >= %s',
            ("x\\' OR 1=1 --",),
        )
        export_sql = cursor.execute.call_args.args[0]
        self.assertIn('DATE_FORMAT(`event_date`, "%Y-%m-01")', export_sql)
        self.assertIn(" WHERE `rate%s` >= 'x\\\\'' OR 1=1 --'", export_sql)
        self.assertEqual(len(cursor.execute.call_args.args), 1)

    def test_open_connections_with_default_session_sqls(self):
        """MySQL must not receive MariaDB-only session parameters."""
        self.mysql = FastSyncTapMySqlMock(connection_config=self.connection_config)
        with patch('pymysql.connect') as mysql_connect_mock:
            mysql_connect_mock.side_effect = [mysql_connect_mock.return_value, MagicMock()]
            mysql_connect_mock.return_value.get_server_info.return_value = '8.0.39'
            self.mysql.open_connections()

        self.assertListEqual(
            self.mysql.executed_queries, [*tap_mysql.DEFAULT_SESSION_SQLS, MYSQL_MAX_EXECUTION_TIME_SQL]
        )
        self.assertIn('SET @@session.net_write_timeout=3600', self.mysql.executed_queries)
        self.assertNotIn(MARIADB_MAX_STATEMENT_TIME_SQL, self.mysql.executed_queries)
        self.assertListEqual(self.mysql.executed_queries_unbuffered, self.mysql.executed_queries)

    def test_open_connections_with_default_mariadb_session_sqls(self):
        """The handshake detects MariaDB even when engine is omitted."""
        self.mysql = FastSyncTapMySqlMock(connection_config=self.connection_config)
        with patch('pymysql.connect') as mysql_connect_mock:
            mysql_connect_mock.side_effect = [mysql_connect_mock.return_value, MagicMock()]
            mysql_connect_mock.return_value.get_server_info.return_value = '11.4.10-MariaDB-log'
            self.mysql.open_connections()

        self.assertListEqual(
            self.mysql.executed_queries,
            [*tap_mysql.DEFAULT_SESSION_SQLS, MARIADB_MAX_STATEMENT_TIME_SQL],
        )
        self.assertListEqual(self.mysql.executed_queries_unbuffered, self.mysql.executed_queries)
        mysql_connect_mock.return_value.get_server_info.assert_called_once_with()
        self.assertEqual(self.mysql.source_engine, MARIADB_ENGINE)
        self.assertTrue(self.mysql.is_mariadb)
        self.assertEqual(self.mysql.connection_config['engine'], MARIADB_ENGINE)
        self.assertFalse(MARIADB_MAX_STATEMENT_TIME_SQL.endswith(';'))

    def test_initialization_does_not_mutate_caller_connection_config(self):
        """FastSync defaults remain private to each source instance."""
        original_config = self.connection_config.copy()

        self.mysql = FastSyncTapMySql(self.connection_config, lambda value: value)

        self.assertEqual(self.connection_config, original_config)
        self.assertIsNot(self.mysql.connection_config, self.connection_config)
        self.assertNotIn('engine', self.mysql.connection_config)
        self.assertEqual(self.mysql.connection_config['charset'], tap_mysql.DEFAULT_CHARSET)

    def test_partial_session_overrides_keep_defaults_on_both_connections(self):
        for engine in ('mysql', 'mariadb'):
            statement_timeout = 'max_statement_time' if engine == 'mariadb' else 'max_execution_time'
            for custom_sqls in ([], None, ['SET SESSION net_write_timeout=7200'],
                                [f'SET SESSION {statement_timeout}=123']):
                with self.subTest(engine=engine, custom_sqls=custom_sqls):
                    config = {**self.connection_config, 'engine': engine, 'session_sqls': custom_sqls}
                    source = FastSyncTapMySqlMock(config)
                    with patch('pymysql.connect') as connect:
                        connect.side_effect = [Mock(), Mock()]
                        source.open_connections()

                    expected = [
                        'SET @@session.time_zone="+0:00"',
                        'SET @@session.wait_timeout=28800',
                        'SET @@session.net_read_timeout=3600',
                        'SET @@session.net_write_timeout=3600',
                        'SET @@session.innodb_lock_wait_timeout=3600',
                    ]
                    if engine == 'mariadb':
                        expected.append('SET @@session.max_statement_time=0')
                    else:
                        expected.append('SET @@session.max_execution_time=0')
                    expected.extend(custom_sqls or [])
                    self.assertEqual(source.executed_queries, expected)
                    self.assertEqual(source.executed_queries_unbuffered, expected)
                    self.assertEqual(config['session_sqls'], custom_sqls)

    def test_unavailable_builtin_timeout_continues_both_sessions_without_reconnecting(self):
        for engine, timeout_sql in (
            ('mysql', MYSQL_MAX_EXECUTION_TIME_SQL), ('mariadb', MARIADB_MAX_STATEMENT_TIME_SQL)
        ):
            with self.subTest(engine=engine):
                source = FastSyncTapMySql({
                    **self.connection_config, 'engine': engine,
                    'session_sqls': ['SET SESSION net_write_timeout=7200'],
                }, lambda value: value)
                connections = [MagicMock(), MagicMock()]

                def execute(sql):
                    if sql == timeout_sql:
                        raise pymysql.err.OperationalError(1193, 'Unknown system variable')

                for conn in connections:
                    conn.cursor.return_value.__enter__.return_value.execute.side_effect = execute
                with patch('pymysql.connect', side_effect=connections) as connect, \
                        patch.object(source, 'query') as query, self.assertLogs(tap_mysql.LOGGER, 'WARNING') as logs:
                    source.open_connections()

                self.assertEqual(connect.call_count, 2)
                query.assert_not_called()
                self.assertEqual(source.source_engine, engine)
                for conn in connections:
                    cursor = conn.cursor.return_value.__enter__.return_value
                    self.assertEqual(cursor.execute.call_args_list, [
                        call(sql) for sql in [*tap_mysql.DEFAULT_SESSION_SQLS, timeout_sql,
                                             'SET SESSION net_write_timeout=7200']
                    ])
                self.assertIn('Built-in timeout not applied', '\n'.join(logs.output))

    def test_session_operational_errors_are_not_reconnected_or_hidden(self):
        cases = [
            (MYSQL_MAX_EXECUTION_TIME_SQL, 1142, False),
            (MYSQL_MAX_EXECUTION_TIME_SQL, 2013, False),
            (tap_mysql.DEFAULT_SESSION_SQLS[0], 1193, False),
            (MYSQL_MAX_EXECUTION_TIME_SQL, 1193, True),
        ]
        for failing_sql, code, custom in cases:
            with self.subTest(sql=failing_sql, code=code, custom=custom):
                source = FastSyncTapMySql({
                    **self.connection_config, 'engine': 'mysql',
                    'session_sqls': [failing_sql] if custom else [],
                }, lambda value: value)
                connections = [MagicMock(), MagicMock()]
                error = pymysql.err.OperationalError(code, 'Session setup failed')
                seen = 0

                def execute(sql):
                    nonlocal seen
                    if sql == failing_sql:
                        seen += 1
                        if not custom or seen > 1:
                            raise error

                connections[0].cursor.return_value.__enter__.return_value.execute.side_effect = execute
                with patch('pymysql.connect', side_effect=connections) as connect, \
                        patch.object(source, 'query') as query, \
                        self.assertRaises(pymysql.err.OperationalError) as raised:
                    source.open_connections()

                self.assertIs(raised.exception, error)
                self.assertEqual(connect.call_count, 2)
                query.assert_not_called()

    def test_reused_omitted_engine_config_still_detects_mariadb(self):
        """An autoresync preflight construction cannot disable worker detection."""
        preflight_source = FastSyncTapMySql(
            self.connection_config,
            lambda value: value,
        )
        self.assertNotIn('engine', preflight_source.connection_config)

        self.mysql = FastSyncTapMySqlMock(connection_config=self.connection_config)
        with patch('pymysql.connect') as mysql_connect_mock:
            mysql_connect_mock.return_value.get_server_info.return_value = '11.4.10-MariaDB-log'
            self.mysql.open_connections()

        self.assertNotIn('engine', self.connection_config)
        self.assertIsNone(self.mysql._configured_engine)
        self.assertEqual(self.mysql.source_engine, MARIADB_ENGINE)
        self.assertIn(MARIADB_MAX_STATEMENT_TIME_SQL, self.mysql.executed_queries)
        mysql_connect_mock.return_value.get_server_info.assert_called_once_with()

    def test_detected_engine_is_cached_across_reconnections(self):
        self.mysql = FastSyncTapMySqlMock(connection_config=self.connection_config)
        with patch('pymysql.connect') as mysql_connect_mock:
            mysql_connect_mock.return_value.get_server_info.return_value = '11.4.10-MariaDB-log'
            self.mysql.open_connections()
            self.mysql.close_connections()
            self.mysql.open_connections()

        mysql_connect_mock.return_value.get_server_info.assert_called_once_with()
        self.assertEqual(self.mysql.source_engine, MARIADB_ENGINE)

    def test_engine_selection_is_reported_once_at_info(self):
        connection = MagicMock()
        connection.get_server_info.return_value = '11.4.10-MariaDB-log'

        with patch.object(tap_mysql, '_REPORTED_SESSION_ENGINE_SELECTIONS', set()), \
                patch.object(tap_mysql, 'LOGGER') as logger:
            tap_mysql.resolve_source_engine(connection)
            tap_mysql.resolve_source_engine(connection)

        logger.info.assert_called_once_with(
            'Using %s source engine (%s)',
            'mariadb',
            'detected',
        )
        logger.debug.assert_called_once_with(
            'Using %s source engine (%s)',
            'mariadb',
            'detected',
        )

    def test_open_connections_prefers_explicit_engine_for_session_defaults(self):
        cases = (
            ('mysql', '11.4.10-MariaDB-log', [*tap_mysql.DEFAULT_SESSION_SQLS, MYSQL_MAX_EXECUTION_TIME_SQL]),
            (
                MARIADB_ENGINE,
                '8.0.39',
                [*tap_mysql.DEFAULT_SESSION_SQLS, MARIADB_MAX_STATEMENT_TIME_SQL],
            ),
        )

        for configured_engine, server_info, expected_sqls in cases:
            with self.subTest(configured_engine=configured_engine):
                self.mysql = FastSyncTapMySqlMock(connection_config={
                    **self.connection_config,
                    'engine': configured_engine,
                })
                with patch('pymysql.connect') as mysql_connect_mock:
                    mysql_connect_mock.side_effect = [mysql_connect_mock.return_value, MagicMock()]
                    mysql_connect_mock.return_value.get_server_info.return_value = server_info
                    self.mysql.open_connections()

                self.assertListEqual(self.mysql.executed_queries, expected_sqls)
                self.assertEqual(self.mysql.source_engine, configured_engine)
                mysql_connect_mock.return_value.get_server_info.assert_not_called()

    def test_close_connections_is_idempotent(self):
        """Each MySQL connection is closed once and its reference is cleared."""
        self.mysql = FastSyncTapMySql(self.connection_config, lambda value: value)
        buffered_connection = Mock()
        unbuffered_connection = Mock()
        self.mysql.conn = buffered_connection
        self.mysql.conn_unbuffered = unbuffered_connection

        self.mysql.close_connections()
        self.mysql.close_connections()

        buffered_connection.close.assert_called_once_with()
        unbuffered_connection.close.assert_called_once_with()
        self.assertIsNone(self.mysql.conn)
        self.assertIsNone(self.mysql.conn_unbuffered)

    def test_close_connections_attempts_both_when_first_close_fails(self):
        """A buffered close failure cannot leak the streaming connection."""
        self.mysql = FastSyncTapMySql(self.connection_config, lambda value: value)
        buffered_connection = Mock()
        buffered_connection.close.side_effect = RuntimeError('buffered close failed')
        unbuffered_connection = Mock()
        self.mysql.conn = buffered_connection
        self.mysql.conn_unbuffered = unbuffered_connection

        self.mysql.close_connections(silent=True)

        buffered_connection.close.assert_called_once_with()
        unbuffered_connection.close.assert_called_once_with()
        self.assertIsNone(self.mysql.conn)
        self.assertIsNone(self.mysql.conn_unbuffered)

    def test_close_connections_handles_a_partially_open_pair(self):
        """Cleanup closes an unbuffered connection even when the first connect failed."""
        self.mysql = FastSyncTapMySql(self.connection_config, lambda value: value)
        unbuffered_connection = Mock()
        self.mysql.conn = None
        self.mysql.conn_unbuffered = unbuffered_connection

        self.mysql.close_connections()

        unbuffered_connection.close.assert_called_once_with()
        self.assertIsNone(self.mysql.conn)
        self.assertIsNone(self.mysql.conn_unbuffered)

    def test_csv_export_distinguishes_null_from_empty_string(self):
        """An empty string is quoted while SQL NULL remains an empty field."""
        output = io.StringIO()
        writer = tap_mysql._create_csv_writer(output)

        writer.writerow([None, '', 'text', 0])

        self.assertEqual(output.getvalue(), ',"","text","0"\r\n')

    def test_csv_export_preserves_multiline_text_and_csv_syntax(self):
        """Quoted CSV preserves text controls and syntax-sensitive characters."""
        value = (
            'line one\nline two\rline three\r\n'
            '\tliteral \\n and \\t, "Unicode: 雪😀"\\'
        )
        output = io.StringIO()
        writer = tap_mysql._create_csv_writer(output)

        writer.writerow([value])

        expected = '"' + value.replace('"', '""') + '"\r\n'
        self.assertEqual(output.getvalue(), expected)
        self.assertEqual(next(csv.reader(io.StringIO(output.getvalue()))), [value])

    def test_text_projection_preserves_multiline_for_mysql_and_mariadb(self):
        """Both engines remove only NUL before quoted CSV serialization."""
        expected_projection = (
            "ELSE concat('REPLACE(cast(`', column_name, "
            "'` AS char CHARACTER SET utf8mb4)', \", CHAR(0), '')\")"
        )

        for engine in ('mysql', MARIADB_ENGINE):
            with self.subTest(engine=engine):
                self.mysql = FastSyncTapMySqlMock(
                    connection_config={
                        **self.connection_config,
                        'engine': engine,
                    }
                )
                with patch.object(
                    self.mysql, 'query', return_value=[]
                ) as query_mock:
                    self.mysql.get_table_columns('my_db.my_table')

                sql = query_mock.call_args.args[0]
                self.assertIn(expected_projection, sql)
                self.assertNotIn(
                    "ELSE concat('REPLACE(REPLACE(REPLACE(cast(`'", sql
                )

    def test_get_connection_to_primary(self):
        """
        Check that get connection uses the right credentials to connect to primary
        """
        creds = {
            'host': 'my_primary_host',
            'port': 3306,
            'user': 'my_primary_user',
            'password': 'my_primary_user',
        }

        conn_params, is_replica = FastSyncTapMySql(
            connection_config=creds,
            tap_type_to_target_type='testing'
        ).get_connection_parameters()
        self.assertFalse(is_replica)
        self.assertEqual(conn_params['host'], creds['host'])
        self.assertEqual(conn_params['port'], creds['port'])
        self.assertEqual(conn_params['user'], creds['user'])
        self.assertEqual(conn_params['password'], creds['password'])
        self.assertEqual(conn_params['charset'], 'utf8mb4')

    def test_get_connection_to_replica(self):
        """
        Check that get connection uses the right credentials to connect to secondary if present
        """
        creds = {
            'host': 'my_primary_host',
            'replica_host': 'my_replica_host',
            'port': 3306,
            'replica_port': 4406,
            'user': 'my_primary_user',
            'replica_user': 'my_replica_user',
            'password': 'my_primary_user',
            'replica_password': 'my_replica_user',
        }

        conn_params, is_replica = FastSyncTapMySql(
            connection_config=creds,
            tap_type_to_target_type='testing'
        ).get_connection_parameters()
        self.assertTrue(is_replica)
        self.assertEqual(conn_params['host'], creds['replica_host'])
        self.assertEqual(conn_params['port'], creds['replica_port'])
        self.assertEqual(conn_params['user'], creds['replica_user'])
        self.assertEqual(conn_params['password'], creds['replica_password'])

    def test_open_connections_with_session_sqls(self):
        """Custom session parameters should be applied if defined"""
        session_sqls = [
            'SET SESSION max_statement_time=0',
            'SET SESSION wait_timeout=28800',
        ]
        self.mysql = FastSyncTapMySqlMock(
            connection_config={
                **self.connection_config,
                'engine': MARIADB_ENGINE,
                **{'session_sqls': session_sqls},
            }
        )
        with patch('pymysql.connect') as mysql_connect_mock:
            mysql_connect_mock.side_effect = [mysql_connect_mock.return_value, MagicMock()]
            mysql_connect_mock.return_value.get_server_info.return_value = '11.4.10-MariaDB-log'
            self.mysql.open_connections()

        self.assertListEqual(
            self.mysql.executed_queries,
            [
                *tap_mysql.DEFAULT_SESSION_SQLS,
                MARIADB_MAX_STATEMENT_TIME_SQL,
                *session_sqls,
            ],
        )
        mysql_connect_mock.return_value.get_server_info.assert_not_called()
        self.assertListEqual(self.mysql.executed_queries_unbuffered, self.mysql.executed_queries)

    def test_open_connections_with_invalid_session_sqls(self):
        """Invalid SQLs in session_sqls should be ignored"""
        session_sqls = [
            'SET SESSION max_statement_time=0',
            'INVALID-SQL-SHOULD-BE-SILENTLY-IGNORED',
            'SET SESSION wait_timeout=28800',
        ]
        self.mysql = FastSyncTapMySqlMock(
            connection_config={
                **self.connection_config,
                **{'session_sqls': session_sqls},
            }
        )
        with patch('pymysql.connect') as mysql_connect_mock:
            mysql_connect_mock.side_effect = [mysql_connect_mock.return_value, MagicMock()]
            mysql_connect_mock.return_value.get_server_info.return_value = '8.0.39'
            self.mysql.open_connections()

        self.assertListEqual(self.mysql.executed_queries, [
            *tap_mysql.DEFAULT_SESSION_SQLS,
            MYSQL_MAX_EXECUTION_TIME_SQL,
            'SET SESSION max_statement_time=0',
            'SET SESSION wait_timeout=28800',
        ])
        self.assertListEqual(self.mysql.executed_queries_unbuffered, self.mysql.executed_queries)

    def test_fetch_current_log_pos_with_gtid_and_replica_mariadb_engine_succeeds(self):
        """
        If using gtid is enabled and engine is replica mariadb, then expect gtid result
        """
        self.connection_config['use_gtid'] = True
        self.connection_config['engine'] = MARIADB_ENGINE

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)
        self.mysql.is_replica = True

        with patch.object(self.mysql, 'query') as query_method_mock:

            expected_gtid = '0-192-444,1-400-10'

            query_method_mock.side_effect = [
                [{'current_gtids': expected_gtid}],
            ]

            with patch('pymysql.connect') as mysql_connect_mock:
                result = self.mysql.fetch_current_log_pos()

                query_method_mock.assert_called_once_with('select @@gtid_slave_pos as current_gtids;')
                mysql_connect_mock.assert_not_called()

            self.assertDictEqual(result, {'gtid': expected_gtid, 'gtid_complete': True})

    def test_fetch_current_log_pos_with_gtid_and_replica_mariadb_engine_gtid_not_found(self):
        """
        If using gtid is enabled and engine is replica mariadb, the gtid is not found, then expect Exception
        """
        self.connection_config['use_gtid'] = True
        self.connection_config['engine'] = MARIADB_ENGINE

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)
        self.mysql.is_replica = True

        with patch.object(self.mysql, 'query') as query_method_mock:

            query_method_mock.return_value = []

            with self.assertRaises(Exception) as context:
                self.mysql.fetch_current_log_pos()

            self.assertEqual('GTID is not enabled.', str(context.exception))

            query_method_mock.assert_called_once_with('select @@gtid_slave_pos as current_gtids;')

    def test_fetch_current_log_pos_with_gtid_and_primary_mariadb_engine_succeeds(self):
        """
        Keep every MariaDB domain, including transactions from previous primaries.
        """
        self.connection_config['use_gtid'] = True
        self.connection_config['engine'] = MARIADB_ENGINE

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)

        with patch.object(self.mysql, 'query') as query_method_mock:

            expected_gtid = '0-192-444,1-400-10'

            query_method_mock.side_effect = [
                [{'current_gtids': expected_gtid}],
            ]

            result = self.mysql.fetch_current_log_pos()

            query_method_mock.assert_called_once_with('select @@gtid_current_pos as current_gtids;')
            self.assertDictEqual(result, {'gtid': expected_gtid, 'gtid_complete': True})

    def test_fetch_current_log_pos_with_gtid_and_primary_mariadb_engine_no_gtid_found_expect_exception(self):
        """
        If using gtid is enabled and engine is primary mariadb which doesn't return gtid, then expect an exception
        """
        self.connection_config['use_gtid'] = True
        self.connection_config['engine'] = MARIADB_ENGINE

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)

        with patch.object(self.mysql, 'query') as query_method_mock:
            query_method_mock.side_effect = [
                []
            ]

            with self.assertRaises(Exception) as context:
                self.mysql.fetch_current_log_pos()

            self.assertEqual('GTID is not enabled.', str(context.exception))

            query_method_mock.assert_has_calls(
                [
                    call('select @@gtid_current_pos as current_gtids;'),
                ]
            )

    def test_fetch_current_log_pos_rejects_empty_mariadb_gtid_set(self):
        """An empty executed set cannot seed Singer auto-position replication."""
        self.connection_config['use_gtid'] = True
        self.connection_config['engine'] = MARIADB_ENGINE

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)

        with patch.object(self.mysql, 'query') as query_method_mock:

            query_method_mock.return_value = [{'current_gtids': ''}]

            with self.assertRaises(Exception) as context:
                self.mysql.fetch_current_log_pos()

            self.assertEqual('GTID is not enabled.', str(context.exception))

            query_method_mock.assert_called_once_with('select @@gtid_current_pos as current_gtids;')

    def test_fetch_current_log_pos_uses_replica_applied_coordinates(self):
        """
        Received events ahead of the replica snapshot must be replayed by Singer.
        """
        self.connection_config['use_gtid'] = False

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)
        self.mysql.is_replica = True

        with patch.object(self.mysql, 'query') as query_method_mock:
            query_method_mock.return_value = [
                {
                    'Master_Log_File': 'binlog.000002',
                    'Read_Master_Log_Pos': 999,
                    'Relay_Master_Log_File': 'binlog.000001',
                    'Exec_Master_Log_Pos': 444,
                }
            ]

            result = self.mysql.fetch_current_log_pos()

            query_method_mock.assert_called_once_with('SHOW REPLICA STATUS')

            self.assertDictEqual(result, {
                'log_file': 'binlog.000001',
                'log_pos': 444,
                'version': 1,
            })

    def test_fetch_current_log_pos_rejects_missing_replica_applied_coordinates(self):
        """Never replace unavailable applied coordinates with received coordinates."""
        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)
        self.mysql.is_replica = True
        for applied_fields in (
            {},
            {'Relay_Master_Log_File': '', 'Exec_Master_Log_Pos': 0},
            {'Relay_Master_Log_File': 'binlog.000001', 'Exec_Master_Log_Pos': None},
        ):
            with self.subTest(applied_fields=applied_fields), patch.object(
                self.mysql, 'query', return_value=[{
                    'Master_Log_File': 'binlog.000002',
                    'Read_Master_Log_Pos': 999,
                    **applied_fields,
                }]
            ):
                with self.assertRaisesRegex(Exception, 'no applied binary log coordinates'):
                    self.mysql.fetch_current_log_pos()

    def test_fetch_current_log_pos_rejects_multiple_replication_channels(self):
        """Do not checkpoint an arbitrary primary when a replica has multiple sources."""
        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)
        self.mysql.is_replica = True
        with patch.object(self.mysql, 'query', return_value=[
            {'Relay_Master_Log_File': 'source1.000001', 'Exec_Master_Log_Pos': 444},
            {'Relay_Master_Log_File': 'source2.000001', 'Exec_Master_Log_Pos': 555},
        ]):
            with self.assertRaisesRegex(Exception, 'single replication channel'):
                self.mysql.fetch_current_log_pos()

    def test_fetch_current_log_pos_with_binlog_coordinate_and_primary_server(self):
        """
        fetch_current_log_pos without enabled usage of gtid will return binlog coordinates from primary server
        """
        self.connection_config['use_gtid'] = False

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)
        self.mysql.is_replica = False

        with patch.object(self.mysql, 'query') as query_method_mock:
            query_method_mock.return_value = [
                {
                    'File': 'binlog_xyz',
                    'Position': 444,
                }
            ]

            result = self.mysql.fetch_current_log_pos()
            self.assertDictEqual(result, {
                'log_file': 'binlog_xyz',
                'log_pos': 444,
                'version': 1,
            })

            query_method_mock.assert_called_once_with('SHOW BINARY LOG STATUS')

    def test_fetch_current_log_pos_with_modern_replica_field_names(self):
        """MySQL 8.4 reports applied source coordinates under its renamed fields."""
        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)
        self.mysql.is_replica = True
        with patch.object(self.mysql, 'query', return_value=[{
            'Source_Log_File': 'binlog.000002',
            'Read_Source_Log_Pos': 999,
            'Relay_Source_Log_File': 'binlog.000001',
            'Exec_Source_Log_Pos': 444,
        }]):
            self.assertEqual(self.mysql.fetch_current_log_pos(), {
                'log_file': 'binlog.000001', 'log_pos': 444, 'version': 1,
            })

    def test_binlog_status_falls_back_for_older_mysql_syntax(self):
        """MySQL releases before the SHOW rename remain supported."""
        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)
        for modern, legacy in (
            ('SHOW BINARY LOG STATUS', 'SHOW MASTER STATUS'),
            ('SHOW REPLICA STATUS', 'SHOW SLAVE STATUS'),
        ):
            with self.subTest(statement=modern), patch.object(self.mysql, 'query') as query_mock:
                query_mock.side_effect = [pymysql.err.ProgrammingError(1064, 'syntax error'), [{'position': 444}]]
                self.assertEqual(self.mysql._query_binlog_status(modern, legacy), [{'position': 444}])
                self.assertEqual(query_mock.call_args_list, [call(modern), call(legacy)])

    def test_binlog_status_preserves_non_syntax_errors(self):
        """Authentication and operational failures do not trigger a legacy retry."""
        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)
        for error in (
            pymysql.err.ProgrammingError(1146, 'missing table'),
            pymysql.err.OperationalError(1227, 'access denied'),
        ):
            with self.subTest(error=error), patch.object(self.mysql, 'query', side_effect=error) as query_mock:
                with self.assertRaises(type(error)):
                    self.mysql._query_binlog_status('SHOW BINARY LOG STATUS', 'SHOW MASTER STATUS')
                query_mock.assert_called_once_with('SHOW BINARY LOG STATUS')

    def test_binlog_status_uses_mariadb_syntax(self):
        """Explicit MariaDB configuration avoids an unsupported syntax probe."""
        self.mysql = FastSyncTapMySql({**self.connection_config, 'engine': MARIADB_ENGINE}, lambda x: x)
        with patch.object(self.mysql, 'query', return_value=[{'position': 444}]) as query_mock:
            self.assertEqual(self.mysql._query_binlog_status(
                'SHOW BINARY LOG STATUS', 'SHOW MASTER STATUS'
            ), [{'position': 444}])
            query_mock.assert_called_once_with('SHOW MASTER STATUS')

    def test_detected_mariadb_engine_controls_binlog_status_and_gtid(self):
        """An omitted engine uses one detected flavor for handover queries."""
        self.mysql = FastSyncTapMySql({
            **self.connection_config,
            'use_gtid': True,
        }, lambda x: x)
        with patch('pymysql.connect') as mysql_connect_mock, patch.object(
            self.mysql, 'query'
        ) as query_mock:
            mysql_connect_mock.return_value.get_server_info.return_value = (
                '11.4.10-MariaDB-log'
            )
            self.mysql.open_connections()
            query_mock.reset_mock()
            query_mock.return_value = [{'current_gtids': '0-192-444'}]

            self.assertEqual(self.mysql.fetch_current_log_pos(), {
                'gtid': '0-192-444',
                'gtid_complete': True,
            })

        query_mock.assert_called_once_with(
            'select @@gtid_current_pos as current_gtids;'
        )

    def test_fetch_current_log_pos_with_gtid_and_mysql_but_gtid_mode_is_off_fails(self):
        """
        If using gtid is enabled and engine is mysql but gtid mode is off, then expect an exception
        """
        self.connection_config['use_gtid'] = True

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)
        self.mysql.is_replica = False

        with patch.object(self.mysql, 'query') as query_method_mock:
            query_method_mock.side_effect = [
                [{'gtid_mode': 'OFF'}]
            ]

            with self.assertRaises(Exception) as context:
                self.mysql.fetch_current_log_pos()

            self.assertEqual('GTID mode is not enabled.', str(context.exception))

            query_method_mock.assert_called_once_with('select @@gtid_mode as gtid_mode;')

    def test_fetch_current_log_pos_with_gtid_and_primary_mysql_engine_finds_gtid(self):
        """
        Snapshot handover retains multiple UUIDs and disjoint intervals.
        """
        self.connection_config['use_gtid'] = True

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)
        self.mysql.is_replica = False

        with patch.object(self.mysql, 'query') as query_method_mock:
            query_method_mock.side_effect = [
                [{'gtid_mode': 'ON'}],
                [{'current_gtids': MYSQL_GTID_SET}],
            ]

            with patch('pymysql.connect') as mysql_connect_mock:

                result = self.mysql.fetch_current_log_pos()
                self.assertDictEqual(result, {
                    'gtid': MYSQL_GTID_SET,
                    'gtid_complete': True,
                })

                query_method_mock.assert_has_calls([
                    call('select @@gtid_mode as gtid_mode;'),
                    call('select @@GLOBAL.gtid_executed as current_gtids;'),
                ])

                mysql_connect_mock.assert_not_called()

    def test_fetch_current_log_pos_with_gtid_and_replica_mysql_engine_finds_gtid(self):
        """
        Replica handover retains its complete applied set without contacting the primary.
        """
        self.connection_config['use_gtid'] = True

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)
        self.mysql.is_replica = True

        with patch.object(self.mysql, 'query') as query_method_mock:
            query_method_mock.side_effect = [
                [{'gtid_mode': 'ON'}],
                [{'current_gtids': MYSQL_GTID_SET}],
            ]

            with patch('pymysql.connect') as mysql_connect_mock:
                result = self.mysql.fetch_current_log_pos()
                self.assertDictEqual(result, {
                    'gtid': MYSQL_GTID_SET,
                    'gtid_complete': True,
                })

                query_method_mock.assert_has_calls([
                    call('select @@gtid_mode as gtid_mode;'),
                    call('select @@GLOBAL.gtid_executed as current_gtids;'),
                ])
                mysql_connect_mock.assert_not_called()

    def test_fetch_current_log_pos_marks_sparse_mysql_gtid_complete(self):
        """A genuine singleton set must not be confused with a legacy scalar watermark."""
        position = '24bc7850-2c16-11e6-a073-0242ac110002:599'
        self.mysql = FastSyncTapMySql({**self.connection_config, 'use_gtid': True}, lambda x: x)
        with patch.object(self.mysql, 'query', side_effect=[
            [{'gtid_mode': 'ON'}], [{'current_gtids': position}],
        ]):
            self.assertEqual(self.mysql.fetch_current_log_pos(), {
                'gtid': position, 'gtid_complete': True,
            })

    def test_fetch_current_log_pos_rejects_empty_mysql_gtid_set(self):
        """GTID mode alone does not guarantee an executed transaction exists."""
        self.mysql = FastSyncTapMySql({**self.connection_config, 'use_gtid': True}, lambda x: x)
        with patch.object(self.mysql, 'query', side_effect=[
            [{'gtid_mode': 'ON'}], [{'current_gtids': ''}],
        ]):
            with self.assertRaisesRegex(Exception, 'No GTID was found'):
                self.mysql.fetch_current_log_pos()

    def test_invalid_dates_are_nulled_for_every_temporal_type(self):
        """MySQL accepts dates no target will take, so FastSync must null them.

        Only the literal 0000-00-00 00:00:00 used to be filtered, so a zero year, a
        month outside 1-12, or a day past the end of the month replicated as-is and
        was rejected or silently altered by the target.

        Asserting on generated SQL rather than a live query, because unit tests here
        have no database. Confirmed against MariaDB 10.6 with ALLOW_INVALID_DATES: the
        old expression returned all four invalid values unchanged, this one NULLs each
        and leaves a valid date intact.
        """
        self.mysql = FastSyncTapMySqlMock(connection_config=self.connection_config)
        with patch.object(self.mysql, 'query', return_value=[]) as query_mock:
            self.mysql.get_table_columns('my_db.my_table')

        sql = query_mock.call_args.args[0]

        # date is CAST to the target type; datetime/timestamp pass through.
        for data_type in ("'date'", "'datetime', 'timestamp'"):
            assert f'WHEN data_type IN ({data_type})' in sql

        # Each guard, and why a naive equality check against the zero date misses it:
        #   zero year      -> 0000-01-01 is a valid-looking date with no year
        #   month 0 or 13  -> 2024-00-15, 2024-13-01
        #   day 0          -> 2024-05-00
        #   day past EOM   -> 2024-02-30, which LAST_DAY resolves per month
        for guard in (
            'YEAR(`',
            '`) = 0 OR MONTH(`',
            '`) NOT BETWEEN 1 AND 12 OR DAY(`',
            '`) = 0 OR DAY(`',
            '`) > DAY(LAST_DAY(DATE_FORMAT(`',
        ):
            assert guard in sql, guard

        # The zero-date-only filter this replaced must be gone from both branches.
        assert 'STR_TO_DATE("0000-00-00 00:00:00"' not in sql
        assert 'nullif(' not in sql

    def test_invalid_date_guard_nulls_rather_than_dropping_the_row(self):
        """A guard that filtered rows would silently lose data instead of the value."""
        self.mysql = FastSyncTapMySqlMock(connection_config=self.connection_config)
        with patch.object(self.mysql, 'query', return_value=[]) as query_mock:
            self.mysql.get_table_columns('my_db.my_table')

        sql = query_mock.call_args.args[0]

        # CASE ... THEN NULL ELSE <value> END, never a WHERE that removes the row.
        assert 'THEN NULL ELSE' in sql
        assert sql.count('THEN NULL ELSE') == 2, 'date and datetime/timestamp branches'

    def test_date_columns_are_cast_to_the_requested_target_type(self):
        """date_type varies per target, so the CAST must honour the caller's choice."""
        self.mysql = FastSyncTapMySqlMock(connection_config=self.connection_config)
        with patch.object(self.mysql, 'query', return_value=[]) as query_mock:
            self.mysql.get_table_columns('my_db.my_table', date_type='timestamp')

        sql = query_mock.call_args.args[0]

        assert 'AS timestamp) END' in sql
        # The datetime branch has no CAST, so the type must not leak into it.
        assert 'AS date) END' not in sql

    def test_boolean_tinyint_modifiers_are_normalized_for_boolean_copy(self):
        """Every TINYINT declaration mapped to BOOLEAN emits only 0, 1, or NULL."""
        self.mysql = FastSyncTapMySqlMock(connection_config=self.connection_config)
        with patch.object(self.mysql, 'query', return_value=[]) as query_mock:
            self.mysql.get_table_columns('my_db.my_table')

        sql = query_mock.call_args.args[0]
        assert "LOWER(column_type) REGEXP '^tinyint[(]1[)]( unsigned)?( zerofill)?$'" in sql
        assert "THEN concat('CASE WHEN `' , column_name , '` is null THEN null" in sql

    def test_explicit_mariadb_iceberg_v3_detects_exact_json_aliases(self):
        """MariaDB JSON aliases become semantic JSON only on the v3 route."""
        connection_config = {
            **self.connection_config,
            'engine': 'mariadb',
            'target_table_format': 'iceberg',
            'iceberg_version': 3,
        }
        self.mysql = FastSyncTapMySqlMock(connection_config=connection_config)

        with patch.object(self.mysql, 'query', return_value=[]) as query_mock:
            self.mysql.get_table_columns('my_db.my_table')

        sql = query_mock.call_args.args[0]
        assert "CASE WHEN is_json_alias THEN 'json' ELSE data_type END" in sql
        assert 'information_schema.check_constraints' in sql
        assert "c.data_type = 'longtext'" in sql
        assert "REPLACE(LOWER(cc.check_clause), ' ', '') =" in sql
        assert "CONCAT('json_valid(`'" in sql
        assert ' LIKE ' not in sql

    def test_detected_mariadb_iceberg_v3_detects_exact_json_aliases(self):
        """An omitted engine still enables MariaDB's JSON alias projection."""
        connection_config = {
            **self.connection_config,
            'target_table_format': 'iceberg',
            'iceberg_version': 3,
        }
        self.mysql = FastSyncTapMySqlMock(connection_config=connection_config)
        with patch('pymysql.connect') as mysql_connect_mock:
            mysql_connect_mock.return_value.get_server_info.return_value = (
                '11.4.10-MariaDB-log'
            )
            self.mysql.open_connections()

        with patch.object(self.mysql, 'query', return_value=[]) as query_mock:
            self.mysql.get_table_columns('my_db.my_table')

        sql = query_mock.call_args.args[0]
        assert "CASE WHEN is_json_alias THEN 'json' ELSE data_type END" in sql
        assert 'information_schema.check_constraints' in sql

    def test_json_alias_detection_does_not_change_other_routes(self):
        """Native MariaDB and genuine MySQL keep their existing discovery query."""
        configs = (
            {**self.connection_config, 'engine': 'mariadb'},
            {
                **self.connection_config,
                'engine': 'mariadb',
                'target_table_format': 'native',
            },
            {
                **self.connection_config,
                'engine': 'mysql',
                'target_table_format': 'iceberg',
                'iceberg_version': 3,
            },
            {
                **self.connection_config,
                'engine': 'mariadb',
                'target_table_format': 'iceberg',
                'iceberg_version': True,
            },
            {
                **self.connection_config,
                'engine': 'mariadb',
                'target_table_format': 'iceberg',
                'iceberg_version': 3.0,
            },
        )

        for connection_config in configs:
            with self.subTest(connection_config=connection_config):
                self.mysql = FastSyncTapMySqlMock(
                    connection_config=connection_config
                )
                with patch.object(
                    self.mysql, 'query', return_value=[]
                ) as query_mock:
                    self.mysql.get_table_columns('my_db.my_table')

                sql = query_mock.call_args.args[0]
                assert 'information_schema.check_constraints' not in sql
                assert 'is_json_alias' not in sql

    def test_semantic_json_alias_maps_to_target_variant(self):
        """The alias marker returned by discovery reaches the JSON type mapper."""
        mapper = Mock(return_value='VARIANT')
        self.mysql = FastSyncTapMySql(self.connection_config, mapper)

        with patch.object(
            self.mysql,
            'get_table_columns',
            return_value=[{
                'column_name': 'payload',
                'data_type': 'json',
                'column_type': 'longtext',
            }],
        ), patch.object(self.mysql, 'get_primary_keys', return_value=['ID']):
            result = self.mysql.map_column_types_to_target('my_db.my_table')

        assert result == {
            'columns': ['"PAYLOAD" VARIANT'],
            'primary_key': ['ID'],
            'source_column_names': ['payload'],
        }
        mapper.assert_called_once_with('json', 'longtext')
