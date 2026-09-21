import unittest

from unittest.mock import patch, MagicMock, call

from pymysql.cursors import Cursor, DictCursor
from pymysql.err import OperationalError
from pymysqlreplication import BinLogStreamReader
from tap_mysql.connection import (
    DEFAULT_SESSION_SQLS,
    MARIADB_MAX_STATEMENT_TIME_SQL,
    MySQLConnection,
    fetch_server_id,
    fetch_server_uuid,
    make_connection_wrapper,
    run_session_sqls,
)


class TestConnection(unittest.TestCase):

    def test_default_charset_supports_four_byte_unicode(self):
        conn = MySQLConnection({'host': 'localhost', 'port': 3306, 'user': 'test', 'password': 'test'})
        self.assertEqual(conn.charset, 'utf8mb4')

    def test_session_sql_defaults_are_detected_from_server(self):
        base_config = {'host': 'localhost', 'port': 3306, 'user': 'test', 'password': 'test'}
        mysql_conn = MySQLConnection(base_config)
        mariadb_conn = MySQLConnection(base_config)
        mysql_conn.get_server_info = MagicMock(return_value='8.0.39')
        mariadb_conn.get_server_info = MagicMock(return_value='11.4.10-MariaDB-log')

        with patch('tap_mysql.connection.run_sql') as run_sql:
            run_session_sqls(mysql_conn)
            mysql_sqls = [args.args[1] for args in run_sql.call_args_list]
            run_sql.reset_mock()
            run_session_sqls(mariadb_conn)
            mariadb_sqls = [args.args[1] for args in run_sql.call_args_list]

        self.assertEqual(mysql_sqls, DEFAULT_SESSION_SQLS)
        self.assertNotIn(MARIADB_MAX_STATEMENT_TIME_SQL, mysql_sqls)
        self.assertEqual(
            mariadb_sqls,
            [*DEFAULT_SESSION_SQLS, MARIADB_MAX_STATEMENT_TIME_SQL],
        )
        mysql_conn.get_server_info.assert_called_once_with()
        mariadb_conn.get_server_info.assert_called_once_with()
        self.assertFalse(MARIADB_MAX_STATEMENT_TIME_SQL.endswith(';'))

    def test_session_engine_selection_is_reported_once_at_info(self):
        conn = MySQLConnection({
            'host': 'localhost', 'port': 3306, 'user': 'test', 'password': 'test',
        })
        conn.get_server_info = MagicMock(return_value='11.4.10-MariaDB-log')

        with patch('tap_mysql.connection._REPORTED_SESSION_ENGINE_SELECTIONS', set()), \
                patch('tap_mysql.connection.LOGGER') as logger, \
                patch('tap_mysql.connection.run_sql'):
            run_session_sqls(conn)
            run_session_sqls(conn)

        logger.info.assert_called_once_with(
            'Using %s source engine for default session settings (%s)',
            'mariadb',
            'detected',
        )
        logger.debug.assert_called_once_with(
            'Using %s source engine for default session settings (%s)',
            'mariadb',
            'detected',
        )

    def test_explicit_engine_selects_defaults_without_server_detection(self):
        base_config = {'host': 'localhost', 'port': 3306, 'user': 'test', 'password': 'test'}
        cases = (
            ('mysql', '11.4.10-MariaDB-log', DEFAULT_SESSION_SQLS),
            (
                'mariadb',
                '8.0.39',
                [*DEFAULT_SESSION_SQLS, MARIADB_MAX_STATEMENT_TIME_SQL],
            ),
        )

        for configured_engine, server_info, expected_sqls in cases:
            with self.subTest(configured_engine=configured_engine):
                conn = MySQLConnection({**base_config, 'engine': configured_engine})
                conn.get_server_info = MagicMock(return_value=server_info)
                with patch('tap_mysql.connection.run_sql') as run_sql:
                    run_session_sqls(conn)

                self.assertEqual(
                    [args.args[1] for args in run_sql.call_args_list],
                    expected_sqls,
                )
                conn.get_server_info.assert_not_called()

    def test_omitted_engine_remains_detectable_after_runtime_defaulting(self):
        config = {'host': 'localhost', 'port': 3306, 'user': 'test', 'password': 'test'}
        MySQLConnection(config)
        config['engine'] = 'mysql'
        conn = MySQLConnection(config)
        conn.get_server_info = MagicMock(return_value='11.4.10-MariaDB-log')

        with patch('tap_mysql.connection.run_sql') as run_sql:
            run_session_sqls(conn)

        self.assertEqual(
            [args.args[1] for args in run_sql.call_args_list],
            [*DEFAULT_SESSION_SQLS, MARIADB_MAX_STATEMENT_TIME_SQL],
        )
        conn.get_server_info.assert_called_once_with()

    def test_custom_session_sqls_extend_and_override_mariadb_defaults(self):
        custom_session_sqls = ['SET @@session.time_zone="+1:00"']
        conn = MySQLConnection({
            'host': 'localhost',
            'port': 3306,
            'user': 'test',
            'password': 'test',
            'session_sqls': custom_session_sqls,
        })
        conn.get_server_info = MagicMock(return_value='11.4.10-MariaDB-log')

        self.assertEqual(conn.session_sqls, custom_session_sqls)
        with patch('tap_mysql.connection.run_sql') as run_sql:
            run_session_sqls(conn)
        self.assertEqual(
            [args.args[1] for args in run_sql.call_args_list],
            [
                *DEFAULT_SESSION_SQLS,
                MARIADB_MAX_STATEMENT_TIME_SQL,
                *custom_session_sqls,
            ],
        )
        conn.get_server_info.assert_called_once_with()

    def test_gtid_reconnect_and_non_network_errors_remain_driver_handled(self):
        for use_gtid, error_code in [(True, 2006), (True, 2013), (False, 1142)]:
            with self.subTest(use_gtid=use_gtid, error_code=error_code):
                config = {'host': 'localhost', 'port': 3306, 'user': 'test', 'password': 'test', 'use_gtid': use_gtid}
                with patch('tap_mysql.connection.connect_with_backoff'):
                    conn = make_connection_wrapper(config)()
                error = OperationalError(error_code, 'source error')
                with patch.object(MySQLConnection, '_read_packet', side_effect=error):
                    with self.assertRaises(OperationalError) as raised:
                        conn._read_packet()
                self.assertIs(raised.exception, error)

    def test_binlog_initial_connection_keeps_operational_error_for_backoff(self):
        config = {'host': 'localhost', 'port': 3306, 'user': 'test', 'password': 'test', 'use_gtid': False}
        with patch.object(MySQLConnection, '_read_packet', side_effect=OperationalError(2006, 'network lost')), \
                patch('tap_mysql.connection.connect_with_backoff', side_effect=lambda conn: conn._read_packet()):
            with self.assertRaises(OperationalError):
                make_connection_wrapper(config)()

    def test_binlog_initial_connection_retries_before_enabling_disconnect_guard(self):
        config = {'host': 'localhost', 'port': 3306, 'user': 'test', 'password': 'test', 'use_gtid': False}
        with patch.object(MySQLConnection, '_read_packet',
                          side_effect=[OperationalError(2006, 'network lost'), None]) as read_packet, \
                patch.object(MySQLConnection, 'connect', autospec=True,
                             side_effect=lambda conn: conn._read_packet()), \
                patch('tap_mysql.connection.run_session_sqls'), patch('backoff._sync.time.sleep'):
            conn = make_connection_wrapper(config)()
        self.assertEqual(read_packet.call_count, 2)
        with patch.object(MySQLConnection, '_read_packet', side_effect=OperationalError(2006, 'network lost')):
            with self.assertRaisesRegex(RuntimeError, 'durable checkpoint'):
                conn._read_packet()

    def test_file_position_metadata_connection_reconnects_after_network_error(self):
        config = {'host': 'localhost', 'port': 3306, 'user': 'test', 'password': 'test', 'use_gtid': False}
        expected_columns = [{'COLUMN_NAME': 'id', 'ORDINAL_POSITION': 1}]
        for error_code in (2006, 2013):
            with self.subTest(error_code=error_code):
                connections = []
                wrapper = make_connection_wrapper(config)

                def connection_factory(**kwargs):
                    conn = wrapper(**kwargs)
                    cur = MagicMock()
                    cur.execute.side_effect = lambda *_: conn._read_packet()
                    cur.fetchall.return_value = expected_columns
                    conn.cursor = MagicMock(return_value=cur)
                    connections.append(conn)
                    return conn

                reader = BinLogStreamReader({}, 123, pymysql_wrapper=connection_factory)
                with patch('tap_mysql.connection.connect_with_backoff'), \
                        patch.object(MySQLConnection, '_read_packet',
                                     side_effect=[OperationalError(error_code, 'network lost'), None]):
                    columns = reader._BinLogStreamReader__get_table_information('db', 'items')
                self.assertEqual(columns, expected_columns)
                self.assertEqual(len(connections), 2)
                self.assertTrue(all(conn.cursorclass is DictCursor for conn in connections))
                self.assertNotIn('cursorclass', config)

    def test_stream_disconnect_guard_does_not_depend_on_cursor_class(self):
        config = {'host': 'localhost', 'port': 3306, 'user': 'test', 'password': 'test', 'use_gtid': False}
        for error_code in (2006, 2013):
            with self.subTest(error_code=error_code):
                with patch('tap_mysql.connection.connect_with_backoff'):
                    conn = make_connection_wrapper(config)(cursorclass=DictCursor)
                with patch.object(MySQLConnection, '_read_packet', side_effect=OperationalError(error_code, 'lost')):
                    with self.assertRaisesRegex(RuntimeError, 'durable checkpoint'):
                        conn._read_packet()

    @patch('tap_mysql.connection.connect_with_backoff')
    def test_fetch_server_id(self, connect_with_backoff):

        mysql_con = MagicMock(spec_set=MySQLConnection).return_value
        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.return_value = [111]

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con

        result = fetch_server_id(mysql_con)

        self.assertEqual(111, result)

        connect_with_backoff.assert_called_with(mysql_con)

        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('SELECT @@server_id'),
            ]
        )

    @patch('tap_mysql.connection.connect_with_backoff')
    def test_fetch_server_uuid(self, connect_with_backoff):

        mysql_con = MagicMock(spec_set=MySQLConnection).return_value
        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.return_value = ['dkfhdsf0-ejr-dfbsf-dnfnsbdmfbdf']

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con

        result = fetch_server_uuid(mysql_con)

        self.assertEqual('dkfhdsf0-ejr-dfbsf-dnfnsbdmfbdf', result)

        connect_with_backoff.assert_called_with(mysql_con)

        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('SELECT @@server_uuid'),
            ]
        )
