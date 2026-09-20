import unittest

from unittest.mock import patch, MagicMock, call

from pymysql.cursors import Cursor
from pymysql.err import OperationalError
from tap_mysql.connection import MySQLConnection, fetch_server_id, fetch_server_uuid, make_connection_wrapper


class TestConnection(unittest.TestCase):

    def test_default_charset_supports_four_byte_unicode(self):
        conn = MySQLConnection({'host': 'localhost', 'port': 3306, 'user': 'test', 'password': 'test'})
        self.assertEqual(conn.charset, 'utf8mb4')

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
