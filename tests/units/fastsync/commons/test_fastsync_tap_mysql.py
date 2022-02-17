from unittest import TestCase
from unittest.mock import patch, call

import pymysql
from pipelinewise.fastsync.commons import tap_mysql
from pipelinewise.fastsync.commons.tap_mysql import FastSyncTapMySql, MARIADB_ENGINE


class FastSyncTapMySqlMock(FastSyncTapMySql):
    """
    Mocked FastSyncTapMySql class
    """

    def __init__(self, connection_config, tap_type_to_target_type=None):
        super().__init__(connection_config, tap_type_to_target_type)

        self.executed_queries_unbuffered = []
        self.executed_queries = []

    # pylint: disable=too-many-arguments
    def query(self, query, conn=None, params=None, return_as_cursor=False, n_retry=1):
        if query.startswith('INVALID-SQL'):
            raise pymysql.err.InternalError

        if conn == self.conn_unbuffered:
            self.executed_queries.append(query)
        else:
            self.executed_queries_unbuffered.append(query)

        return []


# pylint: disable=invalid-name,no-self-use
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

    def test_open_connections_with_default_session_sqls(self):
        """Default session parameters should be applied if no custom session SQLs"""
        self.mysql = FastSyncTapMySqlMock(connection_config=self.connection_config)
        with patch('pymysql.connect') as mysql_connect_mock:
            mysql_connect_mock.return_value = []
            self.mysql.open_connections()

        # Test if session variables applied on both connections
        self.assertListEqual(self.mysql.executed_queries, tap_mysql.DEFAULT_SESSION_SQLS)
        self.assertListEqual(self.mysql.executed_queries_unbuffered, self.mysql.executed_queries)

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
                **{'session_sqls': session_sqls},
            }
        )
        with patch('pymysql.connect') as mysql_connect_mock:
            mysql_connect_mock.return_value = []
            self.mysql.open_connections()

        # Test if session variables applied on both connections
        self.assertListEqual(self.mysql.executed_queries, session_sqls)
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
            mysql_connect_mock.return_value = []
            self.mysql.open_connections()

        # Test if session variables applied on both connections
        self.assertListEqual(self.mysql.executed_queries, [
            'SET SESSION max_statement_time=0',
            'SET SESSION wait_timeout=28800',
        ])
        self.assertListEqual(self.mysql.executed_queries_unbuffered, self.mysql.executed_queries)

    def test_fetch_current_log_pos_with_gtid_and_mysql_engine_fails(self):
        """
        If using gtid is enabled and engine is mysql, then expect NotImplementedError
        """
        self.connection_config['use_gtid'] = True

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)

        with patch('pymysql.connect') as mysql_connect_mock:
            mysql_connect_mock.return_value = []

            with self.assertRaises(NotImplementedError):
                self.mysql.fetch_current_log_pos()

            mysql_connect_mock.assert_not_called()

    def test_fetch_current_log_pos_with_gtid_and_replica_mariadb_engine_succeeds(self):
        """
        If using gtid is enabled and engine is replica mariadb, then expect gtid result
        """
        self.connection_config['use_gtid'] = True
        self.connection_config['engine'] = MARIADB_ENGINE

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)
        self.mysql.is_replica = True

        with patch.object(self.mysql, 'query') as query_method_mock:

            expected_gtid = '0-192-444'

            query_method_mock.return_value = [
                {'current_gtid': expected_gtid}
            ]

            result = self.mysql.fetch_current_log_pos()

            query_method_mock.assert_called_once_with('select @@gtid_slave_pos as current_gtid;')
            self.assertDictEqual(result, {'gtid': expected_gtid})

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

            with self.assertRaises(Exception) as ex:
                self.mysql.fetch_current_log_pos()
                self.assertEqual('GTID is not enabled!', str(ex))

            query_method_mock.assert_called_once_with('select @@gtid_slave_pos as current_gtid;')

    def test_fetch_current_log_pos_with_gtid_and_primary_mariadb_engine_succeeds(self):
        """
        If using gtid is enabled and engine is primary mariadb which has a list of
        gtids with one that has the same server id, then expect gtid result
        """
        self.connection_config['use_gtid'] = True
        self.connection_config['engine'] = MARIADB_ENGINE

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)

        with patch.object(self.mysql, 'query') as query_method_mock:

            expected_gtid = '0-192-444'

            query_method_mock.side_effect = [
                [{'current_gtid': f'0,{expected_gtid},43223,0-333-11,'}],
                [{'server_id': 192}],
            ]

            result = self.mysql.fetch_current_log_pos()

            query_method_mock.assert_has_calls(
                [
                    call('select @@gtid_current_pos as current_gtid;'),
                    call('select @@server_id as server_id;'),
                ]
            )
            self.assertDictEqual(result, {'gtid': expected_gtid})

    def test_fetch_current_log_pos_with_gtid_and_primary_mariadb_engine_no_gtid_found_expect_exception(self):
        """
        If using gtid is enabled and engine is primary mariadb which doesn't return gtid, then expect an exception
        """
        self.connection_config['use_gtid'] = True
        self.connection_config['engine'] = MARIADB_ENGINE

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)

        with patch.object(self.mysql, 'query') as query_method_mock:
            query_method_mock.side_effect = []

            with self.assertRaises(Exception) as ex:
                self.mysql.fetch_current_log_pos()
                self.assertEqual('GTID is not enabled!', str(ex))

            query_method_mock.assert_has_calls(
                [
                    call('select @@gtid_current_pos as current_gtid;'),
                ]
            )

    def test_fetch_current_log_pos_with_gtid_and_primary_mariadb_engine_no_gtid__with_server_id_found_expect_exception(
            self):
        """
        If using gtid is enabled and engine is primary mariadb which has a list of
        gtids with none having the same server id, then expect an exception
        """
        self.connection_config['use_gtid'] = True
        self.connection_config['engine'] = MARIADB_ENGINE

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)

        with patch.object(self.mysql, 'query') as query_method_mock:

            query_method_mock.side_effect = [
                [{'current_gtid': f'0,43223,0-333-11,'}],
                [{'server_id': 192}],
            ]

            with self.assertRaises(Exception) as ex:
                self.mysql.fetch_current_log_pos()
                self.assertEqual('No suitable GTID was found for server 192', str(ex))

            query_method_mock.assert_has_calls(
                [
                    call('select @@gtid_current_pos as current_gtid;'),
                    call('select @@server_id as server_id;'),
                ]
            )

    def test_fetch_current_log_pos_with_binlog_coordinate_and_replica_server(self):
        """
        fetch_current_log_pos without enabled usage of gtid will return binlog coordinates from replica server
        """
        self.connection_config['use_gtid'] = False

        self.mysql = FastSyncTapMySql(self.connection_config, lambda x: x)
        self.mysql.is_replica = True

        with patch.object(self.mysql, 'query') as query_method_mock:
            query_method_mock.return_value = [
                {
                    'Master_Log_File': 'binlog_xyz',
                    'Read_Master_Log_Pos': 444,
                }
            ]

            result = self.mysql.fetch_current_log_pos()

            query_method_mock.assert_called_once_with('SHOW SLAVE STATUS')

            self.assertDictEqual(result, {
                'log_file': 'binlog_xyz',
                'log_pos': 444,
                'version': 1,
            })

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

            query_method_mock.assert_called_once_with('SHOW MASTER STATUS')
