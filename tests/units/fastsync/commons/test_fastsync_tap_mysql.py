from unittest import TestCase
from unittest.mock import patch
import pymysql

from pipelinewise.fastsync.commons import tap_mysql
from pipelinewise.fastsync.commons.tap_mysql import FastSyncTapMySql


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
        self.connection_config = {'host': 'foo.com',
                                  'port': 3306,
                                  'user': 'my_user',
                                  'password': 'secret',
                                  'dbname': 'my_db'}
        self.mysql = None

    def test_open_connections_with_default_session_sqls(self):
        """Default session parameters should be applied if no custom session SQLs"""
        self.mysql = FastSyncTapMySqlMock(connection_config=self.connection_config)
        with patch('pymysql.connect') as mysql_connect_mock:
            mysql_connect_mock.return_value = []
            self.mysql.open_connections()

        # Test if session variables applied on both connections
        assert self.mysql.executed_queries == tap_mysql.DEFAULT_SESSION_SQLS
        assert self.mysql.executed_queries_unbuffered == self.mysql.executed_queries

    def test_open_connections_with_session_sqls(self):
        """Custom session parameters should be applied if defined"""
        session_sqls = [
            'SET SESSION max_statement_time=0',
            'SET SESSION wait_timeout=28800'
        ]
        self.mysql = FastSyncTapMySqlMock(connection_config={**self.connection_config,
                                                             **{'session_sqls': session_sqls}})
        with patch('pymysql.connect') as mysql_connect_mock:
            mysql_connect_mock.return_value = []
            self.mysql.open_connections()

        # Test if session variables applied on both connections
        assert self.mysql.executed_queries == session_sqls
        assert self.mysql.executed_queries_unbuffered == self.mysql.executed_queries

    def test_open_connections_with_invalid_session_sqls(self):
        """Invalid SQLs in session_sqls should be ignored"""
        session_sqls = [
            'SET SESSION max_statement_time=0',
            'INVALID-SQL-SHOULD-BE-SILENTLY-IGNORED',
            'SET SESSION wait_timeout=28800'
        ]
        self.mysql = FastSyncTapMySqlMock(connection_config={**self.connection_config,
                                                             **{'session_sqls': session_sqls}})
        with patch('pymysql.connect') as mysql_connect_mock:
            mysql_connect_mock.return_value = []
            self.mysql.open_connections()

        # Test if session variables applied on both connections
        assert self.mysql.executed_queries == ['SET SESSION max_statement_time=0',
                                               'SET SESSION wait_timeout=28800']
        assert self.mysql.executed_queries_unbuffered == self.mysql.executed_queries
