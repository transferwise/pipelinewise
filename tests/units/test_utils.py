import unittest

from pipelinewise import utils
from pipelinewise.fastsync.commons.tap_mysql import FastSyncTapMySql
from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres


class MockCursor:
    """Mock Cursor class"""

    def __init__(self, *args, **kwargs):  # pylint: disable=unused-argument
        self.rowcount = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def execute(self, sql, params=None):  # pylint: disable=unused-argument
        """Mock execute method"""
        self.rowcount = 2

    def fetchall(self):
        """Mock fetchall method"""
        if self.type == 'mysql':
            return [
                {'table_name': 't1', 'table_rows': 1, 'table_size': 1234},
                {'table_name': 't2', 'table_rows': 3, 'table_size': 1234}
            ]

        if self.type == 'postgres':
            return [
                ['t1', 1, 1234],
                ['t2', 3, 1234],
            ]
        return None

class ConnMock:
    """Mock Connection class"""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __init__(self, cursor_type):
        self.cursor = MockCursor
        self.cursor.type = cursor_type

    def close(self):
        """Mock close method"""
        pass  # pylint: disable=unnecessary-pass


class FastSyncTapMySqlMock(FastSyncTapMySql):
    """
    Mocked FastSyncTapMySql class
    """
    def open_connections(self):
        self.conn = ConnMock('mysql')


class FastSyncTapPostgresMock(FastSyncTapPostgres):
    """
    Mocked FastSyncTapPostgres class
    """
    def open_connection(self):
        self.conn = ConnMock('postgres')


class TestUtils(unittest.TestCase):
    """
    Unit Tests for PipelineWise common utils
    """

    def test_safe_column_name_case_1(self):
        """
        Given an all lower case word would be wrapped in double quotes and capitalized
        """
        input_name = 'group'

        self.assertEqual('"GROUP"', utils.safe_column_name(input_name))

    def test_safe_column_name_case_2(self):
        """
        Given an all lower case word would be wrapped in backticks and capitalized
        """
        input_name = 'group'

        self.assertEqual('`GROUP`', utils.safe_column_name(input_name, '`'))

    def test_safe_column_name_case_3(self):
        """
        Given a mixed-case word would be wrapped in double quotes and capitalized
        """
        input_name = 'CA se'

        self.assertEqual('"CA SE"', utils.safe_column_name(input_name))

    def test_safe_column_name_case_4(self):
        """
        Given a mixed-case word would be wrapped in backticks and capitalized
        """
        input_name = 'CA se'

        self.assertEqual('`CA SE`', utils.safe_column_name(input_name, '`'))

    def test_safe_column_name_is_null(self):
        """
        Given a null word, we should get null back
        """
        input_name = None

        self.assertIsNone(utils.safe_column_name(input_name))

    def test_get_tables_size(self):
        """Test get_tables_size method works correctly"""
        test_schema = 'foo_schema'
        expected_tables_info = [
            {'table_name': f'{test_schema}.t1', 'table_rows': 1, 'table_size': 1234},
            {'table_name': f'{test_schema}.t2', 'table_rows': 3, 'table_size': 1234}
        ]

        connection_config = {'host': 'foo_host',
                             'port': 1234,
                             'user': 'pipelinewise',
                             'password': 'password',
                             'dbname': 'foo_db'}

        mysql_tap = FastSyncTapMySqlMock(connection_config=connection_config, tap_type_to_target_type=None)
        postgres_tap = FastSyncTapPostgresMock(connection_config=connection_config, tap_type_to_target_type=None)
        for tap_obj in (mysql_tap, postgres_tap):
            actual_output = utils.get_tables_size(schema=test_schema, tap=tap_obj)
            self.assertListEqual(actual_output, expected_tables_info)

    def test_get_maximum_value_from_list_of_dicts(self):
        """Test get_maximum_value_from_list_of_dicts method works correctly"""
        input_list = [
            {'foo': 5, 'bar': 9},
            {'foo': 7, 'bar': 2},
            {'foo': 6, 'bar': 5}
        ]

        expected_max_foo = {'foo': 7, 'bar': 2}
        expected_max_bar = {'foo': 5, 'bar': 9}

        actual_max_foo = utils.get_maximum_value_from_list_of_dicts(input_list, 'foo')
        actual_max_bar = utils.get_maximum_value_from_list_of_dicts(input_list, 'bar')
        actual_max_baz = utils.get_maximum_value_from_list_of_dicts(input_list, 'baz')

        self.assertDictEqual(expected_max_foo, actual_max_foo,)
        self.assertDictEqual(expected_max_bar, actual_max_bar)
        self.assertIsNone(actual_max_baz)

    def test_filter_out_selected_tables(self):
        """Test filter_out_selected_tables method works correctly"""
        selected_tables = {'foo.t1', 'foo.t3', 'bar.t2'}
        all_schema_tables = [
            {'table_name': 'foo.t1', 'table_rows': 1, 'table_size': 1111},
            {'table_name': 'foo.t2', 'table_rows': 2, 'table_size': 2222},
            {'table_name': 'foo.t3', 'table_rows': 3, 'table_size': 3333},
            {'table_name': 'foo.something t1 Uppercase', 'table_rows': 4, 'table_size': 1234}
        ]

        expected_output = [
            {'table_name': 'foo.t1', 'table_rows': 1, 'table_size': 1111},
            {'table_name': 'foo.t3', 'table_rows': 3, 'table_size': 3333},
        ]

        actual_output = utils.filter_out_selected_tables(all_schema_tables, selected_tables)

        self.assertListEqual(
            actual_output, expected_output
        )

    def test_get_schema_of_tables_set(self):
        """Test get_schema_of_mysql_tables method works correctly"""
        input_tables_list = {'foo.t1', 'foo.t2', 'bar.t1', 'baz.t4', 'bar.t3'}
        expected_output = {'foo', 'bar', 'baz'}
        actual_output = utils.get_schemas_of_tables_set(input_tables_list)
        self.assertSetEqual(actual_output, expected_output)


if __name__ == '__main__':
    unittest.main()
