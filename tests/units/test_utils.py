import unittest
from tempfile import TemporaryDirectory

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from cryptography.exceptions import UnsupportedAlgorithm



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
                {'table_name': 't1', 'table_size': 1234},
                {'table_name': 't2', 'table_size': 1234}
            ]

        if self.type == 'postgres':
            return [
                ['t1', 1234],
                ['t2', 1234],
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
            {'table_name': f'{test_schema}.t1', 'table_size': 1234},
            {'table_name': f'{test_schema}.t2', 'table_size': 1234}
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
            {'table_name': 'foo.t1', 'table_size': 1111},
            {'table_name': 'foo.t2', 'table_size': 2222},
            {'table_name': 'foo.t3', 'table_size': 3333},
            {'table_name': 'foo.something t1 Uppercase', 'table_size': 1234}
        ]

        expected_output = [
            {'table_name': 'foo.t1', 'table_size': 1111},
            {'table_name': 'foo.t3', 'table_size': 3333},
        ]

        actual_output = utils.filter_out_selected_tables(all_schema_tables, selected_tables)

        self.assertEqual(len(actual_output), 2)

        for item in expected_output:
            self.assertIn(item, actual_output)

    def test_get_schema_of_tables_set(self):
        """Test get_schema_of_mysql_tables method works correctly"""
        input_tables_list = {'foo.t1', 'foo.t2', 'bar.t1', 'baz.t4', 'bar.t3'}
        expected_output = {'foo', 'bar', 'baz'}
        actual_output = utils.get_schemas_of_tables_set(input_tables_list)
        self.assertSetEqual(actual_output, expected_output)

    def test_convert_key_pem_format_to_der_format(self):
        with TemporaryDirectory() as temp_dir:
            with open(f'{temp_dir}/test.pem', 'w', encoding='utf-8') as tmp_file:
                tmp_file.write('''
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC0p0Ap5cnzAest
V9NbIqVlO0WgN210Cl02qqLALdEyJT89XEI7aUc9wJjCvMGso/yBBJ4uKvLu4bx4
2lnkC4EZICWFqSiy3rpizy1NI2tl3dNPLQHzoIe1GIli3NQYcJzgLsYBLQ9+m8Jx
jV9VThf/PiqdRKUy56IH5Nz8DT3YRzaIIMgJTJ2N9YghcL3jYQWnHsZEGVUI7zNA
lSp1n3fp5/um3p5zDTLkO4BuTjjFVw//14Y8vQEr7dbFycd3OblEjlHaZgZBEZ8r
FE3bS/oQbJY/vc9nBSPqBlLNY7v34QA6zPiiKIac9aAijvKRWqXXH3z1PEGxEEV3
WOzRU24xAgMBAAECggEADG4zuo5OupNnuMuBxhQYuGH/NPqLZAAwkMnl//5HFkG8
276E6iyg0810VXYCh5wTDFeigL/AzpIm01QG+muWOwHcwxk0LTapMZJa5iNpSO2e
FCUfLMHfhKUHEw/p4jKhgMWHJ16P4eDa3NBi/m4stYn0CbVG/r00h4GGeSt6FW8X
YC0sE/Om3/VDI5ouVbci7oRyBoOi+FOa/eA4dtDnCM35y1OIzfe3ZgyN6c3i83QN
AK92fkYh+GCz+KjvysKeHmyhNOeu2MXz37L6e2xqu0Maprzpw7d+qLVi25whOpfJ
ufQmFh7XNYB78AgDZnYkoOA6115Fgw3dBOUlFhpAqQKBgQDgzRwfX7YPhOtKik3c
nZL8lMiCC96dXfGsKcfkCoD/0oR6B9OgCeijbXQwmvMJdPWNUz6irc9fntlkYeIZ
G3L9D4JHRxkA6yEp41JIzMhlFkORiuZk36Ap7Wuv2+TAzh9F95LQx9ALbIzb3QeV
w9EpBdYp8SHysIPxIJ4BJ7HpxQKBgQDNuZ/oYPBm1H4SgqcghO8CiNIpmOXZP7iQ
SWY/k2A3VaKoAiSeGhc6LorpZNaWEzZPy7Vnu0k+EBCJ0LVF3Sh4Tx5I/bE7d6vm
sjyS5lPGCzqJWzMR0eObXpz9F8JRfwXcVrS/A3qb7OZfIvzSUPR2trn/neI6Q1DN
je3CHzy1fQKBgQCdriEovIjGb/Reb45Xzcs5Ed9moI7AkRGgMho8kUWUq4Qy2GSP
YAPnBjI2makZnAlU3OwVTZckuhZAPAxMkh1g9czq1CrsowC7EfE4kTOK/EfewbAD
V3xPjHI5gyL8PlhfSl2Xxl/ec4CGA457dUOz450qBDJMuZWCv980bjR0BQKBgCSH
gmpr1CQeNSiqRGzUze/gRZkXSjDyTJ5qOhqt25bXwOMeRkxAi8FMBGR/AE9zp+Ax
Zsu9iLrZdWZTReza4VXDjrgdO/w4OrDjEzhuZ4+x7Ln5FK9kWor7GNsj/eAksvC2
ALAuOPY48YsRFl1t/Iqb1Zka+tGnpFBrlD00+L2tAoGATRwSkDF4jbcvm58Ty24j
wVsBi5Qa76kOlEsKtb7n4IspDw+gpIsPKXHPQsY04+tGIUIFBI71chndZZ5dU7Ro
nqtNBAQ9eB5EUmA33zYTz93+DWnoG/Sqm/0ULLgJFKzaK1YHSf+lv6v036jp9E9R
XXGN8+qde/d1wM7lPXDI9Jk=
-----END PRIVATE KEY-----''')
            try:
                der_format = utils.pem2der(f'{temp_dir}/test.pem')
                serialization.load_der_private_key(der_format, password=None, backend=default_backend())
            except Exception as e:
                self.fail(f'Failed to convert pem to der: {e}')


if __name__ == '__main__':
    unittest.main()
