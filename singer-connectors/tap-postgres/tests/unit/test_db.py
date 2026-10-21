import decimal
import unittest

import datetime
from unittest.mock import MagicMock, patch
import singer.logger as singer_logger

from tap_postgres import db
from tap_postgres.sync_strategies import logical_replication as log_replication


class TestDbFunctions(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.conn_config = {
            'host': 'primary.example.com',
            'dbname': 'source_db',
            'user': 'pipelinewise',
            'password': 'secret',
            'port': 5432,
            'use_secondary': False,
        }

    @patch('tap_postgres.db.psycopg2.connect')
    def test_logical_array_and_hstore_values_do_not_repeat_info_host_logs(self, connect):
        connect.return_value.server_version = 150013
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ([],)
        with patch.object(singer_logger, '_REPORTED_SOURCE_HOSTS', set()), \
                self.assertLogs(db.LOGGER, level='INFO') as logs:
            for _ in range(100):
                self.assertEqual([], log_replication.create_array_elem('{}', 'integer[]', self.conn_config))
                self.assertEqual({}, log_replication.create_hstore_elem(self.conn_config, ''))
        self.assertEqual(connect.call_count, 200)
        self.assertEqual(logs.output, ['INFO:tap_postgres:Connecting to PostgreSQL source host: primary.example.com'])

    @patch('tap_postgres.db.psycopg2.connect')
    def test_host_logs_track_replica_and_logical_primary_connections(self, connect):
        connect.return_value.server_version = 150013
        config = {**self.conn_config, 'use_secondary': True, 'secondary_host': 'replica.example.com'}
        with patch.object(singer_logger, '_REPORTED_SOURCE_HOSTS', set()), \
                self.assertLogs(db.LOGGER, level='INFO') as logs:
            db.open_connection(config)
            self.assertEqual(connect.call_args.kwargs['host'], 'replica.example.com')
            db.open_connection(config, logical_replication=True)
            self.assertEqual(connect.call_args.kwargs['host'], 'primary.example.com')
        self.assertEqual(logs.output, [
            'INFO:tap_postgres:Connecting to PostgreSQL source host: replica.example.com',
            'INFO:tap_postgres:Connecting to PostgreSQL source host: primary.example.com',
        ])

    @patch('tap_postgres.db.psycopg2.connect')
    def test_open_connection_rejects_postgres_before_11_2(self, connect):
        """Every connector connection rejects an unsupported source."""
        connection = connect.return_value
        connection.server_version = 110001

        with self.assertRaisesRegex(
                db.UnsupportedPostgresVersionError,
                'PostgreSQL 11.2 or later is required; '
                'connected server reports server_version_num 110001',
        ):
            db.open_connection(self.conn_config)

        connection.close.assert_called_once_with()

    @patch('tap_postgres.db.psycopg2.connect')
    def test_open_connection_accepts_supported_versions(self, connect):
        """The exact support floor and newer logical connections are accepted."""
        cases = ((110002, False), (110002, True), (180000, True))

        for server_version, logical_replication in cases:
            with self.subTest(
                    server_version=server_version,
                    logical_replication=logical_replication,
            ):
                connect.reset_mock()
                connection = MagicMock()
                connection.server_version = server_version
                connect.return_value = connection

                result = db.open_connection(
                    self.conn_config,
                    logical_replication=logical_replication,
                )

                self.assertIs(connection, result)
                connection.close.assert_not_called()
                if logical_replication:
                    self.assertIs(
                        db.psycopg2.extras.LogicalReplicationConnection,
                        connect.call_args.kwargs['connection_factory'],
                    )
                else:
                    self.assertNotIn('connection_factory', connect.call_args.kwargs)

    def test_value_to_singer_value(self):
        """Test if every element converted from sql_datatype to the correct singer type"""
        # JSON and JSONB should be converted to dictionaries
        self.assertEqual(db.selected_value_to_singer_value_impl('{"test": 123}', 'json'), {'test': 123})
        self.assertEqual(db.selected_value_to_singer_value_impl('{"test": 123}', 'jsonb'), {'test': 123})

        # time with time zone values should be converted to UTC and time zone dropped
        # Hour 24 should be taken as 0
        self.assertEqual(db.selected_value_to_singer_value_impl('12:00:00-0800', 'time with time zone'), '20:00:00')
        self.assertEqual(db.selected_value_to_singer_value_impl('24:00:00-0800', 'time with time zone'), '08:00:00')

        # time without time zone values should be converted to UTC and time zone dropped
        self.assertEqual(db.selected_value_to_singer_value_impl('12:00:00', 'time without time zone'), '12:00:00')
        # Hour 24 should be taken as 0
        self.assertEqual(db.selected_value_to_singer_value_impl('24:00:00', 'time without time zone'), '00:00:00')

        # timestamp with time zone should be converted to iso format
        self.assertEqual(db.selected_value_to_singer_value_impl('2020-05-01T12:00:00-0800',
                                                                'timestamp with time zone'),
                         '2020-05-01T12:00:00-0800')

        # bit should be True only if elem is '1'
        self.assertEqual(db.selected_value_to_singer_value_impl('1', 'bit'), True)
        self.assertEqual(db.selected_value_to_singer_value_impl('0', 'bit'), False)
        self.assertEqual(db.selected_value_to_singer_value_impl(1, 'bit'), False)
        self.assertEqual(db.selected_value_to_singer_value_impl(0, 'bit'), False)

        # boolean should be True in case of numeric 1 and logical True
        self.assertEqual(db.selected_value_to_singer_value_impl(1, 'boolean'), True)
        self.assertEqual(db.selected_value_to_singer_value_impl(True, 'boolean'), True)
        self.assertEqual(db.selected_value_to_singer_value_impl(0, 'boolean'), False)
        self.assertEqual(db.selected_value_to_singer_value_impl(False, 'boolean'), False)

        self.assertEqual(db.selected_value_to_singer_value_impl({'foo': 'bar'}, 'hstore'), {'foo': 'bar'})

        self.assertEqual(db.selected_value_to_singer_value_impl(3.14, 'foo'), 3.14)
        self.assertEqual(db.selected_value_to_singer_value_impl(float('nan'), 'foo'), None)
        self.assertEqual(db.selected_value_to_singer_value_impl(float('inf'), 'foo'), None)

        self.assertEqual(db.selected_value_to_singer_value_impl(decimal.Decimal(7), 'foo'), decimal.Decimal(7))
        self.assertEqual(db.selected_value_to_singer_value_impl(decimal.Decimal('nan'), 'foo'), None)

        self.assertEqual(db.selected_value_to_singer_value_impl(datetime.date(2022, 11, 11), 'foo'),
                         '2022-11-11T00:00:00+00:00')
        self.assertEqual(db.selected_value_to_singer_value_impl(datetime.time(11, 11, 11), 'foo'),
                         '11:11:11')

    def test_prepare_columns_sql(self):
        self.assertEqual(' "my_column" ', db.prepare_columns_sql('my_column'))

    def test_prepare_columns_for_select_sql_with_timestamp_ntz_column(self):
        self.assertEqual(
            'CASE WHEN  "my_column"  < \'0001-01-01 00:00:00.000\' OR '
            ' "my_column"  > \'9999-12-31 23:59:59.999\' THEN \'9999-12-31 23:59:59.999\' '
            'ELSE  "my_column"  END AS  "my_column" ',
            db.prepare_columns_for_select_sql('my_column',
                                              {
                                                  ('properties', 'my_column'): {
                                                      'sql-datatype': 'timestamp without time zone'
                                                  }
                                              }
                                              )
        )

    def test_prepare_columns_for_select_sql_with_timestamp_tz_column(self):
        self.assertEqual(
            'CASE WHEN  "my_column"  < \'0001-01-01 00:00:00.000\' OR '
            ' "my_column"  > \'9999-12-31 23:59:59.999\' THEN \'9999-12-31 23:59:59.999\' '
            'ELSE  "my_column"  END AS  "my_column" ',
            db.prepare_columns_for_select_sql('my_column',
                                              {
                                                  ('properties', 'my_column'): {
                                                      'sql-datatype': 'timestamp with time zone'
                                                  }
                                              }
                                              )
        )

    def test_prepare_columns_for_select_sql_with_timestamp_ntz_array_column(self):
        self.assertEqual(
            ' "my_column" ',
            db.prepare_columns_for_select_sql('my_column',
                                              {
                                                  ('properties', 'my_column'): {
                                                      'sql-datatype': 'timestamp without time zone[]'
                                                  }
                                              }
                                              )
        )

    def test_prepare_columns_for_select_sql_with_timestamp_tz_array_column(self):
        self.assertEqual(
            ' "my_column" ',
            db.prepare_columns_for_select_sql('my_column',
                                              {
                                                  ('properties', 'my_column'): {
                                                      'sql-datatype': 'timestamp with time zone[]'
                                                  }
                                              }
                                              )
        )

    def test_prepare_columns_for_select_sql_with_not_timestamp_column(self):
        self.assertEqual(
            ' "my_column" ',
            db.prepare_columns_for_select_sql('my_column',
                                              {
                                                  ('properties', 'my_column'): {
                                                      'sql-datatype': 'int'
                                                  }
                                              }
                                              )
        )

    def test_prepare_columns_for_select_sql_with_column_not_in_map(self):
        self.assertEqual(
            ' "my_column" ',
            db.prepare_columns_for_select_sql('my_column',
                                              {
                                                  ('properties', 'nope'): {
                                                      'sql-datatype': 'int'
                                                  }
                                              }
                                              )
        )

    def test_selected_value_to_singer_value_impl_with_null_json_returns_None(self):
        output = db.selected_value_to_singer_value_impl(None, 'json')

        self.assertEqual(None, output)

    def test_selected_value_to_singer_value_impl_with_empty_json_returns_empty_dict(self):
        output = db.selected_value_to_singer_value_impl('{}', 'json')

        self.assertEqual({}, output)

    def test_selected_value_to_singer_value_impl_with_non_empty_json_returns_equivalent_dict(self):
        output = db.selected_value_to_singer_value_impl('{"key1": "A", "key2": [{"kk": "yo"}, {}]}', 'json')

        self.assertEqual({
            'key1': 'A',
            'key2': [{'kk': 'yo'}, {}]
        }, output)

    def test_selected_value_to_singer_value_impl_with_null_jsonb_returns_None(self):
        output = db.selected_value_to_singer_value_impl(None, 'jsonb')

        self.assertEqual(None, output)

    def test_selected_value_to_singer_value_impl_with_empty_jsonb_returns_empty_dict(self):
        output = db.selected_value_to_singer_value_impl('{}', 'jsonb')

        self.assertEqual({}, output)

    def test_selected_value_to_singer_value_impl_with_non_empty_jsonb_returns_equivalent_dict(self):
        output = db.selected_value_to_singer_value_impl('{"key1": "A", "key2": [{"kk": "yo"}, {}]}', 'jsonb')

        self.assertEqual({
            'key1': 'A',
            'key2': [{'kk': 'yo'}, {}]
        }, output)

    def test_fully_qualified_column_name(self):
        schema = 'foo_schema'
        table = 'foo_table'
        column = 'foo_column'
        expected_output = f'"{schema}"."{table}"."{column}"'
        actual_output = db.fully_qualified_column_name(schema, table, column)
        self.assertEqual(expected_output, actual_output)

    def test_numeric_precision_returns_default_value_if_column_numeric_precision_greater_than_max_precision(self):
        class Column:
            numeric_precision = db.MAX_PRECISION + 1

        actual_output = db.numeric_precision(Column)
        self.assertEqual(db.MAX_PRECISION, actual_output)

    def test_numeric_scale_returns_default_value_if_column_numeric_scale_greater_than_max_scale(self):
        class Column:
            numeric_scale = db.MAX_SCALE + 1

        actual_output = db.numeric_scale(Column)
        self.assertEqual(db.MAX_SCALE, actual_output)

    def test_selected_array_to_singer_value_if_elem_not_list(self):
        elem = 'foo'
        sql_datatype = 'bit'
        expected_output = False
        actual_output = db.selected_array_to_singer_value(elem, sql_datatype)
        self.assertEqual(expected_output, actual_output)

    def test_selected_array_to_singer_value_if_elem_is_list(self):
        elem = ['foo_1', 'foo_2']
        sql_datatype = 'bit'
        expected_output = [False, False]
        actual_output = db.selected_array_to_singer_value(elem, sql_datatype)
        self.assertListEqual(expected_output, actual_output)

    def test_filter_dbs_sql_clause(self):
        sql = 'foo'
        filter_dbs = 'bar'
        expected_output = "foo AND datname in ('bar')"
        actual_output = db.filter_dbs_sql_clause(sql, filter_dbs)
        self.assertEqual(expected_output, actual_output)

    def test_filter_schemas_sql_clause(self):
        sql = 'foo'
        filter_schemas = 'bar_1, bar_2'
        expected_output = "foo AND n.nspname in ('bar_1','bar_2')"
        actual_output = db.filter_schemas_sql_clause(sql, filter_schemas)
        self.assertEqual(expected_output, actual_output)
