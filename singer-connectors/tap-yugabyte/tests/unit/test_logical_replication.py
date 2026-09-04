import json
import unittest
import decimal

import singer
import psycopg2

from collections import namedtuple
from datetime import datetime, date, timezone
from unittest.mock import Mock, call, mock_open, patch
from dateutil.tz import tzoffset

from tap_yugabyte.sync_strategies import common, logical_replication


class YugabyteCurReplicationSlotMock:
    """
    Yugabyte Cursor Mock with replication slot selection
    """

    def __init__(self, existing_slot_name):
        """Initialise by defining an existing replication slot"""
        self.existing_slot_name = existing_slot_name
        self.replication_slot_found = False

    def execute(self, sql):
        """Simulating to run an SQL query
        If the query is selecting the existing_slot_name then the replication slot found"""
        if sql == f"SELECT * FROM pg_replication_slots WHERE slot_name = '{self.existing_slot_name}'":
            self.replication_slot_found = True

    def fetchall(self):
        """Return the replication slot name as a List if the slot exists."""
        if self.replication_slot_found:
            return [self.existing_slot_name]

        return []


class TestLogicalReplication(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.WalMessage = namedtuple('WalMessage', ['payload', 'data_start'])
        self.conn_info = {'host': 'foo',
                          'dbname': 'foo_db',
                          'user': 'foo_user',
                          'password': 'foo_pass',
                          'port': 12345,
                          'tap_id': 'tap_id_value',
                          'max_run_seconds': 10,
                          'break_at_end_lsn': False,
                          'logical_poll_total_seconds': 1}

        self.logical_streams = [{
            'tap_stream_id': 'foo-bar',
            'schema': {'properties': {'foo_desired': 'b'}},
            'stream': 'test',
            'table_name': 'table_name_value',
            'metadata': [{'metadata': {'sql-datatype': 'test', 'schema-name': 'schema_name_value'},
                          'breadcrumb': ["properties", "foo_desired"]}]
        }]

    def test_streams_to_wal2json_tables(self):
        """Validate if table names are escaped to wal2json format"""
        streams = [
            {'metadata': [{'metadata': {'schema-name': 'public'}}],
             'table_name': 'dummy_table'},
            {'metadata': [{'metadata': {'schema-name': 'public'}}],
             'table_name': 'CaseSensitiveTable'},
            {'metadata': [{'metadata': {'schema-name': 'public'}}],
             'table_name': 'Case Sensitive Table With Space'},
            {'metadata': [{'metadata': {'schema-name': 'CaseSensitiveSchema'}}],
             'table_name': 'dummy_table'},
            {'metadata': [{'metadata': {'schema-name': 'Case Sensitive Schema With Space'}}],
             'table_name': 'CaseSensitiveTable'},
            {'metadata': [{'metadata': {'schema-name': 'Case Sensitive Schema With Space'}}],
             'table_name': 'Case Sensitive Table With Space'},
            {'metadata': [{'metadata': {'schema-name': 'public'}}],
             'table_name': 'table_with_comma_,'},
            {'metadata': [{'metadata': {'schema-name': 'public'}}],
             'table_name': "table_with_quote_'"}
        ]

        self.assertEqual(logical_replication.streams_to_wal2json_tables(streams),
                         'public.dummy_table,'
                         'public.CaseSensitiveTable,'
                         'public.Case\\ Sensitive\\ Table\\ With\\ Space,'
                         'CaseSensitiveSchema.dummy_table,'
                         'Case\\ Sensitive\\ Schema\\ With\\ Space.CaseSensitiveTable,'
                         'Case\\ Sensitive\\ Schema\\ With\\ Space.Case\\ Sensitive\\ Table\\ With\\ Space,'
                         'public.table_with_comma_\\,,'
                         "public.table_with_quote_\\'")

    def test_generate_replication_slot_name(self):
        """Validate if the replication slot name generated correctly"""
        # Provide only database name
        self.assertEqual(logical_replication.generate_replication_slot_name('some_db'),
                         'pipelinewise_some_db')

        # Provide database name and tap_id
        self.assertEqual(logical_replication.generate_replication_slot_name('some_db',
                                                                            'some_tap'),
                         'pipelinewise_some_db_some_tap')

        # Provide database name, tap_id and prefix
        self.assertEqual(logical_replication.generate_replication_slot_name('some_db',
                                                                            'some_tap',
                                                                            prefix='custom_prefix'),
                         'custom_prefix_some_db_some_tap')

        # Replication slot name should be lowercase
        self.assertEqual(logical_replication.generate_replication_slot_name('SoMe_DB',
                                                                            'SoMe_TaP'),
                         'pipelinewise_some_db_some_tap')

        # Invalid characters should be replaced by underscores
        self.assertEqual(logical_replication.generate_replication_slot_name('some-db',
                                                                            'some-tap'),
                         'pipelinewise_some_db_some_tap')

        self.assertEqual(logical_replication.generate_replication_slot_name('some.db',
                                                                            'some.tap'),
                         'pipelinewise_some_db_some_tap')

    def test_locate_replication_slot_by_cur(self):
        """Validate that an existing replication slot is located, and a missing one raises"""
        # Should return the slot name if it exists and tap_id is provided
        cursor = YugabyteCurReplicationSlotMock(existing_slot_name='pipelinewise_some_db_some_tap')
        self.assertEqual(logical_replication.locate_replication_slot_by_cur(cursor,
                                                                            'some_db',
                                                                            'some_tap'),
                         'pipelinewise_some_db_some_tap')

        # Should return the slot name if it exists and tap_id is not provided
        cursor = YugabyteCurReplicationSlotMock(existing_slot_name='pipelinewise_some_db')
        self.assertEqual(logical_replication.locate_replication_slot_by_cur(cursor,
                                                                            'some_db'),
                         'pipelinewise_some_db')

        # Should raise an exception if no matching replication slot is found
        cursor = YugabyteCurReplicationSlotMock(existing_slot_name=None)
        with self.assertRaises(logical_replication.ReplicationSlotNotFoundError):
            logical_replication.locate_replication_slot_by_cur(cursor,
                                                                'some_db',
                                                                'some_tap')

    def test_consume_with_message_payload_is_not_json_expect_same_state(self):
        output = logical_replication.consume_message([],
                                                     {},
                                                     self.WalMessage(payload='this is an invalid json message',
                                                                     data_start=None),
                                                     None,
                                                     {}
                                                     )
        self.assertDictEqual({}, output)

    @patch('tap_yugabyte.sync_strategies.logical_replication.singer.write_message')
    def test_consume_with_message_stream_in_payload_is_not_selected_expect_same_state(self, mocked_write_message):
        output = logical_replication.consume_message(
            [{'tap_stream_id': 'myschema-mytable'}],
            {},
            self.WalMessage(payload='{"action": "U", "schema": "rdsadmin", "table": "rds_heartbeat", '
                                    '"columns": [{"name": "id", "type": "integer", "value": 1}]}',
                            data_start='some lsn'),
            None,
            {}
        )

        self.assertDictEqual({}, output)
        mocked_write_message.assert_not_called()

    def test_consume_with_truncate_action_returns_state_unchanged(self):
        """Truncate (T) actions with schema/table keys are silently skipped"""
        output = logical_replication.consume_message(
            [{'tap_stream_id': 'myschema-mytable'}],
            {},
            self.WalMessage(payload='{"action":"T", "schema": "myschema", "table": "mytable"}',
                            data_start='some lsn'),
            None,
            {}
        )

        self.assertDictEqual({}, output)

    def test_consume_with_begin_action_without_schema_table_returns_state_unchanged(self):
        """Begin (B) messages from wal2json don't include schema/table keys — must not KeyError"""
        output = logical_replication.consume_message(
            [{'tap_stream_id': 'myschema-mytable'}],
            {},
            self.WalMessage(payload='{"action":"B","xid":12345,"timestamp":"2026-04-06 12:00:00.000000+00"}',
                            data_start='some lsn'),
            None,
            {}
        )

        self.assertDictEqual({}, output)

    def test_consume_with_commit_action_without_schema_table_returns_state_unchanged(self):
        """Commit (C) messages from wal2json don't include schema/table keys — must not KeyError"""
        output = logical_replication.consume_message(
            [{'tap_stream_id': 'myschema-mytable'}],
            {},
            self.WalMessage(payload='{"action":"C","xid":12345,"timestamp":"2026-04-06 12:00:00.000000+00"}',
                            data_start='some lsn'),
            None,
            {}
        )

        self.assertDictEqual({}, output)

    @patch('tap_yugabyte.logical_replication.singer.write_message')
    @patch('tap_yugabyte.logical_replication.sync_common.send_schema_message')
    @patch('tap_yugabyte.logical_replication.refresh_streams_schema')
    def test_consume_message_with_new_column_in_payload_will_refresh_schema(self,
                                                                            refresh_schema_mock,
                                                                            send_schema_mock,
                                                                            write_message_mock):
        streams = [
            {
                'tap_stream_id': 'myschema-mytable',
                'stream': 'mytable',
                'schema': {
                    'properties': {
                        'id': {},
                        'date_created': {}
                    }
                },
                'metadata': [
                    {
                        'breadcrumb': [],
                        'metadata': {
                            'is-view': False,
                            'table-key-properties': ['id'],
                            'schema-name': 'myschema'
                        }
                    },
                    {
                        "breadcrumb": [
                            "properties",
                            "id"
                        ],
                        "metadata": {
                            "sql-datatype": "integer",
                            "inclusion": "automatic",
                        }
                    },
                    {
                        "breadcrumb": [
                            "properties",
                            "date_created"
                        ],
                        "metadata": {
                            "sql-datatype": "datetime",
                            "inclusion": "available",
                            "selected": True
                        }
                    }
                ],
            }
        ]

        return_v = logical_replication.consume_message(
            streams,
            {
                'bookmarks': {
                    "myschema-mytable": {
                        "last_replication_method": "LOG_BASED",
                        "lsn": None,
                        "version": 1000,
                    }
                }
            },
            self.WalMessage(
                payload='{"action": "I", '
                        '"schema": "myschema", '
                        '"table": "mytable",'
                        '"columns": ['
                        '{"name": "id", "value": 1}, '
                        '{"name": "date_created", "value": null}, '
                        '{"name": "new_col", "value": "some random text"}'
                        ']}',
                data_start='some lsn'),
            None,
            {}
        )

        self.assertDictEqual(return_v,
                             {
                                 'bookmarks': {
                                     "myschema-mytable": {
                                         "last_replication_method": "LOG_BASED",
                                         "lsn": "some lsn",
                                         "version": 1000,
                                     }
                                 }
                             })

        refresh_schema_mock.assert_called_once_with({}, [streams[0]])
        send_schema_mock.assert_called_once_with(
            streams[0],
            ['lsn'],
            record_update_mode=common.PATCH_RECORD_UPDATE_MODE)
        write_message_mock.assert_called_once()

    def test_selected_value_to_singer_value_impl_with_timestamp_ntz_value_as_string_expect_iso_format(self):
        output = logical_replication.selected_value_to_singer_value_impl('2020-09-01 20:10:56',
                                                                         'timestamp without time zone',
                                                                         None)

        self.assertEqual('2020-09-01T20:10:56+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_ntz_value_as_datetime_expect_iso_format(self):
        output = logical_replication.selected_value_to_singer_value_impl(datetime(2020, 9, 1, 20, 10, 59),
                                                                         'timestamp without time zone',
                                                                         None)

        self.assertEqual('2020-09-01T20:10:59+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_ntz_value_as_string_out_of_range_1(self):
        """
        Test selected_value_to_singer_value_impl with timestamp without tz as string where year is > 9999
        should fallback to max datetime allowed
        """
        output = logical_replication.selected_value_to_singer_value_impl('10000-09-01 20:10:56',
                                                                         'timestamp without time zone',
                                                                         None)

        self.assertEqual('9999-12-31T23:59:59.999+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_ntz_value_as_string_out_of_range_2(self):
        """
        Test selected_value_to_singer_value_impl with timestamp without tz as string where year is < 0001
        should fallback to max datetime allowed
        """
        output = logical_replication.selected_value_to_singer_value_impl('0000-09-01 20:10:56',
                                                                         'timestamp without time zone',
                                                                         None)

        self.assertEqual('9999-12-31T23:59:59.999+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_ntz_value_as_string_BC(self):
        """
        Test selected_value_to_singer_value_impl with timestamp without tz as string where era is BC
        should fallback to max datetime allowed
        """
        output = logical_replication.selected_value_to_singer_value_impl('1000-09-01 20:10:56 BC',
                                                                         'timestamp without time zone',
                                                                         None)

        self.assertEqual('9999-12-31T23:59:59.999+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_ntz_value_as_string_AC(self):
        """
        Test selected_value_to_singer_value_impl with timestamp without tz as string where era is AC
        should fallback to max datetime allowed
        """
        output = logical_replication.selected_value_to_singer_value_impl('1000-09-01 20:10:56 AC',
                                                                         'timestamp without time zone',
                                                                         None)

        self.assertEqual('9999-12-31T23:59:59.999+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_ntz_value_as_string_min(self):
        output = logical_replication.selected_value_to_singer_value_impl('0001-01-01 00:00:00.000123',
                                                                         'timestamp without time zone',
                                                                         None)

        self.assertEqual('0001-01-01T00:00:00.000123+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_ntz_value_as_string_max(self):
        output = logical_replication.selected_value_to_singer_value_impl('9999-12-31 23:59:59.999999',
                                                                         'timestamp without time zone',
                                                                         None)

        self.assertEqual('9999-12-31T23:59:59.999+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_ntz_value_as_datetime_min(self):
        output = logical_replication.selected_value_to_singer_value_impl(datetime(1, 1, 1, 0, 0, 0, 123),
                                                                         'timestamp without time zone',
                                                                         None)

        self.assertEqual('0001-01-01T00:00:00.000123+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_ntz_value_as_datetime_max(self):
        output = logical_replication.selected_value_to_singer_value_impl(datetime(9999, 12, 31, 23, 59, 59, 999999),
                                                                         'timestamp without time zone',
                                                                         None)

        self.assertEqual('9999-12-31T23:59:59.999+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_tz_value_as_string_expect_iso_format(self):
        output = logical_replication.selected_value_to_singer_value_impl('2020-09-01 20:10:56+05',
                                                                         'timestamp with time zone',
                                                                         None)

        self.assertEqual('2020-09-01T20:10:56+05:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_tz_value_as_datetime_expect_iso_format(self):
        output = logical_replication.selected_value_to_singer_value_impl(datetime(2020, 9, 1, 23, 10, 59,
                                                                                  tzinfo=tzoffset(None, -3600)),
                                                                         'timestamp with time zone',
                                                                         None)

        self.assertEqual('2020-09-01T23:10:59-01:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_tz_value_as_string_out_of_range_1(self):
        """
        Test selected_value_to_singer_value_impl with timestamp with tz as string where year is > 9999
        should fallback to max datetime allowed
        """
        output = logical_replication.selected_value_to_singer_value_impl('10000-09-01 20:10:56+06',
                                                                         'timestamp with time zone',
                                                                         None)

        self.assertEqual('9999-12-31T23:59:59.999+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_tz_value_as_string_out_of_range_2(self):
        """
        Test selected_value_to_singer_value_impl with timestamp with tz as string where year is < 0001
        should fallback to max datetime allowed
        """
        output = logical_replication.selected_value_to_singer_value_impl('0000-09-01 20:10:56+01',
                                                                         'timestamp with time zone',
                                                                         None)

        self.assertEqual('9999-12-31T23:59:59.999+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_tz_value_as_string_BC(self):
        """
        Test selected_value_to_singer_value_impl with timestamp with tz as string where era is BC
        should fallback to max datetime allowed
        """
        output = logical_replication.selected_value_to_singer_value_impl('1000-09-01 20:10:56+05 BC',
                                                                         'timestamp with time zone',
                                                                         None)

        self.assertEqual('9999-12-31T23:59:59.999+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_tz_value_as_string_AC(self):
        """
        Test selected_value_to_singer_value_impl with timestamp with tz as string where era is AC
        should fallback to max datetime allowed
        """
        output = logical_replication.selected_value_to_singer_value_impl('1000-09-01 20:10:56-09 AC',
                                                                         'timestamp with time zone',
                                                                         None)

        self.assertEqual('9999-12-31T23:59:59.999+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_tz_value_as_string_min(self):
        output = logical_replication.selected_value_to_singer_value_impl('0001-01-01 00:00:00.000123+04',
                                                                         'timestamp with time zone',
                                                                         None)

        self.assertEqual('9999-12-31T23:59:59.999+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_tz_value_as_string_max(self):
        output = logical_replication.selected_value_to_singer_value_impl('9999-12-31 23:59:59.999999-03',
                                                                         'timestamp with time zone',
                                                                         None)

        self.assertEqual('9999-12-31T23:59:59.999+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_tz_value_as_datetime_min(self):
        output = logical_replication.selected_value_to_singer_value_impl(datetime(1, 1, 1, 0, 0, 0, 123,
                                                                                  tzinfo=tzoffset(None, 14400)),
                                                                         'timestamp with time zone',
                                                                         None)

        self.assertEqual('9999-12-31T23:59:59.999+00:00', output)

    def test_selected_value_to_singer_value_impl_with_timestamp_tz_value_as_datetime_max(self):
        output = logical_replication.selected_value_to_singer_value_impl(datetime(9999, 12, 31, 23, 59, 59, 999999,
                                                                                  tzinfo=tzoffset(None, -14400)),
                                                                         'timestamp with time zone',
                                                                         None)

        self.assertEqual('9999-12-31T23:59:59.999+00:00', output)

    def test_selected_value_to_singer_value_impl_with_date_value_as_string_expect_iso_format(self):
        output = logical_replication.selected_value_to_singer_value_impl('2021-09-07', 'date', None)

        self.assertEqual('2021-09-07T00:00:00+00:00', output)

    def test_selected_value_to_singer_value_impl_with_date_value_as_string_out_of_range(self):
        """
        Test selected_value_to_singer_value_impl with date as string where year
        is > 9999 (which is valid in postgres) should fallback to max date
        allowed
        """
        output = logical_replication.selected_value_to_singer_value_impl('10000-09-01', 'date', None)

        self.assertEqual('9999-12-31T00:00:00+00:00', output)

    def test_row_to_singer_message(self):
        stream = {
            'stream': 'my_stream',
        }

        row = [
            '2020-01-01 10:30:45',
            '2020-01-01 10:30:45 BC',
            '50000-01-01 10:30:45',
            datetime(2020, 1, 1, 10, 30, 45),
            '2020-01-01 10:30:45-02',
            '0000-01-01 10:30:45-02',
            '2020-01-01 10:30:45-02 AC',
            datetime(2020, 1, 1, 10, 30, 45, tzinfo=tzoffset(None, 3600)),
        ]

        columns = [
            'c_timestamp_ntz_1',
            'c_timestamp_ntz_2',
            'c_timestamp_ntz_3',
            'c_timestamp_ntz_4',
            'c_timestamp_tz_1',
            'c_timestamp_tz_2',
            'c_timestamp_tz_3',
            'c_timestamp_tz_4',
        ]

        md_map = {
            (): {'schema-name': 'my_schema'},
            ('properties', 'c_timestamp_ntz_1'): {'sql-datatype': 'timestamp without time zone'},
            ('properties', 'c_timestamp_ntz_2'): {'sql-datatype': 'timestamp without time zone'},
            ('properties', 'c_timestamp_ntz_3'): {'sql-datatype': 'timestamp without time zone'},
            ('properties', 'c_timestamp_ntz_4'): {'sql-datatype': 'timestamp without time zone'},
            ('properties', 'c_timestamp_tz_1'): {'sql-datatype': 'timestamp with time zone'},
            ('properties', 'c_timestamp_tz_2'): {'sql-datatype': 'timestamp with time zone'},
            ('properties', 'c_timestamp_tz_3'): {'sql-datatype': 'timestamp with time zone'},
            ('properties', 'c_timestamp_tz_4'): {'sql-datatype': 'timestamp with time zone'},
        }

        output = logical_replication.row_to_singer_message(stream,
                                                           row,
                                                           1000,
                                                           columns,
                                                           datetime(2020, 9, 1, 10, 10, 10, tzinfo=tzoffset(None, 0)),
                                                           md_map,
                                                           None)

        self.assertEqual('my_schema-my_stream', output.stream)
        self.assertDictEqual({
            'c_timestamp_ntz_1': '2020-01-01T10:30:45+00:00',
            'c_timestamp_ntz_2': '9999-12-31T23:59:59.999+00:00',
            'c_timestamp_ntz_3': '9999-12-31T23:59:59.999+00:00',
            'c_timestamp_ntz_4': '2020-01-01T10:30:45+00:00',
            'c_timestamp_tz_1': '2020-01-01T10:30:45-02:00',
            'c_timestamp_tz_2': '9999-12-31T23:59:59.999+00:00',
            'c_timestamp_tz_3': '9999-12-31T23:59:59.999+00:00',
            'c_timestamp_tz_4': '2020-01-01T10:30:45+01:00',
        }, output.record)

        self.assertEqual(1000, output.version)
        self.assertEqual(datetime(2020, 9, 1, 10, 10, 10, tzinfo=tzoffset(None, 0)), output.time_extracted)

    def test_selected_value_to_singer_value_impl_with_null_json_returns_None(self):
        output = logical_replication.selected_value_to_singer_value_impl(None,
                                                                         'json',
                                                                         None)

        self.assertEqual(None, output)

    def test_selected_value_to_singer_value_impl_with_empty_json_returns_empty_dict(self):
        output = logical_replication.selected_value_to_singer_value_impl('{}',
                                                                         'json',
                                                                         None)

        self.assertEqual({}, output)

    def test_selected_value_to_singer_value_impl_with_non_empty_json_returns_equivalent_dict(self):
        output = logical_replication.selected_value_to_singer_value_impl('{"key1": "A", "key2": [{"kk": "yo"}, {}]}',
                                                                         'json',
                                                                         None)

        self.assertEqual({
            'key1': 'A',
            'key2': [{'kk': 'yo'}, {}]
        }, output)

    def test_selected_value_to_singer_value_impl_with_null_jsonb_returns_None(self):
        output = logical_replication.selected_value_to_singer_value_impl(None,
                                                                         'jsonb',
                                                                         None)

        self.assertEqual(None, output)

    def test_selected_value_to_singer_value_impl_with_empty_jsonb_returns_empty_dict(self):
        output = logical_replication.selected_value_to_singer_value_impl('{}',
                                                                         'jsonb',
                                                                         None)

        self.assertEqual({}, output)

    def test_selected_value_to_singer_value_impl_with_non_empty_jsonb_returns_equivalent_dict(self):
        output = logical_replication.selected_value_to_singer_value_impl('{"key1": "A", "key2": [{"kk": "yo"}, {}]}',
                                                                         'jsonb',
                                                                         None)

        self.assertEqual({
            'key1': 'A',
            'key2': [{'kk': 'yo'}, {}]
        }, output)

    @patch('tap_yugabyte.sync_strategies.logical_replication.yb_db.open_connection')
    def test_fetch_current_lsn(self, mocked_open_connection):
        """Fetch the current HYBRID_TIME LSN boundary through one plain connection."""
        connection = mocked_open_connection.return_value.__enter__.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = [123456789]

        actual_value = logical_replication.fetch_current_lsn(self.conn_info)

        self.assertEqual(123456789, actual_value)
        mocked_open_connection.assert_called_once_with(self.conn_info)
        cursor.execute.assert_called_once_with("SELECT yb_get_current_hybrid_time_lsn()")

    @patch('tap_yugabyte.sync_strategies.logical_replication.uuid.uuid4')
    @patch('tap_yugabyte.sync_strategies.logical_replication.yb_db.open_connection')
    def test_emit_wal_progress_message(self, mocked_open_connection, mocked_uuid):
        """Emit a transactional marker through a plain connection when permitted."""
        connection = mocked_open_connection.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (True,)
        mocked_uuid.return_value.hex = 'unique_marker'

        self.assertEqual(
            'wal_progress:tap_id_value:unique_marker',
            logical_replication.emit_wal_progress_message(self.conn_info),
        )

        mocked_open_connection.assert_called_once_with(self.conn_info)
        self.assertEqual(cursor.execute.call_count, 2)
        self.assertEqual(
            cursor.execute.call_args_list[-1],
            call(
                'SELECT pg_catalog.pg_logical_emit_message(TRUE, %s, %s)',
                ('pipelinewise', 'wal_progress:tap_id_value:unique_marker')
            )
        )
        connection.close.assert_called_once_with()

    @patch('tap_yugabyte.sync_strategies.logical_replication.yb_db.open_connection')
    def test_emit_wal_progress_message_skips_unavailable_function(self, mocked_open_connection):
        """Old or restricted YugabyteDB sources retain the existing behavior."""
        connection = mocked_open_connection.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (False,)

        self.assertIsNone(logical_replication.emit_wal_progress_message(self.conn_info))
        self.assertEqual(cursor.execute.call_count, 1)
        connection.close.assert_called_once_with()

    @patch('tap_yugabyte.sync_strategies.logical_replication.yb_db.open_connection')
    def test_emit_wal_progress_message_ignores_database_errors(self, mocked_open_connection):
        """Failure to emit the optional marker does not prevent replication."""
        connection = mocked_open_connection.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = psycopg2.ProgrammingError('permission denied')

        with self.assertLogs('tap_yugabyte', level='WARNING') as logs:
            self.assertIsNone(logical_replication.emit_wal_progress_message(self.conn_info))

        self.assertIn('continuing without it', logs.output[0])
        connection.close.assert_called_once_with()

    def test_add_automatic_properties_if_debug_lsn_is_off(self):
        """Test if add_automatic_property returns expected value if debug_lsn is off"""
        stream = {'schema': {'properties': {'_sdc_deleted_at': 'foo'}}}
        expected_stream = {
            'schema': {
                'properties': {
                    '_sdc_deleted_at': {
                        'format': 'date-time',
                        'type': ['null', 'string']}
                }
            }
        }
        with self.assertLogs(level='DEBUG') as foo_log:
            actual_stream = logical_replication.add_automatic_properties(stream=stream, debug_lsn=False)
            self.assertDictEqual(expected_stream, actual_stream)
            self.assertEqual(['DEBUG:tap_yugabyte:debug_lsn is OFF'], foo_log.output)

    def test_add_automatic_properties_if_debug_lsn_is_on(self):
        """Test if add_automatic_property returns expected value if debug_lsn is on"""
        stream = {'schema': {'properties': {'_sdc_deleted_at': 'foo'}}}
        expected_stream = {
            'schema': {
                'properties': {
                    '_sdc_deleted_at': {
                        'format': 'date-time',
                        'type': ['null', 'string']
                    },
                    '_sdc_lsn': {'type': ['null', 'string']}
                }
            }
        }
        with self.assertLogs(level='DEBUG') as foo_log:
            actual_stream = logical_replication.add_automatic_properties(stream=stream, debug_lsn=True)
            self.assertDictEqual(expected_stream, actual_stream)
            self.assertEqual(['DEBUG:tap_yugabyte:debug_lsn is ON'], foo_log.output)

    def test_get_stream_version_raises_exception_if_version_is_none(self):
        """Test get_stream_version raises an expection if version is None"""
        tap_stream_id = 'foo'
        state = {'bookmarks': {'bar': {'version': None}}}

        with self.assertRaises(Exception) as exp:
            logical_replication.get_stream_version(tap_stream_id, state)
        self.assertEqual(f'version not found for log miner {tap_stream_id}', str(exp.exception))

    def test_get_stream_version_not_none(self):
        """Test if get_stream_version works correctly"""
        tap_stream_id = 'foo'
        state = {'bookmarks': {'foo': {'version': 'foo_version'}}}
        actual_value = logical_replication.get_stream_version(tap_stream_id, state)
        self.assertEqual(state['bookmarks']['foo']['version'], actual_value)

    def test_tuples_to_map(self):
        """Test if the output of tuples_to_map is as expected"""
        accum = {'foo_key': 'foo_value'}
        t = ['bar_1', 'bar_2']
        expected_output = accum.copy()
        expected_output[t[0]] = t[1]

        actual_output = logical_replication.tuples_to_map(accum, t)
        self.assertEqual(expected_output, actual_output)

    @patch("psycopg2.connect")
    def test_create_hstore_elem(self, mocked_connect):
        """Test if the output of create_hstore_elem is as expected"""
        mocked_cursor = mocked_connect.return_value.__enter__.return_value.cursor
        mocked_fetchone = mocked_cursor.return_value.__enter__.return_value.fetchone
        mocked_fetchone.return_value = (['foo', 'bar'],)
        elem = 'foo=>bar'
        expected_output = {'foo': 'bar'}
        actual_output = logical_replication.create_hstore_elem(self.conn_info, elem)
        self.assertDictEqual(expected_output, actual_output)

    @patch("psycopg2.connect")
    def test_create_array_elem(self, mocked_connect):
        """Test if the output of create_array_elem is as expected"""
        mocked_cursor = mocked_connect.return_value.__enter__.return_value.cursor
        mocked_fetchone = mocked_cursor.return_value.__enter__.return_value.fetchone
        test_values = [('foo', '{bar}', ['bar']),
                       ('bit[]', {1}, [True]),
                       ('foo', None, None),
                       ('boolean[]', {True}, [True]),
                       ('character varying[]', {1, 'foo'}, ['1', "'foo'"]),
                       ('cidr[]', "{127.0.0.1}", ['127.0.0.1/32']),
                       ('citext[]', {1, 'foo'}, ['1', "'foo'"]),
                       ('date[]', '{2022-11-11}', ['2022-11-11']),
                       ('double precision[]', {234.45}, [234.45]),
                       ('hstore[]', {'foo'}, ["'foo'"]),
                       ('integer[]', {123}, [123]),
                       ('inet[]', "{127.0.0.1}", ['127.0.0.1']),
                       ('json[]', {"foo": "bar"}, ["'foo': 'bar'"]),
                       ('jsonb[]', {"foo": "bar"}, ["'foo': 'bar'"]),
                       ('macaddr[]', "{aa:bb:cc:dd:ee:ff}", ['aa:bb:cc:dd:ee:ff']),
                       ('money[]', {12.5}, ['12.5']),
                       ('numeric[]', {12.5}, ['12.5']),
                       ('real[]', {12.5}, [12.5]),
                       ('smallint[]', {12}, [12]),
                       ('text[]', '{foo}', ['foo']),
                       ('time without time zone[]', '{22:22:22}', ['22:22:22']),
                       ('time with time zone[]', '{22:22:22+00:00}', ['22:22:22+00:00']),
                       ('timestamp without time zone[]', '{2022-11-11T22:22:22}', ['2022-11-11T22:22:22']),
                       ('uuid[]', '{aabbccdd}', ['aabbccdd'])]

        for sql_datatype, elem, expected_output in test_values:
            mocked_fetchone.return_value = [(expected_output), ]
            actual_output = logical_replication.create_array_elem(elem, sql_datatype, self.conn_info)
            self.assertEqual(expected_output, actual_output)

    def test_selected_array_to_singer_value(self):
        """Test if selected_array_to_singer_value returns excpected output"""
        sql_datatype = 'date'
        test_values = [
            (['2022-11-11', '2022-11-12'], ['2022-11-11T00:00:00+00:00', '2022-11-12T00:00:00+00:00']),
            ('2022-11-11', '2022-11-11T00:00:00+00:00')
        ]
        for elem, expected_output in test_values:
            actual_output = logical_replication.selected_array_to_singer_value(elem, sql_datatype, self.conn_info)
            self.assertEqual(expected_output, actual_output)

    @patch("psycopg2.connect")
    def test_selected_value_to_singer_value(self, mocked_connect):
        """Test if selected_value_to_singer_value returns expected value"""
        mocked_cursor = mocked_connect.return_value.__enter__.return_value.cursor
        mocked_fetchone = mocked_cursor.return_value.__enter__.return_value.fetchone
        mocked_fetchone.return_value = (['foo'],)
        test_values = [
            ('bar', '{foo}', '{foo}'),
            ('text[]', '{foo}', ['foo'])
        ]
        for sql_datatype, elem, expected_output in test_values:
            actual_output = logical_replication.selected_value_to_singer_value(elem, sql_datatype, self.conn_info)
            self.assertEqual(expected_output, actual_output)

    def test_row_to_singer_message_raises_exception_if_no_sql_datatype_in_md_map(self):
        """Test row_to_singer_message raises exception if no sql_datatype in md_map"""
        stream = 'bar'
        row = ['foo']
        version = 3
        columns = ['foo']
        time_extracted = 5
        md_map = {('properties', 'foo'): {}}

        with self.assertRaises(Exception) as exp:
            logical_replication.row_to_singer_message(
                stream, row, version, columns, time_extracted, md_map, self.conn_info
            )

        self.assertEqual(f'Unable to find sql-datatype for stream {stream}', str(exp.exception))

    def test_row_to_singer_message_simple(self):
        """Test if row_to_singer_message output is as expected"""
        stream = {'stream': 1}
        row = ['foo']
        version = 3
        columns = ['foo']
        time_extracted = None

        md_map = {('properties', 'foo'): {'sql-datatype': '[foo]', 'schema-name': 'bar'}}
        actual_output = logical_replication.row_to_singer_message(
            stream, row, version, columns, time_extracted, md_map, self.conn_info
        )

        expected_output = singer.RecordMessage(stream='None-1', record={'foo': 'foo'}, version=version)
        self.assertEqual(expected_output, actual_output)

    @patch("psycopg2.connect")
    def test_locate_replication_slot(self, mocked_connect):
        """Test locate_replication_slot returns excpected value"""
        mocked_cursor = mocked_connect.return_value.__enter__.return_value.cursor
        mocked_fetchall = mocked_cursor.return_value.__enter__.return_value.fetchall
        mocked_fetchall.return_value = ['foo']
        expected_output = f'pipelinewise_{self.conn_info["dbname"]}_{self.conn_info["tap_id"]}'
        actual_output = logical_replication.locate_replication_slot(self.conn_info)
        self.assertEqual(expected_output, actual_output)

    def test_impl_if_sql_datatype_is_money(self):
        """Test selected_value_to_singer_value_impl if sql_datatype is money"""
        elem = 'foo'
        og_sql_datatype = 'money'
        conn_info = None
        expected_output = elem
        actual_output = logical_replication.selected_value_to_singer_value_impl(elem, og_sql_datatype, conn_info)

        self.assertEqual(expected_output, actual_output)

    def test_impl_if_timestamp_with_time_zone_as_datatype_and_greater_than_fallback_datetime(self):
        """Test selected_value_to_singer_value_impl if datatype is
        timestamp with time zone and greater than FALLBACK_DATETIME"""
        # maximum value is hardcoded! and is 9999-12-31 23:59:59.999000
        elem = datetime(9999, 12, 31, 23, 59, 59, 999001, tzinfo=timezone.utc)
        og_sql_datatype = 'timestamp with time zone'
        conn_info = None
        expected_output = logical_replication.FALLBACK_DATETIME
        actual_output = logical_replication.selected_value_to_singer_value_impl(elem, og_sql_datatype, conn_info)

        self.assertEqual(expected_output, actual_output)

    def test_impl_datatype_timestamp_with_time_zone_and_parsed_elm_greater_than_and_not_datetime(self):
        """Test selected_value_to_singer_value_impl if datatype is
        timestamp with time zone and parsed not datetime value greater than FALLBACK_DATETIME"""
        # maximum value is hardcoded! and is 9999-12-31 23:59:59.999000
        elem = '9999-12-31T23:59:59.9999999+00:00'
        og_sql_datatype = 'timestamp with time zone'
        conn_info = None
        expected_output = logical_replication.FALLBACK_DATETIME
        actual_output = logical_replication.selected_value_to_singer_value_impl(elem, og_sql_datatype, conn_info)

        self.assertEqual(expected_output, actual_output)

    def test_impl_with_sql_datatype_is_date_with_elm_is_datetime(self):
        """Test selected_value_to_singer_value_impl if datatype is date and elm type is datetime"""
        elem = date(2022, 12, 31)
        og_sql_datatype = 'date'
        conn_info = None
        expected_output = '2022-12-31T00:00:00+00:00'

        actual_output = logical_replication.selected_value_to_singer_value_impl(elem, og_sql_datatype, conn_info)
        self.assertEqual(expected_output, actual_output)

    def test_impl_with_sql_datatype_is_date_with_invalid_elem_raises_exception(self):
        """Test selected_value_to_singer_value_impl raises ValueError exception
         if datatype is date and elem is invalid"""
        elem = 'foo'
        og_sql_datatype = 'date'
        conn_info = None
        self.assertRaises(ValueError,
                          logical_replication.selected_value_to_singer_value_impl,
                          elem,
                          og_sql_datatype,
                          conn_info)

    def test_impl_with_sql_datatype_is_time_with_time_zone_and_elem_starts_with_24(self):
        """Test selected_value_to_singer_value_impl if datatype is time with time zone and elem starts with 24"""
        og_sql_datatype = 'time with time zone'
        expected_output = '01:12:11'
        elem = '24:12:11-01'
        conn_info = None
        actual_output = logical_replication.selected_value_to_singer_value_impl(elem, og_sql_datatype, conn_info)

        self.assertEqual(expected_output, actual_output)

    def test_impl_with_sql_datatype_is_time_without_time_zone_elem_starts_with_24(self):
        """Test selected_value_to_singer_value_impl if datatype is time without time zone and elem starts with 24"""
        og_sql_datatype = 'time without time zone'
        expected_output = '00:12:11'
        test_elem = '24:12:11'
        conn_info = None
        actual_output = logical_replication.selected_value_to_singer_value_impl(test_elem, og_sql_datatype, conn_info)

        self.assertEqual(expected_output, actual_output)

    def test_impl_with_sql_datatype_is_bit(self):
        """Test selected_value_to_singer_value_impl if datatype is bit"""
        og_sql_datatype = 'bit'
        elem = True
        conn_info = None
        actual_output = logical_replication.selected_value_to_singer_value_impl(elem, og_sql_datatype, conn_info)

        self.assertTrue(actual_output)

    def test_impl_with_sql_datatype_is_int(self):
        """Test selected_value_to_singer_value_impl if datatype is int"""
        og_sql_datatype = 'foo'
        elem = 23
        conn_info = None
        expected_output = elem
        actual_output = logical_replication.selected_value_to_singer_value_impl(elem, og_sql_datatype, conn_info)

        self.assertEqual(expected_output, actual_output)

    def test_impl_with_sql_datatype_is_boolean(self):
        """Test selected_value_to_singer_value_impl if datatype is boolean"""
        og_sql_datatype = 'boolean'
        elem = 'foo'
        conn_info = None
        expected_output = elem
        actual_output = logical_replication.selected_value_to_singer_value_impl(
            elem,
            og_sql_datatype,
            conn_info
        )

        self.assertEqual(expected_output, actual_output)

    @patch("psycopg2.connect")
    def test_impl_with_sql_datatype_is_hstore(self, mocked_connect):
        """Test selected_value_to_singer_value_impl if datatype is hstore"""
        mocked_cursor = mocked_connect.return_value.__enter__.return_value.cursor
        mocked_fetchone = mocked_cursor.return_value.__enter__.return_value.fetchone
        mocked_fetchone.return_value = (['1', '0', '2', '1'],)
        og_sql_datatype = 'hstore'
        hstore_elem = '1=>0,2=>1'
        expected_output = {'1': '0', '2': '1'}

        actual_output = logical_replication.selected_value_to_singer_value_impl(
            hstore_elem,
            og_sql_datatype,
            self.conn_info
        )

        self.assertEqual(expected_output, actual_output)

    def test_impl_with_sql_datatype_contains_numeric(self):
        """Test selected_value_to_singer_value_impl if datatype contains numeric"""
        og_sql_datatype = 'foo numeric bar'
        elem = '2'
        conn_info = None
        expected_output = decimal.Decimal(elem)
        actual_output = logical_replication.selected_value_to_singer_value_impl(elem, og_sql_datatype, conn_info)

        self.assertEqual(expected_output, actual_output)

    def test_impl_with_float_elem(self):
        """Test selected_value_to_singer_value_impl if elem is float"""
        og_sql_datatype = 'foo'
        elem = 3.14
        conn_info = None
        expected_output = elem
        actual_output = logical_replication.selected_value_to_singer_value_impl(elem, og_sql_datatype, conn_info)

        self.assertEqual(expected_output, actual_output)

    def test_impl_raises_exception_with_invalid_type_of_elem(self):
        """Test selected_value_to_singer_value_impl with invalid type of elem raises an exception"""
        og_sql_datatype = 'foo'
        elem = {}
        conn_info = None
        expected_message = f'do not know how to marshall value of type {type(elem)}'
        with self.assertRaises(Exception) as exp:
            logical_replication.selected_value_to_singer_value_impl(elem, og_sql_datatype, conn_info)

        self.assertEqual(expected_message, str(exp.exception))

    @patch('tap_yugabyte.sync_strategies.logical_replication.refresh_streams_schema')
    @patch('tap_yugabyte.sync_strategies.logical_replication.sync_common.send_schema_message')
    def test_consume_message_if_payload_kind_insert_or_update(self, *args):
        """Test consume_message if the kind of payload is insert or update"""

        data_start = 'foo_start'

        insert_msg = self.WalMessage(data_start=data_start,
                                     payload=json.dumps(
                                         {
                                             'schema': 'foo',
                                             'table': 'bar',
                                             'action': 'I',
                                             'columns': [{'name': '_sdc_deleted_at', 'value': 'foo_column'}],
                                         }
                                     )
                                     )

        update_msg = self.WalMessage(data_start=data_start,
                                     payload=json.dumps(
                                         {
                                             'schema': 'foo',
                                             'table': 'bar',
                                             'action': 'U',
                                             'columns': [{'name': '_sdc_deleted_at', 'value': 'foo_column'}],
                                         }
                                     )
                                     )

        streams = [{
            'tap_stream_id': 'foo-bar',
            'schema': {'properties': {'foo_desired': 'b'}},
            'stream': 'test',
            'metadata': [{'metadata': {'good': 'test'}, 'breadcrumb': 'foo'}]
        }]

        state = {'bookmarks': {'foo-bar': {'foo': 'bar', 'version': 'foo_version'}}}
        time_extracted = datetime(2020, 9, 1, 23, 10, 59, tzinfo=tzoffset(None, -3600))

        expected_output = {
            'bookmarks': {
                'foo-bar': {
                    'foo': 'bar',
                    'lsn': data_start,
                    'version': state['bookmarks']['foo-bar']['version']
                }
            }
        }
        self.conn_info['debug_lsn'] = True
        actual_output = logical_replication.consume_message(streams, state, insert_msg, time_extracted, self.conn_info)
        self.assertDictEqual(expected_output, actual_output)

        actual_output = logical_replication.consume_message(streams, state, update_msg, time_extracted, self.conn_info)
        self.assertDictEqual(expected_output, actual_output)

    @patch('tap_yugabyte.sync_strategies.logical_replication.refresh_streams_schema')
    @patch('tap_yugabyte.sync_strategies.logical_replication.sync_common.send_schema_message')
    def test_consume_message_raises_exception_if_delete_and_no_datatype_for_stream(self, *args):
        """Test consume_message raises exception if kind is delete and could not a datatype for the stream"""

        delete_msg = self.WalMessage(data_start='foo_start',
                                     payload=json.dumps({
                                         'schema': 'foo',
                                         'table': 'bar',
                                         'action': 'D',
                                         'identity': [{'name': 'foo_desired', 'value': 'bar_value'}],
                                     })
                                     )

        streams = [{
            'tap_stream_id': 'foo-bar',
            'schema': {'properties': {'foo_desired': 'b'}},
            'stream': 'test',
            'metadata': [{'metadata': {'foo': 'test'}, 'breadcrumb': ["properties", "foo_desired"]}]
        }]

        state = {'bookmarks': {'foo-bar': {'foo': 'bar', 'version': 'foo'}}}
        time_extracted = datetime(2020, 9, 1, 23, 10, 59, tzinfo=tzoffset(None, 0))
        self.conn_info['debug_lsn'] = True
        expected_message = f'Unable to find sql-datatype for stream {streams[0]}'
        with self.assertRaises(Exception) as exp:
            logical_replication.consume_message(streams, state, delete_msg, time_extracted, self.conn_info)

        self.assertEqual(expected_message, str(exp.exception))

    @patch('tap_yugabyte.sync_strategies.logical_replication.sync_common.send_schema_message')
    @patch('tap_yugabyte.sync_strategies.logical_replication.locate_replication_slot')
    @patch("psycopg2.connect")
    def test_sync_tables_raises_exception_if_psycopg2_programming_error(self,
                                                                        mocked_connect,
                                                                        mocked_locate_rep_slot,
                                                                        send_schema_message):
        """Test if sync_tables raises exception on psycopg2.ProgrammingError"""

        mocked_start_replication = mocked_connect.return_value.cursor.return_value.start_replication

        state = {'bookmarks': {'foo-bar': {'foo': 'bar', 'version': 'foo', 'lsn': 1}}}
        end_lsn = 4
        state_file = 5

        test_replication_slot = 'foo_slot'
        expected_message = 'Unable to start replication with logical replication' \
                           f' (slot {test_replication_slot})'
        mocked_start_replication.side_effect = psycopg2.ProgrammingError(test_replication_slot)
        mocked_locate_rep_slot.return_value = test_replication_slot

        with self.assertRaises(Exception) as exp:
            logical_replication.sync_tables(self.conn_info, self.logical_streams, state, end_lsn, state_file)
        self.assertEqual(expected_message, str(exp.exception))
        send_schema_message.assert_called_once_with(
            self.logical_streams[0],
            ['lsn'],
            record_update_mode=common.PATCH_RECORD_UPDATE_MODE)

    @patch('tap_yugabyte.sync_strategies.logical_replication.datetime.datetime')
    @patch('tap_yugabyte.sync_strategies.logical_replication.locate_replication_slot')
    @patch("psycopg2.connect")
    def test_sync_tables_if_poll_duration_greater_than_logical_poll_total_seconds(self,
                                                                                  mocked_connect,
                                                                                  mocked_locate_rep_slot,
                                                                                  mocked_datetime):
        """Test sync_table works as expected if poll_duration greater than the logical_poll_total_seconds"""
        test_poll_duration = 15

        self.conn_info['max_run_seconds'] = test_poll_duration - 1

        state = {'bookmarks': {'foo-bar': {'foo': 'bar', 'version': 'foo', 'lsn': 4}}}
        end_lsn = 8
        state_file = 5
        rep_slot = 'foo_slot'
        mocked_locate_rep_slot.return_value = rep_slot
        mocked_datetime.now().__sub__().total_seconds.return_value = test_poll_duration
        mocked_start_replication = mocked_connect.return_value.cursor.return_value.start_replication

        actual_output = logical_replication.sync_tables(self.conn_info,
                                                        self.logical_streams,
                                                        state, end_lsn, state_file)

        self.assertDictEqual(state, actual_output)
        mocked_start_replication.assert_called_with(
            slot_name=rep_slot,
            decode=True,
            start_lsn=state['bookmarks']['foo-bar']['lsn'],
            status_interval=10,
            options={
                'format-version': '2',
                'include-transaction': 'true',
                'include-timestamp': 'true',
                'include-types': 'false',
                'actions': 'insert,update,delete',
                'add-tables': 'schema_name_value.table_name_value'}
        )

    @patch('tap_yugabyte.sync_strategies.logical_replication.datetime.datetime')
    @patch('tap_yugabyte.sync_strategies.logical_replication.locate_replication_slot')
    @patch("psycopg2.connect")
    def test_sync_tables_if_reached_max_run_seconds(self,
                                                    mocked_connect,
                                                    mocked_locate_rep_slot,
                                                    mocked_datetime):
        """Test sync_table if reached the max_run_seconds"""
        mocked_datetime.now.return_value = datetime(2022, 11, 11, 11, 11, 11, 11, tzinfo=timezone.utc)
        mocked_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        self.conn_info['max_run_seconds'] = 0

        state = {'bookmarks': {'foo-bar': {'foo': 'bar', 'version': 'foo', 'lsn': 4}}}
        end_lsn = 4
        state_file = 5
        rep_slot = 'foo_slot'

        mocked_start_replication = mocked_connect.return_value.cursor.return_value.start_replication
        mocked_locate_rep_slot.return_value = rep_slot
        actual_output = logical_replication.sync_tables(self.conn_info,
                                                        self.logical_streams,
                                                        state, end_lsn, state_file)

        self.assertDictEqual(state, actual_output)
        mocked_start_replication.assert_called_with(
            slot_name=rep_slot,
            decode=True,
            start_lsn=state['bookmarks']['foo-bar']['lsn'],
            status_interval=10,
            options={
                'format-version': '2',
                'include-transaction': 'true',
                'include-timestamp': 'true',
                'include-types': 'false',
                'actions': 'insert,update,delete',
                'add-tables': 'schema_name_value.table_name_value'
            }
        )

    @patch('tap_yugabyte.sync_strategies.logical_replication.locate_replication_slot')
    @patch("psycopg2.connect")
    def test_sync_tables_raise_exception_if_error_in_message_read(self,
                                                                  mocked_connect,
                                                                  _):
        """Test sync_tables raises exception if error in the message_read"""
        state = {'bookmarks': {'foo-bar': {'foo': 'bar', 'lsn': 4}}}
        end_lsn = 8
        state_file = 5

        expected_message = 'FOO'
        mocked_connect.return_value.cursor.return_value.read_message.side_effect = Exception(expected_message)

        with self.assertRaises(Exception) as exp:
            logical_replication.sync_tables(self.conn_info, self.logical_streams, state, end_lsn, state_file)
        self.assertEqual(expected_message, str(exp.exception))

    @patch('tap_yugabyte.sync_strategies.logical_replication.locate_replication_slot')
    @patch("psycopg2.connect")
    def test_sync_tables_if_break_at_end_lsn_and_msg_data_start_greater_than_end_lsn(self,
                                                                                     mocked_connect,
                                                                                     mocked_locate_rep_slot):
        """Test sync_table if there is break_at_the_end_lsn and message  data_start greater than lsn"""
        end_lsn = 4
        msg_data_start = end_lsn + 1

        class test_message:
            data_start = msg_data_start

        state = {'bookmarks': {'foo-bar': {'foo': 'bar', 'version': 'foo', 'lsn': 15}}}
        self.conn_info['break_at_end_lsn'] = True
        state_file = 5

        mocked_connect.return_value.cursor.return_value.read_message.return_value = test_message()

        mocked_locate_rep_slot.return_value = 'mocked_value_for_replication_slot'
        expected_log_message = 'INFO:tap_yugabyte:Breaking - latest wal message ' \
                               f'{msg_data_start} is' \
                               f' past end_lsn {end_lsn}'
        with self.assertLogs() as captured_log:
            actual_output = logical_replication.sync_tables(self.conn_info, self.logical_streams, state, end_lsn,
                                                            state_file)
            self.assertIn(expected_log_message, captured_log.output)
            self.assertEqual(state, actual_output)

    @patch('tap_yugabyte.sync_strategies.logical_replication.locate_replication_slot')
    @patch('tap_yugabyte.sync_strategies.logical_replication.select')
    @patch("psycopg2.connect")
    def test_sync_tables_if_no_message_and_raised_interrupted_error(self,
                                                                    mocked_connect,
                                                                    mocked_select,
                                                                    mocked_locate_rep_slot):
        """Test sync_tables if there is no message and InterruptedError is raised"""
        end_lsn = 4

        state = {'bookmarks': {'foo-bar': {'foo': 'bar', 'version': 'foo', 'lsn': 15}}}

        state_file = 5

        mocked_select.side_effect = InterruptedError()
        mocked_connect.return_value.cursor.return_value.read_message.return_value = None
        mocked_locate_rep_slot.return_value = 'mocked_value_for_replication_slot'

        actual_output = logical_replication.sync_tables(self.conn_info, self.logical_streams, state, end_lsn,
                                                        state_file)
        self.assertEqual(state, actual_output)

    @patch('tap_yugabyte.sync_strategies.logical_replication.locate_replication_slot')
    @patch("psycopg2.connect")
    def test_sync_tables_if_msg_and_some_specific_cases_for_lsn(self,
                                                                mocked_connect,
                                                                mocked_locate_rep_slot):
        """Test sync_table if there is message and lsn_currently_processing is None
         and lsn_currently_processing is less than lsn_to_flush"""
        end_lsn = 7
        lsn_committed = 15

        class test_message:
            data_start = end_lsn + 1
            payload = '{}'

        state = {'bookmarks': {'foo-bar': {'foo': 'bar', 'version': 'foo', 'lsn': lsn_committed}}}
        self.conn_info['break_at_end_lsn'] = False
        state_file = 55

        mocked_connect.return_value.cursor.return_value.read_message.return_value = test_message()

        mocked_locate_rep_slot.return_value = 'mocked_value_for_replication_slot'
        actual_output = logical_replication.sync_tables(self.conn_info,
                                                        self.logical_streams,
                                                        state, end_lsn, state_file)

        self.assertEqual(state, actual_output)
        mocked_send_feedback = mocked_connect.return_value.cursor.return_value.send_feedback
        mocked_send_feedback.assert_called_with(write_lsn=test_message.data_start,
                                                flush_lsn=test_message.data_start,
                                                reply=True, force=True)


class TestLogicalReplicationFeedback(unittest.TestCase):
    """Verify that only target-acknowledged WAL positions are flushed."""

    def setUp(self):
        self.WalMessage = namedtuple('WalMessage', ['payload', 'data_start'])
        self.conn_info = {
            'host': 'foo',
            'dbname': 'foo_db',
            'user': 'foo_user',
            'password': 'foo_pass',
            'port': 12345,
            'tap_id': 'tap_id_value',
            'max_run_seconds': 10,
            'break_at_end_lsn': False,
            'logical_poll_total_seconds': 1,
        }
        self.logical_streams = [self._stream('foo-bar', 'foo_table')]

    @staticmethod
    def _stream(tap_stream_id, table_name):
        return {
            'tap_stream_id': tap_stream_id,
            'schema': {'properties': {}},
            'stream': tap_stream_id,
            'table_name': table_name,
            'metadata': [{'metadata': {'schema-name': 'public'}, 'breadcrumb': []}],
        }

    @staticmethod
    def _state(stream_lsns):
        return {
            'bookmarks': {
                stream_id: {'lsn': lsn, 'version': 'version'}
                for stream_id, lsn in stream_lsns.items()
            }
        }

    def _message(self, action, lsn, **payload):
        return self.WalMessage(payload=json.dumps({'action': action, **payload}), data_start=lsn)

    @staticmethod
    def _consume_message(streams, state, msg, *_, message_payload=None, **__):
        payload = message_payload if message_payload is not None else json.loads(msg.payload)
        if payload.get('action') in {'I', 'U', 'D'}:
            singer.write_bookmark(state, streams[0]['tap_stream_id'], 'lsn', msg.data_start)
        return state

    @staticmethod
    def _expected_feedback(*lsns):
        return [
            call(write_lsn=lsn, flush_lsn=lsn, reply=True, force=True)
            for lsn in lsns
        ]

    def test_minimum_acknowledged_lsn_uses_oldest_stream(self):
        streams = [self._stream('foo-bar', 'foo_table'), self._stream('foo-baz', 'baz_table')]
        state = self._state({'foo-bar': 125, 'foo-baz': 100})

        self.assertEqual(100, logical_replication._minimum_acknowledged_lsn(state, streams))

    def test_minimum_acknowledged_lsn_rejects_invalid_initial_state(self):
        cases = {
            'missing': self._state({}),
            'none': self._state({'foo-bar': None}),
            'string': self._state({'foo-bar': '100'}),
            'bool': self._state({'foo-bar': True}),
            'negative': self._state({'foo-bar': -1}),
        }

        for name, state in cases.items():
            with self.subTest(name=name), self.assertRaisesRegex(
                    ValueError, 'State does not contain a valid LSN'):
                logical_replication._minimum_acknowledged_lsn(state, self.logical_streams)

    def test_target_acknowledgement_does_not_regress(self):
        lower_state = mock_open(read_data=json.dumps(self._state({'foo-bar': 125})))

        with patch('builtins.open', lower_state):
            acknowledged_lsn = logical_replication._read_target_acknowledged_lsn(
                'state.json', self.logical_streams, previous_safe_lsn=150)

        self.assertEqual(150, acknowledged_lsn)

    def _run_sync(self, state, messages, state_reader, logical_streams=None,
                  wal_progress_content='wal_progress:tap_id_value:current'):
        termination = RuntimeError('unexpected replication termination')
        streams = self.logical_streams if logical_streams is None else logical_streams

        with patch('tap_yugabyte.sync_strategies.logical_replication.FEEDBACK_POLL_INTERVAL', 0), \
                patch('tap_yugabyte.sync_strategies.logical_replication.sync_common.send_schema_message'), \
                patch('tap_yugabyte.sync_strategies.logical_replication.singer.write_message') as mocked_write, \
                patch('tap_yugabyte.sync_strategies.logical_replication.consume_message',
                      side_effect=self._consume_message), \
                patch('tap_yugabyte.sync_strategies.logical_replication.locate_replication_slot',
                      return_value='replication_slot'), \
                patch('builtins.open', state_reader), \
                patch('psycopg2.connect') as mocked_connect:
            cursor = mocked_connect.return_value.cursor.return_value
            cursor.read_message.side_effect = [*messages, termination]

            error = None
            try:
                logical_replication.sync_tables(
                    self.conn_info,
                    streams,
                    state,
                    1000,
                    'state.json',
                    wal_progress_content=wal_progress_content,
                )
            except Exception as exc:  # The harness captures the intentional termination or regression failure.
                error = exc

        return list(cursor.send_feedback.call_args_list), error, termination, list(mocked_write.call_args_list)

    def test_consuming_wal_does_not_advance_feedback_past_target_state(self):
        state = self._state({'foo-bar': 100})
        state_reader = mock_open(read_data=json.dumps(state))
        messages = [self._message('I', 200), self._message('C', 300)]

        feedback, error, termination, _ = self._run_sync(state, messages, state_reader)

        self.assertIs(error, termination)
        self.assertEqual(self._expected_feedback(100), feedback)

    def test_invalid_utf8_payload_is_skipped(self):
        state = self._state({'foo-bar': 100})
        state_reader = mock_open(read_data=json.dumps(state))
        messages = [self.WalMessage(payload=b'\x80\x81abc', data_start=200)]

        feedback, error, termination, _ = self._run_sync(state, messages, state_reader)

        self.assertIs(error, termination)
        self.assertEqual(self._expected_feedback(100), feedback)

    def test_progress_message_advances_state_without_advancing_feedback(self):
        """The tap stops at its committed marker while feedback remains target-bounded."""
        state = self._state({'foo-bar': 100})
        self.conn_info['break_at_end_lsn'] = True
        state_reader = mock_open(read_data=json.dumps(state))
        messages = [
            self._message('B', 200),
            self._message(
                'M',
                210,
                transactional=True,
                prefix='pipelinewise',
                content='wal_progress:tap_id_value:current'
            ),
            self._message('C', 220),
        ]

        feedback, error, _, _ = self._run_sync(state, messages, state_reader)

        self.assertIsNone(error)
        self.assertEqual(220, state['bookmarks']['foo-bar']['lsn'])
        self.assertEqual(self._expected_feedback(100), feedback)

    def test_stale_progress_message_does_not_end_current_sync(self):
        """A marker from an earlier invocation cannot satisfy the current boundary."""
        state = self._state({'foo-bar': 100})
        self.conn_info['break_at_end_lsn'] = True
        state_reader = mock_open(read_data=json.dumps(state))
        messages = [
            self._message('B', 200),
            self._message(
                'M',
                210,
                transactional=True,
                prefix='pipelinewise',
                content='wal_progress:tap_id_value:stale',
            ),
            self._message('C', 220),
            self._message('B', 300),
            self._message(
                'M',
                310,
                transactional=True,
                prefix='pipelinewise',
                content='wal_progress:tap_id_value:current',
            ),
            self._message('C', 320),
        ]

        feedback, error, _, _ = self._run_sync(state, messages, state_reader)

        self.assertIsNone(error)
        self.assertEqual(320, state['bookmarks']['foo-bar']['lsn'])
        self.assertEqual(self._expected_feedback(100), feedback)

    def test_progress_message_does_not_end_continuous_sync(self):
        """A continuous tap publishes and flushes a target-acknowledged marker."""
        state = self._state({'foo-bar': 100})
        old_state = mock_open(read_data=json.dumps(state))
        acknowledged_marker = mock_open(read_data=json.dumps(self._state({'foo-bar': 220})))
        state_reader = Mock(side_effect=[
            old_state.return_value,
            old_state.return_value,
            acknowledged_marker.return_value,
        ])
        messages = [
            self._message('B', 200),
            self._message(
                'M',
                210,
                transactional=True,
                prefix='pipelinewise',
                content='wal_progress:tap_id_value:current'
            ),
            self._message('C', 220),
        ]

        feedback, error, termination, written_messages = self._run_sync(state, messages, state_reader)

        self.assertIs(error, termination)
        self.assertEqual(220, state['bookmarks']['foo-bar']['lsn'])
        self.assertEqual(self._expected_feedback(100, 220), feedback)
        self.assertEqual(2, len(written_messages))
        self.assertEqual(220, written_messages[0].args[0].value['bookmarks']['foo-bar']['lsn'])

    def test_only_own_committed_progress_message_creates_boundary(self):
        """Incomplete, foreign, and non-transactional messages are not safe boundaries."""
        cases = {
            'interrupted_before_commit': ([
                self._message('B', 200),
                self._message(
                    'M',
                    210,
                    transactional=True,
                    prefix='pipelinewise',
                    content='wal_progress:tap_id_value:current',
                ),
            ], 200),
            'foreign_tap': ([
                self._message('B', 200),
                self._message(
                    'M',
                    210,
                    transactional=True,
                    prefix='pipelinewise',
                    content='wal_progress:another_tap:current',
                ),
                self._message('C', 220),
            ], 210),
            'non_transactional': ([
                self._message('B', 200),
                self._message(
                    'M',
                    210,
                    transactional=False,
                    prefix='pipelinewise',
                    content='wal_progress:tap_id_value:current',
                ),
                self._message('C', 220),
            ], 210),
        }

        self.conn_info['break_at_end_lsn'] = True
        for name, (messages, expected_lsn) in cases.items():
            with self.subTest(name=name):
                state = self._state({'foo-bar': 100})
                state_reader = mock_open(read_data=json.dumps(state))

                feedback, error, termination, _ = self._run_sync(state, messages, state_reader)

                self.assertIs(error, termination)
                self.assertEqual(expected_lsn, state['bookmarks']['foo-bar']['lsn'])
                self.assertEqual(self._expected_feedback(100), feedback)

    def test_periodic_bookmark_does_not_acknowledge_processed_lsn(self):
        state = self._state({'foo-bar': 100})
        state_reader = mock_open(read_data=json.dumps(state))
        messages = [
            self._message('I', 200),
            self._message('C', 300),
            self._message('C', 400),
        ]

        with patch(
                'tap_yugabyte.sync_strategies.logical_replication.UPDATE_BOOKMARK_PERIOD',
                1):
            feedback, error, termination, _ = self._run_sync(
                state, messages, state_reader)

        self.assertIs(error, termination)
        self.assertEqual(self._expected_feedback(100), feedback)

    def test_feedback_advances_exactly_with_target_state(self):
        state = self._state({'foo-bar': 100})
        first_state = mock_open(read_data=json.dumps(self._state({'foo-bar': 125})))
        second_state = mock_open(read_data=json.dumps(self._state({'foo-bar': 150})))
        state_reader = Mock(side_effect=[first_state.return_value, second_state.return_value])
        messages = [self._message('I', 200), self._message('C', 300)]

        feedback, error, termination, _ = self._run_sync(state, messages, state_reader)

        self.assertIs(error, termination)
        self.assertEqual(self._expected_feedback(100, 125, 150), feedback)

    def test_feedback_remains_monotonic_when_target_state_regresses(self):
        state = self._state({'foo-bar': 100})
        acknowledged_states = [
            mock_open(read_data=json.dumps(self._state({'foo-bar': lsn}))).return_value
            for lsn in (150, 125, 175)
        ]
        state_reader = Mock(side_effect=acknowledged_states)
        messages = [self._message('I', 200), self._message('C', 300), self._message('C', 400)]

        feedback, error, termination, _ = self._run_sync(state, messages, state_reader)

        self.assertIs(error, termination)
        self.assertEqual(self._expected_feedback(100, 150, 175), feedback)

    def test_feedback_uses_minimum_target_state_lsn_for_multiple_streams(self):
        streams = [self._stream('foo-bar', 'foo_table'), self._stream('foo-baz', 'baz_table')]
        state = self._state({'foo-bar': 125, 'foo-baz': 100})
        target_state = self._state({'foo-bar': 175, 'foo-baz': 150})
        state_reader = mock_open(read_data=json.dumps(target_state))

        feedback, error, termination, _ = self._run_sync(
            state,
            [self._message('I', 200)],
            state_reader,
            logical_streams=streams,
        )

        self.assertIs(error, termination)
        self.assertEqual(self._expected_feedback(100, 150), feedback)

    def test_unavailable_or_invalid_state_retains_previous_safe_lsn(self):
        invalid_state = self._state({'foo-bar': 'not-an-lsn'})
        cases = {
            'absent': Mock(side_effect=FileNotFoundError('state file is absent')),
            'unreadable': Mock(side_effect=PermissionError('state file is unreadable')),
            'truncated': mock_open(read_data='{"bookmarks":'),
            'invalid_json': mock_open(read_data='not-json'),
            'invalid_state': mock_open(read_data=json.dumps(invalid_state)),
        }

        for name, state_reader in cases.items():
            with self.subTest(name=name):
                state = self._state({'foo-bar': 100})
                messages = [self._message('I', 200), self._message('C', 300)]

                feedback, error, termination, _ = self._run_sync(state, messages, state_reader)

                self.assertIs(error, termination)
                self.assertEqual(self._expected_feedback(100), feedback)

    def test_feedback_recovers_after_state_file_is_temporarily_unreadable(self):
        state = self._state({'foo-bar': 100})
        recovered_state = mock_open(read_data=json.dumps(self._state({'foo-bar': 150})))
        state_reader = Mock(side_effect=[
            PermissionError('state file is unreadable'),
            PermissionError('state file is unreadable'),
            recovered_state.return_value,
        ])
        messages = [self._message('I', 200), self._message('C', 300), self._message('C', 400)]

        feedback, error, termination, _ = self._run_sync(state, messages, state_reader)

        self.assertIs(error, termination)
        self.assertEqual(self._expected_feedback(100, 150), feedback)

    def test_read_exception_does_not_acknowledge_consumed_lsn(self):
        state = self._state({'foo-bar': 100})
        state_reader = mock_open(read_data=json.dumps(state))

        feedback, error, termination, _ = self._run_sync(
            state,
            [self._message('I', 200)],
            state_reader,
        )

        self.assertIs(error, termination)
        self.assertEqual(self._expected_feedback(100), feedback)

    def test_feedback_uses_minimum_target_state_lsn_for_multiple_streams_again(self):
        streams = [self._stream('foo-bar', 'foo_table'), self._stream('foo-baz', 'baz_table')]
        state = self._state({'foo-bar': 125, 'foo-baz': 100})
        target_state = self._state({'foo-bar': 175, 'foo-baz': 150})
        state_reader = mock_open(read_data=json.dumps(target_state))

        feedback, error, termination, _ = self._run_sync(
            state,
            [self._message('I', 200)],
            state_reader,
            logical_streams=streams,
        )

        self.assertIs(error, termination)
        self.assertEqual(self._expected_feedback(100, 150), feedback)
