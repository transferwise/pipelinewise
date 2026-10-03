import json
import unittest
import decimal

import singer

from collections import namedtuple
from datetime import datetime, date, timezone
from unittest.mock import Mock, patch
from dateutil.tz import tzoffset

from tap_postgres.sync_strategies import common, logical_replication


class TestLogicalReplication(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.WalMessage = namedtuple('WalMessage', ['payload', 'data_start'])
        self.conn_info = {'host': 'foo',
                          'dbname': 'foo_db',
                          'user': 'foo_user',
                          'password': 'foo_pass',
                          'port': 12345,
                          'use_secondary': False,
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

    def test_generate_replication_slot_name(self):
        self.assertEqual(
            logical_replication.generate_replication_slot_name('some_tap'),
            'ppw_slot_some_tap',
        )
        self.assertEqual(
            logical_replication.generate_replication_slot_name('some_tap', prefix='custom'),
            'custom_some_tap',
        )
        self.assertEqual(
            logical_replication.generate_publication_name('some_tap'),
            'ppw_slot_some_tap',
        )

        for tap_id in ('SomeTap', 'some-tap', '', 'a' * 51):
            with self.subTest(tap_id=tap_id), self.assertRaisesRegex(
                    ValueError, r'tap_id must match \^\[a-z0-9_\]\+\$'):
                logical_replication.generate_replication_slot_name(tap_id)

    def test_consume_with_message_payload_is_not_json_expect_same_state(self):
        output = logical_replication.consume_message([],
                                                     {},
                                                     self.WalMessage(payload='this is an invalid json message',
                                                                     data_start=None),
                                                     None,
                                                     {}
                                                     )
        self.assertDictEqual({}, output)

    @patch('tap_postgres.sync_strategies.logical_replication.singer.write_message')
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

    @patch('tap_postgres.logical_replication.singer.write_message')
    @patch('tap_postgres.logical_replication.sync_common.send_schema_message')
    @patch('tap_postgres.logical_replication.refresh_streams_schema')
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
                        "xmin": None
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
            {'use_secondary': True}
        )

        self.assertDictEqual(return_v,
                             {
                                 'bookmarks': {
                                     "myschema-mytable": {
                                         "last_replication_method": "LOG_BASED",
                                         "lsn": "some lsn",
                                         "version": 1000,
                                         "xmin": None
                                     }
                                 }
                             })

        refresh_schema_mock.assert_called_once_with({'use_secondary': False}, [streams[0]])
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

    def test_row_to_singer_message_with_timestamp_types(self):
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

    @patch('tap_postgres.sync_strategies.logical_replication.post_db.capture_snapshot_boundary')
    def test_fetch_current_lsn(self, capture_snapshot_boundary):
        """Fetch a committed record boundary that an idle replica can replay."""
        capture_snapshot_boundary.return_value = 4294967298
        self.assertEqual(4294967298, logical_replication.fetch_current_lsn(self.conn_info))
        capture_snapshot_boundary.assert_called_once_with(self.conn_info)

    def test_start_replication_sets_wal_sender_timeout_from_postgres_14(self):
        """Every supported PostgreSQL version receives the sender timeout."""
        for version, sets_timeout in ((140000, True), (180000, True)):
            with self.subTest(version=version):
                cursor = Mock()

                logical_replication._start_replication(
                    cursor,
                    'ppw_slot_tap',
                    'ppw_slot_tap',
                    42,
                    version,
                )

                if sets_timeout:
                    cursor.execute.assert_called_once_with(
                        'SET SESSION wal_sender_timeout = 10800000'
                    )
                else:
                    cursor.execute.assert_not_called()
                cursor.start_replication.assert_called_once_with(
                    slot_name='ppw_slot_tap',
                    decode=False,
                    start_lsn=42,
                    status_interval=10,
                    options={
                        'proto_version': '1',
                        'publication_names': 'ppw_slot_tap',
                        'messages': 'true',
                    },
                )

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
            self.assertEqual(['DEBUG:tap_postgres:debug_lsn is OFF'], foo_log.output)

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
            self.assertEqual(['DEBUG:tap_postgres:debug_lsn is ON'], foo_log.output)

    def test_lsn_to_int_return_none_if_lsn_is_none(self):
        """Test lsn_to_int if lsn is None"""
        lsn = None
        actual_output = logical_replication.lsn_to_int(lsn)
        self.assertIsNone(actual_output)

    def test_int_to_lsn(self):
        """Test if int_to_lsn returns expected values"""
        values_to_test = [(None, None),
                          (0, '0/0'),
                          (1, '0/1'),
                          (12, '0/C'),  # length < 32
                          (2 ** 123, '80000000000000000000000/0')  # Length > 32
                          ]
        for lsni, expected_output in values_to_test:
            actual_output = logical_replication.int_to_lsn(lsni)
            self.assertEqual(expected_output, actual_output)

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
        mocked_connect.return_value.server_version = 140018
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
        mocked_connect.return_value.server_version = 140018
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
        mocked_connect.return_value.server_version = 140018
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

    def test_row_to_singer_message(self):
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

    @patch('tap_postgres.sync_strategies.logical_replication.locate_replication_slot_by_cur')
    @patch('tap_postgres.sync_strategies.logical_replication.post_db.open_connection')
    def test_locate_replication_slot(self, mocked_open_connection, mocked_locate):
        connection = mocked_open_connection.return_value.__enter__.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        mocked_locate.return_value = 'ppw_slot_tap_id_value'

        self.assertEqual(
            'ppw_slot_tap_id_value',
            logical_replication.locate_replication_slot(self.conn_info),
        )
        mocked_locate.assert_called_once_with(
            cursor,
            'foo_db',
            'tap_id_value',
            allow_wal2json_migration=True,
            previous_tap_id=None,
        )

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
        mocked_connect.return_value.server_version = 140018
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

    @patch('tap_postgres.sync_strategies.logical_replication.refresh_streams_schema')
    @patch('tap_postgres.sync_strategies.logical_replication.sync_common.send_schema_message')
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

    @patch('tap_postgres.sync_strategies.logical_replication.refresh_streams_schema')
    @patch('tap_postgres.sync_strategies.logical_replication.sync_common.send_schema_message')
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
