import unittest
import bson
import pytz

from datetime import datetime
from unittest.mock import patch, Mock, PropertyMock, MagicMock
from pymongo.change_stream import CollectionChangeStream, ChangeStream
from pymongo.collection import Collection
from pymongo.database import Database
from singer import RecordMessage

import tap_mongodb.sync_strategies.change_streams as change_streams
from tap_mongodb.sync_strategies import common


class TestChangeStreams(unittest.TestCase):

    maxDiff = None

    def tearDown(self) -> None:
        common.SCHEMA_COUNT.clear()
        common.SCHEMA_TIMES.clear()

    def test_update_bookmarks(self):
        state = {
            'bookmarks': {
                'stream1': {},
                'stream2': {},
                'stream3': {},
            }
        }

        token = {'data': 'this is a token'}

        new_state = change_streams.update_bookmarks(state, {'stream1', 'stream2'}, token)

        self.assertEqual({
            'bookmarks': {
                'stream1': {'token': token},
                'stream2': {'token': token},
                'stream3': {}
            }
        }, new_state)

    def test_get_buffer_rows_from_db(self):
        result = ['a', 'b', 'c']

        mock_enter = Mock()
        mock_enter.return_value = result

        mock_find = Mock().return_value
        mock_find.__enter__ = mock_enter
        mock_find.__exit__ = Mock()

        mock_coll = Mock(spec_set=Collection).return_value
        mock_coll.find.return_value = mock_find

        self.assertListEqual(
            result,
            list(change_streams.get_buffer_rows_from_db(mock_coll, {1, 2, 3})))

        mock_enter.assert_called_once()

    def test_token_from_state_with_no_tokens_in_state_expect_none(self):
        state = {
            'bookmarks': {
                'mydb-stream1': {
                },
                'mydb-stream2': {
                },
                'mydb-stream3': {
                },
            }
        }

        self.assertIsNone(change_streams.get_token_from_state({'mydb-stream1','mydb-stream2','mydb-stream3'}, state))

    def test_token_from_state_with_streams_to_sync_expect_None(self):
        state = {
            'bookmarks': {
                'mydb-stream1': {
                    'token':
                        {'_data': '825EBCF4CF000000972B022C0100296E5A1004A50DD58E7B964E14B0FC769A85D61D5646645F696400645EBCF4CF5A06C441DC02E1080005'}
                },
                'mydb-stream2': {
                    'token':
                        {'_data': '825EBCF4CF000000972B022C0100296E5A1004A50DD58E7B964E14B0FC769A85D61D5646645F696400645EBCF4CF5A06C441DC02E1090004'}
                },
                'mydb-stream3': {
                    'token':
                        {'_data': '825EBCF4CF000000972B022C0100296E5A1004A50DD58E7B964E14B0FC769A85D61D5646645F696400645EBCF4CF5A06C441DC02E10A0006'}
                },
            }
        }

        self.assertIsNone(change_streams.get_token_from_state(set([]), state))

    def test_token_from_state_with_state_and_streams_to_sync(self):
        state = {
            'bookmarks': {
                'mydb-stream1': {
                    'token':
                        {
                            '_data': '825EBCF4CF000000972B022C0100296E5A1004A50DD58E7B964E14B0FC769A85D61D5646645F696400645EBCF4CF5A06C441DC02E1080005'}
                },
                'mydb-stream2': {
                    'token':
                        {
                            '_data': '825EBCF4CF000000972B022C0100296E5A1004A50DD58E7B964E14B0FC769A85D61D5646645F696400645EBCF4CF5A06C441DC02E1080004'}
                },
                'mydb-stream3': {
                    'token':
                        {
                            '_data': '825EBCF4CF000000972B022C0100296E5A1004A50DD58E7B964E14B0FC769A85D61D5646645F696400645EBCF4CF5A06C441DC02E1080006'}
                },
                'mydb-stream4': {
                    'token':
                        {
                            '_data': '825EBCF4CF000000972B022C0100296E5A1004A50DD58E7B964E14B0FC769A85D61D5646645F696400645EBCF4CF5A06C441DC02E1080001'}
                },
                'mydb-stream5': {
                    'token':None
                },
            }
        }

        self.assertEqual({'_data': '825EBCF4CF000000972B022C0100296E5A1004A50DD58E7B964E14B0FC769A85D61D5646645F696400645EBCF4CF5A06C441DC02E1080004'},
        change_streams.get_token_from_state({'mydb-stream1','mydb-stream2','mydb-stream3','mydb-stream5'}, state))

    @patch('tap_mongodb.sync_strategies.change_streams.singer.write_message')
    @patch('tap_mongodb.sync_strategies.change_streams.get_buffer_rows_from_db')
    def test_sync_database(self, get_buffer_rows_from_db_mock, write_message_mock):
        common.SCHEMA_COUNT['mydb-stream1'] = 0
        common.SCHEMA_COUNT['mydb-stream2'] = 0
        common.SCHEMA_COUNT['mydb-stream3'] = 0

        common.SCHEMA_TIMES['mydb-stream1'] = 0
        common.SCHEMA_TIMES['mydb-stream2'] = 0
        common.SCHEMA_TIMES['mydb-stream3'] = 0

        common.COUNTS['mydb-stream1'] = 0
        common.COUNTS['mydb-stream2'] = 0
        common.COUNTS['mydb-stream3'] = 0

        common.TIMES['mydb-stream1'] = 0
        common.TIMES['mydb-stream2'] = 0
        common.TIMES['mydb-stream3'] = 0

        messages = []

        get_buffer_rows_from_db_mock.return_value = [
            {
                '_id': 'id13',
                'key2': 'eeeeef',
            }
        ]
        write_message_mock.side_effect = messages.append

        state = {
            'bookmarks': {
                'mydb-stream1': {
                    'token':
                        {'_data': '825EBCF4CF000000972B022C0100296E5A1004A50DD58E7B964E14B0FC769A85D61D5646645F696400645EBCF4CF5A06C441DC02E1080005'}
                },
                'mydb-stream2': {
                    'token':
                        {'_data': '825EBCF4CF000000972B022C0100296E5A1004A50DD58E7B964E14B0FC769A85D61D5646645F696400645EBCF4CF5A06C441DC02E1080004'}
                },
                'mydb-stream3': {
                    'token':
                        {'_data': '825EBCF4CF000000972B022C0100296E5A1004A50DD58E7B964E14B0FC769A85D61D5646645F696400645EBCF4CF5A06C441DC02E1080006'}
                },
                'mydb-stream4': {
                    'token':
                        {'_data': '825EBCF4CF000000972B022C0100296E5A1004A50DD58E7B964E14B0FC769A85D61D5646645F696400645EBCF4CF5A06C441DC02E1080001'}
                },
                'mydb-stream5': {
                    'token': None
                },
            }
        }

        streams = {
            'mydb-stream1': {
                'tap_stream_id': 'mydb-stream1',
                'table_name': 'stream1',
                'stream': 'stream1',
                'metadata': [
                    {
                        'breadcrumb': [],
                        'metadata': {
                            'database-name': 'mydb'
                        }
                    }
                ]
            },
            'mydb-stream2': {
                'tap_stream_id': 'mydb-stream2',
                'table_name': 'stream2',
                'stream': 'stream2',
                'metadata': [
                    {
                        'breadcrumb': [],
                        'metadata': {
                            'database-name': 'mydb'
                        }
                    }
                ]
            }, 'mydb-stream3': {
                'tap_stream_id': 'mydb-stream3',
                'table_name': 'stream3',
                'stream': 'stream3',
                'metadata': [
                    {
                        'breadcrumb': [],
                        'metadata': {
                            'database-name': 'mydb'
                        }
                    }
                ]
            }
        }

        changes = [
            Mock(spec_set=ChangeStream, return_value={
                'operationType': 'insert',
                'ns': {'db': 'mydb', 'coll': 'stream1'},
                'fullDocument': {
                    '_id': 'id11',
                    'key1': 1,
                    'key2': 'abc',
                    'key3': {'a': 1, 'b': datetime(2020, 4, 10, 14, 50, 55, 0, tzinfo=pytz.utc)}
                }
            }).return_value,
            Mock(spec_set=ChangeStream, return_value={
                'operationType': 'update',
                'ns': {'db': 'mydb', 'coll': 'stream1'},
                'documentKey': {
                    '_id': 'id12'
                }

            }).return_value,
            Mock(spec_set=ChangeStream, return_value={
                'operationType': 'insert',
                'ns': {'db': 'mydb', 'coll': 'stream2'},
                'fullDocument': {
                    '_id': 'id21',
                    'key6': 12,
                    'key10': 'abc',
                    'key11': [1,2,3, bson.int64.Int64(10)]
                }
            }).return_value,
            Mock(spec_set=ChangeStream, return_value={
                'operationType': 'delete',
                'ns': {'db': 'mydb', 'coll': 'stream2'},
                'documentKey': {
                    '_id': 'id22'
                },
                'clusterTime': bson.timestamp.Timestamp(1588636800, 0)  # datetime.datetime(2020, 5, 5, 3, 0, 0, 0)
            }).return_value,
            Mock(spec_set=ChangeStream, return_value={
                'operationType': 'insert',
                'ns': {'db': 'mydb', 'coll': 'stream1'},
                'fullDocument': {
                    '_id': 'id13',
                    'key3': bson.timestamp.Timestamp(1588636800, 0),
                }
            }).return_value,
            None
        ]

        cursor_mock = Mock(spec_set=CollectionChangeStream).return_value
        type(cursor_mock).alive = PropertyMock(return_value=True)
        type(cursor_mock).resume_token = PropertyMock(side_effect=[
            {
                '_data': 'token1',
                'some_extra_property': b'\x81'
            },
            {
                '_data': 'token2',
                'some_extra_property': b'\x81'
            },

            {
                '_data': 'token3',
                'some_extra_property': b'\x81'
            },
            {
                '_data': 'token4',
                'some_extra_property': b'\x81'
            },

            {
                '_data': 'token5',
                'some_extra_property': b'\x81'
            },
            {
                '_data': 'token6',
                'some_extra_property': b'\x81'
            }
        ])

        cursor_mock.try_next.side_effect = changes

        mock_enter = Mock()
        mock_enter.return_value = cursor_mock

        mock_watch = Mock().return_value
        mock_watch.__enter__ = mock_enter
        mock_watch.__exit__ = Mock()

        mock_db = MagicMock(spec_set=Database).return_value
        mock_db.watch.return_value = mock_watch
        type(mock_db).name = PropertyMock(return_value='mydb')

        change_streams.sync_database(mock_db, streams, state, 1, 1)

        self.assertEqual({
            'bookmarks': {
                'mydb-stream1': {
                    'token': {
                        '_data': 'token6'
                    },
                },
                'mydb-stream2': {
                    'token': {
                        '_data': 'token6'
                    },
                },
                'mydb-stream3': {
                    'token': {
                        '_data': 'token6'
                    },
                },
                'mydb-stream4': {
                    'token':
                        {'_data': '825EBCF4CF000000972B022C0100296E5A1004A50DD58E7B964E14B0FC769A85D61D5646645F696400645EBCF4CF5A06C441DC02E1080001'}
                },
                'mydb-stream5': {
                    'token': None
                },
            }
        }, state)

        self.assertListEqual([
            'RecordMessage',  # insert
            'RecordMessage',  # update
            'RecordMessage',  # insert
            'RecordMessage',  # delete
            'RecordMessage',  # insert
            'StateMessage',
        ], [msg.__class__.__name__ for msg in messages])

        self.assertListEqual([
            {'_id': 'id11', 'document': {'_id': 'id11', 'key1': 1, 'key2': 'abc','key3': {'a': 1, 'b': '2020-04-10T14:50:55.000000Z'}}, common.SDC_DELETED_AT: None},
            {'_id': 'id13', 'document': {'_id': 'id13', 'key2': 'eeeeef'}, common.SDC_DELETED_AT: None},
            {'_id': 'id21', 'document':{'_id': 'id21', 'key6': 12, 'key10': 'abc','key11': [1,2,3, '10']}, common.SDC_DELETED_AT: None},
            {'_id': 'id22', 'document': {'_id': 'id22'}, common.SDC_DELETED_AT: '2020-05-05T00:00:00.000000Z'},
            {'_id': 'id13', 'document': {'_id': 'id13', 'key3': '2020-05-05T00:00:00.000000Z'}, common.SDC_DELETED_AT: None},
        ], [msg.record for msg in messages if isinstance(msg, RecordMessage)])

        self.assertEqual(common.COUNTS['mydb-stream1'], 3)
        self.assertEqual(common.COUNTS['mydb-stream2'], 2)
        self.assertEqual(common.COUNTS['mydb-stream3'], 0)

    @patch('tap_mongodb.sync_strategies.change_streams.singer.write_message')
    @patch('tap_mongodb.sync_strategies.change_streams.get_buffer_rows_from_db')
    def test_flush_buffer_with_3_rows_returns_3(self, get_rows_mock, write_message_mock):
        common.SCHEMA_COUNT['mydb-stream1'] = 0
        common.SCHEMA_COUNT['mydb-stream2'] = 0
        common.SCHEMA_COUNT['mydb-stream3'] = 0

        common.SCHEMA_TIMES['mydb-stream1'] = 0
        common.SCHEMA_TIMES['mydb-stream2'] = 0
        common.SCHEMA_TIMES['mydb-stream3'] = 0

        get_rows_mock.return_value = [
            {'_id': '1', 'key1': 1},
            {'_id': '2', 'key2': ['a', 'b']},
            {'_id': '3', 'key3': bson.timestamp.Timestamp(1588636800, 0)},
        ]

        buffer = {
            'mydb-stream1': {'1', '2', '3', '4'},
            'mydb-stream2': None,
            'mydb-stream3': {},
        }

        streams = {
            'mydb-stream1': {
                'table_name': 'stream1',
                'tap_stream_id': 'mydb-stream1',
                'stream': 'stream1',
                'metadata': [
                    {
                        'breadcrumb': [],
                        'metadata': {
                            'database-name': 'mydb'
                        }
                    }
                ]
            },
            'mydb-stream2': {
                'table_name': 'stream2',
                'tap_stream_id': 'mydb-stream2',
                'stream': 'stream1',
                'metadata': [
                    {
                        'breadcrumb': [],
                        'metadata': {
                            'database-name': 'mydb'
                        }
                    }
                ]
            },
            'mydb-stream3': {
                'table_name': 'stream3',
                'tap_stream_id': 'mydb-stream3',
                'stream': 'stream1',
                'metadata': [
                    {
                        'breadcrumb': [],
                        'metadata': {
                            'database-name': 'mydb'
                        }
                    }
                ]
            }
        }
        messages = []
        rows_saved = {
            'mydb-stream1': 0,
            'mydb-stream2': 0,
            'mydb-stream3': 0,
        }

        write_message_mock.side_effect = messages.append

        change_streams.flush_buffer(buffer, streams,
                                    Mock(spec_set=Database, return_value={
                                        'stream1': Mock(),
                                        'stream2': Mock(),
                                        'stream3': Mock(),
                                    }).return_value,
                                    rows_saved)

        self.assertEqual(3, rows_saved['mydb-stream1'])
        self.assertEqual(0, rows_saved['mydb-stream2'])
        self.assertEqual(0, rows_saved['mydb-stream3'])

        self.assertListEqual([
            'RecordMessage',
            'RecordMessage',
            'RecordMessage',
        ], [m.__class__.__name__ for m in messages])

        self.assertFalse(buffer['mydb-stream1'])
        self.assertFalse(buffer['mydb-stream2'])
        self.assertFalse(buffer['mydb-stream3'])
