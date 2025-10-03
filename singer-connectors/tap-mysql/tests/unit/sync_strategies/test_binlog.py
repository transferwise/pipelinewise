import datetime
import socket

import pytz
import os

from collections import namedtuple
from typing import Dict
from unittest import TestCase
from unittest.mock import patch, Mock, call, MagicMock

from pymysql import InternalError
from pymysql.cursors import Cursor
from pymysqlreplication.constants import FIELD_TYPE
from pymysqlreplication.event import RotateEvent, MariadbGtidEvent, GtidEvent
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from singer import CatalogEntry, Schema, Catalog, RecordMessage, StateMessage, SchemaMessage

from tap_mysql import connection
from tap_mysql.connection import MySQLConnection
from tap_mysql.sync_strategies import binlog

Column = namedtuple('Column', ['name', 'type'])


def get_binlogevent(class_name, attrs: Dict):
    mock = Mock(spec=class_name)

    for att, val in attrs.items():
        setattr(mock, att, val)

    return mock


class TestBinlogSyncStrategy(TestCase):

    def setUp(self) -> None:
        self.maxDiff = None

    def tearDown(self) -> None:
        pass

    def test_add_automatic_properties(self):
        catalog = CatalogEntry(
            tap_stream_id='db-stream',
            schema=Schema(
                properties={
                    'x': Schema(type='int')
                }
            )
        )

        columns = ['x']

        binlog.add_automatic_properties(catalog, columns)

        self.assertEqual(2, len(catalog.schema.properties))
        self.assertEqual(catalog.schema.properties['x'].type, 'int')

        self.assertEqual(catalog.schema.properties[binlog.SDC_DELETED_AT].type, ['null', 'string'])
        self.assertEqual(catalog.schema.properties[binlog.SDC_DELETED_AT].format, 'date-time')

        self.assertListEqual(['x', binlog.SDC_DELETED_AT], columns)

    @patch('tap_mysql.sync_strategies.binlog.calculate_bookmark',
           return_value=('binlog0001', 50))
    @patch('tap_mysql.sync_strategies.binlog.fetch_current_log_file_and_pos',
           return_value=('binlog0003', 1000))
    @patch('tap_mysql.sync_strategies.binlog.utils.now', return_value=datetime.datetime(2020, 10, 13, 8, 29, 58,
                                                                                        tzinfo=pytz.UTC))
    @patch('tap_mysql.sync_strategies.binlog.discover_catalog')
    @patch('tap_mysql.sync_strategies.binlog.make_connection_wrapper')
    def test_sync_binlog_stream_with_log_file_and_pos(self,
                                                      make_connection_wrapper_mock,
                                                      discover_catalog_mock,
                                                      *args):

        # we're dealing with local datetimes, so tests passing depend on the local timezone
        # pin the TZ to EET to avoid flakiness
        os.environ['TZ'] = 'EET'

        config = {
            'server_id': '123',
            'use_gtid': False,
            'engine': connection.MYSQL_ENGINE,
        }
        mysql_con = Mock(spec_set=MySQLConnection)

        catalog = {
            'my_db-stream1': {
                'catalog_entry': CatalogEntry(
                    table='stream1',
                    stream='my_db-stream1',
                    tap_stream_id='my_db-stream1',
                    schema=Schema(
                        properties={
                            'c_int': Schema(inclusion='available', type=['null', 'integer']),
                            'c_varchar': Schema(inclusion='available', type=['null', 'string']),
                            'c_timestamp': Schema(inclusion='available', type=['null', 'string'], format='date-time'),
                        }
                    ),
                    metadata=[
                        {
                            'breadcrumb': [],
                            'metadata': {
                                'selected-by-default': False,
                                'database-name': 'my_db',
                                'row-count': 1,
                                'replication-method': 'LOG_BASED',
                                'selected': True,
                                'is-view': False,
                                'table-key-properties': ['c_int']
                            }
                        },
                        {
                            'breadcrumb': ['properties', 'c_int'],
                            'metadata': {
                                'selected-by-default': True,
                                'sql-datatype': 'int(11)',
                                'datatype': 'int'
                            }
                        },
                        {
                            'breadcrumb': ['properties', 'c_varchar'],
                            'metadata': {
                                'selected-by-default': True,
                                'sql-datatype': 'varchar(100)',
                                'datatype': 'varchar'
                            }
                        },
                        {
                            'breadcrumb': ['properties', 'c_blob'],
                            'metadata': {
                                'selected-by-default': False,
                                'sql-datatype': 'blob',
                                'datatype': 'blob'
                            }
                        },
                        {
                            'breadcrumb': ['properties', 'c_timestamp'],
                            'metadata': {
                                'selected-by-default': True,
                                'sql-datatype': 'timestamp',
                                'datatype': 'timestamp'
                            }
                        }
                    ]
                ),
                'desired_columns': {'c_int', 'c_varchar', 'c_timestamp'}
            },
            'my_db-stream2': {
                'catalog_entry': CatalogEntry(
                    table='stream2',
                    stream='my_db-stream2',
                    tap_stream_id='my_db-stream2',
                    schema=Schema(
                        properties={
                            'c_bool': Schema(inclusion='available', type=['null', 'bool']),
                            'c_double': Schema(inclusion='available', type=['null', 'number']),
                            'c_time': Schema(inclusion='available', type=['null', 'string'], format='time'),
                        }
                    ),
                    metadata=[
                        {
                            'breadcrumb': [],
                            'metadata': {
                                'selected-by-default': False,
                                'database-name': 'my_db',
                                'row-count': 1,
                                'replication-method': 'LOG_BASED',
                                'selected': True,
                                'is-view': False,
                                'table-key-properties': []
                            }
                        },
                        {
                            'breadcrumb': ['properties', 'c_bool'],
                            'metadata': {
                                'selected-by-default': True,
                                'sql-datatype': 'tinyint(1)'
                            }
                        },
                        {
                            'breadcrumb': ['properties', 'c_double'],
                            'metadata': {
                                'selected-by-default': True,
                                'sql-datatype': 'double'
                            }
                        },
                        {
                            'breadcrumb': ['properties', 'c_time'],
                            'metadata': {
                                'selected-by-default': True,
                                'sql-datatype': 'time'
                            }
                        }
                    ]
                ),
                'desired_columns': {'c_bool', 'c_double', 'c_time'}
            }
        }

        singer_messages = []

        state = {
            'bookmarks': {
                'my_db-stream1': {
                    'version': 1
                },
                'my_db-stream2': {
                    'version': 1
                }
            }
        }

        with patch('tap_mysql.sync_strategies.binlog.singer.write_message') as write_msg:
            write_msg.side_effect = lambda msg: singer_messages.append(msg)

            with patch('tap_mysql.sync_strategies.binlog.BinLogStreamReader',
                       autospec=True) as reader_mock:
                def iter_mock(_):
                    log_files = [
                        'binlog0001',
                        'binlog0001',
                        'binlog0002',
                        'binlog0002',
                        'binlog0002',
                        'binlog0003',
                        'binlog0003',
                        'binlog0003',
                        'binlog0003',
                        'binlog0003',
                        'binlog0003'
                    ]

                    log_positions = [
                        50,
                        300,
                        4,
                        100,
                        250,
                        7,
                        20,
                        140,
                        300,
                        470,
                        999,
                    ]

                    for idx, x in enumerate([
                        get_binlogevent(WriteRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream2',
                            'columns': [
                                Column('c_bool', FIELD_TYPE.TINY),
                                Column('c_time', FIELD_TYPE.TIME2),
                                Column('c_double', FIELD_TYPE.DOUBLE),
                            ],
                            'rows': [
                                {'values': {
                                    'c_bool': True,
                                    'c_time': datetime.time(20, 1, 14),
                                    'c_double': 19.44
                                }},
                                {'values': {
                                    'c_bool': False,
                                    'c_time': datetime.time(9, 10, 24),
                                    'c_double': 0.54
                                }},
                            ]
                        }),
                        get_binlogevent(UpdateRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream2',
                            'columns': [
                                Column('c_bool', FIELD_TYPE.TINY),
                                Column('c_time', FIELD_TYPE.TIME2),
                                Column('c_double', FIELD_TYPE.DOUBLE),
                            ],
                            'rows': [
                                {'after_values': {
                                    'c_bool': False,
                                    'c_time': datetime.time(8, 13, 12),
                                    'c_double': 100.22
                                }},
                                {'after_values': {
                                    'c_bool': False,
                                    'c_time': datetime.time(0, 10, 59, 44),
                                    'c_double': 0.54344
                                }},
                                {'after_values': {
                                    'c_bool': False,
                                    'c_time': datetime.time(0, 0, 0, 38),
                                    'c_double': 1.565667
                                }},
                            ]
                        }),
                        get_binlogevent(RotateEvent, {
                            'next_binlog': 'binlog0002',
                            'position': 4,
                        }),
                        get_binlogevent(UpdateRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream1',
                            'columns': [
                                Column('c_int', FIELD_TYPE.INT24),
                                Column('c_varchar', FIELD_TYPE.VARCHAR),
                                Column('c_timestamp', FIELD_TYPE.TIMESTAMP2),
                                Column('c_blob', FIELD_TYPE.BLOB),
                            ],
                            'rows': [
                                {'after_values': {
                                    'c_int': 1,
                                    'c_timestamp': datetime.datetime(2021, 3, 24, 12, 12, 56),
                                    'c_varchar': 'varchar 1',
                                    'c_blob': b'dfhdfhsdhf'
                                }},
                                {'after_values': {
                                    'c_int': 2,
                                    'c_timestamp': datetime.datetime(2019, 12, 24, 5, 1, 6),
                                    'c_varchar': 'varchar 2',
                                    'c_blob': b'dflldskjf'
                                }}
                            ]
                        }),
                        get_binlogevent(DeleteRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream3',
                            'columns': []
                        }),
                        get_binlogevent(RotateEvent, {
                            'next_binlog': 'binlog0003',
                            'position': 7,
                        }),
                        get_binlogevent(WriteRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream2',
                            'columns': [
                                Column('c_bool', FIELD_TYPE.TINY),
                                Column('c_double', FIELD_TYPE.DOUBLE),
                                Column('c_time', FIELD_TYPE.TIME2),
                            ],
                            'rows': [
                                {'values': {
                                    'c_bool': False,
                                    'c_time': datetime.time(0, 0, 0, 38),
                                    'c_double': 10000.234
                                }}
                            ]
                        }),
                        get_binlogevent(WriteRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream1',
                            'columns': [
                                Column('c_int', FIELD_TYPE.INT24),
                                Column('c_varchar', FIELD_TYPE.VARCHAR),
                                Column('__dropped_col_2__', FIELD_TYPE.TIMESTAMP2),
                                Column('c_blob', FIELD_TYPE.BLOB),
                            ],
                            'rows': [
                                {'values': {
                                    'c_int': 3,
                                    'c_varchar': 'varchar 3',
                                    'c_blob': b'dfhdfhsdhf'
                                }},
                                {'values': {
                                    'c_int': 4,
                                    'c_varchar': 'varchar 4',
                                    'c_blob': b'32fgdf243'
                                }}
                            ]
                        }),
                        get_binlogevent(DeleteRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream1',
                            'timestamp': datetime.datetime.timestamp(datetime.datetime(2021, 1, 1, 10, 20, 55,
                                                                                       tzinfo=pytz.UTC)),
                            'columns': [
                                Column('c_int', FIELD_TYPE.INT24),
                                Column('c_varchar', FIELD_TYPE.VARCHAR),
                                Column('c_datetime', FIELD_TYPE.TIMESTAMP2),
                                Column('c_blob', FIELD_TYPE.BLOB),
                            ],
                            'rows': [
                                {'values': {
                                    'c_int': 5,
                                    'c_datetime': datetime.datetime(2002, 8, 20, 8, 5, 9),
                                    'c_varchar': 'varchar 5',
                                    'c_blob': b'dfhdfhsdhf'
                                }},
                                {'values': {
                                    'c_int': 6,
                                    'c_datetime': datetime.datetime(2020, 1, 1, 0, 0, 0),
                                    'c_varchar': 'varchar 6',
                                    'c_blob': b'32fgdf243'
                                }},
                                {'values': {
                                    'c_int': 7,
                                    'c_datetime': datetime.datetime(2021, 1, 1, 3, 4, 0, 67483),
                                    'c_varchar': 'varchar 7',
                                    'c_blob': None
                                }}
                            ]
                        }),
                        get_binlogevent(WriteRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream2',
                            'columns': [
                                Column('c_bool', FIELD_TYPE.TINY),
                                Column('c_double', FIELD_TYPE.DOUBLE),
                                Column('c_time', FIELD_TYPE.TIME2),
                                Column('c_json', FIELD_TYPE.JSON),
                            ],
                            'rows': [
                                {'values': {
                                    'c_bool': False,
                                    'c_time': datetime.time(12, 30, 0, 354676),
                                    'c_double': 10000,
                                    'c_json': {'a': 1, 'b': 2}
                                }},
                                {'values': {
                                    'c_bool': True,
                                    'c_time': datetime.time(12, 30, 0, 0),
                                    'c_double': 10.40,
                                    'c_json': [{}, {}]
                                }},
                                {'values': {
                                    'c_bool': True,
                                    'c_time': None,
                                    'c_double': -457.10,
                                    'c_json': None
                                }}
                            ]
                        }),
                        get_binlogevent(DeleteRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream1',
                            'timestamp': datetime.datetime.timestamp(datetime.datetime(2021, 1, 1, 20, 0, 0,
                                                                                       tzinfo=pytz.UTC)),
                            'columns': [
                                Column('c_int', FIELD_TYPE.INT24),
                                Column('c_varchar', FIELD_TYPE.VARCHAR),
                                Column('c_datetime', FIELD_TYPE.TIMESTAMP2),
                                Column('c_tiny_blob', FIELD_TYPE.TINY_BLOB),
                                Column('c_blob', FIELD_TYPE.BLOB),
                            ],
                            'rows': [
                                {'values': {
                                    'c_int': 8,
                                    'c_datetime': datetime.datetime(2002, 8, 20, 3, 5, 9),
                                    'c_varchar': 'varchar 8',
                                    'c_blob': b'464thh',
                                    'c_tiny_blob': b'1'
                                }},
                                {'values': {
                                    'c_int': 9,
                                    'c_datetime': None,
                                    'c_varchar': 'varchar 9',
                                    'c_blob': b'32fgdf243',
                                    'c_tiny_blob': None
                                }},
                                {'values': {
                                    'c_int': 10,
                                    'c_datetime': None,
                                    'c_varchar': 'varchar 10',
                                    'c_blob': None,
                                    'c_tiny_blob': b'1'
                                }}
                            ]
                        }),
                    ]):
                        reader_mock.return_value.log_file = log_files[idx]
                        reader_mock.return_value.log_pos = log_positions[idx]
                        yield x

                reader_mock.close.return_value = 'Closing'
                reader_mock.return_value.auto_position = None

                reader_mock.return_value.__iter__ = iter_mock

                discover_catalog_mock.side_effect = [
                    Catalog([
                        CatalogEntry(
                            table='stream1',
                            stream='my_db-stream1',
                            tap_stream_id='my_db-stream1',
                            schema=Schema(
                                properties={
                                    'c_int': Schema(inclusion='available', type=['null', 'integer']),
                                    'c_varchar': Schema(inclusion='available', type=['null', 'string']),
                                    'c_datetime': Schema(inclusion='available', type=['null', 'string'],
                                                         format='date-time'),
                                }
                            ),
                            metadata=[
                                {
                                    'breadcrumb': [],
                                    'metadata': {
                                        'selected-by-default': False,
                                        'database-name': 'my_db',
                                        'row-count': 1,
                                        'replication-method': 'LOG_BASED',
                                        'selected': True,
                                        'is-view': False,
                                        'table-key-properties': ['c_int']
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_int'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'int(11)',
                                        'datatype': 'int'
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_varchar'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'varchar(100)',
                                        'datatype': 'varchar'
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_blob'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'blob',
                                        'datatype': 'blob'
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_tiny_blob'],
                                    'metadata': {
                                        'selected-by-default': False,
                                        'sql-datatype': 'blob',
                                        'datatype': 'blob'
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_datetime'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'timestamp',
                                        'datatype': 'timestamp'
                                    }
                                }
                            ]
                        ),
                    ]),
                    Catalog([
                        CatalogEntry(
                            table='stream2',
                            stream='my_db-stream2',
                            tap_stream_id='my_db-stream2',
                            schema=Schema(
                                properties={
                                    'c_bool': Schema(inclusion='available', type=['null', 'bool']),
                                    'c_double': Schema(inclusion='available', type=['null', 'number']),
                                    'c_time': Schema(inclusion='available', type=['null', 'string'], format='time'),
                                    'c_json': Schema(inclusion='available', type=['null', 'string']),
                                }
                            ),
                            metadata=[
                                {
                                    'breadcrumb': [],
                                    'metadata': {
                                        'selected-by-default': False,
                                        'database-name': 'my_db',
                                        'row-count': 1,
                                        'replication-method': 'LOG_BASED',
                                        'selected': True,
                                        'is-view': False,
                                        'table-key-properties': []
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_bool'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'tinyint(1)'
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_double'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'double'
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_time'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'time'
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_json'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'longtext'
                                    }
                                }
                            ]
                        )
                    ])

                ]

                binlog.sync_binlog_stream(
                    mysql_con,
                    config,
                    catalog,
                    state
                )

                discover_catalog_mock.assert_has_calls([
                    call(mysql_con, None, 'stream1'),
                    call(mysql_con, None, 'stream2'),
                ], any_order=False)

                self.assertListEqual([type(msg) for msg in singer_messages], [
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    SchemaMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    SchemaMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    StateMessage,
                ])

                self.assertListEqual([msg for msg in singer_messages if isinstance(msg, RecordMessage)], [
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': True,
                                      'c_time': datetime.time(20, 1, 14),
                                      'c_double': 19.44
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': False,
                                      'c_time': datetime.time(9, 10, 24),
                                      'c_double': 0.54
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': False,
                                      'c_time': datetime.time(8, 13, 12),
                                      'c_double': 100.22
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': False,
                                      'c_time': datetime.time(0, 10, 59, 44),
                                      'c_double': 0.54344
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': False,
                                      'c_time': datetime.time(0, 0, 0, 38),
                                      'c_double': 1.565667
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 1,
                                      'c_timestamp': '2021-03-24T10:12:56+00:00',
                                      'c_varchar': 'varchar 1'
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 2,
                                      'c_timestamp': '2019-12-24T03:01:06+00:00',
                                      'c_varchar': 'varchar 2'
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': False,
                                      'c_time': datetime.time(0, 0, 0, 38),
                                      'c_double': 10000.234
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 3,
                                      'c_varchar': 'varchar 3',
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 4,
                                      'c_varchar': 'varchar 4',
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 5,
                                      'c_datetime': '2002-08-20T05:05:09+00:00',
                                      'c_varchar': 'varchar 5',
                                      '_sdc_deleted_at': '2021-01-01T10:20:55+00:00'
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 6,
                                      'c_datetime': '2019-12-31T22:00:00+00:00',
                                      'c_varchar': 'varchar 6',
                                      '_sdc_deleted_at': '2021-01-01T10:20:55+00:00'
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 7,
                                      'c_datetime': '2021-01-01T01:04:00.067483+00:00',
                                      'c_varchar': 'varchar 7',
                                      '_sdc_deleted_at': '2021-01-01T10:20:55+00:00'
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': False,
                                      'c_time': datetime.time(12, 30, 0, 354676),
                                      'c_double': 10000,
                                      'c_json': '{"a": 1, "b": 2}'
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': True,
                                      'c_time': datetime.time(12, 30, 0),
                                      'c_double': 10.40,
                                      'c_json': '[{}, {}]'
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': True,
                                      'c_time': None,
                                      'c_double': -457.10,
                                      'c_json': 'null'
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 8,
                                      'c_datetime': '2002-08-20T00:05:09+00:00',
                                      'c_varchar': 'varchar 8',
                                      '_sdc_deleted_at': '2021-01-01T20:00:00+00:00'
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 9,
                                      'c_datetime': None,
                                      'c_varchar': 'varchar 9',
                                      '_sdc_deleted_at': '2021-01-01T20:00:00+00:00'
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 10,
                                      'c_datetime': None,
                                      'c_varchar': 'varchar 10',
                                      '_sdc_deleted_at': '2021-01-01T20:00:00+00:00'
                                  })
                ])

                self.assertListEqual([msg for msg in singer_messages if isinstance(msg, StateMessage)],
                                     [
                                         StateMessage(value={
                                             'bookmarks': {
                                                 'my_db-stream1': {
                                                     'log_file': 'binlog0003',
                                                     'log_pos': 999,
                                                     'version': 1
                                                 },
                                                 'my_db-stream2': {
                                                     'log_file': 'binlog0003',
                                                     'log_pos': 999,
                                                     'version': 1
                                                 },

                                             }
                                         }),
                                     ])

                reader_mock.assert_called_once_with(
                    **{
                        'connection_settings': {},
                        'pymysql_wrapper': make_connection_wrapper_mock.return_value,
                        'is_mariadb': False,
                        'server_id': 123,
                        'report_slave': socket.gethostname(),
                        'only_events': [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, RotateEvent],
                        'log_file': 'binlog0001',
                        'log_pos': 50,
                        'resume_stream': True,
                    }
                )

                self.assertEqual(1, reader_mock.return_value.close.call_count)

    @patch('tap_mysql.sync_strategies.binlog.calculate_gtid_bookmark',
           return_value='0-123-555')
    @patch('tap_mysql.sync_strategies.binlog.fetch_current_log_file_and_pos',
           return_value=('binlog0003', 1000))
    @patch('tap_mysql.sync_strategies.binlog.utils.now', return_value=datetime.datetime(2020, 10, 13, 8, 29, 58,
                                                                                        tzinfo=pytz.UTC))
    @patch('tap_mysql.sync_strategies.binlog.discover_catalog')
    @patch('tap_mysql.sync_strategies.binlog.make_connection_wrapper')
    def test_sync_binlog_stream_with_gtid(self,
                                          make_connection_wrapper_mock,
                                          discover_catalog_mock,
                                          *args):

        # we're dealing with local datetimes, so tests passing depend on the local timezone
        # pin the TZ to EET to avoid flakiness
        os.environ['TZ'] = 'EET'

        config = {
            'server_id': '123',
            'use_gtid': True,
            'engine': 'mariadb',
        }
        mysql_con = Mock(spec_set=MySQLConnection)

        catalog = {
            'my_db-stream1': {
                'catalog_entry': CatalogEntry(
                    table='stream1',
                    stream='my_db-stream1',
                    tap_stream_id='my_db-stream1',
                    schema=Schema(
                        properties={
                            'c_int': Schema(inclusion='available', type=['null', 'integer']),
                            'c_varchar': Schema(inclusion='available', type=['null', 'string']),
                            'c_timestamp': Schema(inclusion='available', type=['null', 'string'], format='date-time'),
                        }
                    ),
                    metadata=[
                        {
                            'breadcrumb': [],
                            'metadata': {
                                'selected-by-default': False,
                                'database-name': 'my_db',
                                'row-count': 1,
                                'replication-method': 'LOG_BASED',
                                'selected': True,
                                'is-view': False,
                                'table-key-properties': ['c_int']
                            }
                        },
                        {
                            'breadcrumb': ['properties', 'c_int'],
                            'metadata': {
                                'selected-by-default': True,
                                'sql-datatype': 'int(11)',
                                'datatype': 'int'
                            }
                        },
                        {
                            'breadcrumb': ['properties', 'c_varchar'],
                            'metadata': {
                                'selected-by-default': True,
                                'sql-datatype': 'varchar(100)',
                                'datatype': 'varchar'
                            }
                        },
                        {
                            'breadcrumb': ['properties', 'c_blob'],
                            'metadata': {
                                'selected-by-default': False,
                                'sql-datatype': 'blob',
                                'datatype': 'blob'
                            }
                        },
                        {
                            'breadcrumb': ['properties', 'c_timestamp'],
                            'metadata': {
                                'selected-by-default': True,
                                'sql-datatype': 'timestamp',
                                'datatype': 'timestamp'
                            }
                        }
                    ]
                ),
                'desired_columns': {'c_int', 'c_varchar', 'c_timestamp'}
            },
            'my_db-stream2': {
                'catalog_entry': CatalogEntry(
                    table='stream2',
                    stream='my_db-stream2',
                    tap_stream_id='my_db-stream2',
                    schema=Schema(
                        properties={
                            'c_bool': Schema(inclusion='available', type=['null', 'bool']),
                            'c_double': Schema(inclusion='available', type=['null', 'number']),
                            'c_time': Schema(inclusion='available', type=['null', 'string'], format='time'),
                        }
                    ),
                    metadata=[
                        {
                            'breadcrumb': [],
                            'metadata': {
                                'selected-by-default': False,
                                'database-name': 'my_db',
                                'row-count': 1,
                                'replication-method': 'LOG_BASED',
                                'selected': True,
                                'is-view': False,
                                'table-key-properties': []
                            }
                        },
                        {
                            'breadcrumb': ['properties', 'c_bool'],
                            'metadata': {
                                'selected-by-default': True,
                                'sql-datatype': 'tinyint(1)'
                            }
                        },
                        {
                            'breadcrumb': ['properties', 'c_double'],
                            'metadata': {
                                'selected-by-default': True,
                                'sql-datatype': 'double'
                            }
                        },
                        {
                            'breadcrumb': ['properties', 'c_time'],
                            'metadata': {
                                'selected-by-default': True,
                                'sql-datatype': 'time'
                            }
                        }
                    ]
                ),
                'desired_columns': {'c_bool', 'c_double', 'c_time'}
            }
        }

        singer_messages = []

        state = {
            'bookmarks': {
                'my_db-stream1': {
                    'version': 1
                },
                'my_db-stream2': {
                    'version': 1
                }
            }
        }

        with patch('tap_mysql.sync_strategies.binlog.singer.write_message') as write_msg:
            write_msg.side_effect = lambda msg: singer_messages.append(msg)

            with patch('tap_mysql.sync_strategies.binlog.BinLogStreamReader',
                       autospec=True) as reader_mock:
                def iter_mock(_):
                    log_files = [
                        'binlog0001',
                        'binlog0001',
                        'binlog0002',
                        'binlog0002',
                        'binlog0002',
                        'binlog0002',
                        'binlog0002',
                        'binlog0003',
                        'binlog0003',
                        'binlog0003',
                        'binlog0003',
                        'binlog0003',
                        'binlog0003'
                    ]

                    log_positions = [
                        50,
                        300,
                        520,
                        4,
                        100,
                        250,
                        7,
                        14,
                        20,
                        140,
                        300,
                        470,
                        999,
                    ]

                    for idx, x in enumerate([
                        get_binlogevent(WriteRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream2',
                            'columns': [
                                Column('c_bool', FIELD_TYPE.TINY),
                                Column('c_time', FIELD_TYPE.TIME2),
                                Column('c_double', FIELD_TYPE.DOUBLE),
                            ],
                            'rows': [
                                {'values': {
                                    'c_bool': True,
                                    'c_time': datetime.time(20, 1, 14),
                                    'c_double': 19.44
                                }},
                                {'values': {
                                    'c_bool': False,
                                    'c_time': datetime.time(9, 10, 24),
                                    'c_double': 0.54
                                }},
                            ]
                        }),
                        get_binlogevent(UpdateRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream2',
                            'columns': [
                                Column('c_bool', FIELD_TYPE.TINY),
                                Column('c_time', FIELD_TYPE.TIME2),
                                Column('c_double', FIELD_TYPE.DOUBLE),
                            ],
                            'rows': [
                                {'after_values': {
                                    'c_bool': False,
                                    'c_time': datetime.time(8, 13, 12),
                                    'c_double': 100.22
                                }},
                                {'after_values': {
                                    'c_bool': False,
                                    'c_time': datetime.time(0, 10, 59, 44),
                                    'c_double': 0.54344
                                }},
                                {'after_values': {
                                    'c_bool': False,
                                    'c_time': datetime.time(0, 0, 0, 38),
                                    'c_double': 1.565667
                                }},
                            ]
                        }),
                        get_binlogevent(MariadbGtidEvent, {
                            'gtid': '0-123-556',
                        }),
                        get_binlogevent(UpdateRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream1',
                            'columns': [
                                Column('c_int', FIELD_TYPE.INT24),
                                Column('c_varchar', FIELD_TYPE.VARCHAR),
                                Column('c_timestamp', FIELD_TYPE.TIMESTAMP2),
                                Column('c_blob', FIELD_TYPE.BLOB),
                            ],
                            'rows': [
                                {'after_values': {
                                    'c_int': 1,
                                    'c_timestamp': datetime.datetime(2021, 3, 24, 12, 12, 56),
                                    'c_varchar': 'varchar 1',
                                    'c_blob': b'dfhdfhsdhf'
                                }},
                                {'after_values': {
                                    'c_int': 2,
                                    'c_timestamp': datetime.datetime(2019, 12, 24, 5, 1, 6),
                                    'c_varchar': 'varchar 2',
                                    'c_blob': b'dflldskjf'
                                }}
                            ]
                        }),
                        get_binlogevent(DeleteRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream3',
                            'columns': []
                        }),
                        get_binlogevent(MariadbGtidEvent, {
                            'gtid': '0-123-556',
                        }),
                        get_binlogevent(WriteRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream2',
                            'columns': [
                                Column('c_bool', FIELD_TYPE.TINY),
                                Column('c_double', FIELD_TYPE.DOUBLE),
                                Column('c_time', FIELD_TYPE.TIME2),
                            ],
                            'rows': [
                                {'values': {
                                    'c_bool': False,
                                    'c_time': datetime.time(0, 0, 0, 38),
                                    'c_double': 10000.234
                                }}
                            ]
                        }),
                        get_binlogevent(WriteRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream1',
                            'columns': [
                                Column('c_int', FIELD_TYPE.INT24),
                                Column('c_varchar', FIELD_TYPE.VARCHAR),
                                Column('__dropped_col_2__', FIELD_TYPE.TIMESTAMP2),
                                Column('c_blob', FIELD_TYPE.BLOB),
                            ],
                            'rows': [
                                {'values': {
                                    'c_int': 3,
                                    'c_varchar': 'varchar 3',
                                    'c_blob': b'dfhdfhsdhf'
                                }},
                                {'values': {
                                    'c_int': 4,
                                    'c_varchar': 'varchar 4',
                                    'c_blob': b'32fgdf243'
                                }}
                            ]
                        }),
                        get_binlogevent(DeleteRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream1',
                            'timestamp': datetime.datetime.timestamp(datetime.datetime(2021, 1, 1, 10, 20, 55,
                                                                                       tzinfo=pytz.UTC)),
                            'columns': [
                                Column('c_int', FIELD_TYPE.INT24),
                                Column('c_varchar', FIELD_TYPE.VARCHAR),
                                Column('c_datetime', FIELD_TYPE.TIMESTAMP2),
                                Column('c_blob', FIELD_TYPE.BLOB),
                            ],
                            'rows': [
                                {'values': {
                                    'c_int': 5,
                                    'c_datetime': datetime.datetime(2002, 8, 20, 8, 5, 9),
                                    'c_varchar': 'varchar 5',
                                    'c_blob': b'dfhdfhsdhf'
                                }},
                                {'values': {
                                    'c_int': 6,
                                    'c_datetime': datetime.datetime(2020, 1, 1, 0, 0, 0),
                                    'c_varchar': 'varchar 6',
                                    'c_blob': b'32fgdf243'
                                }},
                                {'values': {
                                    'c_int': 7,
                                    'c_datetime': datetime.datetime(2021, 1, 1, 3, 4, 0, 67483),
                                    'c_varchar': 'varchar 7',
                                    'c_blob': None
                                }}
                            ]
                        }),
                        get_binlogevent(MariadbGtidEvent, {
                            'gtid': '0-123-557',
                        }),
                        get_binlogevent(WriteRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream2',
                            'columns': [
                                Column('c_bool', FIELD_TYPE.TINY),
                                Column('c_double', FIELD_TYPE.DOUBLE),
                                Column('c_time', FIELD_TYPE.TIME2),
                                Column('c_json', FIELD_TYPE.JSON),
                            ],
                            'rows': [
                                {'values': {
                                    'c_bool': False,
                                    'c_time': datetime.time(12, 30, 0, 354676),
                                    'c_double': 10000,
                                    'c_json': {'a': 1, 'b': 2}
                                }},
                                {'values': {
                                    'c_bool': True,
                                    'c_time': datetime.time(12, 30, 0, 0),
                                    'c_double': 10.40,
                                    'c_json': [{}, {}]
                                }},
                                {'values': {
                                    'c_bool': True,
                                    'c_time': None,
                                    'c_double': -457.10,
                                    'c_json': None
                                }}
                            ]
                        }),
                        get_binlogevent(DeleteRowsEvent, {
                            'schema': 'my_db',
                            'table': 'stream1',
                            'timestamp': datetime.datetime.timestamp(datetime.datetime(2021, 1, 1, 20, 0, 0,
                                                                                       tzinfo=pytz.UTC)),
                            'columns': [
                                Column('c_int', FIELD_TYPE.INT24),
                                Column('c_varchar', FIELD_TYPE.VARCHAR),
                                Column('c_datetime', FIELD_TYPE.TIMESTAMP2),
                                Column('c_tiny_blob', FIELD_TYPE.TINY_BLOB),
                                Column('c_blob', FIELD_TYPE.BLOB),
                            ],
                            'rows': [
                                {'values': {
                                    'c_int': 8,
                                    'c_datetime': datetime.datetime(2002, 8, 20, 3, 5, 9),
                                    'c_varchar': 'varchar 8',
                                    'c_blob': b'464thh',
                                    'c_tiny_blob': b'1'
                                }},
                                {'values': {
                                    'c_int': 9,
                                    'c_datetime': None,
                                    'c_varchar': 'varchar 9',
                                    'c_blob': b'32fgdf243',
                                    'c_tiny_blob': None
                                }},
                                {'values': {
                                    'c_int': 10,
                                    'c_datetime': None,
                                    'c_varchar': 'varchar 10',
                                    'c_blob': None,
                                    'c_tiny_blob': b'1'
                                }}
                            ]
                        }),
                        get_binlogevent(MariadbGtidEvent, {
                            'gtid': '0-123-558',
                        }),
                    ]):
                        reader_mock.return_value.log_file = log_files[idx]
                        reader_mock.return_value.log_pos = log_positions[idx]
                        yield x

                reader_mock.close.return_value = 'Closing'
                reader_mock.return_value.auto_position = None

                reader_mock.return_value.__iter__ = iter_mock

                discover_catalog_mock.side_effect = [
                    Catalog([
                        CatalogEntry(
                            table='stream1',
                            stream='my_db-stream1',
                            tap_stream_id='my_db-stream1',
                            schema=Schema(
                                properties={
                                    'c_int': Schema(inclusion='available', type=['null', 'integer']),
                                    'c_varchar': Schema(inclusion='available', type=['null', 'string']),
                                    'c_datetime': Schema(inclusion='available', type=['null', 'string'],
                                                         format='date-time'),
                                }
                            ),
                            metadata=[
                                {
                                    'breadcrumb': [],
                                    'metadata': {
                                        'selected-by-default': False,
                                        'database-name': 'my_db',
                                        'row-count': 1,
                                        'replication-method': 'LOG_BASED',
                                        'selected': True,
                                        'is-view': False,
                                        'table-key-properties': ['c_int']
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_int'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'int(11)',
                                        'datatype': 'int'
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_varchar'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'varchar(100)',
                                        'datatype': 'varchar'
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_blob'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'blob',
                                        'datatype': 'blob'
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_tiny_blob'],
                                    'metadata': {
                                        'selected-by-default': False,
                                        'sql-datatype': 'blob',
                                        'datatype': 'blob'
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_datetime'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'timestamp',
                                        'datatype': 'timestamp'
                                    }
                                }
                            ]
                        ),
                    ]),
                    Catalog([
                        CatalogEntry(
                            table='stream2',
                            stream='my_db-stream2',
                            tap_stream_id='my_db-stream2',
                            schema=Schema(
                                properties={
                                    'c_bool': Schema(inclusion='available', type=['null', 'bool']),
                                    'c_double': Schema(inclusion='available', type=['null', 'number']),
                                    'c_time': Schema(inclusion='available', type=['null', 'string'], format='time'),
                                    'c_json': Schema(inclusion='available', type=['null', 'string']),
                                }
                            ),
                            metadata=[
                                {
                                    'breadcrumb': [],
                                    'metadata': {
                                        'selected-by-default': False,
                                        'database-name': 'my_db',
                                        'row-count': 1,
                                        'replication-method': 'LOG_BASED',
                                        'selected': True,
                                        'is-view': False,
                                        'table-key-properties': []
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_bool'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'tinyint(1)'
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_double'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'double'
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_time'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'time'
                                    }
                                },
                                {
                                    'breadcrumb': ['properties', 'c_json'],
                                    'metadata': {
                                        'selected-by-default': True,
                                        'sql-datatype': 'longtext'
                                    }
                                }
                            ]
                        )
                    ])

                ]

                binlog.sync_binlog_stream(
                    mysql_con,
                    config,
                    catalog,
                    state
                )

                discover_catalog_mock.assert_has_calls([
                    call(mysql_con, None, 'stream1'),
                    call(mysql_con, None, 'stream2'),
                ], any_order=False)

                self.assertListEqual([type(msg) for msg in singer_messages], [
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    SchemaMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    SchemaMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    RecordMessage,
                    StateMessage,
                ])

                self.assertListEqual([msg for msg in singer_messages if isinstance(msg, RecordMessage)], [
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': True,
                                      'c_time': datetime.time(20, 1, 14),
                                      'c_double': 19.44
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': False,
                                      'c_time': datetime.time(9, 10, 24),
                                      'c_double': 0.54
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': False,
                                      'c_time': datetime.time(8, 13, 12),
                                      'c_double': 100.22
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': False,
                                      'c_time': datetime.time(0, 10, 59, 44),
                                      'c_double': 0.54344
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': False,
                                      'c_time': datetime.time(0, 0, 0, 38),
                                      'c_double': 1.565667
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 1,
                                      'c_timestamp': '2021-03-24T10:12:56+00:00',
                                      'c_varchar': 'varchar 1'
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 2,
                                      'c_timestamp': '2019-12-24T03:01:06+00:00',
                                      'c_varchar': 'varchar 2'
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': False,
                                      'c_time': datetime.time(0, 0, 0, 38),
                                      'c_double': 10000.234
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 3,
                                      'c_varchar': 'varchar 3',
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 4,
                                      'c_varchar': 'varchar 4',
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 5,
                                      'c_datetime': '2002-08-20T05:05:09+00:00',
                                      'c_varchar': 'varchar 5',
                                      '_sdc_deleted_at': '2021-01-01T10:20:55+00:00'
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 6,
                                      'c_datetime': '2019-12-31T22:00:00+00:00',
                                      'c_varchar': 'varchar 6',
                                      '_sdc_deleted_at': '2021-01-01T10:20:55+00:00'
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 7,
                                      'c_datetime': '2021-01-01T01:04:00.067483+00:00',
                                      'c_varchar': 'varchar 7',
                                      '_sdc_deleted_at': '2021-01-01T10:20:55+00:00'
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': False,
                                      'c_time': datetime.time(12, 30, 0, 354676),
                                      'c_double': 10000,
                                      'c_json': '{"a": 1, "b": 2}'
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': True,
                                      'c_time': datetime.time(12, 30, 0),
                                      'c_double': 10.40,
                                      'c_json': '[{}, {}]'
                                  }),
                    RecordMessage(stream='my_db-stream2',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_bool': True,
                                      'c_time': None,
                                      'c_double': -457.10,
                                      'c_json': 'null'
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 8,
                                      'c_datetime': '2002-08-20T00:05:09+00:00',
                                      'c_varchar': 'varchar 8',
                                      '_sdc_deleted_at': '2021-01-01T20:00:00+00:00'
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 9,
                                      'c_datetime': None,
                                      'c_varchar': 'varchar 9',
                                      '_sdc_deleted_at': '2021-01-01T20:00:00+00:00'
                                  }),
                    RecordMessage(stream='my_db-stream1',
                                  time_extracted=datetime.datetime(2020, 10, 13, 8, 29, 58, tzinfo=pytz.UTC),
                                  version=1,
                                  record={
                                      'c_int': 10,
                                      'c_datetime': None,
                                      'c_varchar': 'varchar 10',
                                      '_sdc_deleted_at': '2021-01-01T20:00:00+00:00'
                                  })
                ])

                self.assertListEqual([msg for msg in singer_messages if isinstance(msg, StateMessage)],
                                     [
                                         StateMessage(value={
                                             'bookmarks': {
                                                 'my_db-stream1': {
                                                     'gtid': '0-123-558',
                                                     'log_file': 'binlog0003',
                                                     'log_pos': 999,
                                                     'version': 1,
                                                 },
                                                 'my_db-stream2': {
                                                     'gtid': '0-123-558',
                                                     'log_file': 'binlog0003',
                                                     'log_pos': 999,
                                                     'version': 1
                                                 },

                                             }
                                         }),
                                     ])

                reader_mock.assert_called_once_with(
                    **{
                        'connection_settings': {},
                        'pymysql_wrapper': make_connection_wrapper_mock.return_value,
                        'is_mariadb': True,
                        'server_id': 123,
                        'report_slave': socket.gethostname(),
                        'only_events': [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, GtidEvent, MariadbGtidEvent],
                        'auto_position': '0-123-555',
                    }
                )

                self.assertEqual(1, reader_mock.return_value.close.call_count)

    @patch('tap_mysql.sync_strategies.binlog.connect_with_backoff')
    def test_verify_binlog_config_success(self, connect_with_backoff):

        mysql_con = MagicMock(spec_set=MySQLConnection).return_value

        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.side_effect = [
            ['ROW'],
            ['FULL']
        ]

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con

        binlog.verify_binlog_config(mysql_con)

        connect_with_backoff.assert_called_with(mysql_con)
        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('SELECT  @@binlog_format'),
                call('SELECT  @@binlog_row_image'),
            ]
        )

    @patch('tap_mysql.sync_strategies.binlog.connect_with_backoff')
    def test_verify_binlog_config_fail_if_not_FULL(self, connect_with_backoff):

        mysql_con = MagicMock(spec_set=MySQLConnection).return_value

        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.side_effect = [
            ['ROW'],
            ['Not-FULL']
        ]

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con

        with self.assertRaises(Exception) as context:
            binlog.verify_binlog_config(mysql_con)

        self.assertEqual("Unable to replicate binlog stream because binlog_row_image is not set to 'FULL': "
                         "Not-FULL.", str(context.exception))

        connect_with_backoff.assert_called_with(mysql_con)
        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('SELECT  @@binlog_format'),
                call('SELECT  @@binlog_row_image'),
            ]
        )

    @patch('tap_mysql.sync_strategies.binlog.connect_with_backoff')
    def test_verify_binlog_config_fail_if_not_ROW(self, connect_with_backoff):

        mysql_con = MagicMock(spec_set=MySQLConnection).return_value

        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.side_effect = [
            ['Not-ROW'],
        ]

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con

        with self.assertRaises(Exception) as context:
            binlog.verify_binlog_config(mysql_con)

        self.assertEqual("Unable to replicate binlog stream because binlog_format is not set to 'ROW': Not-ROW.",
                         str(context.exception))

        connect_with_backoff.assert_called_with(mysql_con)
        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('SELECT  @@binlog_format'),
            ]
        )

    @patch('tap_mysql.sync_strategies.binlog.connect_with_backoff')
    def test_verify_binlog_config_fail_if_binlog_row_image_not_supported(self, connect_with_backoff):

        mysql_con = MagicMock(spec_set=MySQLConnection).return_value

        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.side_effect = [
            ['ROW'],
        ]

        cur_mock.__enter__.return_value.execute.side_effect = [
            None,
            InternalError(1193)
        ]

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con

        with self.assertRaises(Exception) as context:
            binlog.verify_binlog_config(mysql_con)

        self.assertEqual("Unable to replicate binlog stream because binlog_row_image "
                         "system variable does not exist. MySQL version must be at "
                         "least 5.6.2 to use binlog replication.",
                         str(context.exception))

        connect_with_backoff.assert_called_with(mysql_con)
        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('SELECT  @@binlog_format'),
                call('SELECT  @@binlog_row_image'),
            ]
        )

    @patch('tap_mysql.sync_strategies.binlog.connect_with_backoff')
    def test_verify_gtid_config_success(self, connect_with_backoff):

        mysql_con = MagicMock(spec_set=MySQLConnection).return_value

        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.side_effect = [
            ['ON'],
        ]

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con

        binlog.verify_gtid_config(mysql_con)

        connect_with_backoff.assert_called_with(mysql_con)
        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('select @@gtid_mode;'),
            ]
        )

    @patch('tap_mysql.sync_strategies.binlog.connect_with_backoff')
    def test_verify_gtid_config_fail_if_not_on(self, connect_with_backoff):

        mysql_con = MagicMock(spec_set=MySQLConnection).return_value

        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.side_effect = [
            ['OFF'],
        ]

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con

        with self.assertRaises(Exception) as context:
            binlog.verify_gtid_config(mysql_con)

        self.assertEqual("Unable to replicate binlog stream because GTID mode is not enabled.", str(context.exception))

        connect_with_backoff.assert_called_with(mysql_con)
        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('select @@gtid_mode;'),
            ]
        )

    @patch('tap_mysql.sync_strategies.binlog.connect_with_backoff')
    def test_fetch_current_log_file_and_pos_success(self, connect_with_backoff):
        mysql_con = MagicMock(spec_set=MySQLConnection).return_value
        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.side_effect = [
            ['binlog.000033', 345, ''],
        ]

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con

        result = binlog.fetch_current_log_file_and_pos(mysql_con)

        self.assertEqual(result, ('binlog.000033', 345))

        connect_with_backoff.assert_called_with(mysql_con)
        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('SHOW MASTER STATUS'),
            ]
        )

    @patch('tap_mysql.sync_strategies.binlog.connect_with_backoff')
    def test_fetch_current_log_file_and_pos_fail_if_no_result(self, connect_with_backoff):
        mysql_con = MagicMock(spec_set=MySQLConnection).return_value
        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.side_effect = [
            None
        ]

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con

        with self.assertRaises(Exception) as context:
            binlog.fetch_current_log_file_and_pos(mysql_con)

        self.assertEqual('MySQL binary logging is not enabled.', str(context.exception))

        connect_with_backoff.assert_called_with(mysql_con)
        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('SHOW MASTER STATUS'),
            ]
        )

    @patch('tap_mysql.sync_strategies.binlog.connection.fetch_server_uuid')
    @patch('tap_mysql.sync_strategies.binlog.connect_with_backoff')
    def test_fetch_current_gtid_pos_for_mysql_not_found_expect_exception(
            self, connect_with_backoff, fetch_server_uuid):
        mysql_con = MagicMock(spec_set=MySQLConnection).return_value
        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.side_effect = [
            ['3E11FA47-71CA-11E1-9E21-C80AA9429562:1,3E11FA47-71BB-11E1-9E33-C80AA9429562:2:143,0-3-1123,,'
             '3E11FA47-71CA-11E1-9E33-C80AA9429562:2:332'],
        ]

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con
        fetch_server_uuid.return_value = '3E11FA47-71CA-11E1-9E33-C80AA9429562'

        with self.assertRaises(Exception):
            binlog.fetch_current_gtid_pos(mysql_con, connection.MYSQL_ENGINE)

        connect_with_backoff.assert_called_with(mysql_con)
        fetch_server_uuid.assert_called_with(mysql_con)
        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('select @@GLOBAL.gtid_executed;'),
            ]
        )

    @patch('tap_mysql.sync_strategies.binlog.connection.fetch_server_uuid')
    @patch('tap_mysql.sync_strategies.binlog.connect_with_backoff')
    def test_fetch_current_gtid_pos_for_mysql_succeeds(
            self, connect_with_backoff, fetch_server_uuid):

        mysql_con = MagicMock(spec_set=MySQLConnection).return_value
        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.side_effect = [
            ['3E11FA47-71CA-11E1-9E33-C80AA9429562:1,3E11FA47-71BB-11E1-9E33-C80AA9429562:2:143,0-3-1123,,'
             '3E11FA47-71CA-11E1-9E33-C80AA9429562:2:332'],
        ]

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con
        fetch_server_uuid.return_value = '3E11FA47-71CA-11E1-9E33-C80AA9429562'

        result = binlog.fetch_current_gtid_pos(mysql_con, connection.MYSQL_ENGINE)

        self.assertEqual('3E11FA47-71CA-11E1-9E33-C80AA9429562:1', result)

        connect_with_backoff.assert_called_with(mysql_con)
        fetch_server_uuid.assert_called_with(mysql_con)

        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('select @@GLOBAL.gtid_executed;'),
            ]
        )

    @patch('tap_mysql.sync_strategies.binlog.connection.fetch_server_id')
    @patch('tap_mysql.sync_strategies.binlog.connect_with_backoff')
    def test_fetch_current_gtid_pos_for_mariadb_no_gtid_found_expect_exception(
            self, connect_with_backoff, fetch_server_id):
        mysql_con = MagicMock(spec_set=MySQLConnection).return_value
        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.side_effect = [
            None
        ]

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con
        fetch_server_id.return_value = 2

        with self.assertRaises(Exception) as context:
            binlog.fetch_current_gtid_pos(mysql_con, connection.MARIADB_ENGINE)

        self.assertIn('GTID is not present on this server!', str(context.exception))

        connect_with_backoff.assert_called_with(mysql_con)
        fetch_server_id.assert_called_with(mysql_con)

        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('select @@gtid_current_pos;'),
            ]
        )

    @patch('tap_mysql.sync_strategies.binlog.connection.fetch_server_id')
    @patch('tap_mysql.sync_strategies.binlog.connect_with_backoff')
    def test_fetch_current_gtid_pos_no_gtid_found_for_given_server_expect_exception(
            self, connect_with_backoff, fetch_server_id):

        mysql_con = MagicMock(spec_set=MySQLConnection).return_value
        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.side_effect = [
            ['0, 0-4-222,']
        ]

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con

        fetch_server_id.return_value = 2

        with self.assertRaises(Exception) as context:
            binlog.fetch_current_gtid_pos(mysql_con, connection.MARIADB_ENGINE)

        self.assertIn('No suitable GTID was found for server', str(context.exception))

        connect_with_backoff.assert_called_with(mysql_con)
        fetch_server_id.assert_called_with(mysql_con)
        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('select @@gtid_current_pos;'),
            ]
        )

    def test_calculate_gtid_bookmark_for_mariadb_returns_earliest(self):

        binlog_streams = {
            'stream1': {'schema': {}},
            'stream2': {'schema': {}},
            'stream3': {'schema': {}},
        }

        state = {
            'bookmarks': {
                'stream1': {'gtid': '0-3-165'},
                'stream2': {'gtid': '0-20-12'},
                'stream3': {'gtid': '0-12-43'},
                'stream4': {'gtid': '0-1-1'},
                'stream6': {'gtid': '0-3-4'},
                'stream5': {},
            }
        }

        mysql_conn = Mock(spec_set=MySQLConnection)

        result = binlog.calculate_gtid_bookmark(mysql_conn, binlog_streams, state, connection.MARIADB_ENGINE)

        self.assertEqual(result, '0-20-12')

    @patch('tap_mysql.sync_strategies.binlog.connect_with_backoff')
    def test_calculate_gtid_bookmark_for_mariadb_no_gtid_found_would_infer_from_binlog(self, connect_with_backoff):

        binlog_streams = {
            'stream1': {'schema': {}},
            'stream2': {'schema': {}},
            'stream3': {'schema': {}},
        }

        state = {
            'bookmarks': {
                'stream1': {'log_file': 'binlog.040', 'log_pos': 138},
                'stream2': {'log_file': 'binlog.040', 'log_pos': 50},
                'stream3': {'log_file': 'binlog.032', 'log_pos': 14},
            }
        }
        mysql_con = MagicMock(spec_set=MySQLConnection).return_value
        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.side_effect = [
            ['0-4-222'],
            [4]
        ]
        cur_mock.__enter__.return_value.fetchall.return_value = [
            ('binlog.030',),
            ('binlog.031',),
            ('binlog.032',),
            ('binlog.033',),
            ('binlog.034',),
            ('binlog.040',),
            ('binlog.041',),
        ]

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con

        result = binlog.calculate_gtid_bookmark(mysql_con, binlog_streams, state, connection.MARIADB_ENGINE)

        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('SHOW BINARY LOGS'),
                call("select BINLOG_GTID_POS('binlog.032', 14);"),
                call("SELECT @@server_id"),
            ]
        )

        self.assertEqual(result, '0-4-222')

    @patch('tap_mysql.sync_strategies.binlog.connect_with_backoff')
    def test_calculate_gtid_bookmark_for_mariadb_no_gtid_found_would_infer_from_binlog_returns_many_gtids(self,
                                                                                              connect_with_backoff):

        binlog_streams = {
            'stream1': {'schema': {}},
            'stream2': {'schema': {}},
            'stream3': {'schema': {}},
        }

        state = {
            'bookmarks': {
                'stream1': {'log_file': 'binlog.040', 'log_pos': 138},
                'stream2': {'log_file': 'binlog.040', 'log_pos': 50},
                'stream3': {'log_file': 'binlog.032', 'log_pos': 14},
            }
        }
        mysql_con = MagicMock(spec_set=MySQLConnection).return_value
        cur_mock = MagicMock(spec_set=Cursor).return_value
        cur_mock.__enter__.return_value.fetchone.side_effect = [
            ['0-4-222,,3-4,5-66-2213,6-89-7222'],
            [89]
        ]
        cur_mock.__enter__.return_value.fetchall.return_value = [
            ('binlog.030',),
            ('binlog.031',),
            ('binlog.032',),
            ('binlog.033',),
            ('binlog.034',),
            ('binlog.040',),
            ('binlog.041',),
        ]

        mysql_con.__enter__.return_value.cursor.return_value = cur_mock

        connect_with_backoff.return_value = mysql_con

        result = binlog.calculate_gtid_bookmark(mysql_con, binlog_streams, state, connection.MARIADB_ENGINE)

        cur_mock.__enter__.return_value.execute.assert_has_calls(
            [
                call('SHOW BINARY LOGS'),
                call("select BINLOG_GTID_POS('binlog.032', 14);"),
                call("SELECT @@server_id"),
            ]
        )

        self.assertEqual(result, '6-89-7222')

    @patch('tap_mysql.sync_strategies.binlog.calculate_bookmark')
    @patch('tap_mysql.sync_strategies.binlog.connect_with_backoff')
    def test_calculate_gtid_bookmark_for_mariadb_no_gtid_nor_binlog_found_expect_exception(self,
                                                                                           connect_with_backoff,
                                                                                           calculate_bookmark):

        binlog_streams = {
            'stream1': {'schema': {}},
            'stream2': {'schema': {}},
            'stream3': {'schema': {}},
        }

        state = {
            'bookmarks': {}
        }
        mysql_conn = Mock(spec_set=MySQLConnection)
        connect_with_backoff.return_value = mysql_conn
        calculate_bookmark.return_value = None, None

        with self.assertRaises(Exception) as context:
            binlog.calculate_gtid_bookmark(mysql_conn, binlog_streams, state, connection.MARIADB_ENGINE)

        self.assertEqual("No binlog coordinates in state to infer gtid position! Cannot resume logical replication",
                         str(context.exception))

    def test_calculate_gtid_bookmark_for_mysql_returns_earliest(self):

        binlog_streams = {
            'stream1': {'schema': {}},
            'stream2': {'schema': {}},
            'stream3': {'schema': {}},
            'stream4': {'schema': {}},
        }

        state = {
            'bookmarks': {
                'stream1': {'gtid': '3E11FA47-71CA-11E1-9E33-C80AA9429562:1-165'},
                'stream2': {'gtid': '3E11FA47-71CA-11E1-9E33-C80AA9429562:12'},
                'stream3': {'gtid': '3E11FA47-71CA-11E1-9E33-C80AA9429562:1-43'},
                'stream4': {'gtid': '3E11FA47-71CA-11E1-9E33-C80AA9429562:1-2'},
                'stream6': {'gtid': '3E11FA47-71CA-11E1-9E33-C80AA9429562:1'},
                'stream5': {},
            }
        }
        mysql_conn = Mock(spec_set=MySQLConnection)
        result = binlog.calculate_gtid_bookmark(mysql_conn, binlog_streams, state, connection.MYSQL_ENGINE)

        self.assertEqual(result, '3E11FA47-71CA-11E1-9E33-C80AA9429562:1-2')

    def test_calculate_gtid_bookmark_for_mysql_no_gtid_found_expect_exception(self):

        binlog_streams = {
            'stream1': {'schema': {}},
            'stream2': {'schema': {}},
            'stream3': {'schema': {}},
        }

        state = {
            'bookmarks': {}
        }
        mysql_conn = Mock(spec_set=MySQLConnection)

        with self.assertRaises(Exception) as context:
            binlog.calculate_gtid_bookmark(mysql_conn, binlog_streams, state, connection.MYSQL_ENGINE)

        self.assertEqual("Couldn't find any gtid in state bookmarks to resume logical replication",
                         str(context.exception))

    def test_row_to_singer_record(self):
        catalog_entry = CatalogEntry(
            stream='stream',
            schema=Schema.from_dict({
                'type': 'object',
                'properties': {
                    'time': {
                        'type': 'string',
                        'format': 'time',
                    },
                },
            }),
        )
        message = binlog.row_to_singer_record(
            catalog_entry,
            version=1,
            row={'time': datetime.timedelta(hours=8, minutes=30)},
            db_column_map={},
            time_extracted=datetime.datetime.now(datetime.timezone.utc),
        )

        assert message.stream == 'stream'
        assert message.version == 1
        assert message.record == {'time': '08:30:00'}
        assert message.time_extracted is not None
