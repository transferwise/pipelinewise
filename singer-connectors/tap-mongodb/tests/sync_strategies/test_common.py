import time
import unittest
import uuid
from unittest.mock import patch

import bson

from datetime import datetime

from bson import ObjectId, Timestamp, MinKey
from dateutil.tz import tzutc

import tap_mongodb.sync_strategies.common as common
from tap_mongodb.errors import UnsupportedKeyTypeException


class TestRowToSchemaMessage(unittest.TestCase):

    def test_calculate_destination_stream_name_with_include_schema_True(self):
        """

        """
        stream = {
            'stream': 'myStream',
            'metadata': [
                {
                    "breadcrumb": [],
                    "metadata": {
                        "database-name": "myDb",
                    }
                }
            ]
        }
        with patch('tap_mongodb.common.INCLUDE_SCHEMAS_IN_DESTINATION_STREAM_NAME') as constant_mock:
            constant_mock.return_value = True
            self.assertEqual('myDb-myStream', common.calculate_destination_stream_name(stream))

    def test_calculate_destination_stream_name_with_include_schema_False(self):
        """

        """
        stream = {
            'stream': 'myStream',
            'metadata': [
                {
                    "breadcrumb": [],
                    "metadata": {
                        "database-name": "myDb",
                    }
                }
            ]
        }
        common.INCLUDE_SCHEMAS_IN_DESTINATION_STREAM_NAME = False
        self.assertEqual('myStream', common.calculate_destination_stream_name(stream))

    def test_get_stream_version_with_none_version_returns_new_version(self):

        state = {
            'bookmarks': {
                'myStream': {}
            }
        }
        self.assertGreaterEqual(time.time()*1000, common.get_stream_version('myStream', state))

    def test_get_stream_version_with_defined_version_returns_the_same_version(self):

        state = {
            'bookmarks': {
                'myStream': {'version': 123}
            }
        }
        self.assertEqual(123, common.get_stream_version('myStream', state))

    def test_class_to_string_with_bson_Timestamp_should_return_concatenated_time(self):
        ts = bson.Timestamp(200000, 80)

        self.assertEqual('200000.80', common.class_to_string(ts, 'Timestamp'))

    def test_class_to_string_with_unsupported_type_raises_exception(self):
        with self.assertRaises(UnsupportedKeyTypeException):
            common.class_to_string('a', 'random type')

    def test_transform_value_with_naive_datetime_should_return_utc_formatted_date(self):
        import tzlocal
        import pytz

        date = datetime(2020, 5, 13, 15, 00, 00)
        output = common.transform_value(date, None)

        local_tz = tzlocal.get_localzone()
        dt = local_tz.localize(date).astimezone(pytz.UTC)

        fmt = '%Y-%m-%dT%H:%M:%S.%fZ'

        self.assertEqual(dt.strftime(fmt), output)

    def test_transform_value_with_datetime_should_return_utc_formatted_date(self):
        import tzlocal
        import pytz

        amsterdam = pytz.timezone('Europe/Amsterdam')
        date = amsterdam.localize(datetime(2020, 5, 13, 15, 00, 00))

        self.assertEqual('2020-05-13T13:00:00.000000Z', common.transform_value(date, None))

    def test_transform_value_with_bytes_should_return_decoded_string(self):
        b = b'Pythonnnn'

        self.assertEqual('UHl0aG9ubm5u', common.transform_value(b, None))

    def test_transform_value_with_UUID_should_return_str(self):
        uid = '123e4567-e89b-12d3-a456-426652340000'
        self.assertEqual(uid, common.transform_value(uuid.UUID(uid), None))

    def test_string_to_class_with_UUID(self):
        uid = '123e4567-e89b-12d3-a456-426652340000'
        self.assertEqual(bson.Binary.from_uuid(uuid.UUID(uid)), common.string_to_class(uid, 'UUID'))

    def test_string_to_class_with_formatted_utc_datetime(self):
        dt = '2020-05-10T12:01:50.000000Z'
        self.assertEqual(datetime(2020, 5, 10, 12, 1, 50, tzinfo=tzutc()), common.string_to_class(dt, 'datetime'))

    def test_string_to_class_with_ObjectId(self):
        ob = '0123456789ab0123456789ab'
        self.assertEqual(ObjectId('0123456789ab0123456789ab'), common.string_to_class(ob, 'ObjectId'))

    def test_string_to_class_with_Timestamp(self):
        ob = '3000.0'
        self.assertEqual(Timestamp(3000, 0), common.string_to_class(ob, 'Timestamp'))

    def test_string_to_class_with_unsupported_type_raises_exception(self):
        with self.assertRaises(UnsupportedKeyTypeException):
            common.string_to_class(1, 'some random type')

    def test_row_to_singer_record_successful_transformation_without_deleted(self):
        stream = {
            'stream': 'myStream',
            'metadata': [
                {
                    'breadcrumb': [],
                    'metadata': {}
                }
            ]
        }

        row = {
            '_id': ObjectId('0123456789ab0123456789ab'),
            'key1': 10,
            'key2': Timestamp(1589379991, 4696183),
            'key3': 1.5,
            'key4': MinKey()
        }
        dt = datetime(2020, 5, 13, 14, 10, 10, tzinfo=tzutc())

        result = common.row_to_singer_record(stream, row, dt, None, 100)

        self.assertEqual({
            'type': 'RECORD',
            'stream': 'myStream',
            'record': {
                '_id': '0123456789ab0123456789ab',
                'document': {
                    '_id': '0123456789ab0123456789ab',
                    'key1': 10,
                    'key2': '2020-05-13T14:26:31.000000Z',
                    'key3': 1.5
                },
                common.SDC_DELETED_AT: None,
            },
            'version': 100,
            'time_extracted': '2020-05-13T14:10:10.000000Z',
        }, result.asdict())


    def test_row_to_singer_record_successful_transformation_with_deleted(self):
        stream = {
            'stream': 'myStream',
            'metadata': [
                {
                    'breadcrumb': [],
                    'metadata': {}
                }
            ]
        }

        row = {
            '_id': ObjectId('0123456789ab0123456789ab'),
            'key1': 10,
            'key2': Timestamp(1589379991, 4696183),
            'key3': 1.5
        }
        dt = datetime(2020, 5, 13, 14, 10, 10, tzinfo=tzutc())

        result = common.row_to_singer_record(stream, row, dt, datetime(2020, 5, 20, 15, 0, 0, 0, tzinfo=tzutc()), 100)

        self.assertEqual({
            'type': 'RECORD',
            'stream': 'myStream',
            'record': {
                '_id': '0123456789ab0123456789ab',
                'document': {
                    '_id': '0123456789ab0123456789ab',
                    'key1': 10,
                    'key2': '2020-05-13T14:26:31.000000Z',
                    'key3': 1.5
                },
                common.SDC_DELETED_AT: '2020-05-20T15:00:00.000000Z',
            },
            'version': 100,
            'time_extracted': '2020-05-13T14:10:10.000000Z',
        }, result.asdict())
