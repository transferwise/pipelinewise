import time
from unittest import TestCase
from unittest.mock import patch, Mock

from bson import ObjectId, Timestamp
from mock import PropertyMock, call
from pymongo import MongoClient
from pymongo.change_stream import DatabaseChangeStream
from pymongo.database import Database

from pipelinewise.fastsync.commons.errors import TableNotFoundError, ExportError
from pipelinewise.fastsync.commons.tap_mongodb import FastSyncTapMongoDB


# pylint: disable=invalid-name,no-self-use
class TestFastSyncTapMongoDB(TestCase):
    """
    Unit tests for fastsync tap mongo
    """
    def setUp(self) -> None:
        """Initialise test FastSyncTapPostgres object"""
        self.connection_config = {'host': 'foo.com',
                                  'port': 3306,
                                  'user': 'my_user',
                                  'password': 'secret',
                                  'auth_database': 'admin',
                                  'database': 'my_db'
                                  }
        self.mongo = FastSyncTapMongoDB(self.connection_config,
                                        lambda x: {
                                            'string': 'text',
                                            'date': 'time with timezone'
                                        }.get(x, 'default'))

    def test_open_connections(self):
        """
            Test open_connection method
            it should create a Database Mock
        """
        with patch('pipelinewise.fastsync.commons.tap_mongodb.MongoClient') as mongo_client_mock:
            mongo_client_mock.return_value = {
                'my_db': Mock(spec_set=Database)
            }
            self.mongo.open_connection()

        self.assertIsInstance(self.mongo.database, Mock)

    def test_close_connection(self):
        """
        Test close_connection method, It should call the close method on the client
        """
        self.mongo.database = Mock(spec_set=Database)

        client = Mock(spec_set=MongoClient)
        client.return_value.close.side_effect = True

        type(self.mongo.database).client = PropertyMock(return_value=client)

        self.mongo.close_connection()

        client.close.assert_called_once()

    def test_copy_table_with_collection_not_found_expect_exception(self):
        """
        Test copy_table method with a collection name that's not found in the db, thus raising a TableNotFoundError
        error
        """
        self.mongo.database = Mock(spec_set=Database).return_value
        self.mongo.database.list_collection_names.return_value = ['col1', 'col2', 'col3']

        with self.assertRaises(TableNotFoundError):
            self.mongo.copy_table('my_col', 'file.csv.gzip', 'tmp')

        self.mongo.database.list_collection_names.assert_called_once()

    def test_copy_table_with_collection_found_but_export_failed_expect_exception(self):
        """
        Test copy_table method with a collection name that's not found in the db, thus raising a TableNotFoundError
        error
        """
        self.mongo.database = Mock(spec_set=Database).return_value
        self.mongo.database.list_collection_names.return_value = ['col1', 'col2', 'col3', 'my_col']

        with patch('pipelinewise.fastsync.commons.tap_mongodb.subprocess.call') as call_mock:
            call_mock.return_value = 1

            with self.assertRaises(ExportError):
                self.mongo.copy_table('my_col', 'file.csv.gzip', 'tmp')

            call_mock.assert_called_once_with([
                'mongodump',
                '--uri', '"mongodb://my_user:secret@foo.com:3306/my_db?authSource=admin"',
                '--forceTableScan',
                '--gzip',
                '-c', 'my_col',
                '-o', 'tmp'
            ])

        self.mongo.database.list_collection_names.assert_called_once()

    def test_copy_table_with_collection_found_success(self):
        """
        Test copy_table method with a collection name that's in the db, the copy should continue successfully
        """
        self.mongo.database = Mock(spec_set=Database).return_value
        self.mongo.database.list_collection_names.return_value = ['col1', 'col2', 'col3', 'my_col']

        with patch('pipelinewise.fastsync.commons.tap_mongodb.subprocess.call') as call_mock:
            call_mock.return_value = 0

            with patch('pipelinewise.fastsync.commons.tap_mongodb.os.remove') as os_remove_mock:
                os_remove_mock.return_value = True

                with patch('pipelinewise.fastsync.commons.tap_mongodb.gzip') as gzip_mock:
                    mock_enter = Mock()

                    with patch('pipelinewise.fastsync.commons.tap_mongodb.bson.decode_iter') as bson_decode_iter_mock:

                        bson_decode_iter_mock.return_value = [
                            {'_id': ObjectId('0123456789ab0123456789aa'), 'key1': 1, 'key2': time.time()},
                            {'_id': ObjectId('0123456789ab0123456789ab'), 'key1': 2},
                            {'_id': ObjectId('0123456789ab0123456789ac'), 'key3': Timestamp(10000, 50)},
                        ]

                        mock_enter.return_value.open.return_value = Mock()

                        gzip_mock.return_value.__enter__ = mock_enter
                        gzip_mock.return_value.__exit__ = Mock()

                        self.mongo.copy_table('my_col', 'file.csv.gzip', 'tmp')

                        call_mock.assert_called_once_with([
                            'mongodump',
                            '--uri', '"mongodb://my_user:secret@foo.com:3306/my_db?authSource=admin"',
                            '--forceTableScan',
                            '--gzip',
                            '-c', 'my_col',
                            '-o', 'tmp'
                        ])

                        os_remove_mock.assert_has_calls(
                            [call('tmp/my_db/my_col.metadata.json.gz'), call('tmp/my_db/my_col.bson.gz')])
                        self.assertEqual(2, os_remove_mock.call_count)
                        bson_decode_iter_mock.assert_called_once()

    def test_fetch_current_log_pos_return_first_token(self):
        """
        Test fetch_current_log_pos should return the the first encountered token
        """
        cursor_mock = Mock(spec_set=DatabaseChangeStream).return_value
        type(cursor_mock).alive = PropertyMock(return_value=True)
        type(cursor_mock).resume_token = PropertyMock(side_effect=['token1', 'token2',
                                                                   'token3', 'token4'])
        cursor_mock.try_next.side_effect = [{}, {}, {}]

        mock_enter = Mock()
        mock_enter.return_value = cursor_mock

        mock_watch = Mock().return_value
        mock_watch.__enter__ = mock_enter
        mock_watch.__exit__ = Mock()

        self.mongo.database = Mock(spec_set=Database).return_value
        self.mongo.database.watch.return_value = mock_watch

        self.assertDictEqual({
            'token': 'token1'
        }, self.mongo.fetch_current_log_pos())

    def test_fetch_current_incremental_key_pos(self):
        """
        test fetch_current_incremental_key_pos which is currently not implemented for a good reasom
        """
        with self.assertRaises(NotImplementedError):
            self.mongo.fetch_current_incremental_key_pos('db.table', 'key')

    def test_map_column_types_to_target(self):
        """
        test map_column_types_to_target method, it shoudl retuns columns mapping using the mocked callable in the setup

        """
        self.assertDictEqual({
            'columns': ['_ID text', 'DOCUMENT default', '_SDC_EXTRACTED_AT default',
                        '_SDC_BATCHED_AT default', '_SDC_DELETED_AT text'],
            'primary_key': ['_ID']
        }, self.mongo.map_column_types_to_target())
