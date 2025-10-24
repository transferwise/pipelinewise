"""Unit tests for target-iceberg db_sync module"""
import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from target_iceberg.db_sync import DbSync


class TestDbSync(unittest.TestCase):
    """Test DbSync class"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = {
            'aws_region': 'us-east-1',
            's3_bucket': 'test-bucket',
            's3_key_prefix': 'iceberg/',
            'glue_catalog_id': '123456789012',
            'default_target_schema': 'test_schema',
            'batch_size_rows': 1000,
            'hard_delete': False
        }

    @patch('target_iceberg.db_sync.load_catalog')
    def test_init_catalog_with_access_keys(self, mock_load_catalog):
        """Test catalog initialization with AWS access keys"""
        config = {
            **self.config,
            'aws_access_key_id': 'test_key',
            'aws_secret_access_key': 'test_secret'
        }

        db_sync = DbSync(config)

        mock_load_catalog.assert_called_once()
        call_args = mock_load_catalog.call_args
        self.assertEqual(call_args[0][0], 'glue_catalog')
        self.assertEqual(call_args[1]['glue.id'], '123456789012')
        self.assertEqual(call_args[1]['s3.access-key-id'], 'test_key')
        self.assertEqual(call_args[1]['s3.secret-access-key'], 'test_secret')

    @patch('target_iceberg.db_sync.load_catalog')
    def test_init_catalog_with_session_token(self, mock_load_catalog):
        """Test catalog initialization with AWS session token"""
        config = {
            **self.config,
            'aws_access_key_id': 'test_key',
            'aws_secret_access_key': 'test_secret',
            'aws_session_token': 'test_token'
        }

        db_sync = DbSync(config)

        call_args = mock_load_catalog.call_args
        self.assertEqual(call_args[1]['s3.session-token'], 'test_token')
        self.assertEqual(call_args[1]['glue.session-token'], 'test_token')

    @patch('target_iceberg.db_sync.load_catalog')
    def test_singer_to_iceberg_type_conversions(self, mock_load_catalog):
        """Test Singer type to Iceberg type conversions"""
        db_sync = DbSync(self.config)

        # String types
        self.assertEqual(str(type(db_sync._singer_to_iceberg_type('string'))),
                        "<class 'pyiceberg.types.StringType'>")
        self.assertEqual(str(type(db_sync._singer_to_iceberg_type('string', 'date-time'))),
                        "<class 'pyiceberg.types.TimestampType'>")
        self.assertEqual(str(type(db_sync._singer_to_iceberg_type('string', 'date'))),
                        "<class 'pyiceberg.types.DateType'>")

        # Numeric types
        self.assertEqual(str(type(db_sync._singer_to_iceberg_type('integer'))),
                        "<class 'pyiceberg.types.LongType'>")
        self.assertEqual(str(type(db_sync._singer_to_iceberg_type('number'))),
                        "<class 'pyiceberg.types.DoubleType'>")

        # Boolean
        self.assertEqual(str(type(db_sync._singer_to_iceberg_type('boolean'))),
                        "<class 'pyiceberg.types.BooleanType'>")

        # Nullable types (list with null)
        self.assertEqual(str(type(db_sync._singer_to_iceberg_type(['string', 'null']))),
                        "<class 'pyiceberg.types.StringType'>")
        self.assertEqual(str(type(db_sync._singer_to_iceberg_type(['null', 'integer']))),
                        "<class 'pyiceberg.types.LongType'>")

    @patch('target_iceberg.db_sync.load_catalog')
    def test_create_iceberg_schema(self, mock_load_catalog):
        """Test creating Iceberg schema from Singer schema"""
        db_sync = DbSync(self.config)

        singer_schema = {
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': ['string', 'null']},
                'email': {'type': 'string'},
                'age': {'type': ['integer', 'null']},
                'created_at': {'type': 'string', 'format': 'date-time'},
                'is_active': {'type': 'boolean'}
            }
        }
        key_properties = ['id']

        iceberg_schema = db_sync._create_iceberg_schema(singer_schema, key_properties)

        # Check that all fields are created
        self.assertEqual(len(iceberg_schema.fields), 6)

        # Check field names
        field_names = [f.name for f in iceberg_schema.fields]
        self.assertIn('id', field_names)
        self.assertIn('name', field_names)
        self.assertIn('email', field_names)
        self.assertIn('age', field_names)
        self.assertIn('created_at', field_names)
        self.assertIn('is_active', field_names)

    @patch('target_iceberg.db_sync.load_catalog')
    def test_get_table_location(self, mock_load_catalog):
        """Test S3 table location generation"""
        db_sync = DbSync(self.config)

        location = db_sync._get_table_location('my_schema', 'my_table')
        self.assertEqual(location, 's3://test-bucket/iceberg/my_schema/my_table')

        # Test with trailing slash in prefix
        config_with_slash = {**self.config, 's3_key_prefix': 'iceberg/'}
        db_sync_slash = DbSync(config_with_slash)
        location_slash = db_sync_slash._get_table_location('my_schema', 'my_table')
        self.assertEqual(location_slash, 's3://test-bucket/iceberg/my_schema/my_table')

    @patch('target_iceberg.db_sync.load_catalog')
    def test_create_schema_if_not_exists_new_table(self, mock_load_catalog):
        """Test creating a new Iceberg table"""
        mock_catalog = Mock()
        mock_catalog.load_table.side_effect = Exception('Table not found')
        mock_catalog.create_namespace.return_value = None
        mock_table = Mock()
        mock_catalog.create_table.return_value = mock_table

        db_sync = DbSync(self.config)
        db_sync.catalog = mock_catalog

        singer_schema = {
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': 'string'}
            }
        }
        key_properties = ['id']

        db_sync.create_schema_if_not_exists('test_stream', singer_schema, key_properties)

        # Verify table creation was called
        mock_catalog.create_table.assert_called_once()
        call_kwargs = mock_catalog.create_table.call_args[1]

        # Verify CoW properties are set
        self.assertIn('properties', call_kwargs)
        self.assertEqual(call_kwargs['properties']['write.format.default'], 'parquet')
        self.assertEqual(call_kwargs['properties']['write.delete.mode'], 'copy-on-write')
        self.assertEqual(call_kwargs['properties']['format-version'], '2')

    @patch('target_iceberg.db_sync.load_catalog')
    def test_create_schema_if_not_exists_existing_table(self, mock_load_catalog):
        """Test loading an existing Iceberg table"""
        mock_catalog = Mock()
        mock_table = Mock()
        mock_catalog.load_table.return_value = mock_table

        db_sync = DbSync(self.config)
        db_sync.catalog = mock_catalog

        singer_schema = {
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': 'string'}
            }
        }
        key_properties = ['id']

        db_sync.create_schema_if_not_exists('test_stream', singer_schema, key_properties)

        # Verify table was loaded, not created
        mock_catalog.load_table.assert_called_once()
        mock_catalog.create_table.assert_not_called()

    @patch('target_iceberg.db_sync.load_catalog')
    def test_process_record(self, mock_load_catalog):
        """Test processing records"""
        db_sync = DbSync(self.config)
        db_sync.record_buffers['test_stream'] = []

        record = {'id': 1, 'name': 'Test'}
        db_sync.process_record('test_stream', record)

        self.assertEqual(len(db_sync.record_buffers['test_stream']), 1)
        self.assertEqual(db_sync.record_buffers['test_stream'][0], record)

    @patch('target_iceberg.db_sync.load_catalog')
    @patch('target_iceberg.db_sync.pa')
    def test_flush_stream(self, mock_pa, mock_load_catalog):
        """Test flushing buffered records"""
        mock_table = Mock()
        mock_schema_fields = [
            Mock(name='id'),
            Mock(name='name')
        ]
        mock_schema = Mock()
        mock_schema.fields = mock_schema_fields
        mock_table.schema.return_value = mock_schema

        mock_pa_table = Mock()
        mock_pa.table.return_value = mock_pa_table

        db_sync = DbSync(self.config)
        db_sync.tables['test_stream'] = mock_table
        db_sync.record_buffers['test_stream'] = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]

        db_sync.flush_stream('test_stream')

        # Verify append was called
        mock_table.append.assert_called_once_with(mock_pa_table)

        # Verify buffer was cleared
        self.assertEqual(len(db_sync.record_buffers['test_stream']), 0)

    @patch('target_iceberg.db_sync.load_catalog')
    def test_batch_size_triggers_flush(self, mock_load_catalog):
        """Test that reaching batch size triggers automatic flush"""
        config = {**self.config, 'batch_size_rows': 2}
        db_sync = DbSync(config)
        db_sync.record_buffers['test_stream'] = []

        mock_table = Mock()
        mock_schema_fields = [Mock(name='id')]
        mock_schema = Mock()
        mock_schema.fields = mock_schema_fields
        mock_table.schema.return_value = mock_schema
        db_sync.tables['test_stream'] = mock_table

        with patch('target_iceberg.db_sync.pa'):
            # Add records - should auto-flush when batch size is reached
            db_sync.process_record('test_stream', {'id': 1})
            self.assertEqual(len(db_sync.record_buffers['test_stream']), 1)

            db_sync.process_record('test_stream', {'id': 2})
            # Should have flushed and cleared buffer
            self.assertEqual(len(db_sync.record_buffers['test_stream']), 0)

    @patch('target_iceberg.db_sync.load_catalog')
    def test_snowflake_integration_disabled(self, mock_load_catalog):
        """Test initialization without Snowflake integration"""
        db_sync = DbSync(self.config)
        self.assertIsNone(db_sync.snowflake_conn)

    @patch('target_iceberg.db_sync.load_catalog')
    @patch('target_iceberg.db_sync.snowflake')
    def test_snowflake_integration_enabled(self, mock_snowflake_module, mock_load_catalog):
        """Test initialization with Snowflake integration"""
        config = {
            **self.config,
            'snowflake_integration': {
                'enabled': True,
                'account': 'test_account',
                'user': 'test_user',
                'password': 'test_pass',
                'database': 'test_db'
            }
        }

        mock_conn = Mock()
        mock_snowflake_module.connector.connect.return_value = mock_conn

        db_sync = DbSync(config)

        mock_snowflake_module.connector.connect.assert_called_once()
        self.assertIsNotNone(db_sync.snowflake_conn)


if __name__ == '__main__':
    unittest.main()
