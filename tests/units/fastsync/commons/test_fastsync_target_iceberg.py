"""Unit tests for fastsync target_iceberg module"""
import unittest
from unittest.mock import Mock, patch, MagicMock, call

from pipelinewise.fastsync.commons.target_iceberg import FastSyncTargetIceberg


class S3MockIceberg:
    """Mocked S3 client"""

    def __init__(self):
        self.files_downloaded = []

    def download_file(self, bucket, key, local_file):
        """Mock S3 download"""
        self.files_downloaded.append((bucket, key, local_file))


class FastSyncTargetIcebergMock(FastSyncTargetIceberg):
    """Mocked FastSyncTargetIceberg class"""

    def __init__(self, connection_config, transformation_config=None):
        self.connection_config = connection_config
        self.transformation_config = transformation_config
        self.catalog = None
        self.s3_client = S3MockIceberg()
        self.snowflake_conn = None


class TestFastSyncTargetIceberg(unittest.TestCase):
    """Test FastSyncTargetIceberg class"""

    def setUp(self):
        """Set up test fixtures"""
        self.connection_config = {
            'aws_region': 'us-east-1',
            's3_bucket': 'test-bucket',
            's3_key_prefix': 'iceberg/',
            'glue_catalog_id': '123456789012',
            'default_target_schema': 'test_schema'
        }

    @patch('pipelinewise.fastsync.commons.target_iceberg.boto3')
    @patch('pipelinewise.fastsync.commons.target_iceberg.load_catalog')
    def test_init_catalog_with_access_keys(self, mock_load_catalog, mock_boto3):
        """Test catalog initialization with AWS access keys"""
        config = {
            **self.connection_config,
            'aws_access_key_id': 'test_key',
            'aws_secret_access_key': 'test_secret'
        }

        target = FastSyncTargetIceberg(config)

        mock_load_catalog.assert_called_once()
        call_kwargs = mock_load_catalog.call_args[1]
        self.assertEqual(call_kwargs['glue.id'], '123456789012')
        self.assertEqual(call_kwargs['s3.access-key-id'], 'test_key')
        self.assertEqual(call_kwargs['s3.secret-access-key'], 'test_secret')

    @patch('pipelinewise.fastsync.commons.target_iceberg.boto3')
    @patch('pipelinewise.fastsync.commons.target_iceberg.load_catalog')
    def test_init_catalog_with_session_token(self, mock_load_catalog, mock_boto3):
        """Test catalog initialization with AWS session token"""
        config = {
            **self.connection_config,
            'aws_access_key_id': 'test_key',
            'aws_secret_access_key': 'test_secret',
            'aws_session_token': 'test_token'
        }

        target = FastSyncTargetIceberg(config)

        call_kwargs = mock_load_catalog.call_args[1]
        self.assertEqual(call_kwargs['s3.session-token'], 'test_token')
        self.assertEqual(call_kwargs['glue.session-token'], 'test_token')

    @patch('pipelinewise.fastsync.commons.target_iceberg.boto3')
    @patch('pipelinewise.fastsync.commons.target_iceberg.load_catalog')
    def test_get_table_location(self, mock_load_catalog, mock_boto3):
        """Test S3 table location generation"""
        target = FastSyncTargetIceberg(self.connection_config)

        location = target._get_table_location('my_schema', 'my_table')
        self.assertEqual(location, 's3://test-bucket/iceberg/my_schema/my_table')

        # Test with trailing slash
        config_with_slash = {**self.connection_config, 's3_key_prefix': 'iceberg/'}
        target_slash = FastSyncTargetIceberg(config_with_slash)
        location_slash = target_slash._get_table_location('my_schema', 'my_table')
        self.assertEqual(location_slash, 's3://test-bucket/iceberg/my_schema/my_table')

    @patch('pipelinewise.fastsync.commons.target_iceberg.boto3')
    @patch('pipelinewise.fastsync.commons.target_iceberg.load_catalog')
    def test_create_schema(self, mock_load_catalog, mock_boto3):
        """Test schema/namespace creation"""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        target = FastSyncTargetIceberg(self.connection_config)

        # Test creating new schema
        target.create_schema('new_schema')
        mock_catalog.create_namespace.assert_called_once_with('new_schema')

        # Test when schema already exists (should not raise)
        mock_catalog.create_namespace.side_effect = Exception('Already exists')
        target.create_schema('existing_schema')  # Should not raise

    @patch('pipelinewise.fastsync.commons.target_iceberg.boto3')
    @patch('pipelinewise.fastsync.commons.target_iceberg.load_catalog')
    def test_drop_table(self, mock_load_catalog, mock_boto3):
        """Test table dropping"""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        target = FastSyncTargetIceberg(self.connection_config)

        # Test dropping existing table
        target.drop_table('test_schema', 'test_table')
        mock_catalog.drop_table.assert_called_once_with('test_schema.test_table')

        # Test dropping non-existent table (should not raise)
        mock_catalog.drop_table.side_effect = Exception('Not found')
        target.drop_table('test_schema', 'nonexistent')  # Should not raise

    @patch('pipelinewise.fastsync.commons.target_iceberg.boto3')
    @patch('pipelinewise.fastsync.commons.target_iceberg.load_catalog')
    def test_create_table_with_cow_properties(self, mock_load_catalog, mock_boto3):
        """Test table creation includes Copy-on-Write properties"""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        target = FastSyncTargetIceberg(self.connection_config)

        columns = [
            {'name': 'id', 'type': 'BIGINT', 'not_null': True},
            {'name': 'name', 'type': 'STRING'},
            {'name': 'amount', 'type': 'DOUBLE'},
            {'name': 'created_at', 'type': 'TIMESTAMP'}
        ]
        primary_key = ['id']

        target.create_table('test_schema', 'test_table', columns, primary_key)

        # Verify create_table was called
        mock_catalog.create_table.assert_called_once()
        call_kwargs = mock_catalog.create_table.call_args[1]

        # Verify CoW properties are set
        self.assertIn('properties', call_kwargs)
        properties = call_kwargs['properties']
        self.assertEqual(properties['write.format.default'], 'parquet')
        self.assertEqual(properties['write.delete.mode'], 'copy-on-write')
        self.assertEqual(properties['format-version'], '2')

        # Verify identifier
        self.assertEqual(call_kwargs['identifier'], 'test_schema.test_table')

    @patch('pipelinewise.fastsync.commons.target_iceberg.boto3')
    @patch('pipelinewise.fastsync.commons.target_iceberg.load_catalog')
    def test_create_table_type_mapping(self, mock_load_catalog, mock_boto3):
        """Test column type mapping"""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        target = FastSyncTargetIceberg(self.connection_config)

        columns = [
            {'name': 'col_string', 'type': 'STRING'},
            {'name': 'col_bigint', 'type': 'BIGINT'},
            {'name': 'col_integer', 'type': 'INTEGER'},
            {'name': 'col_double', 'type': 'DOUBLE'},
            {'name': 'col_numeric', 'type': 'NUMERIC'},
            {'name': 'col_boolean', 'type': 'BOOLEAN'},
            {'name': 'col_timestamp', 'type': 'TIMESTAMP'},
            {'name': 'col_date', 'type': 'DATE'},
            {'name': 'col_binary', 'type': 'BINARY'},
            {'name': 'col_unknown', 'type': 'UNKNOWN_TYPE'}
        ]

        target.create_table('test_schema', 'test_table', columns, [])

        # Verify schema was created (implicitly tests type mapping worked)
        mock_catalog.create_table.assert_called_once()
        call_kwargs = mock_catalog.create_table.call_args[1]
        schema = call_kwargs['schema']

        # All columns should be in schema
        self.assertEqual(len(schema.fields), len(columns))

    @patch('pipelinewise.fastsync.commons.target_iceberg.boto3')
    @patch('pipelinewise.fastsync.commons.target_iceberg.load_catalog')
    def test_create_table_with_partitioning(self, mock_load_catalog, mock_boto3):
        """Test table creation with partition columns"""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        config = {
            **self.connection_config,
            'partition_columns': ['year', 'month']
        }
        target = FastSyncTargetIceberg(config)

        columns = [
            {'name': 'id', 'type': 'BIGINT'},
            {'name': 'year', 'type': 'INTEGER'},
            {'name': 'month', 'type': 'INTEGER'},
            {'name': 'data', 'type': 'STRING'}
        ]

        target.create_table('test_schema', 'test_table', columns, [])

        # Verify partition spec was created
        mock_catalog.create_table.assert_called_once()
        call_kwargs = mock_catalog.create_table.call_args[1]
        self.assertIsNotNone(call_kwargs.get('partition_spec'))

    @patch('pipelinewise.fastsync.commons.target_iceberg.boto3')
    @patch('pipelinewise.fastsync.commons.target_iceberg.load_catalog')
    def test_grant_privilege_is_noop(self, mock_load_catalog, mock_boto3):
        """Test grant_privilege is a no-op for Iceberg"""
        target = FastSyncTargetIceberg(self.connection_config)

        # Should not raise an exception
        target.grant_privilege('test_schema', 'SELECT', 'public')

    @patch('pipelinewise.fastsync.commons.target_iceberg.boto3')
    @patch('pipelinewise.fastsync.commons.target_iceberg.load_catalog')
    def test_snowflake_integration_disabled(self, mock_load_catalog, mock_boto3):
        """Test initialization without Snowflake integration"""
        target = FastSyncTargetIceberg(self.connection_config)
        self.assertIsNone(target.snowflake_conn)

    @patch('pipelinewise.fastsync.commons.target_iceberg.snowflake')
    @patch('pipelinewise.fastsync.commons.target_iceberg.boto3')
    @patch('pipelinewise.fastsync.commons.target_iceberg.load_catalog')
    def test_snowflake_integration_enabled(self, mock_load_catalog, mock_boto3, mock_snowflake):
        """Test initialization with Snowflake integration"""
        config = {
            **self.connection_config,
            'snowflake_integration': {
                'enabled': True,
                'account': 'test_account',
                'user': 'test_user',
                'password': 'test_pass',
                'database': 'test_db',
                'external_volume': 'test_volume',
                'catalog_integration': 'test_integration'
            }
        }

        mock_conn = Mock()
        mock_snowflake.connector.connect.return_value = mock_conn

        target = FastSyncTargetIceberg(config)

        mock_snowflake.connector.connect.assert_called_once()
        self.assertIsNotNone(target.snowflake_conn)

    @patch('pipelinewise.fastsync.commons.target_iceberg.snowflake')
    @patch('pipelinewise.fastsync.commons.target_iceberg.boto3')
    @patch('pipelinewise.fastsync.commons.target_iceberg.load_catalog')
    def test_create_snowflake_external_table(self, mock_load_catalog, mock_boto3, mock_snowflake):
        """Test Snowflake external table creation"""
        config = {
            **self.connection_config,
            'snowflake_integration': {
                'enabled': True,
                'account': 'test_account',
                'user': 'test_user',
                'password': 'test_pass',
                'database': 'test_db',
                'external_volume': 'test_volume',
                'catalog_integration': 'test_integration'
            }
        }

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_snowflake.connector.connect.return_value = mock_conn
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        target = FastSyncTargetIceberg(config)

        columns = [{'name': 'id', 'type': 'BIGINT'}]
        target.create_table('test_schema', 'test_table', columns, [])

        # Verify Snowflake SQL was executed
        self.assertGreater(mock_cursor.execute.call_count, 0)

        # Check that CREATE ICEBERG TABLE command was issued
        calls = [str(call) for call in mock_cursor.execute.call_args_list]
        create_table_calls = [c for c in calls if 'CREATE' in c and 'ICEBERG' in c]
        self.assertGreater(len(create_table_calls), 0)

    @patch('pipelinewise.fastsync.commons.target_iceberg.boto3')
    @patch('pipelinewise.fastsync.commons.target_iceberg.load_catalog')
    def test_close_connection(self, mock_load_catalog, mock_boto3):
        """Test connection cleanup"""
        target = FastSyncTargetIcebergMock(self.connection_config)
        mock_snowflake_conn = Mock()
        target.snowflake_conn = mock_snowflake_conn

        target.close_connection()

        mock_snowflake_conn.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
