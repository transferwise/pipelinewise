"""
Integration tests for target-iceberg

These tests require:
- AWS credentials configured
- S3 bucket access
- AWS Glue Catalog access
- (Optional) Snowflake account for Snowflake integration tests

Set ICEBERG_INTEGRATION_TESTS=1 environment variable to run these tests
"""
import os
import json
import unittest
from unittest.mock import patch, Mock

# Skip integration tests unless explicitly enabled
INTEGRATION_TESTS_ENABLED = os.getenv('ICEBERG_INTEGRATION_TESTS', '0') == '1'


@unittest.skipUnless(INTEGRATION_TESTS_ENABLED, 'Integration tests disabled. Set ICEBERG_INTEGRATION_TESTS=1 to enable.')
class TestTargetIcebergIntegration(unittest.TestCase):
    """Integration tests for target-iceberg"""

    @classmethod
    def setUpClass(cls):
        """Set up test configuration"""
        cls.config = {
            'aws_region': os.getenv('AWS_REGION', 'us-east-1'),
            's3_bucket': os.getenv('ICEBERG_TEST_BUCKET', 'test-bucket'),
            's3_key_prefix': 'integration-tests/',
            'glue_catalog_id': os.getenv('AWS_ACCOUNT_ID', '123456789012'),
            'default_target_schema': 'test_integration',
            'batch_size_rows': 100
        }

    def test_create_table_and_insert_data(self):
        """Test creating table and inserting data"""
        from target_iceberg.db_sync import DbSync

        db_sync = DbSync(self.config)

        # Create schema
        singer_schema = {
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': 'string'},
                'created_at': {'type': 'string', 'format': 'date-time'}
            }
        }
        key_properties = ['id']

        db_sync.create_schema_if_not_exists('test_stream', singer_schema, key_properties)

        # Insert test data
        records = [
            {'id': 1, 'name': 'Alice', 'created_at': '2024-01-01T00:00:00Z'},
            {'id': 2, 'name': 'Bob', 'created_at': '2024-01-02T00:00:00Z'}
        ]

        for record in records:
            db_sync.process_record('test_stream', record)

        # Flush to Iceberg
        db_sync.flush_stream('test_stream')

        # Verify table exists in catalog
        table = db_sync.catalog.load_table('test_integration.test_stream')
        self.assertIsNotNone(table)

        # Cleanup
        db_sync.close()

    def test_table_has_cow_properties(self):
        """Test that created tables have Copy-on-Write properties"""
        from target_iceberg.db_sync import DbSync

        db_sync = DbSync(self.config)

        singer_schema = {
            'properties': {
                'id': {'type': 'integer'}
            }
        }

        db_sync.create_schema_if_not_exists('cow_test_stream', singer_schema, ['id'])

        # Load table and check properties
        table = db_sync.catalog.load_table('test_integration.cow_test_stream')
        properties = table.properties

        self.assertEqual(properties.get('write.format.default'), 'parquet')
        self.assertEqual(properties.get('write.delete.mode'), 'copy-on-write')
        self.assertEqual(properties.get('format-version'), '2')

        db_sync.close()


@unittest.skipUnless(
    INTEGRATION_TESTS_ENABLED and os.getenv('SNOWFLAKE_ACCOUNT'),
    'Snowflake integration tests require SNOWFLAKE_ACCOUNT to be set'
)
class TestSnowflakeIntegration(unittest.TestCase):
    """Integration tests for Snowflake external tables"""

    @classmethod
    def setUpClass(cls):
        """Set up Snowflake test configuration"""
        cls.config = {
            'aws_region': os.getenv('AWS_REGION', 'us-east-1'),
            's3_bucket': os.getenv('ICEBERG_TEST_BUCKET'),
            's3_key_prefix': 'integration-tests/',
            'glue_catalog_id': os.getenv('AWS_ACCOUNT_ID'),
            'default_target_schema': 'test_integration',
            'snowflake_integration': {
                'enabled': True,
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'user': os.getenv('SNOWFLAKE_USER'),
                'password': os.getenv('SNOWFLAKE_PASSWORD'),
                'database': os.getenv('SNOWFLAKE_DATABASE', 'TEST_DB'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
                'external_volume': os.getenv('SNOWFLAKE_EXTERNAL_VOLUME'),
                'catalog_integration': os.getenv('SNOWFLAKE_CATALOG_INTEGRATION')
            }
        }

    def test_create_snowflake_external_table(self):
        """Test creating Snowflake external Iceberg table"""
        from target_iceberg.db_sync import DbSync

        db_sync = DbSync(self.config)

        singer_schema = {
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': 'string'}
            }
        }

        db_sync.create_schema_if_not_exists('snowflake_test', singer_schema, ['id'])

        # Verify Snowflake connection was established
        self.assertIsNotNone(db_sync.snowflake_conn)

        # Query Snowflake to verify external table exists
        cursor = db_sync.snowflake_conn.cursor()
        cursor.execute(f"SHOW ICEBERG TABLES IN SCHEMA {self.config['snowflake_integration']['database']}.test_integration")
        tables = cursor.fetchall()

        # Check if our table is in the list
        table_names = [row[1] for row in tables]
        self.assertIn('snowflake_test', table_names)

        db_sync.close()


if __name__ == '__main__':
    unittest.main()
