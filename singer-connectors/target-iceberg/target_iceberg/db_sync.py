"""Database synchronization for Iceberg target"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

import singer
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, LongType, DoubleType, BooleanType,
    TimestampType, DateType, BinaryType, DecimalType, StructType,
    ListType, MapType, IntegerType, FloatType
)
from pyiceberg.table import Table
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform, YearTransform, MonthTransform, DayTransform
import pyarrow as pa

LOGGER = logging.getLogger('target_iceberg')


class DbSync:
    """Handle database synchronization to Iceberg"""

    def __init__(self, config: Dict):
        """Initialize DbSync with configuration"""
        self.config = config
        self.catalog = None
        self.tables: Dict[str, Table] = {}
        self.record_buffers: Dict[str, List[Dict]] = {}
        self.batch_size = config.get('batch_size_rows', 100000)
        self.hard_delete = config.get('hard_delete', False)

        # Initialize catalog
        self._init_catalog()

        # Initialize Snowflake connection if enabled
        self.snowflake_conn = None
        if config.get('snowflake_integration', {}).get('enabled', False):
            self._init_snowflake()

    def _init_catalog(self) -> None:
        """Initialize PyIceberg catalog with Glue backend"""
        catalog_config = {
            'type': 'glue',
            'glue.id': self.config['glue_catalog_id'],
            'glue.region': self.config.get('aws_region', 'us-east-1'),
            's3.region': self.config.get('aws_region', 'us-east-1'),
        }

        # Add AWS credentials
        if 'aws_access_key_id' in self.config:
            catalog_config['s3.access-key-id'] = self.config['aws_access_key_id']
            catalog_config['s3.secret-access-key'] = self.config['aws_secret_access_key']
            catalog_config['glue.access-key-id'] = self.config['aws_access_key_id']
            catalog_config['glue.secret-access-key'] = self.config['aws_secret_access_key']

        if 'aws_session_token' in self.config:
            catalog_config['s3.session-token'] = self.config['aws_session_token']
            catalog_config['glue.session-token'] = self.config['aws_session_token']

        self.catalog = load_catalog('glue_catalog', **catalog_config)
        LOGGER.info('Initialized Glue catalog')

    def _init_snowflake(self) -> None:
        """Initialize Snowflake connection for external table management"""
        try:
            import snowflake.connector

            sf_config = self.config['snowflake_integration']
            self.snowflake_conn = snowflake.connector.connect(
                account=sf_config['account'],
                user=sf_config.get('user'),
                password=sf_config.get('password'),
                database=sf_config.get('database'),
                warehouse=sf_config.get('warehouse'),
                role=sf_config.get('role')
            )
            LOGGER.info('Initialized Snowflake connection for external tables')
        except Exception as e:
            LOGGER.warning('Failed to initialize Snowflake connection: %s', e)
            self.snowflake_conn = None

    def _singer_to_iceberg_type(self, singer_type: Any, singer_format: Optional[str] = None) -> Any:
        """Convert Singer schema type to Iceberg type"""
        # Handle nullable types
        if isinstance(singer_type, list):
            # Remove 'null' and get the actual type
            non_null_types = [t for t in singer_type if t != 'null']
            if not non_null_types:
                return StringType()
            singer_type = non_null_types[0]

        # Handle string formats
        if singer_type == 'string':
            if singer_format == 'date-time':
                return TimestampType()
            elif singer_format == 'date':
                return DateType()
            else:
                return StringType()
        elif singer_type == 'integer':
            return LongType()
        elif singer_type == 'number':
            return DoubleType()
        elif singer_type == 'boolean':
            return BooleanType()
        elif singer_type == 'object':
            return StringType()  # Store as JSON string
        elif singer_type == 'array':
            return StringType()  # Store as JSON string
        else:
            return StringType()

    def _create_iceberg_schema(self, singer_schema: Dict, key_properties: List[str]) -> Schema:
        """Create Iceberg schema from Singer schema"""
        fields = []
        field_id = 1

        properties = singer_schema.get('properties', {})

        for field_name, field_def in properties.items():
            field_type = field_def.get('type', 'string')
            field_format = field_def.get('format')

            iceberg_type = self._singer_to_iceberg_type(field_type, field_format)

            # Check if field is required (not nullable)
            is_nullable = True
            if isinstance(field_type, list):
                is_nullable = 'null' in field_type

            required = not is_nullable and field_name in key_properties

            fields.append(
                NestedField(
                    field_id=field_id,
                    name=field_name,
                    field_type=iceberg_type,
                    required=required
                )
            )
            field_id += 1

        return Schema(*fields)

    def _get_table_location(self, schema_name: str, table_name: str) -> str:
        """Get S3 location for table"""
        s3_bucket = self.config['s3_bucket']
        s3_prefix = self.config.get('s3_key_prefix', 'iceberg/')

        # Remove trailing slash if present
        s3_prefix = s3_prefix.rstrip('/')

        return f's3://{s3_bucket}/{s3_prefix}/{schema_name}/{table_name}'

    def _create_partition_spec(self, schema: Schema, partition_columns: List[str]) -> Optional[PartitionSpec]:
        """Create partition spec from column names"""
        if not partition_columns:
            return None

        # TODO: Support more partition transforms (year, month, day, bucket, truncate)
        # For now, use identity transform
        spec_builder = []
        for col_name in partition_columns:
            # Find field in schema
            field = next((f for f in schema.fields if f.name == col_name), None)
            if field:
                spec_builder.append(
                    PartitionField(
                        source_id=field.field_id,
                        field_id=1000 + field.field_id,
                        transform=IdentityTransform(),
                        name=col_name
                    )
                )

        return PartitionSpec(*spec_builder) if spec_builder else None

    def create_schema_if_not_exists(
        self,
        stream: str,
        schema: Dict,
        key_properties: List[str]
    ) -> None:
        """Create Iceberg table if it doesn't exist"""
        # Parse stream name (format: schema-table or just table)
        if '-' in stream:
            schema_name, table_name = stream.split('-', 1)
        else:
            schema_name = self.config.get('default_target_schema', 'default')
            table_name = stream

        table_identifier = f'{schema_name}.{table_name}'

        try:
            # Try to load existing table
            table = self.catalog.load_table(table_identifier)
            self.tables[stream] = table
            LOGGER.info('Loaded existing table: %s', table_identifier)

            # TODO: Handle schema evolution

        except Exception:
            # Table doesn't exist, create it
            LOGGER.info('Creating new table: %s', table_identifier)

            # Create namespace if it doesn't exist
            try:
                self.catalog.create_namespace(schema_name)
            except Exception:
                pass  # Namespace might already exist

            # Create Iceberg schema
            iceberg_schema = self._create_iceberg_schema(schema, key_properties)

            # Get partition columns from config
            partition_columns = self.config.get('partition_columns', [])
            partition_spec = self._create_partition_spec(iceberg_schema, partition_columns)

            # Get table location
            location = self._get_table_location(schema_name, table_name)

            # Create table with Copy-on-Write properties for Snowflake compatibility
            table = self.catalog.create_table(
                identifier=table_identifier,
                schema=iceberg_schema,
                location=location,
                partition_spec=partition_spec,
                properties={
                    'write.format.default': 'parquet',
                    'write.delete.mode': 'copy-on-write',
                    'format-version': '2'
                }
            )

            self.tables[stream] = table
            LOGGER.info('Created table: %s at %s (CoW format)', table_identifier, location)

            # Create Snowflake external table if enabled
            if self.snowflake_conn:
                self._create_snowflake_external_table(schema_name, table_name, table_identifier)

        # Initialize record buffer for this stream
        if stream not in self.record_buffers:
            self.record_buffers[stream] = []

    def _create_snowflake_external_table(
        self,
        schema_name: str,
        table_name: str,
        glue_table_identifier: str
    ) -> None:
        """Create Snowflake external Iceberg table"""
        try:
            sf_config = self.config['snowflake_integration']
            sf_database = sf_config['database']
            sf_schema = schema_name  # Use same schema name in Snowflake
            external_volume = sf_config['external_volume']
            catalog_integration = sf_config['catalog_integration']

            cursor = self.snowflake_conn.cursor()

            # Create schema if not exists
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {sf_database}.{sf_schema}')

            # Create external Iceberg table
            glue_catalog_id = self.config['glue_catalog_id']
            sql = f'''
            CREATE OR REPLACE ICEBERG TABLE {sf_database}.{sf_schema}.{table_name}
                EXTERNAL_VOLUME = '{external_volume}'
                CATALOG = '{catalog_integration}'
                CATALOG_TABLE_NAME = '{glue_table_identifier}'
            '''

            cursor.execute(sql)
            LOGGER.info('Created Snowflake external table: %s.%s.%s', sf_database, sf_schema, table_name)

        except Exception as e:
            LOGGER.warning('Failed to create Snowflake external table: %s', e)

    def process_record(self, stream: str, record: Dict, version: Optional[int] = None) -> None:
        """Process a record, buffering for batch write"""
        self.record_buffers[stream].append(record)

        # Flush if batch size reached
        if len(self.record_buffers[stream]) >= self.batch_size:
            self.flush_stream(stream)

    def flush_stream(self, stream: str) -> None:
        """Flush buffered records for a stream to Iceberg"""
        if stream not in self.record_buffers or not self.record_buffers[stream]:
            return

        records = self.record_buffers[stream]
        table = self.tables[stream]

        LOGGER.info('Flushing %d records to %s', len(records), stream)

        try:
            # Convert records to PyArrow table
            # Note: This is a simplified implementation
            # In production, you'd want to handle type conversion more carefully

            # Get schema field names
            field_names = [field.name for field in table.schema().fields]

            # Prepare data dictionary
            data = {field_name: [] for field_name in field_names}

            for record in records:
                for field_name in field_names:
                    value = record.get(field_name)
                    data[field_name].append(value)

            # Create PyArrow table
            pa_table = pa.table(data)

            # Append to Iceberg table
            table.append(pa_table)

            LOGGER.info('Successfully flushed %d records to %s', len(records), stream)

        except Exception as e:
            LOGGER.error('Failed to flush records to %s: %s', stream, e)
            raise

        # Clear buffer
        self.record_buffers[stream] = []

    def flush_all_streams(self) -> None:
        """Flush all buffered streams"""
        for stream in list(self.record_buffers.keys()):
            self.flush_stream(stream)

    def close(self) -> None:
        """Close connections"""
        if self.snowflake_conn:
            self.snowflake_conn.close()
