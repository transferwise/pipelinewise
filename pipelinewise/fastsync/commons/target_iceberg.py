"""
FastSync Target for Apache Iceberg on S3
"""
import gzip
import json
import logging
import os
from typing import List, Dict, Optional, Any
from datetime import datetime

import boto3
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, LongType, DoubleType, BooleanType,
    TimestampType, DateType, BinaryType, DecimalType, IntegerType
)
from pyiceberg.table import Table
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
import pyarrow as pa
import pyarrow.parquet as pq

LOGGER = logging.getLogger(__name__)


class FastSyncTargetIceberg:
    """
    Common functions for fastsync to Apache Iceberg
    """

    def __init__(self, connection_config: Dict, transformation_config: Optional[Dict] = None):
        """Initialize FastSync target for Iceberg"""
        self.connection_config = connection_config
        self.transformation_config = transformation_config
        self.catalog = None
        self.s3_client = None
        self.snowflake_conn = None

        # Initialize connections
        self._init_catalog()
        self._init_s3()

        # Initialize Snowflake if enabled
        if connection_config.get('snowflake_integration', {}).get('enabled', False):
            self._init_snowflake()

    def _init_catalog(self) -> None:
        """Initialize PyIceberg catalog with Glue backend"""
        catalog_config = {
            'type': 'glue',
            'glue.id': self.connection_config['glue_catalog_id'],
            'glue.region': self.connection_config.get('aws_region', 'us-east-1'),
            's3.region': self.connection_config.get('aws_region', 'us-east-1'),
        }

        # Add AWS credentials
        if 'aws_access_key_id' in self.connection_config:
            catalog_config['s3.access-key-id'] = self.connection_config['aws_access_key_id']
            catalog_config['s3.secret-access-key'] = self.connection_config['aws_secret_access_key']
            catalog_config['glue.access-key-id'] = self.connection_config['aws_access_key_id']
            catalog_config['glue.secret-access-key'] = self.connection_config['aws_secret_access_key']

        if 'aws_session_token' in self.connection_config:
            catalog_config['s3.session-token'] = self.connection_config['aws_session_token']
            catalog_config['glue.session-token'] = self.connection_config['aws_session_token']

        self.catalog = load_catalog('glue_catalog', **catalog_config)
        LOGGER.info('Initialized Glue catalog for FastSync')

    def _init_s3(self) -> None:
        """Initialize S3 client"""
        session_kwargs = {
            'region_name': self.connection_config.get('aws_region', 'us-east-1')
        }

        if 'aws_access_key_id' in self.connection_config:
            session_kwargs['aws_access_key_id'] = self.connection_config['aws_access_key_id']
            session_kwargs['aws_secret_access_key'] = self.connection_config['aws_secret_access_key']

        if 'aws_session_token' in self.connection_config:
            session_kwargs['aws_session_token'] = self.connection_config['aws_session_token']

        session = boto3.Session(**session_kwargs)
        self.s3_client = session.client('s3')

    def _init_snowflake(self) -> None:
        """Initialize Snowflake connection for external table management"""
        try:
            import snowflake.connector

            sf_config = self.connection_config['snowflake_integration']
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

    def _get_table_location(self, schema_name: str, table_name: str) -> str:
        """Get S3 location for table"""
        s3_bucket = self.connection_config['s3_bucket']
        s3_prefix = self.connection_config.get('s3_key_prefix', 'iceberg/')
        s3_prefix = s3_prefix.rstrip('/')
        return f's3://{s3_bucket}/{s3_prefix}/{schema_name}/{table_name}'

    def create_schema(self, schema_name: str) -> None:
        """Create schema (namespace) if not exists"""
        try:
            self.catalog.create_namespace(schema_name)
            LOGGER.info('Created namespace: %s', schema_name)
        except Exception:
            LOGGER.info('Namespace already exists: %s', schema_name)

    def drop_table(self, schema_name: str, table_name: str, is_temporary: bool = False) -> None:
        """Drop table if exists"""
        table_identifier = f'{schema_name}.{table_name}'
        try:
            self.catalog.drop_table(table_identifier)
            LOGGER.info('Dropped table: %s', table_identifier)
        except Exception as e:
            LOGGER.debug('Table does not exist or could not be dropped: %s - %s', table_identifier, e)

    def create_table(
        self,
        schema_name: str,
        table_name: str,
        columns: List[Dict],
        primary_key: Optional[List[str]] = None,
        is_temporary: bool = False,
        sort_columns: bool = False
    ) -> None:
        """Create Iceberg table"""
        table_identifier = f'{schema_name}.{table_name}'

        # Convert columns to Iceberg schema
        fields = []
        field_id = 1

        for col in columns:
            col_name = col['name']
            col_type = col['type']

            # Map to Iceberg types
            if col_type == 'STRING':
                iceberg_type = StringType()
            elif col_type in ('BIGINT', 'INTEGER'):
                iceberg_type = LongType()
            elif col_type in ('DOUBLE', 'NUMERIC'):
                iceberg_type = DoubleType()
            elif col_type == 'BOOLEAN':
                iceberg_type = BooleanType()
            elif col_type == 'TIMESTAMP':
                iceberg_type = TimestampType()
            elif col_type == 'DATE':
                iceberg_type = DateType()
            elif col_type == 'BINARY':
                iceberg_type = BinaryType()
            else:
                iceberg_type = StringType()

            # Check if column is nullable
            nullable = not col.get('not_null', False)
            required = not nullable and (primary_key and col_name in primary_key)

            fields.append(
                NestedField(
                    field_id=field_id,
                    name=col_name,
                    field_type=iceberg_type,
                    required=required
                )
            )
            field_id += 1

        iceberg_schema = Schema(*fields)

        # Get partition columns from config
        partition_columns = self.connection_config.get('partition_columns', [])
        partition_spec = self._create_partition_spec(iceberg_schema, partition_columns)

        # Get table location
        location = self._get_table_location(schema_name, table_name)

        # Create table with Copy-on-Write properties for Snowflake compatibility
        try:
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
            LOGGER.info('Created Iceberg table: %s at %s (CoW format)', table_identifier, location)

            # Create Snowflake external table if enabled
            if self.snowflake_conn:
                self._create_snowflake_external_table(schema_name, table_name, table_identifier)

        except Exception as e:
            LOGGER.error('Failed to create table %s: %s', table_identifier, e)
            raise

    def _create_partition_spec(self, schema: Schema, partition_columns: List[str]) -> Optional[PartitionSpec]:
        """Create partition spec from column names"""
        if not partition_columns:
            return None

        spec_builder = []
        for col_name in partition_columns:
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

    def _create_snowflake_external_table(
        self,
        schema_name: str,
        table_name: str,
        glue_table_identifier: str
    ) -> None:
        """Create Snowflake external Iceberg table"""
        try:
            sf_config = self.connection_config['snowflake_integration']
            sf_database = sf_config['database']
            external_volume = sf_config['external_volume']
            catalog_integration = sf_config['catalog_integration']

            cursor = self.snowflake_conn.cursor()

            # Create schema if not exists
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {sf_database}.{schema_name}')

            # Create external Iceberg table
            sql = f'''
            CREATE OR REPLACE ICEBERG TABLE {sf_database}.{schema_name}.{table_name}
                EXTERNAL_VOLUME = '{external_volume}'
                CATALOG = '{catalog_integration}'
                CATALOG_TABLE_NAME = '{glue_table_identifier}'
            '''

            cursor.execute(sql)
            LOGGER.info('Created Snowflake external table: %s.%s.%s', sf_database, schema_name, table_name)

        except Exception as e:
            LOGGER.warning('Failed to create Snowflake external table: %s', e)

    def copy_to_table(
        self,
        s3_key: str,
        schema_name: str,
        table_name: str,
        size_bytes: int,
        is_temporary: bool = False,
        skip_csv_header: bool = False
    ) -> None:
        """
        Load data from S3 CSV file into Iceberg table
        This method reads the CSV, converts to Parquet, and appends to Iceberg
        """
        table_identifier = f'{schema_name}.{table_name}'

        try:
            # Load table
            table = self.catalog.load_table(table_identifier)

            # Download and read CSV from S3
            s3_bucket = self.connection_config['s3_bucket']
            local_file = f'/tmp/{table_name}_{datetime.now().timestamp()}.csv.gz'

            LOGGER.info('Downloading %s from S3...', s3_key)
            self.s3_client.download_file(s3_bucket, s3_key, local_file)

            # Read CSV and convert to PyArrow table
            LOGGER.info('Reading CSV and converting to Parquet...')
            with gzip.open(local_file, 'rt', encoding='utf-8') as f:
                # Get schema field names
                field_names = [field.name for field in table.schema().fields]

                # Parse CSV
                import csv
                reader = csv.DictReader(f, fieldnames=field_names if not skip_csv_header else None)

                if skip_csv_header:
                    next(reader)  # Skip header

                # Collect records
                records = list(reader)

            # Convert to PyArrow table
            data = {field_name: [] for field_name in field_names}

            for record in records:
                for field_name in field_names:
                    value = record.get(field_name)
                    # TODO: Type conversion based on schema
                    data[field_name].append(value)

            pa_table = pa.table(data)

            # Append to Iceberg table
            LOGGER.info('Appending %d rows to Iceberg table %s', len(records), table_identifier)
            table.append(pa_table)

            LOGGER.info('Successfully loaded data into %s', table_identifier)

            # Cleanup
            os.remove(local_file)

        except Exception as e:
            LOGGER.error('Failed to copy data to table %s: %s', table_identifier, e)
            raise

    def grant_privilege(self, schema: str, privilege: str, grant_target: str) -> None:
        """
        Grant privilege - not applicable for Iceberg/S3
        Access control is managed via AWS IAM
        """
        LOGGER.debug('Grant privilege called but not applicable for Iceberg on S3')

    def close_connection(self) -> None:
        """Close connections"""
        if self.snowflake_conn:
            self.snowflake_conn.close()
