import json
import os
import unittest
import uuid

import target_snowflake

from target_snowflake.db_sync import (
    DbSync,
    TABLE_FORMAT_MANAGED_ICEBERG_V3,
    TABLE_FORMAT_MISSING,
    TABLE_FORMAT_NATIVE,
    column_type,
)
from target_snowflake.exceptions import TableFormatDiscoveryException
from target_snowflake.upload_clients.s3_upload_client import S3UploadClient

try:
    import tests.integration.utils as test_utils
except ImportError:
    import utils as test_utils


REQUIRED_SNOWFLAKE_ENV = (
    'TARGET_SNOWFLAKE_ACCOUNT',
    'TARGET_SNOWFLAKE_DBNAME',
    'TARGET_SNOWFLAKE_USER',
    'TARGET_SNOWFLAKE_PRIVATE_KEY',
    'TARGET_SNOWFLAKE_WAREHOUSE',
)


class TestManagedIcebergV3Integration(unittest.TestCase):
    def setUp(self):
        missing_env = [name for name in REQUIRED_SNOWFLAKE_ENV if not os.environ.get(name)]
        if not (
            os.environ.get('TARGET_SNOWFLAKE_FILE_FORMAT_CSV')
            or os.environ.get('TARGET_SNOWFLAKE_FILE_FORMAT')
        ):
            missing_env.append('TARGET_SNOWFLAKE_FILE_FORMAT_CSV or TARGET_SNOWFLAKE_FILE_FORMAT')
        if missing_env:
            self.fail(f'Missing Snowflake integration environment: {", ".join(missing_env)}')

        run_id = uuid.uuid4().hex[:12].upper()
        self.schema = f'PW_ICEBERG_V3_{run_id}'
        self.config = test_utils.get_db_config()
        self.config.update({
            'default_target_schema': self.schema,
            'target_table_format': 'iceberg',
            'iceberg_version': 3,
            'data_flattening_max_level': 0,
            'hard_delete': True,
            'disable_table_cache': True,
            'file_format': (
                self.config.get('file_format')
                or os.environ.get('TARGET_SNOWFLAKE_FILE_FORMAT')
            ),
            'client_side_encryption_master_key': (
                self.config.get('client_side_encryption_master_key') or ''
            ),
        })

        base_s3_prefix = self.config.get('s3_key_prefix') or ''
        if base_s3_prefix and not base_s3_prefix.endswith('/'):
            base_s3_prefix += '/'
        self.s3_prefix = f'{base_s3_prefix}iceberg-v3-integration/{run_id}/'
        self.config['s3_key_prefix'] = self.s3_prefix

        self.snowflake = DbSync(self.config)
        self.database = self.snowflake.query(
            'SELECT CURRENT_DATABASE() AS "DATABASE_NAME"'
        )[0]['DATABASE_NAME']
        self.schema_fqtn = self._qualified_name(self.database, self.schema)

        self.addCleanup(self._remove_s3_objects)
        self.addCleanup(self._drop_schema)
        self.snowflake.query(f'CREATE SCHEMA {self.schema_fqtn}')

    @staticmethod
    def _quote_identifier(identifier):
        return '"' + identifier.replace('"', '""') + '"'

    @classmethod
    def _qualified_name(cls, *identifiers):
        return '.'.join(cls._quote_identifier(identifier) for identifier in identifiers)

    def _drop_schema(self):
        self.snowflake.query(f'DROP SCHEMA IF EXISTS {self.schema_fqtn} CASCADE')

    def _remove_s3_objects(self):
        bucket = self.config.get('s3_bucket')
        if not bucket:
            return

        s3_client = S3UploadClient(self.config).s3_client
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=self.s3_prefix):
            objects = [{'Key': item['Key']} for item in page.get('Contents', [])]
            if objects:
                s3_client.delete_objects(Bucket=bucket, Delete={'Objects': objects})

    def _create_table(self, table_name, table_format=None, iceberg_version=None):
        table_fqtn = self._qualified_name(self.database, self.schema, table_name)
        if table_format == 'iceberg':
            copy_on_write = (
                " ICEBERG_MERGE_ON_READ_BEHAVIOR='DISABLED'"
                if iceberg_version == 3
                else ''
            )
            self.snowflake.query(
                f'CREATE ICEBERG TABLE {table_fqtn} ("ID" NUMBER(19,0)) '
                f"CATALOG='SNOWFLAKE' ICEBERG_VERSION={iceberg_version}{copy_on_write}"
            )
        else:
            self.snowflake.query(f'CREATE TABLE {table_fqtn} ("ID" NUMBER(19,0))')

    def test_discovers_exact_table_format_with_wildcard_like_identifiers(self):
        missing_name = 'MISSING_%_TABLE'
        native_name = 'NATIVE_%_TABLE'
        iceberg_v2_name = 'ICEBERG_V2_%_TABLE'
        iceberg_v3_name = 'ICEBERG_V3_%_TABLE'

        self._create_table(f'{missing_name}_DECOY')
        self._create_table(native_name)
        self._create_table(iceberg_v2_name, table_format='iceberg', iceberg_version=2)
        self._create_table(iceberg_v3_name, table_format='iceberg', iceberg_version=3)

        self.assertEqual(
            self.snowflake.discover_table_format(self.schema, missing_name),
            TABLE_FORMAT_MISSING,
        )
        self.assertEqual(
            self.snowflake.discover_table_format(self.schema, native_name),
            TABLE_FORMAT_NATIVE,
        )
        with self.assertRaisesRegex(
            TableFormatDiscoveryException,
            'unsupported ICEBERG_VERSION 2',
        ):
            self.snowflake.discover_table_format(self.schema, iceberg_v2_name)
        self.assertEqual(
            self.snowflake.discover_table_format(self.schema, iceberg_v3_name),
            TABLE_FORMAT_MANAGED_ICEBERG_V3,
        )

    def test_update_columns_preserves_existing_zoned_timestamp_value(self):
        stream = 'zoned_timestamp_preservation'
        table_name = stream.upper()
        table_fqtn = self._qualified_name(self.database, self.schema, table_name)
        timestamp_value = '2026-08-21 14:35:42.123456 +05:45'
        timestamp_expression = (
            f"TO_TIMESTAMP_TZ('{timestamp_value}', "
            "'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')::TIMESTAMP_LTZ(6)"
        )
        self.snowflake.query(
            f'CREATE ICEBERG TABLE {table_fqtn} ("CREATED_AT" TIMESTAMP_LTZ(6)) '
            "CATALOG='SNOWFLAKE' ICEBERG_VERSION=3 "
            "ICEBERG_MERGE_ON_READ_BEHAVIOR='DISABLED'"
        )
        self.snowflake.query(
            f'INSERT INTO {table_fqtn} ("CREATED_AT") SELECT {timestamp_expression}'
        )
        self.assertEqual(
            self.snowflake.discover_table_format(self.schema, table_name),
            TABLE_FORMAT_MANAGED_ICEBERG_V3,
        )

        stream_schema_message = {
            'stream': stream,
            'schema': {
                'type': 'object',
                'properties': {
                    'created_at': {'type': ['null', 'string'], 'format': 'date-time'},
                },
            },
            'key_properties': [],
            'bookmark_properties': [],
        }
        stream_sync = DbSync(self.config, stream_schema_message)
        self.assertEqual(
            column_type(
                stream_sync.flatten_schema['created_at'],
                is_iceberg_table=True,
                iceberg_version=3,
            ).upper(),
            'TIMESTAMP_NTZ(6)',
        )

        stream_sync.update_columns(is_iceberg_table=True, iceberg_version=3)

        column_rows = self.snowflake.query(
            'SELECT "COLUMN_NAME", "DATA_TYPE", "DATETIME_PRECISION" '
            f'FROM {self._quote_identifier(self.database)}."INFORMATION_SCHEMA"."COLUMNS" '
            f"WHERE \"TABLE_SCHEMA\" = '{self.schema}' "
            f"AND \"TABLE_NAME\" = '{table_name}' "
            "AND \"COLUMN_NAME\" LIKE 'CREATED_AT%' "
            'ORDER BY "ORDINAL_POSITION"'
        )
        self.assertEqual(
            column_rows,
            [{
                'COLUMN_NAME': 'CREATED_AT',
                'DATA_TYPE': 'TIMESTAMP_LTZ',
                'DATETIME_PRECISION': 6,
            }],
        )
        value_rows = self.snowflake.query(
            f'SELECT "CREATED_AT" = {timestamp_expression} AS "VALUE_PRESERVED" '
            f'FROM {table_fqtn}'
        )
        self.assertEqual(value_rows, [{'VALUE_PRESERVED': True}])

    def test_singer_loads_variant_values_into_explicit_managed_iceberg_v3(self):
        stream = 'source-variant__payload'
        large_text = 'large-json-value-' + ('x' * (70 * 1024))
        rich_object = {
            'nested': {'json_null': None},
            'empty_object': {},
            'empty_array': [],
            'unicode': 'Καλημέρα コンニチハ 和毛泽东',
            'escaped': 'quote " backslash \\ newline\n',
        }
        rich_array = [
            None,
            {},
            [],
            'Καλημέρα コンニチハ',
            {'nested': None},
            'quote " backslash \\',
        ]
        records = [
            {
                'id': 1,
                'object_value': rich_object,
                'array_value': rich_array,
                'large_object': {'blob': large_text},
            },
            {
                'id': 2,
                'object_value': {},
                'array_value': [],
                'large_object': {},
            },
            {
                'id': 3,
                'object_value': None,
                'array_value': None,
                'large_object': None,
            },
        ]
        schema_message = {
            'type': 'SCHEMA',
            'stream': stream,
            'schema': {
                'type': 'object',
                'properties': {
                    'id': {'type': ['integer']},
                    'object_value': {'type': ['null', 'object']},
                    'array_value': {'type': ['null', 'array'], 'items': {}},
                    'large_object': {'type': ['null', 'object']},
                },
            },
            'key_properties': ['id'],
            'bookmark_properties': [],
        }
        singer_lines = [json.dumps(schema_message)]
        singer_lines.extend(
            json.dumps({
                'type': 'RECORD',
                'stream': stream,
                'record': record,
                'time_extracted': '2026-08-19T12:00:00Z',
            }, ensure_ascii=False)
            for record in records
        )

        target_snowflake.persist_lines(self.config, singer_lines)

        table_name = 'VARIANT__PAYLOAD'
        table_fqtn = self._qualified_name(self.database, self.schema, table_name)
        self.assertEqual(
            self.snowflake.discover_table_format(self.schema, table_name),
            TABLE_FORMAT_MANAGED_ICEBERG_V3,
        )
        merge_on_read_rows = self.snowflake.query(
            "SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR' "
            f'IN TABLE {table_fqtn}'
        )
        self.assertEqual(len(merge_on_read_rows), 1)
        self.assertEqual(str(merge_on_read_rows[0]['value']).upper(), 'DISABLED')
        self.assertEqual(str(merge_on_read_rows[0]['level']).upper(), 'TABLE')
        column_types = self.snowflake.query(
            'SELECT "COLUMN_NAME", "DATA_TYPE" '
            f'FROM {self._quote_identifier(self.database)}."INFORMATION_SCHEMA"."COLUMNS" '
            f"WHERE \"TABLE_SCHEMA\" = '{self.schema}' "
            f"AND \"TABLE_NAME\" = '{table_name}' "
            'ORDER BY "ORDINAL_POSITION"'
        )
        self.assertEqual(
            {column['COLUMN_NAME']: column['DATA_TYPE'] for column in column_types},
            {
                'ID': 'NUMBER',
                'OBJECT_VALUE': 'VARIANT',
                'ARRAY_VALUE': 'VARIANT',
                'LARGE_OBJECT': 'VARIANT',
                '_SDC_EXTRACTED_AT': 'TIMESTAMP_NTZ',
                '_SDC_BATCHED_AT': 'TIMESTAMP_NTZ',
                '_SDC_DELETED_AT': 'TEXT',
            },
        )

        loaded_rows = self.snowflake.query(
            'SELECT "ID", '
            'TO_JSON("OBJECT_VALUE") AS "OBJECT_VALUE", '
            'TO_JSON("ARRAY_VALUE") AS "ARRAY_VALUE", '
            'TO_JSON("LARGE_OBJECT") AS "LARGE_OBJECT", '
            'IS_NULL_VALUE("OBJECT_VALUE":"nested":"json_null") AS "NESTED_JSON_NULL", '
            '"OBJECT_VALUE" IS NULL AS "OBJECT_SQL_NULL", '
            '"ARRAY_VALUE" IS NULL AS "ARRAY_SQL_NULL" '
            f'FROM {table_fqtn} ORDER BY "ID"'
        )

        self.assertEqual(len(loaded_rows), 3)
        self.assertEqual([row['ID'] for row in loaded_rows], [1, 2, 3])
        self.assertEqual(json.loads(loaded_rows[0]['OBJECT_VALUE']), rich_object)
        self.assertEqual(json.loads(loaded_rows[0]['ARRAY_VALUE']), rich_array)
        self.assertEqual(json.loads(loaded_rows[0]['LARGE_OBJECT']), {'blob': large_text})
        self.assertGreater(len(large_text.encode('utf-8')), 64 * 1024)
        self.assertTrue(loaded_rows[0]['NESTED_JSON_NULL'])
        self.assertFalse(loaded_rows[0]['OBJECT_SQL_NULL'])
        self.assertFalse(loaded_rows[0]['ARRAY_SQL_NULL'])

        self.assertEqual(json.loads(loaded_rows[1]['OBJECT_VALUE']), {})
        self.assertEqual(json.loads(loaded_rows[1]['ARRAY_VALUE']), [])
        self.assertEqual(json.loads(loaded_rows[1]['LARGE_OBJECT']), {})
        self.assertFalse(loaded_rows[1]['OBJECT_SQL_NULL'])
        self.assertFalse(loaded_rows[1]['ARRAY_SQL_NULL'])

        self.assertIsNone(loaded_rows[2]['OBJECT_VALUE'])
        self.assertIsNone(loaded_rows[2]['ARRAY_VALUE'])
        self.assertIsNone(loaded_rows[2]['LARGE_OBJECT'])
        self.assertTrue(loaded_rows[2]['OBJECT_SQL_NULL'])
        self.assertTrue(loaded_rows[2]['ARRAY_SQL_NULL'])

    def test_singer_preserves_multiline_varchar_in_managed_iceberg_v3(self):
        stream = 'source-multiline__iceberg'
        record = {
            'id': 1,
            'script': 'def value = "初雪, ok"\r\n\tprintln(value)\nreturn value',
            'lf': 'left\nright',
            'cr': 'left\rright',
            'crlf': 'left\r\nright',
            'tab': 'left\tright',
            'literal_escapes': r'left\nright\tend\N',
            'literal_null_marker': r'\N',
            'quoted_comma': 'say "hello", world',
            'unicode': '初雪',
            'trailing_backslash': 'C:\\data\\',
            'empty_value': '',
            'null_value': None,
        }
        properties = {'id': {'type': ['integer']}}
        properties.update({
            column: {'type': ['null', 'string']}
            for column in record
            if column != 'id'
        })
        singer_lines = [
            json.dumps({
                'type': 'SCHEMA',
                'stream': stream,
                'schema': {'type': 'object', 'properties': properties},
                'key_properties': ['id'],
                'bookmark_properties': [],
            }),
            json.dumps({
                'type': 'RECORD',
                'stream': stream,
                'record': record,
                'time_extracted': '2026-08-24T12:00:00Z',
            }, ensure_ascii=False),
        ]

        target_snowflake.persist_lines(self.config, singer_lines)

        table_name = 'MULTILINE__ICEBERG'
        table_fqtn = self._qualified_name(self.database, self.schema, table_name)
        self.assertEqual(
            self.snowflake.discover_table_format(self.schema, table_name),
            TABLE_FORMAT_MANAGED_ICEBERG_V3,
        )
        selected_columns = ', '.join(
            self._quote_identifier(column.upper())
            for column in record
        )
        self.assertEqual(
            self.snowflake.query(f'SELECT {selected_columns} FROM {table_fqtn}'),
            [{column.upper(): value for column, value in record.items()}],
        )
        script_column = self.snowflake.query(
            'SELECT "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH" '
            f'FROM {self._quote_identifier(self.database)}."INFORMATION_SCHEMA"."COLUMNS" '
            f"WHERE \"TABLE_SCHEMA\" = '{self.schema}' "
            f"AND \"TABLE_NAME\" = '{table_name}' "
            "AND \"COLUMN_NAME\" = 'SCRIPT'"
        )
        self.assertEqual(
            script_column,
            [{'DATA_TYPE': 'TEXT', 'CHARACTER_MAXIMUM_LENGTH': 134217728}],
        )
