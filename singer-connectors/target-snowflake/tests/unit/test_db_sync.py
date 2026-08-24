import json
import unittest

from unittest.mock import MagicMock, patch, call

from target_snowflake import db_sync
from target_snowflake.exceptions import (
    PrimaryKeyNotFoundException,
    TableFormatDiscoveryException,
    TableFormatMismatchException,
)
from target_snowflake.file_formats.csv import REQUIRED_FILE_FORMAT_OPTIONS


def _csv_file_format_result():
    return [{
        'type': 'CSV',
        'format_options': json.dumps(REQUIRED_FILE_FORMAT_OPTIONS),
    }]


class TestDBSync(unittest.TestCase):
    """
    Unit Tests
    """

    def setUp(self):
        self.config = {}

        self.json_types = {
            'str': {"type": ["string"]},
            'str_or_null': {"type": ["string", "null"]},
            'dt': {"type": ["string"], "format": "date-time"},
            'dt_or_null': {"type": ["string", "null"], "format": "date-time"},
            'd': {"type": ["string"], "format": "date"},
            'd_or_null': {"type": ["string", "null"], "format": "date"},
            'time': {"type": ["string"], "format": "time"},
            'time_or_null': {"type": ["string", "null"], "format": "time"},
            'binary': {"type": ["string", "null"], "format": "binary"},
            'num': {"type": ["number"]},
            'int': {"type": ["integer"]},
            'int_or_str': {"type": ["integer", "string"]},
            'bool': {"type": ["boolean"]},
            'obj': {"type": ["object"]},
            'arr': {"type": ["array"]},
        }

    def test_config_validation(self):
        """Test configuration validator"""
        validator = db_sync.validate_config
        empty_config = {}
        minimal_config = {
            'account': "dummy-value",
            'dbname': "dummy-value",
            'user': "dummy-value",
            'private_key': "dummy-key",
            'warehouse': "dummy-value",
            'default_target_schema': "dummy-value",
            'file_format': "dummy-value"
        }

        # Config validator returns a list of errors
        # If the list is empty then the configuration is valid otherwise invalid

        # Empty configuration should fail - (nr_of_errors >= 0)
        self.assertGreater(len(validator(empty_config)), 0)

        # Minimal configuration should pass - (nr_of_errors == 0)
        self.assertEqual(len(validator(minimal_config)), 0)

        # Configuration without schema references - (nr_of_errors >= 0)
        config_with_no_schema = minimal_config.copy()
        config_with_no_schema.pop('default_target_schema')
        self.assertGreater(len(validator(config_with_no_schema)), 0)

        # Configuration with schema mapping - (nr_of_errors >= 0)
        config_with_schema_mapping = minimal_config.copy()
        config_with_schema_mapping.pop('default_target_schema')
        config_with_schema_mapping['schema_mapping'] = {
            "dummy_stream": {
                "target_schema": "dummy_schema"
            }
        }
        self.assertEqual(len(validator(config_with_schema_mapping)), 0)

        # Configuration with external stage
        config_with_external_stage = minimal_config.copy()
        config_with_external_stage['s3_bucket'] = 'dummy-value'
        config_with_external_stage['stage'] = 'dummy-value'
        self.assertEqual(len(validator(config_with_external_stage)), 0)

        # Configuration with invalid stage: Only s3_bucket defined - (nr_of_errors >= 0)
        config_with_external_stage = minimal_config.copy()
        config_with_external_stage['s3_bucket'] = 'dummy-value'
        self.assertGreater(len(validator(config_with_external_stage)), 0)

        # Configuration with invalid stage: Only stage defined - (nr_of_errors >= 0)
        config_with_external_stage = minimal_config.copy()
        config_with_external_stage['stage'] = 'dummy-value'
        self.assertGreater(len(validator(config_with_external_stage)), 0)

        # Configuration with archive_load_files but no s3_bucket
        config_with_archive_load_files = minimal_config.copy()
        config_with_archive_load_files['archive_load_files'] = True
        self.assertGreater(len(validator(config_with_external_stage)), 0)

    def test_column_type_mapping(self):
        """Test JSON type to Snowflake column type mappings"""
        mapper = db_sync.column_type

        # Snowflake column types
        sf_types = {
            'str': 'varchar(134217728)',
            'str_or_null': 'varchar(134217728)',
            'dt': 'timestamp_ntz',
            'dt_or_null': 'timestamp_ntz',
            'd': 'date',
            'd_or_null': 'date',
            'time': 'time',
            'time_or_null': 'time',
            'binary': 'binary',
            'num': 'float',
            'int': 'number',
            'int_or_str': 'varchar(134217728)',
            'bool': 'boolean',
            'obj': 'variant',
            'arr': 'variant',
        }

        # Mapping from JSON schema types to Snowflake column types
        for key, val in self.json_types.items():
            self.assertEqual(mapper(val), sf_types[key])

    def test_column_trans(self):
        """Test column transformation"""
        trans = db_sync.column_trans

        # Snowflake column transformations
        sf_trans = {
            'str': '',
            'str_or_null': '',
            'dt': '',
            'dt_or_null': '',
            'd': '',
            'd_or_null': '',
            'time': '',
            'time_or_null': '',
            'binary': 'to_binary',
            'num': '',
            'int': '',
            'int_or_str': '',
            'bool': '',
            'obj': 'parse_json',
            'arr': 'parse_json',
        }

        # Getting transformations for every JSON type
        for key, val in self.json_types.items():
            self.assertEqual(trans(val), sf_trans[key])

    def test_mariadb_json_root_union_maps_to_variant(self):
        """Every MariaDB JSON root type retains VARIANT parsing."""
        mariadb_json = {
            'type': ['null', 'object', 'array', 'string', 'number', 'boolean'],
            'format': 'mariadb-json',
        }

        self.assertEqual(
            db_sync.column_type(
                mariadb_json,
                is_iceberg_table=True,
                iceberg_version=3,
            ),
            'variant',
        )
        self.assertEqual(db_sync.column_trans(mariadb_json), 'parse_json')

    def test_create_query_tag(self):
        self.assertIsNone(db_sync.create_query_tag(None))
        self.assertEqual(db_sync.create_query_tag('This is a test query tag'), 'This is a test query tag')
        self.assertEqual(db_sync.create_query_tag('Loading into {{database}}.{{schema}}.{{table}}',
                                                  database='test_database',
                                                  schema='test_schema',
                                                  table='test_table'),
                         'Loading into test_database.test_schema.test_table')
        self.assertEqual(db_sync.create_query_tag('Loading into {{database}}.{{schema}}.{{table}}',
                                                  database=None,
                                                  schema=None,
                                                  table=None), 'Loading into ..')

        # JSON formatted query tags with variables
        json_query_tag = db_sync.create_query_tag(
            '{"database": "{{database}}", "schema": "{{schema}}", "table": "{{table}}"}',
            database='test_database',
            schema='test_schema',
            table='test_table')
        # Load the generated JSON formatted query tag to make sure it's a valid JSON
        self.assertEqual(json.loads(json_query_tag), {
            'database': 'test_database',
            'schema': 'test_schema',
            'table': 'test_table'
        })

        # JSON formatted query tags with variables quotes in the middle
        json_query_tag = db_sync.create_query_tag(
            '{"database": "{{database}}", "schema": "{{schema}}", "table": "{{table}}"}',
            database='test"database',
            schema='test"schema',
            table='test"table')

        # Load the generated JSON formatted query tag to make sure it's a valid JSON
        self.assertEqual(json.loads(json_query_tag), {
            'database': 'test"database',
            'schema': 'test"schema',
            'table': 'test"table'
        })

        # JSON formatted query tags with quoted variables
        json_query_tag = db_sync.create_query_tag(
            '{"database": "{{database}}", "schema": "{{schema}}", "table": "{{table}}"}',
            database='"test_database"',
            schema='"test_schema"',
            table='"test_table"')
        # Load the generated JSON formatted query tag to make sure it's a valid JSON
        self.assertEqual(json.loads(json_query_tag), {
            'database': 'test_database',
            'schema': 'test_schema',
            'table': 'test_table'
        })

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_parallelism(self, query_patch):
        query_patch.return_value = _csv_file_format_result()

        minimal_config = {
            'account': "dummy-value",
            'dbname': "dummy-value",
            'user': "dummy-value",
            'private_key': "dummy-key",
            'warehouse': "dummy-value",
            'default_target_schema': "dummy-value",
            'file_format': "dummy-value"
        }

        # Using external stages should allow parallelism
        external_stage_with_parallel = {
            's3_bucket': 'dummy-bucket',
            'stage': 'dummy_schema.dummy_stage',
            'parallelism': 5
        }

        self.assertEqual(db_sync.DbSync({**minimal_config,
                                         **external_stage_with_parallel}).connection_config['parallelism'], 5)

        # Using table stages should allow parallelism
        table_stage_with_parallel = {
            'parallelism': 5
        }
        self.assertEqual(db_sync.DbSync({**minimal_config,
                                         **table_stage_with_parallel}).connection_config['parallelism'], 5)

    @patch('target_snowflake.upload_clients.s3_upload_client.S3UploadClient.copy_object')
    @patch('target_snowflake.db_sync.DbSync.query')
    def test_copy_to_archive(self, query_patch, copy_object_patch):
        query_patch.return_value = _csv_file_format_result()
        minimal_config = {
            'account': "dummy-value",
            'dbname': "dummy-value",
            'user': "dummy-value",
            'private_key': "dummy-key",
            'warehouse': "dummy-value",
            'default_target_schema': "dummy-value",
            'file_format': "dummy-value",
            's3_bucket': 'dummy-bucket',
            'stage': 'dummy_schema.dummy_stage'
        }

        # Assert default values (same bucket, 'archive' as the archive prefix)
        s3_config = {}
        dbsync = db_sync.DbSync({**minimal_config, **s3_config})
        dbsync.copy_to_archive('source/file', 'tap/schema/file', {'meta': "data"})

        self.assertEqual(copy_object_patch.call_args[0][0], 'dummy-bucket/source/file')
        self.assertEqual(copy_object_patch.call_args[0][1], 'dummy-bucket')
        self.assertEqual(copy_object_patch.call_args[0][2], 'archive/tap/schema/file')

        # Assert custom archive bucket and prefix
        s3_config = {
            'archive_load_files_s3_bucket': "custom-bucket",
            'archive_load_files_s3_prefix': "custom-prefix"
        }
        dbsync = db_sync.DbSync({**minimal_config, **s3_config})
        dbsync.copy_to_archive('source/file', 'tap/schema/file', {'meta': "data"})

        self.assertEqual(copy_object_patch.call_args[0][0], 'dummy-bucket/source/file')
        self.assertEqual(copy_object_patch.call_args[0][1], 'custom-bucket')
        self.assertEqual(copy_object_patch.call_args[0][2], 'custom-prefix/tap/schema/file')

    def test_safe_column_name(self):
        self.assertEqual(db_sync.safe_column_name("columnname"), '"COLUMNNAME"')
        self.assertEqual(db_sync.safe_column_name("columnName"), '"COLUMNNAME"')
        self.assertEqual(db_sync.safe_column_name("column-name"), '"COLUMN-NAME"')
        self.assertEqual(db_sync.safe_column_name("column name"), '"COLUMN NAME"')

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_record_primary_key_string(self, query_patch):
        query_patch.return_value = _csv_file_format_result()
        minimal_config = {
            'account': "dummy-value",
            'dbname': "dummy-value",
            'user': "dummy-value",
            'private_key': "dummy-key",
            'warehouse': "dummy-value",
            'default_target_schema': "dummy-value",
            'file_format': "dummy-value"
        }

        stream_schema_message = {"stream": "public-table1",
                                 "schema": {
                                     "properties": {
                                         "id": {"type": ["integer"]},
                                         "c_str": {"type": ["null", "string"]},
                                         "c_bool": {"type": ["boolean"]}
                                     }},
                                 "key_properties": ["id"]}

        # Single primary key string
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        self.assertEqual(dbsync.record_primary_key_string({'id': 123}), '123')

        # Composite primary key string
        stream_schema_message['key_properties'] = ['id', 'c_str']
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        self.assertEqual(dbsync.record_primary_key_string({'id': 123, 'c_str': 'xyz'}), '123,xyz')

        # Missing field as PK
        stream_schema_message['key_properties'] = ['invalid_col']
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        with self.assertRaisesRegex(PrimaryKeyNotFoundException,
                                    r"Primary key 'invalid_col' does not exist in record or is null\. Available "
                                    r"fields: \['id', 'c_str'\]"):
            dbsync.record_primary_key_string({'id': 123, 'c_str': 'xyz'})

        # Null PK field
        stream_schema_message['key_properties'] = ['id']
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        with self.assertRaisesRegex(PrimaryKeyNotFoundException,
                                    r"Primary key 'id' does not exist in record or is null\. Available "
                                    r"fields: \['id', 'c_str'\]"):
            dbsync.record_primary_key_string({'id': None, 'c_str': 'xyz'})

        # falsy PK field accepted
        stream_schema_message['key_properties'] = ['id']
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        self.assertEqual(dbsync.record_primary_key_string({'id': 0, 'c_str': 'xyz'}), '0')

        # falsy PK field accepted
        stream_schema_message['key_properties'] = ['id', 'c_bool']
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        self.assertEqual(dbsync.record_primary_key_string({'id': 1, 'c_bool': False, 'c_str': 'xyz'}), '1,False')

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_patch_record_mode_and_present_flattened_columns(self, query_patch):
        query_patch.return_value = _csv_file_format_result()
        minimal_config = {
            'account': 'dummy-value',
            'dbname': 'dummy-value',
            'user': 'dummy-value',
            'private_key': 'dummy-key',
            'warehouse': 'dummy-value',
            'default_target_schema': 'dummy-value',
            'file_format': 'dummy-value',
            'data_flattening_max_level': 1,
        }
        stream_schema_message = {
            'stream': 'public-table1',
            'schema': {
                'x-pipelinewise-record-update-mode': 'PATCH',
                'properties': {
                    'id': {'type': ['integer']},
                    'payload': {'type': ['null', 'string']},
                    '_sdc_extracted_at': {'type': ['null', 'string']},
                    '_sdc_batched_at': {'type': ['null', 'string']},
                    '_sdc_deleted_at': {'type': ['null', 'string']},
                    'profile': {
                        'type': ['null', 'object'],
                        'properties': {
                            'first': {'type': ['null', 'string']},
                            'last': {'type': ['null', 'string']},
                        },
                    },
                    'untouched': {'type': ['null', 'string']},
                },
            },
            'key_properties': ['id'],
        }

        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)

        self.assertEqual(dbsync.record_update_mode, db_sync.RECORD_UPDATE_MODE_PATCH)
        self.assertEqual(
            dbsync.present_column_names({'id': 1, 'payload': None, 'profile': {'first': 'Ada'}}),
            ('id', 'payload', 'profile__first', 'profile__last'),
        )
        self.assertEqual(
            dbsync.present_column_names({
                'id': 1,
                '_sdc_extracted_at': '2026-08-08T12:00:00Z',
                '_sdc_batched_at': '2026-08-08T12:00:01Z',
                '_sdc_deleted_at': '2026-08-08T12:00:02Z',
            }),
            ('_sdc_batched_at', '_sdc_deleted_at', '_sdc_extracted_at', 'id'),
        )

    @patch('target_snowflake.db_sync.DbSync.query')
    @patch('target_snowflake.db_sync.DbSync._load_file_merge')
    def test_load_file_restricts_patch_update_columns(self, load_file_merge_patch, query_patch):
        query_patch.return_value = _csv_file_format_result()
        load_file_merge_patch.return_value = (0, 1)
        minimal_config = {
            'account': 'dummy-value',
            'dbname': 'dummy-value',
            'user': 'dummy-value',
            'private_key': 'dummy-key',
            'warehouse': 'dummy-value',
            'default_target_schema': 'dummy-value',
            'file_format': 'dummy-value',
        }
        stream_schema_message = {
            'stream': 'public-table1',
            'schema': {
                'properties': {
                    'id': {'type': ['integer']},
                    'payload': {'type': ['null', 'string']},
                },
            },
            'key_properties': ['id'],
        }
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)

        dbsync.load_file('dummy-key', 1, 256, update_column_names=('id',))

        self.assertEqual(load_file_merge_patch.call_args.kwargs['update_columns'], {'"ID"'})

    @patch('target_snowflake.db_sync.DbSync.query')
    @patch('target_snowflake.db_sync.DbSync._load_file_merge')
    def test_merge_failure_message(self, load_file_merge_patch, query_patch):
        LOGGER_NAME = "target_snowflake"
        query_patch.return_value = _csv_file_format_result()
        minimal_config = {
            'account': "dummy_account",
            'dbname': "dummy_dbname",
            'user': "dummy_user",
            'private_key': "dummy_key",
            'warehouse': "dummy_warehouse",
            'default_target_schema': "dummy_default_target_schema",
            'file_format': "dummy_file_format",
        }

        stream_schema_message = {
            "stream": "dummy_stream",
            "schema": {
                "properties": {
                    "id": {"type": ["integer"]},
                    "c_str": {"type": ["null", "string"]}
                }
            },
            "key_properties": ["id"]
        }

        # Single primary key string
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        load_file_merge_patch.side_effect = Exception()
        expected_msg = (
            f'ERROR:{LOGGER_NAME}:Error while executing MERGE query '
            f'for table "{minimal_config["default_target_schema"]}."{stream_schema_message["stream"].upper()}"" '
            f'in stream "{stream_schema_message["stream"]}"'
        )
        with self.assertRaises(Exception), self.assertLogs(logger=LOGGER_NAME, level="ERROR") as captured_logs:
            dbsync.load_file(s3_key="dummy-key", count=256, size_bytes=256)
        self.assertIn(expected_msg, captured_logs.output)

    @patch('target_snowflake.db_sync.DbSync.query')
    @patch('target_snowflake.db_sync.DbSync._load_file_copy')
    def test_copy_failure_message(self, load_file_copy_patch, query_patch):
        LOGGER_NAME = "target_snowflake"
        query_patch.return_value = _csv_file_format_result()
        minimal_config = {
            'account': "dummy_account",
            'dbname': "dummy_dbname",
            'user': "dummy_user",
            'private_key': "dummy_key",
            'warehouse': "dummy_warehouse",
            'default_target_schema': "dummy_default_target_schema",
            'file_format': "dummy_file_format",
        }

        stream_schema_message = {
            "stream": "dummy_stream",
            "schema": {
                "properties": {
                    "id": {"type": ["integer"]},
                    "c_str": {"type": ["null", "string"]}
                }
            },
            "key_properties": []
        }

        # Single primary key string
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        load_file_copy_patch.side_effect = Exception()
        expected_msg = (
            f'ERROR:{LOGGER_NAME}:Error while executing COPY query '
            f'for table "{minimal_config["default_target_schema"]}."{stream_schema_message["stream"].upper()}"" '
            f'in stream "{stream_schema_message["stream"]}"'
        )
        with self.assertRaises(Exception), self.assertLogs(logger=LOGGER_NAME, level="ERROR") as captured_logs:
            dbsync.load_file(s3_key="dummy-key", count=256, size_bytes=256)
        self.assertIn(expected_msg, captured_logs.output)

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_with_no_changes_to_pk(self, query_patch):
        minimal_config = {
            'account': "dummy-account",
            'dbname': "dummy-db",
            'user': "dummy-user",
            'private_key': "dummy-key",
            'warehouse': "dummy-wh",
            'default_target_schema': "dummy-schema",
            'file_format': "dummy-file-format"
        }

        stream_schema_message = {"stream": "public-table1",
                                 "schema": {
                                     "properties": {
                                         "id": {"type": ["integer"]},
                                         "c_str": {"type": ["null", "string"]}}},
                                 "key_properties": ["id"]}

        table_cache = [
            {
                'SCHEMA_NAME': 'DUMMY-SCHEMA',
                'TABLE_NAME': 'TABLE1',
                'COLUMN_NAME': 'ID',
                'DATA_TYPE': 'NUMBER'
            },
            {
                'SCHEMA_NAME': 'DUMMY-SCHEMA',
                'TABLE_NAME': 'TABLE1',
                'COLUMN_NAME': 'C_STR',
                'DATA_TYPE': 'TEXT'
            }
        ]
        query_patch.side_effect = [
            _csv_file_format_result(),           # SHOW FILE FORMATS
            [{'name': 'TABLE1', 'is_iceberg': 'N'}],
            [{'column_name': 'ID'}],     # show primary keys
            None                          # ALTER TABLE
        ]

        dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
        dbsync.sync_table()

        query_patch.assert_has_calls([
            call('SHOW FILE FORMATS LIKE \'dummy-file-format\''),
            call('SHOW TABLES IN SCHEMA "DUMMY-DB"."DUMMY-SCHEMA" STARTS WITH \'TABLE1\''),
            call('show primary keys in table dummy-db.dummy-schema."TABLE1";'),
            call(['alter table dummy-schema."TABLE1" alter column "ID" drop not null;'])
        ])

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_with_new_pk_in_stream(self, query_patch):
        minimal_config = {
            'account': "dummy-account",
            'dbname': "dummy-db",
            'user': "dummy-user",
            'private_key': "dummy-key",
            'warehouse': "dummy-wh",
            'default_target_schema': "dummy-schema",
            'file_format': "dummy-file-format"
        }

        stream_schema_message = {"stream": "public-table1",
                                 "schema": {
                                     "properties": {
                                         "id": {"type": ["integer"]},
                                         "c_str": {"type": ["null", "string"]},
                                         "name": {"type": ["string"]},
                                     }
                                 },
                                 "key_properties": ["id", "name"]}

        table_cache = [
            {
                'SCHEMA_NAME': 'DUMMY-SCHEMA',
                'TABLE_NAME': 'TABLE1',
                'COLUMN_NAME': 'ID',
                'DATA_TYPE': 'NUMBER'
            },
            {
                'SCHEMA_NAME': 'DUMMY-SCHEMA',
                'TABLE_NAME': 'TABLE1',
                'COLUMN_NAME': 'C_STR',
                'DATA_TYPE': 'TEXT'
            },
            {
                'SCHEMA_NAME': 'DUMMY-SCHEMA',
                'TABLE_NAME': 'TABLE1',
                'COLUMN_NAME': 'NAME',
                'DATA_TYPE': 'TEXT'
            }
        ]
        query_patch.side_effect = [
            _csv_file_format_result(),           # SHOW FILE FORMATS
            [{'name': 'TABLE1', 'is_iceberg': 'N'}],
            [{'column_name': 'ID'}],     # show primary keys
            None                          # ALTER TABLE
        ]

        dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
        dbsync.sync_table()

        # due to usage of sets in the code, order of columns in queries in not guaranteed
        # so have to break assertions to account for this.
        calls = query_patch.call_args_list
        self.assertEqual(4, len(calls))

        self.assertEqual('SHOW FILE FORMATS LIKE \'dummy-file-format\'', calls[0][0][0])
        self.assertEqual(
            'SHOW TABLES IN SCHEMA "DUMMY-DB"."DUMMY-SCHEMA" STARTS WITH \'TABLE1\'',
            calls[1][0][0],
        )
        self.assertEqual('show primary keys in table dummy-db.dummy-schema."TABLE1";', calls[2][0][0])

        self.assertEqual('alter table dummy-schema."TABLE1" drop primary key;', calls[3][0][0][0])

        self.assertIn(calls[3][0][0][1], {'alter table dummy-schema."TABLE1" add primary key("ID", "NAME");',
                                          'alter table dummy-schema."TABLE1" add primary key("NAME", "ID");'})

        self.assertListEqual(sorted(calls[3][0][0][2:]),
                             [
                                 'alter table dummy-schema."TABLE1" alter column "ID" drop not null;',
                                 'alter table dummy-schema."TABLE1" alter column "NAME" drop not null;',
                             ]
                             )

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_with_stream_that_changes_to_have_no_pk(self, query_patch):
        minimal_config = {
            'account': "dummy-account",
            'dbname': "dummy-db",
            'user': "dummy-user",
            'private_key': "dummy-key",
            'warehouse': "dummy-wh",
            'default_target_schema': "dummy-schema",
            'file_format': "dummy-file-format"
        }

        stream_schema_message = {"stream": "public-table1",
                                 "schema": {
                                     "properties": {
                                         "id": {"type": ["integer"]},
                                         "c_str": {"type": ["null", "string"]}}},
                                 "key_properties": []}

        table_cache = [
            {
                'SCHEMA_NAME': 'DUMMY-SCHEMA',
                'TABLE_NAME': 'TABLE1',
                'COLUMN_NAME': 'ID',
                'DATA_TYPE': 'NUMBER'
            },
            {
                'SCHEMA_NAME': 'DUMMY-SCHEMA',
                'TABLE_NAME': 'TABLE1',
                'COLUMN_NAME': 'C_STR',
                'DATA_TYPE': 'TEXT'
            }
        ]
        query_patch.side_effect = [
            _csv_file_format_result(),           # SHOW FILE FORMATS
            [{'name': 'TABLE1', 'is_iceberg': 'N'}],
            [{'column_name': 'ID'}],     # show primary keys
            None                          # ALTER TABLE
        ]

        dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
        dbsync.sync_table()

        query_patch.assert_has_calls([
            call('SHOW FILE FORMATS LIKE \'dummy-file-format\''),
            call('SHOW TABLES IN SCHEMA "DUMMY-DB"."DUMMY-SCHEMA" STARTS WITH \'TABLE1\''),
            call('show primary keys in table dummy-db.dummy-schema."TABLE1";'),
            call(['alter table dummy-schema."TABLE1" drop primary key;',
                  'alter table dummy-schema."TABLE1" alter column "ID" drop not null;'])
        ])

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_with_stream_that_has_no_pk_but_get_a_new_one(self, query_patch):
        minimal_config = {
            'account': "dummy-account",
            'dbname': "dummy-db",
            'user': "dummy-user",
            'private_key': "dummy-key",
            'warehouse': "dummy-wh",
            'default_target_schema': "dummy-schema",
            'file_format': "dummy-file-format"
        }

        stream_schema_message = {"stream": "public-table1",
                                 "schema": {
                                     "properties": {
                                         "id": {"type": ["integer"]},
                                         "c_str": {"type": ["null", "string"]}}},
                                 "key_properties": ['id']}

        table_cache = [
            {
                'SCHEMA_NAME': 'DUMMY-SCHEMA',
                'TABLE_NAME': 'TABLE1',
                'COLUMN_NAME': 'ID',
                'DATA_TYPE': 'NUMBER'
            },
            {
                'SCHEMA_NAME': 'DUMMY-SCHEMA',
                'TABLE_NAME': 'TABLE1',
                'COLUMN_NAME': 'C_STR',
                'DATA_TYPE': 'TEXT'
            }
        ]
        query_patch.side_effect = [
            _csv_file_format_result(),           # SHOW FILE FORMATS
            [{'name': 'TABLE1', 'is_iceberg': 'N'}],
            [],                           # show primary keys (no existing PK)
            None                          # ALTER TABLE add PK
        ]

        dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
        dbsync.sync_table()

        query_patch.assert_has_calls([
            call('SHOW FILE FORMATS LIKE \'dummy-file-format\''),
            call('SHOW TABLES IN SCHEMA "DUMMY-DB"."DUMMY-SCHEMA" STARTS WITH \'TABLE1\''),
            call('show primary keys in table dummy-db.dummy-schema."TABLE1";'),
            call(['alter table dummy-schema."TABLE1" add primary key("ID");',
                  'alter table dummy-schema."TABLE1" alter column "ID" drop not null;'])
        ])

    def test_column_clause_native(self):
        """Native column clauses use maximum-width strings without Iceberg mapping."""
        self.assertEqual(
            db_sync.column_clause('my_text', self.json_types['str']),
            '"MY_TEXT" varchar(134217728)'
        )
        self.assertEqual(
            db_sync.column_clause('my_obj', self.json_types['obj']),
            '"MY_OBJ" variant'
        )
        self.assertEqual(
            db_sync.column_clause('my_int', self.json_types['int']),
            '"MY_INT" number'
        )

    def test_column_type_mapping_explicit_iceberg_v3(self):
        """The explicit v3 mapping uses only v3-compatible column types."""
        self.assertEqual(
            db_sync.column_type(
                self.json_types['str'],
                is_iceberg_table=True,
                iceberg_version=3,
            ),
            'varchar(134217728)',
        )
        self.assertEqual(
            db_sync.column_type(
                self.json_types['int_or_str'],
                is_iceberg_table=True,
                iceberg_version=3,
            ),
            'varchar(134217728)',
        )
        self.assertEqual(
            db_sync.column_type(self.json_types['obj'], is_iceberg_table=True, iceberg_version=3),
            'variant',
        )
        self.assertEqual(
            db_sync.column_type(self.json_types['arr'], is_iceberg_table=True, iceberg_version=3),
            'variant',
        )
        self.assertEqual(
            db_sync.column_type(self.json_types['int'], is_iceberg_table=True, iceberg_version=3),
            'number(38,0)',
        )
        self.assertEqual(
            db_sync.column_type(self.json_types['num'], is_iceberg_table=True, iceberg_version=3),
            'double',
        )
        self.assertEqual(
            db_sync.column_type(
                self.json_types['time'],
                is_iceberg_table=True,
                iceberg_version=3,
            ),
            'time(6)',
        )
        self.assertEqual(
            db_sync.column_type(
                self.json_types['dt'],
                is_iceberg_table=True,
                iceberg_version=3,
            ),
            'timestamp_ntz(6)',
        )
        self.assertEqual(
            db_sync.column_type(
                self.json_types['binary'],
                is_iceberg_table=True,
                iceberg_version=3,
            ),
            'binary(67108864)',
        )
        self.assertEqual(
            db_sync.column_type(self.json_types['binary']),
            'binary',
        )
        for invalid_version in (None, 2, 4, 3.0, True, '3'):
            with self.subTest(invalid_version=invalid_version), self.assertRaisesRegex(
                ValueError,
                'requires integer version 3',
            ):
                db_sync.column_type(
                    self.json_types['obj'],
                    is_iceberg_table=True,
                    iceberg_version=invalid_version,
                )

    @staticmethod
    def _format_discovery_sync(query_results):
        sync = object.__new__(db_sync.DbSync)
        sync.connection_config = {'dbname': 'dummy-db'}
        sync.query = MagicMock(side_effect=query_results)
        return sync

    @staticmethod
    def _query_sync():
        sync = object.__new__(db_sync.DbSync)
        sync.logger = MagicMock()
        connection_context = MagicMock()
        connection = connection_context.__enter__.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.rowcount = 0
        cursor.sfqid = 'query-id'
        cursor.fetchall.return_value = []
        sync.open_connection = MagicMock(return_value=connection_context)
        return sync, cursor

    @staticmethod
    def _table_sync_config(**overrides):
        config = {
            'account': 'dummy-account',
            'dbname': 'dummy-db',
            'user': 'dummy-user',
            'private_key': 'dummy-key',
            'warehouse': 'dummy-wh',
            'default_target_schema': 'dummy-schema',
            'file_format': 'dummy-file-format',
            'hard_delete': True,
        }
        config.update(overrides)
        return config

    @staticmethod
    def _table_sync_schema():
        return {
            'stream': 'public-table1',
            'schema': {
                'properties': {
                    'id': {'type': ['integer']},
                    'payload': {'type': ['object']},
                },
            },
            'key_properties': ['id'],
        }

    def test_query_without_params_does_not_bind_wildcard_like_identifier(self):
        sync, cursor = self._query_sync()
        query = (
            'SHOW TABLES IN SCHEMA "DUMMY-DB"."SCHEMA_1%" '
            "STARTS WITH 'TABLE_1%'"
        )

        sync.query(query)

        cursor.execute.assert_called_once_with(query)

    def test_query_retains_last_query_id_binding_for_query_lists(self):
        sync, cursor = self._query_sync()
        queries = [
            'SHOW TABLES IN SCHEMA "DUMMY-DB"."SCHEMA_1%"',
            'SELECT * FROM TABLE(RESULT_SCAN(%(LAST_QID)s))',
        ]

        sync.query(queries)

        self.assertEqual(cursor.execute.call_args_list[0], call('START TRANSACTION'))
        self.assertEqual(cursor.execute.call_args_list[1], call(queries[0]))
        self.assertEqual(cursor.execute.call_args_list[2].args[0], queries[1])
        self.assertEqual(cursor.execute.call_args_list[2].args[1]['LAST_QID'], 'query-id')

    def test_query_retains_caller_supplied_params(self):
        sync, cursor = self._query_sync()
        query = 'SELECT %(value)s'

        sync.query(query, params={'value': 1})

        cursor.execute.assert_called_once_with(
            query,
            {'value': 1, 'LAST_QID': None},
        )

    def test_discover_table_format_missing_and_native_use_exact_name(self):
        missing = self._format_discovery_sync([
            [{'name': 'TABLE10', 'is_iceberg': 'N'}],
        ])
        self.assertEqual(
            missing.discover_table_format('dummy_schema', 'table1'),
            db_sync.TABLE_FORMAT_MISSING,
        )

        native = self._format_discovery_sync([
            [
                {'name': 'TABLE10', 'is_iceberg': 'N'},
                {'name': 'TABLE1', 'is_iceberg': 'N'},
            ],
        ])
        self.assertEqual(
            native.discover_table_format('dummy_schema', 'table1'),
            db_sync.TABLE_FORMAT_NATIVE,
        )
        native.query.assert_called_once_with(
            'SHOW TABLES IN SCHEMA "DUMMY-DB"."DUMMY_SCHEMA" STARTS WITH \'TABLE1\''
        )

    def test_discover_table_format_treats_wildcard_characters_as_literals(self):
        sync = self._format_discovery_sync([
            [
                {'name': 'TABLE_1%_OTHER', 'is_iceberg': 'N'},
                {'name': 'TABLE_1%', 'is_iceberg': 'N'},
            ],
        ])

        self.assertEqual(
            sync.discover_table_format('schema_1%', 'table_1%'),
            db_sync.TABLE_FORMAT_NATIVE,
        )
        sync.query.assert_called_once_with(
            'SHOW TABLES IN SCHEMA "DUMMY-DB"."SCHEMA_1%" STARTS WITH \'TABLE_1%\''
        )

    def test_discover_table_format_managed_v3(self):
        sync = self._format_discovery_sync([
            [{'name': 'TABLE1', 'is_iceberg': 'Y'}],
            [
                {'name': 'TABLE10', 'catalog_name': 'SNOWFLAKE'},
                {'name': 'TABLE1', 'catalog_name': 'SNOWFLAKE'},
            ],
            [{'key': 'ICEBERG_VERSION', 'value': '3'}],
            [
                {
                    'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                    'value': 'DISABLED',
                    'level': 'TABLE',
                },
            ],
            [
                {
                    'COLUMN_NAME': 'ID',
                    'DATA_TYPE': 'NUMBER',
                    'NUMERIC_PRECISION': 38,
                    'NUMERIC_SCALE': 0,
                    'CHARACTER_MAXIMUM_LENGTH': None,
                    'IS_NULLABLE': 'NO',
                },
                {
                    'COLUMN_NAME': 'BODY',
                    'DATA_TYPE': 'TEXT',
                    'CHARACTER_MAXIMUM_LENGTH': 134217728,
                    'IS_NULLABLE': 'YES',
                },
            ],
        ])
        self.assertEqual(
            sync.discover_table_format('dummy_schema', 'table1'),
            db_sync.TABLE_FORMAT_MANAGED_ICEBERG_V3,
        )
        self.assertEqual(
            sync.query.call_args_list[-2],
            call(
                "SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR' IN TABLE "
                '"DUMMY-DB"."DUMMY_SCHEMA"."TABLE1"'
            ),
        )
        self.assertEqual(
            sync.query.call_args_list[-1],
            call(
                'SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, '
                'DATETIME_PRECISION, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE '
                'FROM "DUMMY-DB".INFORMATION_SCHEMA.COLUMNS '
                'WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s '
                'ORDER BY ORDINAL_POSITION',
                params={'schema': 'DUMMY_SCHEMA', 'table': 'TABLE1'},
            ),
        )

    def test_discover_managed_v3_rejects_narrow_varchar_before_mutation(self):
        """An existing narrow string fails before target-snowflake can evolve it."""
        sync = self._format_discovery_sync([
            [{'name': 'TABLE1', 'is_iceberg': 'Y'}],
            [{'name': 'TABLE1', 'catalog_name': 'SNOWFLAKE'}],
            [{'key': 'ICEBERG_VERSION', 'value': '3'}],
            [{
                'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                'value': 'DISABLED',
                'level': 'TABLE',
            }],
            [{
                'COLUMN_NAME': 'BODY',
                'DATA_TYPE': 'TEXT',
                'CHARACTER_MAXIMUM_LENGTH': 16777216,
            }],
        ])

        with self.assertRaises(TableFormatMismatchException) as error:
            sync.discover_table_format('dummy_schema', 'table1')

        self.assertIn('CHARACTER_MAXIMUM_LENGTH 16777216', str(error.exception))
        self.assertIn('VARCHAR(134217728)', str(error.exception))
        self.assertIn('ALTER ICEBERG TABLE', str(error.exception))
        self.assertIn('recreate the table', str(error.exception))
        self.assertTrue(
            all(
                query_call.args[0].startswith(('SELECT', 'SHOW'))
                for query_call in sync.query.call_args_list
            )
        )

    def test_discover_managed_v3_rejects_non_table_copy_on_write_contract(self):
        invalid_parameters = (
            (
                [
                    {
                        'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                        'value': 'AUTO',
                        'level': 'ACCOUNT',
                    },
                ],
                "value 'AUTO' at level 'ACCOUNT'",
            ),
            (
                [
                    {
                        'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                        'value': 'ENABLED',
                        'level': 'TABLE',
                    },
                ],
                "value 'ENABLED' at level 'TABLE'",
            ),
            (
                [
                    {
                        'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                        'value': 'DISABLED',
                        'level': 'ACCOUNT',
                    },
                ],
                "value 'DISABLED' at level 'ACCOUNT'",
            ),
            ([], 'did not return exactly one'),
            (None, 'did not return exactly one'),
            (
                [
                    {
                        'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                        'value': 'DISABLED',
                        'level': 'TABLE',
                    },
                    {
                        'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                        'value': 'DISABLED',
                        'level': 'TABLE',
                    },
                ],
                'did not return exactly one',
            ),
            (
                [
                    {
                        'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                        'value': 'DISABLED',
                    },
                ],
                'returned malformed',
            ),
            ([None], 'returned malformed'),
        )
        for parameter_rows, message in invalid_parameters:
            with self.subTest(parameter_rows=parameter_rows):
                sync = self._format_discovery_sync([
                    [{'name': 'TABLE1', 'is_iceberg': 'Y'}],
                    [{'name': 'TABLE1', 'catalog_name': 'SNOWFLAKE'}],
                    [{'key': 'ICEBERG_VERSION', 'value': '3'}],
                    parameter_rows,
                ])

                with self.assertRaisesRegex(
                    TableFormatDiscoveryException,
                    message,
                ) as error:
                    sync.discover_table_format('dummy_schema', 'table1')

                self.assertIn(
                    'ALTER ICEBERG TABLE "DUMMY-DB"."DUMMY_SCHEMA"."TABLE1" SET '
                    "ICEBERG_MERGE_ON_READ_BEHAVIOR = 'DISABLED'",
                    str(error.exception),
                )
                self.assertNotIn(
                    'ALTER ICEBERG TABLE',
                    ' '.join(
                        query_call.args[0]
                        for query_call in sync.query.call_args_list
                    ),
                )

    def test_discover_table_format_external_iceberg_does_not_read_version(self):
        sync = self._format_discovery_sync([
            [{'name': 'TABLE1', 'is_iceberg': 'YES'}],
            [{'name': 'TABLE1', 'catalog_name': 'EXTERNAL_CATALOG'}],
        ])
        self.assertEqual(
            sync.discover_table_format('dummy_schema', 'table1'),
            db_sync.TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG,
        )
        self.assertEqual(sync.query.call_count, 2)

    def test_discover_table_format_rejects_incomplete_or_unknown_metadata(self):
        cases = (
            (
                [[{'name': 'TABLE1'}]],
                "did not return 'is_iceberg'",
            ),
            (
                [[{'name': 'TABLE1', 'is_iceberg': 'Y'}], []],
                'Iceberg metadata is incomplete',
            ),
            (
                [
                    [{'name': 'TABLE1', 'is_iceberg': 'Y'}],
                    [{'name': 'TABLE1', 'catalog_name': None}],
                ],
                'invalid catalog_name',
            ),
            (
                [
                    [{'name': 'TABLE1', 'is_iceberg': 'Y'}],
                    [{'name': 'TABLE1', 'catalog_name': 'SNOWFLAKE'}],
                    [{'key': 'ICEBERG_VERSION', 'value': '2'}],
                ],
                'unsupported ICEBERG_VERSION 2',
            ),
            (
                [
                    [{'name': 'TABLE1', 'is_iceberg': 'Y'}],
                    [{'name': 'TABLE1', 'catalog_name': 'SNOWFLAKE'}],
                    [{'key': 'ICEBERG_VERSION', 'value': '4'}],
                ],
                'unsupported ICEBERG_VERSION 4',
            ),
        )
        for query_results, message in cases:
            with self.subTest(message=message):
                sync = self._format_discovery_sync(query_results)
                with self.assertRaisesRegex(TableFormatDiscoveryException, message):
                    sync.discover_table_format('dummy_schema', 'table1')

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_version_column_sql(self, query_patch):
        """version_column uses ALTER ICEBERG TABLE when is_iceberg_table=True, ALTER TABLE otherwise"""
        query_patch.return_value = _csv_file_format_result()
        minimal_config = {
            'account': "dummy-account",
            'dbname': "dummy-db",
            'user': "dummy-user",
            'private_key': "dummy-key",
            'warehouse': "dummy-wh",
            'default_target_schema': "dummy-schema",
            'file_format': "dummy-file-format"
        }
        stream_schema_message = {
            "stream": "public-table1",
            "schema": {"properties": {"id": {"type": ["integer"]}}},
            "key_properties": ["id"]
        }
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)

        dbsync.version_column('"ID"', 'public-table1', is_iceberg_table=False)
        sql = query_patch.call_args[0][0]
        self.assertIn('ALTER TABLE', sql)
        self.assertNotIn('ICEBERG', sql)
        self.assertIn('RENAME COLUMN', sql)

        dbsync.version_column('"ID"', 'public-table1', is_iceberg_table=True)
        sql = query_patch.call_args[0][0]
        self.assertIn('ALTER ICEBERG TABLE', sql)
        self.assertIn('RENAME COLUMN', sql)

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_add_column_sql(self, query_patch):
        """add_column uses ALTER ICEBERG TABLE when is_iceberg_table=True, ALTER TABLE otherwise"""
        query_patch.return_value = _csv_file_format_result()
        minimal_config = {
            'account': "dummy-account",
            'dbname': "dummy-db",
            'user': "dummy-user",
            'private_key': "dummy-key",
            'warehouse': "dummy-wh",
            'default_target_schema': "dummy-schema",
            'file_format': "dummy-file-format"
        }
        stream_schema_message = {
            "stream": "public-table1",
            "schema": {"properties": {"id": {"type": ["integer"]}}},
            "key_properties": ["id"]
        }
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)

        dbsync.add_column(
            '"NEW_COL" varchar(134217728)',
            'public-table1',
            is_iceberg_table=False,
        )
        sql = query_patch.call_args[0][0]
        self.assertIn('ALTER TABLE', sql)
        self.assertNotIn('ICEBERG', sql)
        self.assertIn('ADD COLUMN', sql)

        dbsync.add_column(
            '"NEW_COL" varchar(134217728)',
            'public-table1',
            is_iceberg_table=True,
        )
        sql = query_patch.call_args[0][0]
        self.assertIn('ALTER ICEBERG TABLE', sql)
        self.assertIn('ADD COLUMN', sql)

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_native_adds_new_string_at_max_width(self, query_patch):
        """Native Singer evolution creates new string columns at maximum width."""
        query_patch.return_value = _csv_file_format_result()
        stream_schema = {
            'stream': 'public-table1',
            'schema': {
                'properties': {
                    'id': {'type': ['integer']},
                    'new_col': {'type': ['null', 'string']},
                },
            },
            'key_properties': ['id'],
        }
        table_cache = [{
            'SCHEMA_NAME': 'DUMMY-SCHEMA',
            'TABLE_NAME': 'TABLE1',
            'COLUMN_NAME': 'ID',
            'DATA_TYPE': 'NUMBER',
        }]
        dbsync = db_sync.DbSync(
            self._table_sync_config(),
            stream_schema,
            table_cache,
        )
        query_patch.reset_mock()

        dbsync.update_columns()

        self.assertIn(
            call(
                'ALTER TABLE dummy-schema."TABLE1" '
                'ADD COLUMN "NEW_COL" varchar(134217728)'
            ),
            query_patch.call_args_list,
        )

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_native_keeps_existing_string_column(self, query_patch):
        """A pre-existing native string is not widened, renamed, or re-added."""
        query_patch.return_value = _csv_file_format_result()
        stream_schema = {
            'stream': 'public-table1',
            'schema': {'properties': {'body': {'type': ['string']}}},
            'key_properties': [],
        }

        for current_type in ('TEXT', 'VARCHAR', 'VARCHAR(16777216)'):
            with self.subTest(current_type=current_type):
                table_cache = [{
                    'SCHEMA_NAME': 'DUMMY-SCHEMA',
                    'TABLE_NAME': 'TABLE1',
                    'COLUMN_NAME': 'BODY',
                    'DATA_TYPE': current_type,
                }]
                dbsync = db_sync.DbSync(
                    self._table_sync_config(),
                    stream_schema,
                    table_cache,
                )
                query_patch.reset_mock()

                dbsync.update_columns()

                query_patch.assert_not_called()

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_explicit_v3_adds_new_column(self, query_patch):
        """Explicit v3 schema evolution issues ALTER ICEBERG TABLE ADD COLUMN."""
        query_patch.return_value = _csv_file_format_result()
        stream_schema_message = {
            "stream": "public-table1",
            "schema": {
                "properties": {
                    "id": {"type": ["integer"]},
                    "new_col": {"type": ["null", "string"]}
                }
            },
            "key_properties": ["id"]
        }
        # table_cache only has 'id' — 'new_col' is missing and should be added
        table_cache = [
            {'SCHEMA_NAME': 'DUMMY-SCHEMA', 'TABLE_NAME': 'TABLE1', 'COLUMN_NAME': 'ID', 'DATA_TYPE': 'NUMBER'}
        ]
        dbsync = db_sync.DbSync(
            self._table_sync_config(target_table_format='iceberg', iceberg_version=3),
            stream_schema_message,
            table_cache,
        )
        dbsync.update_columns(is_iceberg_table=True, iceberg_version=3)

        add_column_calls = [
            query_call
            for query_call in query_patch.call_args_list
            if 'ADD COLUMN' in str(query_call)
        ]
        self.assertEqual(
            add_column_calls,
            [call(
                'ALTER ICEBERG TABLE dummy-schema."TABLE1" '
                'ADD COLUMN "NEW_COL" varchar(134217728)'
            )],
        )

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_explicit_v3_number_no_spurious_alter(self, query_patch):
        """An existing NUMBER column matches explicit v3 NUMBER(38,0)."""
        query_patch.return_value = _csv_file_format_result()
        stream_schema_message = {
            "stream": "public-table1",
            "schema": {
                "properties": {
                    "id": {"type": ["integer"]},
                }
            },
            "key_properties": ["id"]
        }
        table_cache = [
            {'SCHEMA_NAME': 'DUMMY-SCHEMA', 'TABLE_NAME': 'TABLE1', 'COLUMN_NAME': 'ID', 'DATA_TYPE': 'NUMBER'}
        ]
        dbsync = db_sync.DbSync(
            self._table_sync_config(target_table_format='iceberg', iceberg_version=3),
            stream_schema_message,
            table_cache,
        )
        dbsync.update_columns(is_iceberg_table=True, iceberg_version=3)

        alter_calls = [str(c) for c in query_patch.call_args_list
                       if 'ADD COLUMN' in str(c) or 'RENAME COLUMN' in str(c)]
        self.assertEqual(len(alter_calls), 0,
                         msg="NUMBER should not be re-altered for NUMBER(38,0) on Iceberg table")

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_explicit_v3_accepts_reported_text_for_varchar(self, query_patch):
        """Snowflake's TEXT alias matches a preflight-validated max-width VARCHAR."""
        query_patch.return_value = _csv_file_format_result()
        stream_schema = {
            'stream': 'public-table1',
            'schema': {'properties': {'body': {'type': ['string']}}},
            'key_properties': [],
        }
        table_cache = [{
            'SCHEMA_NAME': 'DUMMY-SCHEMA',
            'TABLE_NAME': 'TABLE1',
            'COLUMN_NAME': 'BODY',
            'DATA_TYPE': 'TEXT',
        }]
        dbsync = db_sync.DbSync(
            self._table_sync_config(target_table_format='iceberg', iceberg_version=3),
            stream_schema,
            table_cache,
        )
        query_patch.reset_mock()

        dbsync.update_columns(is_iceberg_table=True, iceberg_version=3)

        query_patch.assert_not_called()

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_explicit_v3_adds_variant(self, query_patch):
        query_patch.return_value = _csv_file_format_result()
        table_cache = [
            {
                'SCHEMA_NAME': 'DUMMY-SCHEMA',
                'TABLE_NAME': 'TABLE1',
                'COLUMN_NAME': 'ID',
                'DATA_TYPE': 'NUMBER',
            },
        ]
        dbsync = db_sync.DbSync(
            self._table_sync_config(target_table_format='iceberg', iceberg_version=3),
            self._table_sync_schema(),
            table_cache,
        )
        query_patch.reset_mock()

        dbsync.update_columns(is_iceberg_table=True, iceberg_version=3)

        add_column_calls = [
            query_call
            for query_call in query_patch.call_args_list
            if 'ADD COLUMN' in str(query_call)
        ]
        self.assertEqual(
            add_column_calls,
            [call('ALTER ICEBERG TABLE dummy-schema."TABLE1" ADD COLUMN "PAYLOAD" variant')],
        )

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_explicit_v3_accepts_reported_float_for_double(self, query_patch):
        """Snowflake reports an Iceberg DOUBLE column through the FLOAT alias."""
        query_patch.return_value = _csv_file_format_result()
        table_cache = [
            {
                'SCHEMA_NAME': 'DUMMY-SCHEMA',
                'TABLE_NAME': 'TABLE1',
                'COLUMN_NAME': 'AMOUNT',
                'DATA_TYPE': 'FLOAT',
            },
        ]
        stream_schema = {
            'stream': 'public-table1',
            'schema': {'properties': {'amount': {'type': ['number']}}},
            'key_properties': [],
        }
        dbsync = db_sync.DbSync(
            self._table_sync_config(target_table_format='iceberg', iceberg_version=3),
            stream_schema,
            table_cache,
        )
        query_patch.reset_mock()

        dbsync.update_columns(is_iceberg_table=True, iceberg_version=3)

        query_patch.assert_not_called()

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_explicit_v3_preserves_existing_timestamp_family(self, query_patch):
        """The precision-qualified v3 mapping retains compatible timestamp data."""
        query_patch.return_value = _csv_file_format_result()
        stream_schema = {
            'stream': 'public-table1',
            'schema': {
                'properties': {
                    'created_at': {'type': ['string'], 'format': 'date-time'},
                },
            },
            'key_properties': [],
        }

        for current_type in ('TIMESTAMP_NTZ(9)', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ'):
            with self.subTest(current_type=current_type):
                table_cache = [
                    {
                        'SCHEMA_NAME': 'DUMMY-SCHEMA',
                        'TABLE_NAME': 'TABLE1',
                        'COLUMN_NAME': 'CREATED_AT',
                        'DATA_TYPE': current_type,
                    },
                ]
                dbsync = db_sync.DbSync(
                    self._table_sync_config(
                        target_table_format='iceberg',
                        iceberg_version=3,
                    ),
                    stream_schema,
                    table_cache,
                )
                query_patch.reset_mock()

                dbsync.update_columns(is_iceberg_table=True, iceberg_version=3)

                query_patch.assert_not_called()

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_explicit_v3_replaces_non_timestamp_with_timestamp(self, query_patch):
        """A date-time schema does not preserve an unrelated existing type."""
        query_patch.return_value = _csv_file_format_result()
        stream_schema = {
            'stream': 'public-table1',
            'schema': {
                'properties': {
                    'created_at': {'type': ['string'], 'format': 'date-time'},
                },
            },
            'key_properties': [],
        }

        for current_type in ('DATE', 'NUMBER', 'TEXT', 'BOOLEAN', 'VARIANT'):
            with self.subTest(current_type=current_type):
                table_cache = [
                    {
                        'SCHEMA_NAME': 'DUMMY-SCHEMA',
                        'TABLE_NAME': 'TABLE1',
                        'COLUMN_NAME': 'CREATED_AT',
                        'DATA_TYPE': current_type,
                    },
                ]
                dbsync = db_sync.DbSync(
                    self._table_sync_config(
                        target_table_format='iceberg',
                        iceberg_version=3,
                    ),
                    stream_schema,
                    table_cache,
                )
                query_patch.reset_mock()

                dbsync.update_columns(is_iceberg_table=True, iceberg_version=3)

                alter_queries = [
                    query_call.args[0]
                    for query_call in query_patch.call_args_list
                    if (
                        query_call.args
                        and isinstance(query_call.args[0], str)
                        and query_call.args[0].startswith('ALTER ICEBERG TABLE')
                    )
                ]
                self.assertEqual(len(alter_queries), 2)
                self.assertIn('RENAME COLUMN "CREATED_AT"', alter_queries[0])
                self.assertEqual(
                    alter_queries[1],
                    'ALTER ICEBERG TABLE dummy-schema."TABLE1" '
                    'ADD COLUMN "CREATED_AT" timestamp_ntz(6)',
                )

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_does_not_implicitly_convert_text_and_variant(self, query_patch):
        query_patch.return_value = _csv_file_format_result()

        for current_type, property_schema, expected_message in (
            ('TEXT', {'type': ['object']}, 'explicit Iceberg v3 mapping requires VARIANT'),
            (
                'VARIANT',
                {'type': ['string']},
                r'current Singer schema requires VARCHAR\(134217728\)',
            ),
        ):
            with self.subTest(current_type=current_type):
                table_cache = [
                    {
                        'SCHEMA_NAME': 'DUMMY-SCHEMA',
                        'TABLE_NAME': 'TABLE1',
                        'COLUMN_NAME': 'ID',
                        'DATA_TYPE': 'NUMBER',
                    },
                    {
                        'SCHEMA_NAME': 'DUMMY-SCHEMA',
                        'TABLE_NAME': 'TABLE1',
                        'COLUMN_NAME': 'PAYLOAD',
                        'DATA_TYPE': current_type,
                    },
                ]
                stream_schema = self._table_sync_schema()
                stream_schema['schema']['properties']['payload'] = property_schema
                dbsync = db_sync.DbSync(
                    self._table_sync_config(
                        target_table_format='iceberg', iceberg_version=3
                    ),
                    stream_schema,
                    table_cache,
                )
                query_patch.reset_mock()

                with self.assertRaisesRegex(
                    TableFormatMismatchException,
                    expected_message,
                ):
                    dbsync.update_columns(
                        is_iceberg_table=True,
                        iceberg_version=3,
                    )

                query_patch.assert_not_called()

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_iceberg_type_change_versions_and_re_adds(self, query_patch):
        """A type mismatch on Iceberg renames the old column then adds the new one via ICEBERG DDL"""
        query_patch.return_value = _csv_file_format_result()
        stream_schema_message = {
            "stream": "public-table1",
            "schema": {
                "properties": {
                    "id": {"type": ["integer"]},
                    "amount": {"type": ["number"]},  # schema says float; table has TEXT — mismatch
                }
            },
            "key_properties": ["id"]
        }
        table_cache = [
            {'SCHEMA_NAME': 'DUMMY-SCHEMA', 'TABLE_NAME': 'TABLE1', 'COLUMN_NAME': 'ID', 'DATA_TYPE': 'NUMBER'},
            {'SCHEMA_NAME': 'DUMMY-SCHEMA', 'TABLE_NAME': 'TABLE1', 'COLUMN_NAME': 'AMOUNT', 'DATA_TYPE': 'TEXT'},
        ]
        dbsync = db_sync.DbSync(
            self._table_sync_config(target_table_format='iceberg', iceberg_version=3),
            stream_schema_message,
            table_cache,
        )
        dbsync.update_columns(is_iceberg_table=True, iceberg_version=3)

        all_calls = [str(c) for c in query_patch.call_args_list]
        rename_calls = [s for s in all_calls if 'RENAME COLUMN' in s]
        add_calls = [s for s in all_calls if 'ADD COLUMN' in s]

        self.assertEqual(len(rename_calls), 1, msg="Expected exactly one RENAME COLUMN for type change")
        self.assertIn('ALTER ICEBERG TABLE', rename_calls[0])
        self.assertEqual(len(add_calls), 1, msg="Expected exactly one ADD COLUMN after versioning")
        self.assertIn('ALTER ICEBERG TABLE', add_calls[0])

    def test_config_validation_rejects_removed_iceberg_create(self):
        base = self._table_sync_config()

        for value in (True, False, 'true', None):
            with self.subTest(value=value):
                errors = db_sync.validate_config({**base, 'iceberg_create': value})
                self.assertTrue(
                    any("'iceberg_create' is no longer supported" in error for error in errors)
                )

    def test_config_validation_explicit_table_format(self):
        base = self._table_sync_config()

        valid_configs = (
            {'target_table_format': 'native'},
            {'target_table_format': 'iceberg', 'iceberg_version': 3},
        )
        for extra_config in valid_configs:
            with self.subTest(extra_config=extra_config):
                self.assertEqual(db_sync.validate_config({**base, **extra_config}), [])

        direct_iceberg_config = {
            **base,
            'target_table_format': 'iceberg',
            'iceberg_version': 3,
        }
        direct_iceberg_config.pop('hard_delete')
        self.assertTrue(
            any("'hard_delete'" in error for error in db_sync.validate_config(direct_iceberg_config))
        )

        invalid_configs = (
            ({'target_table_format': None}, "'target_table_format'"),
            ({'target_table_format': 'parquet'}, "'target_table_format'"),
            ({'target_table_format': 'iceberg'}, "'iceberg_version'"),
            ({'target_table_format': 'iceberg', 'iceberg_version': 2}, "'iceberg_version'"),
            ({'target_table_format': 'iceberg', 'iceberg_version': 3.0}, "'iceberg_version'"),
            ({'target_table_format': 'iceberg', 'iceberg_version': '3'}, "'iceberg_version'"),
            ({'target_table_format': 'iceberg', 'iceberg_version': 4}, "'iceberg_version'"),
            ({'target_table_format': 'iceberg', 'iceberg_version': True}, "'iceberg_version'"),
            ({'target_table_format': 'native', 'iceberg_version': 3}, "'iceberg_version'"),
            ({'target_table_format': 'native', 'iceberg_version': None}, "'iceberg_version'"),
            ({'iceberg_version': 3}, "'iceberg_version'"),
            ({'iceberg_version': None}, "'iceberg_version'"),
            (
                {'target_table_format': 'iceberg', 'iceberg_version': 3, 'hard_delete': False},
                "'hard_delete'",
            ),
            (
                {'target_table_format': 'iceberg', 'iceberg_version': 3, 'hard_delete': 'true'},
                "'hard_delete'",
            ),
        )
        for extra_config, expected_error in invalid_configs:
            with self.subTest(extra_config=extra_config):
                self.assertTrue(
                    any(
                        expected_error in error
                        for error in db_sync.validate_config({**base, **extra_config})
                    )
                )

    def test_create_iceberg_table_query_ddl(self):
        """create_iceberg_table_query generates correct Iceberg DDL with Iceberg column types"""
        config = self._table_sync_config(
            target_table_format='iceberg', iceberg_version=3
        )
        stream_schema_message = {
            "stream": "public-table1",
            "schema": {
                "properties": {
                    "id": {"type": ["integer"]},
                    "payload": {"type": ["object"]},
                    "name": {"type": ["null", "string"]},
                }
            },
            "key_properties": ["id"]
        }

        with patch('target_snowflake.db_sync.DbSync.query', return_value=_csv_file_format_result()):
            dbsync = db_sync.DbSync(config, stream_schema_message)

        ddl = dbsync.create_iceberg_table_query(iceberg_version=3)

        self.assertEqual(
            ddl,
            'CREATE ICEBERG TABLE IF NOT EXISTS dummy-schema."TABLE1" '
            '("ID" number(38,0), "NAME" varchar(134217728), "PAYLOAD" variant, PRIMARY KEY("ID")) '
            "CATALOG='SNOWFLAKE' ICEBERG_VERSION=3 DATA_RETENTION_TIME_IN_DAYS=1 "
            "TARGET_FILE_SIZE='16MB' ENABLE_DATA_COMPACTION=TRUE "
            "ICEBERG_MERGE_ON_READ_BEHAVIOR='DISABLED'",
        )
        self.assertIn('CREATE ICEBERG TABLE IF NOT EXISTS', ddl)
        self.assertIn('DATA_RETENTION_TIME_IN_DAYS', ddl)
        self.assertIn('TARGET_FILE_SIZE', ddl)
        self.assertIn('ENABLE_DATA_COMPACTION', ddl)
        self.assertIn("ICEBERG_MERGE_ON_READ_BEHAVIOR='DISABLED'", ddl)
        self.assertEqual(
            ddl.count("ICEBERG_MERGE_ON_READ_BEHAVIOR='DISABLED'"),
            1,
        )
        self.assertNotIn('ENABLE_ICEBERG_MERGE_ON_READ', ddl)
        self.assertIn('number(38,0)', ddl)
        self.assertIn('"NAME" varchar(134217728)', ddl)
        self.assertIn('"PAYLOAD" variant', ddl)
        # PRIMARY KEY is included
        self.assertIn('PRIMARY KEY', ddl)
        self.assertNotIn('EXTERNAL_VOLUME', ddl)
        self.assertIn("CATALOG='SNOWFLAKE'", ddl)
        self.assertIn('ICEBERG_VERSION=3', ddl)
        self.assertNotIn('BASE_LOCATION', ddl)

        for invalid_version in (None, 2, 4, 3.0, True, '3'):
            with self.subTest(invalid_version=invalid_version), self.assertRaisesRegex(
                ValueError,
                'creation requires integer version 3',
            ):
                dbsync.create_iceberg_table_query(iceberg_version=invalid_version)

    def test_create_iceberg_table_query_explicit_v3_uses_variant(self):
        config = self._table_sync_config(target_table_format='iceberg', iceberg_version=3)
        stream_schema_message = self._table_sync_schema()
        stream_schema_message['schema']['properties'].update({
            'event_time': {'type': ['string'], 'format': 'time'},
            'created_at': {'type': ['string'], 'format': 'date-time'},
        })

        with patch('target_snowflake.db_sync.DbSync.query', return_value=_csv_file_format_result()):
            dbsync = db_sync.DbSync(config, stream_schema_message)

        ddl = dbsync.create_iceberg_table_query(iceberg_version=3)

        self.assertIn('"PAYLOAD" variant', ddl)
        self.assertIn('"ID" number(38,0)', ddl)
        self.assertIn('"EVENT_TIME" time(6)', ddl)
        self.assertIn('"CREATED_AT" timestamp_ntz(6)', ddl)
        self.assertIn("CATALOG='SNOWFLAKE'", ddl)
        self.assertIn('ICEBERG_VERSION=3', ddl)
        self.assertNotIn('EXTERNAL_VOLUME', ddl)
        self.assertNotIn('BASE_LOCATION', ddl)

    def test_create_iceberg_table_query_no_pk_when_no_key_properties(self):
        """create_iceberg_table_query omits PRIMARY KEY when stream has no key_properties"""
        config = self._table_sync_config(
            target_table_format='iceberg', iceberg_version=3
        )
        stream_schema_message = {
            "stream": "public-table1",
            "schema": {"properties": {"id": {"type": ["integer"]}}},
            "key_properties": []
        }
        with patch('target_snowflake.db_sync.DbSync.query', return_value=_csv_file_format_result()):
            dbsync = db_sync.DbSync(config, stream_schema_message)

        ddl = dbsync.create_iceberg_table_query(iceberg_version=3)
        self.assertNotIn('PRIMARY KEY', ddl)

    def test_create_table_query_native_uses_maximum_varchar(self):
        """New native strings are wide and Iceberg settings do not leak."""
        stream_schema = self._table_sync_schema()
        stream_schema['schema']['properties']['name'] = {'type': ['string']}
        with patch('target_snowflake.db_sync.DbSync.query', return_value=_csv_file_format_result()):
            dbsync = db_sync.DbSync(
                self._table_sync_config(),
                stream_schema,
            )

        ddl = dbsync.create_table_query()

        self.assertEqual(
            ddl,
            'CREATE TABLE IF NOT EXISTS dummy-schema."TABLE1" '
            '("ID" number, "NAME" varchar(134217728), "PAYLOAD" variant, '
            'PRIMARY KEY("ID")) '
            'data_retention_time_in_days = 1 ',
        )
        self.assertNotIn('ICEBERG_MERGE_ON_READ_BEHAVIOR', ddl)
        self.assertNotIn('ENABLE_ICEBERG_MERGE_ON_READ', ddl)

    @patch('target_snowflake.db_sync.DbSync.grant_privilege')
    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_explicit_create_rejects_concurrent_v2_table(self, query_patch, grant_patch):
        query_patch.return_value = _csv_file_format_result()
        dbsync = db_sync.DbSync(
            self._table_sync_config(
                target_table_format='iceberg', iceberg_version=3
            ),
            self._table_sync_schema(),
        )
        query_patch.side_effect = [
            [],
            None,
            [{'name': 'TABLE1', 'is_iceberg': 'Y'}],
            [{'name': 'TABLE1', 'catalog_name': 'SNOWFLAKE'}],
            [{'key': 'ICEBERG_VERSION', 'value': '2'}],
        ]

        with self.assertRaisesRegex(TableFormatDiscoveryException, 'unsupported ICEBERG_VERSION 2'):
            dbsync.sync_table()

        grant_patch.assert_not_called()

    def test_sync_table_existing_explicit_v3_adds_column_with_v3_mapping(self):
        """An existing explicit-v3 table uses Iceberg DDL and the v3 mapping."""
        config = self._table_sync_config(
            target_table_format='iceberg', iceberg_version=3
        )
        table_cache = [
            {
                'SCHEMA_NAME': 'DUMMY-SCHEMA',
                'TABLE_NAME': 'TABLE1',
                'COLUMN_NAME': 'ID',
                'DATA_TYPE': 'NUMBER',
            },
        ]
        with patch('target_snowflake.db_sync.DbSync.query') as query_patch:
            query_patch.return_value = _csv_file_format_result()
            dbsync = db_sync.DbSync(config, self._table_sync_schema(), table_cache)
            query_patch.reset_mock()

            with patch.object(
                dbsync,
                'discover_table_format',
                return_value=db_sync.TABLE_FORMAT_MANAGED_ICEBERG_V3,
            ), patch.object(dbsync, 'get_table_columns', return_value=table_cache):
                dbsync.sync_table()

        query_patch.assert_called_once_with(
            'ALTER ICEBERG TABLE dummy-schema."TABLE1" ADD COLUMN "PAYLOAD" variant'
        )

    @patch('target_snowflake.db_sync.DbSync.grant_privilege')
    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_rejects_existing_v3_without_table_copy_on_write_before_mutation(
        self,
        query_patch,
        grant_patch,
    ):
        query_patch.return_value = _csv_file_format_result()
        dbsync = db_sync.DbSync(
            self._table_sync_config(target_table_format='iceberg', iceberg_version=3),
            self._table_sync_schema(),
        )
        query_patch.reset_mock()
        query_patch.side_effect = [
            [{'name': 'TABLE1', 'is_iceberg': 'Y'}],
            [{'name': 'TABLE1', 'catalog_name': 'SNOWFLAKE'}],
            [{'key': 'ICEBERG_VERSION', 'value': '3'}],
            [
                {
                    'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                    'value': 'AUTO',
                    'level': 'ACCOUNT',
                },
            ],
        ]

        with patch.object(dbsync, 'update_columns') as update_columns, self.assertRaisesRegex(
            TableFormatDiscoveryException,
            "requires value 'DISABLED' at level 'TABLE'",
        ):
            dbsync.sync_table()

        update_columns.assert_not_called()
        grant_patch.assert_not_called()
        self.assertEqual(query_patch.call_count, 4)
        self.assertTrue(
            all(query_call.args[0].startswith('SHOW ') for query_call in query_patch.call_args_list)
        )

    @patch('target_snowflake.db_sync.DbSync.grant_privilege')
    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_omitted_format_creates_and_verifies_native_table(
        self, query_patch, grant_patch
    ):
        query_patch.return_value = _csv_file_format_result()
        dbsync = db_sync.DbSync(
            self._table_sync_config(),
            self._table_sync_schema(),
        )
        query_patch.reset_mock()

        with patch.object(
            dbsync,
            'discover_table_format',
            side_effect=(db_sync.TABLE_FORMAT_MISSING, db_sync.TABLE_FORMAT_NATIVE),
        ) as discover_format, patch.object(dbsync, '_refresh_table_pks'):
            dbsync.sync_table()

        discover_format.assert_has_calls([
            call('dummy-schema', 'TABLE1'),
            call('dummy-schema', 'TABLE1'),
        ])
        self.assertEqual(query_patch.call_count, 1)
        self.assertIn('CREATE TABLE IF NOT EXISTS', query_patch.call_args.args[0])
        self.assertNotIn('ICEBERG', query_patch.call_args.args[0])
        grant_patch.assert_called_once()

    @patch('target_snowflake.db_sync.DbSync.grant_privilege')
    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_explicit_v3_create_is_verified_before_grants(self, query_patch, grant_patch):
        query_patch.return_value = _csv_file_format_result()
        dbsync = db_sync.DbSync(
            self._table_sync_config(target_table_format='iceberg', iceberg_version=3),
            self._table_sync_schema(),
        )
        query_patch.reset_mock()
        query_patch.side_effect = [
            [],
            None,
            [{'name': 'TABLE1', 'is_iceberg': 'Y'}],
            [{'name': 'TABLE1', 'catalog_name': 'SNOWFLAKE'}],
            [{'key': 'ICEBERG_VERSION', 'value': '3'}],
            [
                {
                    'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                    'value': 'DISABLED',
                    'level': 'TABLE',
                },
            ],
            [
                {
                    'COLUMN_NAME': 'ID',
                    'DATA_TYPE': 'NUMBER',
                    'NUMERIC_PRECISION': 38,
                    'NUMERIC_SCALE': 0,
                    'CHARACTER_MAXIMUM_LENGTH': None,
                    'IS_NULLABLE': 'NO',
                },
                {
                    'COLUMN_NAME': 'PAYLOAD',
                    'DATA_TYPE': 'VARIANT',
                    'CHARACTER_MAXIMUM_LENGTH': None,
                    'IS_NULLABLE': 'YES',
                },
            ],
        ]
        grant_patch.side_effect = (
            lambda *args, **kwargs: self.assertEqual(query_patch.call_count, 7)
        )

        dbsync.sync_table()

        create_query = query_patch.call_args_list[1].args[0]
        self.assertIn('CREATE ICEBERG TABLE IF NOT EXISTS', create_query)
        self.assertIn('"PAYLOAD" variant', create_query)
        self.assertIn("CATALOG='SNOWFLAKE'", create_query)
        self.assertIn('ICEBERG_VERSION=3', create_query)
        self.assertEqual(
            query_patch.call_args_list[-2],
            call(
                "SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR' IN TABLE "
                '"DUMMY-DB"."DUMMY-SCHEMA"."TABLE1"'
            ),
        )
        self.assertEqual(
            query_patch.call_args_list[-1],
            call(
                'SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, '
                'DATETIME_PRECISION, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE '
                'FROM "DUMMY-DB".INFORMATION_SCHEMA.COLUMNS '
                'WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s '
                'ORDER BY ORDINAL_POSITION',
                params={'schema': 'DUMMY-SCHEMA', 'table': 'TABLE1'},
            ),
        )
        grant_patch.assert_called_once()

    @patch('target_snowflake.db_sync.DbSync.grant_privilege')
    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_new_v3_rejects_if_not_exists_race_without_copy_on_write(
        self,
        query_patch,
        grant_patch,
    ):
        query_patch.return_value = _csv_file_format_result()
        dbsync = db_sync.DbSync(
            self._table_sync_config(target_table_format='iceberg', iceberg_version=3),
            self._table_sync_schema(),
        )
        query_patch.reset_mock()
        query_patch.side_effect = [
            [],
            None,
            [{'name': 'TABLE1', 'is_iceberg': 'Y'}],
            [{'name': 'TABLE1', 'catalog_name': 'SNOWFLAKE'}],
            [{'key': 'ICEBERG_VERSION', 'value': '3'}],
            [
                {
                    'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                    'value': 'AUTO',
                    'level': 'ACCOUNT',
                },
            ],
        ]

        with self.assertRaisesRegex(
            TableFormatDiscoveryException,
            "requires value 'DISABLED' at level 'TABLE'",
        ):
            dbsync.sync_table()

        grant_patch.assert_not_called()
        self.assertEqual(query_patch.call_count, 6)
        mutation_queries = [
            query_call.args[0]
            for query_call in query_patch.call_args_list
            if not query_call.args[0].startswith('SHOW ')
        ]
        self.assertEqual(len(mutation_queries), 1)
        self.assertIn('CREATE ICEBERG TABLE IF NOT EXISTS', mutation_queries[0])
        self.assertNotIn('ALTER ICEBERG TABLE', mutation_queries[0])

    @patch('target_snowflake.db_sync.DbSync.grant_privilege')
    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_explicit_create_rejects_concurrent_wrong_format(self, query_patch, grant_patch):
        query_patch.return_value = _csv_file_format_result()
        dbsync = db_sync.DbSync(
            self._table_sync_config(target_table_format='iceberg', iceberg_version=3),
            self._table_sync_schema(),
        )
        query_patch.reset_mock()

        with patch.object(
            dbsync,
            'discover_table_format',
            side_effect=(db_sync.TABLE_FORMAT_MISSING, db_sync.TABLE_FORMAT_NATIVE),
        ):
            with self.assertRaisesRegex(
                TableFormatMismatchException,
                'is native after creation, but target_table_format requires managed_iceberg_v3',
            ):
                dbsync.sync_table()

        self.assertEqual(query_patch.call_count, 1)
        self.assertIn('CREATE ICEBERG TABLE IF NOT EXISTS', query_patch.call_args.args[0])
        grant_patch.assert_not_called()

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_explicit_format_must_match_existing_table(self, query_patch):
        cases = (
            ('iceberg', db_sync.TABLE_FORMAT_NATIVE),
            ('iceberg', db_sync.TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG),
            ('native', db_sync.TABLE_FORMAT_MANAGED_ICEBERG_V3),
        )
        for requested_format, physical_format in cases:
            with self.subTest(requested_format=requested_format, physical_format=physical_format):
                config = self._table_sync_config(target_table_format=requested_format)
                if requested_format == 'iceberg':
                    config['iceberg_version'] = 3
                query_patch.return_value = _csv_file_format_result()
                dbsync = db_sync.DbSync(config, self._table_sync_schema())
                query_patch.reset_mock()

                with patch.object(dbsync, 'discover_table_format', return_value=physical_format), \
                        patch.object(dbsync, 'update_columns') as update_columns:
                    with self.assertRaises(TableFormatMismatchException):
                        dbsync.sync_table()

                query_patch.assert_not_called()
                update_columns.assert_not_called()

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_omitted_format_rejects_existing_iceberg_before_mutation(
        self, query_patch
    ):
        """Omitted format means native and cannot load an existing Iceberg table."""
        query_patch.return_value = _csv_file_format_result()
        dbsync = db_sync.DbSync(
            self._table_sync_config(),
            self._table_sync_schema(),
        )
        query_patch.reset_mock()

        with patch.object(
            dbsync,
            'discover_table_format',
            return_value=db_sync.TABLE_FORMAT_MANAGED_ICEBERG_V3,
        ), self.assertRaisesRegex(
            TableFormatMismatchException,
            'the default target format requires native',
        ):
            dbsync.sync_table()

        query_patch.assert_not_called()
