import json
import unittest

from unittest.mock import MagicMock, patch, call

from target_snowflake import db_sync
from target_snowflake.exceptions import (
    PrimaryKeyNotFoundException,
    TableFormatDiscoveryException,
    TableFormatMismatchException,
)


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
            'str': 'text',
            'str_or_null': 'text',
            'dt': 'timestamp_ntz',
            'dt_or_null': 'timestamp_ntz',
            'd': 'date',
            'd_or_null': 'date',
            'time': 'time',
            'time_or_null': 'time',
            'binary': 'binary',
            'num': 'float',
            'int': 'number',
            'int_or_str': 'text',
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
        query_patch.return_value = [{'type': 'CSV'}]

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
        query_patch.return_value = [{'type': 'CSV'}]
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
        query_patch.return_value = [{'type': 'CSV'}]
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
        query_patch.return_value = [{'type': 'CSV'}]
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
        query_patch.return_value = [{'type': 'CSV'}]
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
        query_patch.return_value = [{'type': 'CSV'}]
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
        query_patch.return_value = [{'type': 'CSV'}]
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
            [{'type': 'CSV'}],           # SHOW FILE FORMATS
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
            [{'type': 'CSV'}],           # SHOW FILE FORMATS
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
            [{'type': 'CSV'}],           # SHOW FILE FORMATS
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
            [{'type': 'CSV'}],           # SHOW FILE FORMATS
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

    # -----------------------------------------------------------------------
    # Tests for WDL-155: Iceberg column type handling
    # -----------------------------------------------------------------------

    def test_column_type_mapping_iceberg(self):
        """With is_iceberg_table=True, variant → text and integer → number(19,0); others unchanged"""
        mapper = db_sync.column_type

        # variant types (object/array) map to 'text' for Iceberg
        self.assertEqual(mapper(self.json_types['obj'], is_iceberg_table=True), 'text')
        self.assertEqual(mapper(self.json_types['arr'], is_iceberg_table=True), 'text')

        # integer maps to 'number(19,0)' for Iceberg
        self.assertEqual(mapper(self.json_types['int'], is_iceberg_table=True), 'number(19,0)')

        # All other types should be unchanged
        unchanged = {
            'str': 'text',
            'str_or_null': 'text',
            'dt': 'timestamp_ntz',
            'dt_or_null': 'timestamp_ntz',
            'd': 'date',
            'd_or_null': 'date',
            'time': 'time',
            'time_or_null': 'time',
            'binary': 'binary',
            'num': 'float',
            'int_or_str': 'text',
            'bool': 'boolean',
        }
        for key, expected in unchanged.items():
            self.assertEqual(mapper(self.json_types[key], is_iceberg_table=True), expected,
                             msg=f"column_type mismatch for '{key}' with is_iceberg_table=True")

    def test_column_clause_iceberg(self):
        """column_clause should emit Iceberg-compatible types when is_iceberg_table=True"""
        # variant → text
        self.assertEqual(
            db_sync.column_clause('my_obj', self.json_types['obj'], is_iceberg_table=True),
            '"MY_OBJ" text'
        )
        # integer → number(19,0)
        self.assertEqual(
            db_sync.column_clause('my_int', self.json_types['int'], is_iceberg_table=True),
            '"MY_INT" number(19,0)'
        )
        # Standard (non-iceberg) path is unchanged
        self.assertEqual(
            db_sync.column_clause('my_obj', self.json_types['obj']),
            '"MY_OBJ" variant'
        )
        self.assertEqual(
            db_sync.column_clause('my_int', self.json_types['int']),
            '"MY_INT" number'
        )

    def test_column_type_mapping_explicit_iceberg_v3(self):
        """Only the explicit v3 mapping retains JSON-like values as VARIANT."""
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
            'number(19,0)',
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
        with self.assertRaisesRegex(ValueError, 'only version 3'):
            db_sync.column_type(
                self.json_types['obj'],
                is_iceberg_table=True,
                iceberg_version=2,
            )

        # Omitted version is the legacy route and retains its original DDL.
        self.assertEqual(
            db_sync.column_type(self.json_types['time'], is_iceberg_table=True),
            'time',
        )
        self.assertEqual(
            db_sync.column_type(self.json_types['dt'], is_iceberg_table=True),
            'timestamp_ntz',
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

    def test_discover_table_format_managed_v2_and_v3(self):
        for version, expected in (
            (2, db_sync.TABLE_FORMAT_MANAGED_ICEBERG_V2),
            (3, db_sync.TABLE_FORMAT_MANAGED_ICEBERG_V3),
        ):
            with self.subTest(version=version):
                sync = self._format_discovery_sync([
                    [{'name': 'TABLE1', 'is_iceberg': 'Y'}],
                    [
                        {'name': 'TABLE10', 'catalog_name': 'SNOWFLAKE'},
                        {'name': 'TABLE1', 'catalog_name': 'SNOWFLAKE'},
                    ],
                    [{'key': 'ICEBERG_VERSION', 'value': str(version)}],
                ])
                self.assertEqual(
                    sync.discover_table_format('dummy_schema', 'table1'),
                    expected,
                )
                self.assertEqual(
                    sync.query.call_args_list[-1],
                    call(
                        "SHOW PARAMETERS LIKE 'ICEBERG_VERSION' IN TABLE "
                        '"DUMMY-DB"."DUMMY_SCHEMA"."TABLE1"'
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
        query_patch.return_value = [{'type': 'CSV'}]
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
        query_patch.return_value = [{'type': 'CSV'}]
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

        dbsync.add_column('"NEW_COL" text', 'public-table1', is_iceberg_table=False)
        sql = query_patch.call_args[0][0]
        self.assertIn('ALTER TABLE', sql)
        self.assertNotIn('ICEBERG', sql)
        self.assertIn('ADD COLUMN', sql)

        dbsync.add_column('"NEW_COL" text', 'public-table1', is_iceberg_table=True)
        sql = query_patch.call_args[0][0]
        self.assertIn('ALTER ICEBERG TABLE', sql)
        self.assertIn('ADD COLUMN', sql)

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_iceberg_adds_new_column(self, query_patch):
        """update_columns with is_iceberg_table=True should issue ALTER ICEBERG TABLE ADD COLUMN"""
        query_patch.return_value = [{'type': 'CSV'}]
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
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
        dbsync.update_columns(is_iceberg_table=True)

        add_calls = [str(c) for c in query_patch.call_args_list if 'ADD COLUMN' in str(c)]
        self.assertEqual(len(add_calls), 1)
        self.assertIn('ALTER ICEBERG TABLE', add_calls[0])

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_iceberg_number_no_spurious_alter(self, query_patch):
        """Existing NUMBER column in Iceberg table should not be re-altered: number(19,0) base matches NUMBER"""
        query_patch.return_value = [{'type': 'CSV'}]
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
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
        dbsync.update_columns(is_iceberg_table=True)

        alter_calls = [str(c) for c in query_patch.call_args_list
                       if 'ADD COLUMN' in str(c) or 'RENAME COLUMN' in str(c)]
        self.assertEqual(len(alter_calls), 0,
                         msg="NUMBER should not be re-altered for number(19,0) on Iceberg table")

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_iceberg_text_not_altered_for_variant_schema(self, query_patch):
        """Existing TEXT column in Iceberg table is not re-altered when schema type is variant (object/array)"""
        query_patch.return_value = [{'type': 'CSV'}]
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
            "schema": {
                "properties": {
                    "id": {"type": ["integer"]},
                    "payload": {"type": ["object"]},
                }
            },
            "key_properties": ["id"]
        }
        # 'payload' was previously migrated variant → text; schema still says object/variant
        table_cache = [
            {'SCHEMA_NAME': 'DUMMY-SCHEMA', 'TABLE_NAME': 'TABLE1', 'COLUMN_NAME': 'ID', 'DATA_TYPE': 'NUMBER'},
            {'SCHEMA_NAME': 'DUMMY-SCHEMA', 'TABLE_NAME': 'TABLE1', 'COLUMN_NAME': 'PAYLOAD', 'DATA_TYPE': 'TEXT'},
        ]
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
        dbsync.update_columns(is_iceberg_table=True)

        alter_calls = [str(c) for c in query_patch.call_args_list
                       if 'ADD COLUMN' in str(c) or 'RENAME COLUMN' in str(c)]
        self.assertEqual(len(alter_calls), 0,
                         msg="TEXT should not be re-altered when schema is variant on an Iceberg table")

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_explicit_v3_adds_variant(self, query_patch):
        query_patch.return_value = [{'type': 'CSV'}]
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
    def test_update_columns_does_not_implicitly_convert_text_and_variant(self, query_patch):
        query_patch.return_value = [{'type': 'CSV'}]

        for current_type, iceberg_version, property_schema, expected_message in (
            ('TEXT', 3, {'type': ['object']}, 'explicit Iceberg v3 mapping requires VARIANT'),
            ('VARIANT', None, {'type': ['object']}, 'target_table_format is omitted'),
            ('VARIANT', 3, {'type': ['string']}, 'current Singer schema requires TEXT'),
        ):
            with self.subTest(current_type=current_type, iceberg_version=iceberg_version):
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
                    self._table_sync_config(),
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
                        iceberg_version=iceberg_version,
                    )

                query_patch.assert_not_called()

        with self.assertRaisesRegex(ValueError, 'Unsupported Iceberg TEXT/VARIANT mismatch'):
            db_sync._iceberg_text_variant_mismatch_reason('TEXT', None)

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_update_columns_iceberg_type_change_versions_and_re_adds(self, query_patch):
        """A type mismatch on Iceberg renames the old column then adds the new one via ICEBERG DDL"""
        query_patch.return_value = [{'type': 'CSV'}]
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
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
        dbsync.update_columns(is_iceberg_table=True)

        all_calls = [str(c) for c in query_patch.call_args_list]
        rename_calls = [s for s in all_calls if 'RENAME COLUMN' in s]
        add_calls = [s for s in all_calls if 'ADD COLUMN' in s]

        self.assertEqual(len(rename_calls), 1, msg="Expected exactly one RENAME COLUMN for type change")
        self.assertIn('ALTER ICEBERG TABLE', rename_calls[0])
        self.assertEqual(len(add_calls), 1, msg="Expected exactly one ADD COLUMN after versioning")
        self.assertIn('ALTER ICEBERG TABLE', add_calls[0])

    # -----------------------------------------------------------------------
    # Tests for iceberg_create config option
    # -----------------------------------------------------------------------

    def test_config_validation_iceberg_create(self):
        """iceberg_create=True; config is self-contained"""
        base = {
            'account': "dummy-value",
            'dbname': "dummy-value",
            'user': "dummy-value",
            'private_key': "dummy-key",
            'warehouse': "dummy-value",
            'default_target_schema': "dummy-value",
            'file_format': "dummy-value",
        }

        # iceberg_create=False → no error
        self.assertEqual(len(db_sync.validate_config({**base, 'iceberg_create': False})), 0)

        # iceberg_create=True → also no error (external volume is not required in config)
        self.assertEqual(len(db_sync.validate_config({**base, 'iceberg_create': True})), 0)

    def test_config_validation_explicit_table_format(self):
        base = self._table_sync_config()

        valid_configs = (
            {'target_table_format': 'native'},
            {'target_table_format': 'native', 'iceberg_create': False},
            {'target_table_format': 'iceberg', 'iceberg_version': 3},
            {
                'target_table_format': 'iceberg',
                'iceberg_version': 3,
                'iceberg_create': True,
                'hard_delete': True,
            },
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
            ({'target_table_format': 'iceberg', 'iceberg_version': '3'}, "'iceberg_version'"),
            ({'target_table_format': 'native', 'iceberg_version': 3}, "'iceberg_version'"),
            ({'target_table_format': 'native', 'iceberg_version': None}, "'iceberg_version'"),
            ({'iceberg_version': 3}, "'iceberg_version'"),
            ({'iceberg_version': None}, "'iceberg_version'"),
            ({'target_table_format': 'native', 'iceberg_create': True}, 'conflicts'),
            (
                {'target_table_format': 'iceberg', 'iceberg_version': 3, 'iceberg_create': False},
                'conflicts',
            ),
            (
                {'target_table_format': 'iceberg', 'iceberg_version': 3, 'hard_delete': False},
                "'hard_delete'",
            ),
            (
                {'target_table_format': 'iceberg', 'iceberg_version': 3, 'hard_delete': 'true'},
                "'hard_delete'",
            ),
            ({'iceberg_create': 'true'}, "'iceberg_create'"),
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
        minimal_config = {
            'account': "dummy-account",
            'dbname': "dummy-db",
            'user': "dummy-user",
            'private_key': "dummy-key",
            'warehouse': "dummy-wh",
            'default_target_schema': "dummy-schema",
            'file_format': "dummy-file-format",
            'iceberg_create': True,
        }
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

        with patch('target_snowflake.db_sync.DbSync.query', return_value=[{'type': 'CSV'}]):
            dbsync = db_sync.DbSync(minimal_config, stream_schema_message)

        ddl = dbsync.create_iceberg_table_query()

        self.assertEqual(
            ddl,
            'CREATE ICEBERG TABLE IF NOT EXISTS dummy-schema."TABLE1" '
            '("ID" number(19,0), "NAME" text, "PAYLOAD" text, PRIMARY KEY("ID")) '
            " DATA_RETENTION_TIME_IN_DAYS=1 TARGET_FILE_SIZE='16MB' ENABLE_DATA_COMPACTION=TRUE",
        )
        self.assertIn('CREATE ICEBERG TABLE IF NOT EXISTS', ddl)
        self.assertIn('DATA_RETENTION_TIME_IN_DAYS', ddl)
        self.assertIn('TARGET_FILE_SIZE', ddl)
        self.assertIn('ENABLE_DATA_COMPACTION', ddl)
        # Iceberg column types: integer → number(19,0), object → text
        self.assertIn('number(19,0)', ddl)
        self.assertIn('"PAYLOAD" text', ddl)
        # PRIMARY KEY is included
        self.assertIn('PRIMARY KEY', ddl)
        # Snowflake-external managed keywords must NOT appear — external volume is configured in Snowflake directly
        self.assertNotIn('EXTERNAL_VOLUME', ddl)
        self.assertNotIn('CATALOG', ddl)
        self.assertNotIn('BASE_LOCATION', ddl)

    def test_create_iceberg_table_query_explicit_v3_uses_variant(self):
        config = self._table_sync_config(target_table_format='iceberg', iceberg_version=3)
        stream_schema_message = self._table_sync_schema()
        stream_schema_message['schema']['properties'].update({
            'event_time': {'type': ['string'], 'format': 'time'},
            'created_at': {'type': ['string'], 'format': 'date-time'},
        })

        with patch('target_snowflake.db_sync.DbSync.query', return_value=[{'type': 'CSV'}]):
            dbsync = db_sync.DbSync(config, stream_schema_message)

        ddl = dbsync.create_iceberg_table_query(iceberg_version=3)

        self.assertIn('"PAYLOAD" variant', ddl)
        self.assertIn('"EVENT_TIME" time(6)', ddl)
        self.assertIn('"CREATED_AT" timestamp_ntz(6)', ddl)
        self.assertIn("CATALOG='SNOWFLAKE'", ddl)
        self.assertIn('ICEBERG_VERSION=3', ddl)
        self.assertNotIn('EXTERNAL_VOLUME', ddl)
        self.assertNotIn('BASE_LOCATION', ddl)

    def test_create_iceberg_table_query_no_pk_when_no_key_properties(self):
        """create_iceberg_table_query omits PRIMARY KEY when stream has no key_properties"""
        minimal_config = {
            'account': "dummy-account",
            'dbname': "dummy-db",
            'user': "dummy-user",
            'private_key': "dummy-key",
            'warehouse': "dummy-wh",
            'default_target_schema': "dummy-schema",
            'file_format': "dummy-file-format",
            'iceberg_create': True,
        }
        stream_schema_message = {
            "stream": "public-table1",
            "schema": {"properties": {"id": {"type": ["integer"]}}},
            "key_properties": []
        }
        with patch('target_snowflake.db_sync.DbSync.query', return_value=[{'type': 'CSV'}]):
            dbsync = db_sync.DbSync(minimal_config, stream_schema_message)

        ddl = dbsync.create_iceberg_table_query()
        self.assertNotIn('PRIMARY KEY', ddl)

    @patch('target_snowflake.db_sync.DbSync.grant_privilege')
    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_creates_iceberg_when_iceberg_create_true(self, query_patch, grant_patch):
        """sync_table issues CREATE ICEBERG TABLE DDL when iceberg_create=True and table does not exist"""
        minimal_config = {
            'account': "dummy-account",
            'dbname': "dummy-db",
            'user': "dummy-user",
            'private_key': "dummy-key",
            'warehouse': "dummy-wh",
            'default_target_schema': "dummy-schema",
            'file_format': "dummy-file-format",
            'iceberg_create': True,
        }
        stream_schema_message = {
            "stream": "public-table1",
            "schema": {"properties": {"id": {"type": ["integer"]}}},
            "key_properties": ["id"]
        }
        query_patch.side_effect = [
            [{'type': 'CSV'}],    # SHOW FILE FORMATS (during __init__)
            [],                    # SHOW TABLES (missing)
            None,                  # CREATE ICEBERG TABLE
        ]
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        dbsync.sync_table()

        create_calls = [str(c) for c in query_patch.call_args_list if 'CREATE ICEBERG TABLE' in str(c)]
        self.assertEqual(len(create_calls), 1, msg="Expected exactly one CREATE ICEBERG TABLE call")

    def test_sync_table_does_not_recreate_existing_iceberg_table(self):
        """sync_table skips CREATE when table already exists as Iceberg; goes to update_columns path"""
        minimal_config = {
            'account': "dummy-account",
            'dbname': "dummy-db",
            'user': "dummy-user",
            'private_key': "dummy-key",
            'warehouse': "dummy-wh",
            'default_target_schema': "dummy-schema",
            'file_format': "dummy-file-format",
            'iceberg_create': True,
        }
        stream_schema_message = {
            "stream": "public-table1",
            "schema": {"properties": {"id": {"type": ["integer"]}}},
            "key_properties": ["id"]
        }
        # table_cache provides the existing table so found_tables is non-empty
        table_cache = [
            {'SCHEMA_NAME': 'DUMMY-SCHEMA', 'TABLE_NAME': 'TABLE1', 'COLUMN_NAME': 'ID', 'DATA_TYPE': 'NUMBER'}
        ]
        with patch('target_snowflake.db_sync.DbSync.query') as query_patch:
            query_patch.side_effect = [
                [{'type': 'CSV'}],               # SHOW FILE FORMATS (__init__)
                [{'name': 'TABLE1', 'is_iceberg': 'Y'}],
                [{'name': 'TABLE1', 'catalog_name': 'SNOWFLAKE'}],
                [{'key': 'ICEBERG_VERSION', 'value': '2'}],
            ]
            dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
            dbsync.sync_table()

        create_calls = [str(c) for c in query_patch.call_args_list if 'CREATE' in str(c)]
        self.assertEqual(len(create_calls), 0, msg="CREATE must not be issued for an already-existing Iceberg table")

    @patch('target_snowflake.db_sync.DbSync.grant_privilege')
    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_creates_regular_table_when_iceberg_create_false(self, query_patch, grant_patch):
        """sync_table issues a regular CREATE TABLE when iceberg_create is False and table does not exist"""
        minimal_config = {
            'account': "dummy-account",
            'dbname': "dummy-db",
            'user': "dummy-user",
            'private_key': "dummy-key",
            'warehouse': "dummy-wh",
            'default_target_schema': "dummy-schema",
            'file_format': "dummy-file-format",
            'iceberg_create': False,
        }
        stream_schema_message = {
            "stream": "public-table1",
            "schema": {"properties": {"id": {"type": ["integer"]}}},
            "key_properties": ["id"]
        }
        query_patch.side_effect = [
            [{'type': 'CSV'}],    # SHOW FILE FORMATS (during __init__)
            [],     # SHOW TABLES (missing)
            None,   # CREATE TABLE
            [],     # show primary keys
            None,   # ALTER TABLE
        ]
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        dbsync.sync_table()

        create_calls = [str(c) for c in query_patch.call_args_list if 'CREATE' in str(c)]
        self.assertEqual(len(create_calls), 1)
        self.assertNotIn('ICEBERG', create_calls[0])

    @patch('target_snowflake.db_sync.DbSync.grant_privilege')
    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_explicit_v3_create_is_verified_before_grants(self, query_patch, grant_patch):
        query_patch.return_value = [{'type': 'CSV'}]
        dbsync = db_sync.DbSync(
            self._table_sync_config(target_table_format='iceberg', iceberg_version=3),
            self._table_sync_schema(),
        )
        query_patch.reset_mock()

        with patch.object(
            dbsync,
            'discover_table_format',
            side_effect=(db_sync.TABLE_FORMAT_MISSING, db_sync.TABLE_FORMAT_MANAGED_ICEBERG_V3),
        ) as discover_format:
            grant_patch.side_effect = (
                lambda *args, **kwargs: self.assertEqual(discover_format.call_count, 2)
            )
            dbsync.sync_table()

        discover_format.assert_has_calls([
            call('dummy-schema', 'TABLE1'),
            call('dummy-schema', 'TABLE1'),
        ])
        create_query = query_patch.call_args_list[0].args[0]
        self.assertIn('CREATE ICEBERG TABLE IF NOT EXISTS', create_query)
        self.assertIn('"PAYLOAD" variant', create_query)
        self.assertIn("CATALOG='SNOWFLAKE'", create_query)
        self.assertIn('ICEBERG_VERSION=3', create_query)
        grant_patch.assert_called_once()

    @patch('target_snowflake.db_sync.DbSync.grant_privilege')
    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_explicit_create_rejects_concurrent_wrong_format(self, query_patch, grant_patch):
        query_patch.return_value = [{'type': 'CSV'}]
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
            ('iceberg', db_sync.TABLE_FORMAT_MANAGED_ICEBERG_V2),
            ('iceberg', db_sync.TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG),
            ('native', db_sync.TABLE_FORMAT_MANAGED_ICEBERG_V3),
        )
        for requested_format, physical_format in cases:
            with self.subTest(requested_format=requested_format, physical_format=physical_format):
                config = self._table_sync_config(target_table_format=requested_format)
                if requested_format == 'iceberg':
                    config['iceberg_version'] = 3
                query_patch.return_value = [{'type': 'CSV'}]
                dbsync = db_sync.DbSync(config, self._table_sync_schema())
                query_patch.reset_mock()

                with patch.object(dbsync, 'discover_table_format', return_value=physical_format), \
                        patch.object(dbsync, 'update_columns') as update_columns:
                    with self.assertRaises(TableFormatMismatchException):
                        dbsync.sync_table()

                query_patch.assert_not_called()
                update_columns.assert_not_called()

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_legacy_config_uses_existing_physical_format(self, query_patch):
        cases = (
            (True, db_sync.TABLE_FORMAT_NATIVE, False),
            (False, db_sync.TABLE_FORMAT_MANAGED_ICEBERG_V2, True),
            (False, db_sync.TABLE_FORMAT_MANAGED_ICEBERG_V3, True),
            (False, db_sync.TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG, True),
        )
        for iceberg_create, physical_format, is_iceberg in cases:
            with self.subTest(iceberg_create=iceberg_create, physical_format=physical_format):
                query_patch.return_value = [{'type': 'CSV'}]
                dbsync = db_sync.DbSync(
                    self._table_sync_config(iceberg_create=iceberg_create),
                    self._table_sync_schema(),
                )
                query_patch.reset_mock()

                with patch.object(dbsync, 'discover_table_format', return_value=physical_format), \
                        patch.object(dbsync, 'update_columns') as update_columns, \
                        patch.object(dbsync, '_refresh_table_pks') as refresh_table_pks:
                    dbsync.sync_table()

                update_columns.assert_called_once_with(is_iceberg, None)
                if is_iceberg:
                    refresh_table_pks.assert_not_called()
                else:
                    refresh_table_pks.assert_called_once_with()
                query_patch.assert_not_called()

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_v3_variant_requires_explicit_configuration(self, query_patch):
        """An omitted v3 format reports the missing configuration before mutation."""
        query_patch.return_value = [{'type': 'CSV'}]
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
                'DATA_TYPE': 'VARIANT',
            },
        ]
        dbsync = db_sync.DbSync(
            self._table_sync_config(),
            self._table_sync_schema(),
            table_cache,
        )
        query_patch.reset_mock()

        with patch.object(
            dbsync,
            'discover_table_format',
            return_value=db_sync.TABLE_FORMAT_MANAGED_ICEBERG_V3,
        ), self.assertRaisesRegex(
            TableFormatMismatchException,
            'target_table_format is omitted.*restore target_table_format: iceberg',
        ):
            dbsync.sync_table()

        query_patch.assert_not_called()
