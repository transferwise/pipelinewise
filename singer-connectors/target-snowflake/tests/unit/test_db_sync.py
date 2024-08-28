import json
import unittest

from unittest.mock import patch, call

from target_snowflake import db_sync
from target_snowflake.exceptions import PrimaryKeyNotFoundException


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
            'password': "dummy-value",
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
            'password': "dummy-value",
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
            'password': "dummy-value",
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
            'password': "dummy-value",
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
    @patch('target_snowflake.db_sync.DbSync._load_file_merge')
    def test_merge_failure_message(self, load_file_merge_patch, query_patch):
        LOGGER_NAME = "target_snowflake"
        query_patch.return_value = [{'type': 'CSV'}]
        minimal_config = {
            'account': "dummy_account",
            'dbname': "dummy_dbname",
            'user': "dummy_user",
            'password': "dummy_password",
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
            'password': "dummy_password",
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
            'password': "dummy-passwd",
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
            [{'type': 'CSV'}],
            [{'column_name': 'ID'}],
            None
        ]

        dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
        dbsync.sync_table()

        query_patch.assert_has_calls([
            call('SHOW FILE FORMATS LIKE \'dummy-file-format\''),
            call('show primary keys in table dummy-db.dummy-schema."TABLE1";'),
            call(['alter table dummy-schema."TABLE1" alter column "ID" drop not null;'])
        ])

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_with_new_pk_in_stream(self, query_patch):
        minimal_config = {
            'account': "dummy-account",
            'dbname': "dummy-db",
            'user': "dummy-user",
            'password': "dummy-passwd",
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
            [{'type': 'CSV'}],
            [{'column_name': 'ID'}],
            None
        ]

        dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
        dbsync.sync_table()

        # due to usage of sets in the code, order of columns in queries in not guaranteed
        # so have to break assertions to account for this.
        calls = query_patch.call_args_list
        self.assertEqual(3, len(calls))

        self.assertEqual('SHOW FILE FORMATS LIKE \'dummy-file-format\'', calls[0][0][0])
        self.assertEqual('show primary keys in table dummy-db.dummy-schema."TABLE1";', calls[1][0][0])

        self.assertEqual('alter table dummy-schema."TABLE1" drop primary key;', calls[2][0][0][0])

        self.assertIn(calls[2][0][0][1], {'alter table dummy-schema."TABLE1" add primary key("ID", "NAME");',
                                          'alter table dummy-schema."TABLE1" add primary key("NAME", "ID");'})

        self.assertListEqual(sorted(calls[2][0][0][2:]),
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
            'password': "dummy-passwd",
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
            [{'type': 'CSV'}],
            [{'column_name': 'ID'}],
            None
        ]

        dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
        dbsync.sync_table()

        query_patch.assert_has_calls([
            call('SHOW FILE FORMATS LIKE \'dummy-file-format\''),
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
            'password': "dummy-passwd",
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
            [{'type': 'CSV'}],
            [],
            None
        ]

        dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
        dbsync.sync_table()

        query_patch.assert_has_calls([
            call('SHOW FILE FORMATS LIKE \'dummy-file-format\''),
            call('show primary keys in table dummy-db.dummy-schema."TABLE1";'),
            call(['alter table dummy-schema."TABLE1" add primary key("ID");',
                  'alter table dummy-schema."TABLE1" alter column "ID" drop not null;'])
        ])
