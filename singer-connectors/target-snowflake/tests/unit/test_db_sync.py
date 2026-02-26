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
            [],                           # SHOW TERSE ICEBERG TABLES (not Iceberg)
            [{'column_name': 'ID'}],     # show primary keys
            None                          # ALTER TABLE
        ]

        dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
        dbsync.sync_table()

        query_patch.assert_has_calls([
            call('SHOW FILE FORMATS LIKE \'dummy-file-format\''),
            call("SHOW TERSE ICEBERG TABLES LIKE 'TABLE1' IN SCHEMA DUMMY-DB.dummy-schema"),
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
            [],                           # SHOW TERSE ICEBERG TABLES (not Iceberg)
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
        self.assertEqual("SHOW TERSE ICEBERG TABLES LIKE 'TABLE1' IN SCHEMA DUMMY-DB.dummy-schema", calls[1][0][0])
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
            [],                           # SHOW TERSE ICEBERG TABLES (not Iceberg)
            [{'column_name': 'ID'}],     # show primary keys
            None                          # ALTER TABLE
        ]

        dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
        dbsync.sync_table()

        query_patch.assert_has_calls([
            call('SHOW FILE FORMATS LIKE \'dummy-file-format\''),
            call("SHOW TERSE ICEBERG TABLES LIKE 'TABLE1' IN SCHEMA DUMMY-DB.dummy-schema"),
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
            [],                           # SHOW TERSE ICEBERG TABLES (not Iceberg)
            [],                           # show primary keys (no existing PK)
            None                          # ALTER TABLE add PK
        ]

        dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
        dbsync.sync_table()

        query_patch.assert_has_calls([
            call('SHOW FILE FORMATS LIKE \'dummy-file-format\''),
            call("SHOW TERSE ICEBERG TABLES LIKE 'TABLE1' IN SCHEMA DUMMY-DB.dummy-schema"),
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
    @patch('target_snowflake.db_sync.DbSync.get_tables')
    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_creates_iceberg_when_iceberg_create_true(self, query_patch, get_tables_patch, grant_patch):
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
        # get_tables returns [] so found_tables is empty (table doesn't exist yet)
        get_tables_patch.return_value = []
        query_patch.side_effect = [
            [{'type': 'CSV'}],    # SHOW FILE FORMATS (during __init__)
            [],                    # SHOW TERSE ICEBERG TABLES (not Iceberg yet)
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
                [{'table_name': 'TABLE1'}],       # SHOW TERSE ICEBERG TABLES → is_iceberg_table=True
            ]
            dbsync = db_sync.DbSync(minimal_config, stream_schema_message, table_cache)
            dbsync.sync_table()

        create_calls = [str(c) for c in query_patch.call_args_list if 'CREATE' in str(c)]
        self.assertEqual(len(create_calls), 0, msg="CREATE must not be issued for an already-existing Iceberg table")

    @patch('target_snowflake.db_sync.DbSync.grant_privilege')
    @patch('target_snowflake.db_sync.DbSync.get_tables')
    @patch('target_snowflake.db_sync.DbSync.query')
    def test_sync_table_creates_regular_table_when_iceberg_create_false(self, query_patch, get_tables_patch, grant_patch):
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
        get_tables_patch.return_value = []
        query_patch.side_effect = [
            [{'type': 'CSV'}],    # SHOW FILE FORMATS (during __init__)
            [],     # SHOW TERSE ICEBERG TABLES
            None,   # CREATE TABLE
            [],     # show primary keys
            None,   # ALTER TABLE
        ]
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        dbsync.sync_table()

        create_calls = [str(c) for c in query_patch.call_args_list if 'CREATE' in str(c)]
        self.assertEqual(len(create_calls), 1)
        self.assertNotIn('ICEBERG', create_calls[0])
