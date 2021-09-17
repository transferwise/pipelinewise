import json
from unittest import TestCase
from unittest.mock import MagicMock

from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake


# pylint: disable=too-few-public-methods
class S3Mock:
    """
    Mocked boto3
    """

    def __init__(self):
        pass

    # pylint: disable=invalid-name
    def delete_object(self, Bucket, Key):
        """Do nothing when trying to delete file on s3"""

    def copy_object(self, **kwargs):
        """Mock if needed"""

    # pylint: disable=no-self-use, unused-argument
    def head_object(self, **kwargs):
        """Mock if needed"""
        return {}


class FastSyncTargetSnowflakeMock(FastSyncTargetSnowflake):
    """
    Mocked FastSyncTargetSnowflake class
    """

    def __init__(self, connection_config, transformation_config=None):
        super().__init__(connection_config, transformation_config)

        self.executed_queries = []
        self.s3 = S3Mock()

    def query(self, query, params=None, query_tag_props=None):
        self.executed_queries.append(query)
        return []


class TestFastSyncTargetSnowflake(TestCase):
    """
    Unit tests for fastsync target snowflake
    """

    def setUp(self) -> None:
        """Initialise test FastSyncTargetPostgres object"""
        self.snowflake = FastSyncTargetSnowflakeMock(
            connection_config={'s3_bucket': 'dummy_bucket', 'stage': 'dummy_stage'},
            transformation_config={},
        )

    def test_create_schema(self):
        """Validate if create schema queries generated correctly"""
        self.snowflake.create_schema('new_schema')
        self.assertListEqual(
            self.snowflake.executed_queries, ['CREATE SCHEMA IF NOT EXISTS new_schema']
        )

    def test_drop_table(self):
        """Validate if drop table queries generated correctly"""
        self.snowflake.drop_table('test_schema', 'test_table')
        self.snowflake.drop_table('test_schema', 'test_table', is_temporary=True)
        self.snowflake.drop_table('test_schema', 'UPPERCASE_TABLE')
        self.snowflake.drop_table('test_schema', 'UPPERCASE_TABLE', is_temporary=True)
        self.snowflake.drop_table('test_schema', 'test table with space')
        self.snowflake.drop_table(
            'test_schema', 'test table with space', is_temporary=True
        )
        assert self.snowflake.executed_queries == [
            'DROP TABLE IF EXISTS test_schema."TEST_TABLE"',
            'DROP TABLE IF EXISTS test_schema."TEST_TABLE_TEMP"',
            'DROP TABLE IF EXISTS test_schema."UPPERCASE_TABLE"',
            'DROP TABLE IF EXISTS test_schema."UPPERCASE_TABLE_TEMP"',
            'DROP TABLE IF EXISTS test_schema."TEST TABLE WITH SPACE"',
            'DROP TABLE IF EXISTS test_schema."TEST TABLE WITH SPACE_TEMP"',
        ]

    def test_create_table(self):
        """Validate if create table queries generated correctly"""
        # Create table with standard table and column names
        self.snowflake.executed_queries = []
        self.snowflake.create_table(
            target_schema='test_schema',
            table_name='test_table',
            columns=['"ID" INTEGER', '"TXT" VARCHAR'],
            primary_key=['"ID"'],
        )
        assert self.snowflake.executed_queries == [
            'CREATE OR REPLACE TABLE test_schema."TEST_TABLE" ('
            '"ID" INTEGER,"TXT" VARCHAR,'
            '_SDC_EXTRACTED_AT TIMESTAMP_NTZ,'
            '_SDC_BATCHED_AT TIMESTAMP_NTZ,'
            '_SDC_DELETED_AT VARCHAR'
            ', PRIMARY KEY ("ID"))'
        ]

        # Create table with reserved words in table and column names
        self.snowflake.executed_queries = []
        self.snowflake.create_table(
            target_schema='test_schema',
            table_name='order',
            columns=['"ID" INTEGER', '"TXT" VARCHAR', '"SELECT" VARCHAR'],
            primary_key=['"ID"'],
        )
        assert self.snowflake.executed_queries == [
            'CREATE OR REPLACE TABLE test_schema."ORDER" ('
            '"ID" INTEGER,"TXT" VARCHAR,"SELECT" VARCHAR,'
            '_SDC_EXTRACTED_AT TIMESTAMP_NTZ,'
            '_SDC_BATCHED_AT TIMESTAMP_NTZ,'
            '_SDC_DELETED_AT VARCHAR'
            ', PRIMARY KEY ("ID"))'
        ]

        # Create table with mixed lower and uppercase and space characters
        self.snowflake.executed_queries = []
        self.snowflake.create_table(
            target_schema='test_schema',
            table_name='TABLE with SPACE',
            columns=['"ID" INTEGER', '"COLUMN WITH SPACE" CHARACTER VARYING'],
            primary_key=['"ID"'],
        )
        assert self.snowflake.executed_queries == [
            'CREATE OR REPLACE TABLE test_schema."TABLE WITH SPACE" ('
            '"ID" INTEGER,"COLUMN WITH SPACE" CHARACTER VARYING,'
            '_SDC_EXTRACTED_AT TIMESTAMP_NTZ,'
            '_SDC_BATCHED_AT TIMESTAMP_NTZ,'
            '_SDC_DELETED_AT VARCHAR'
            ', PRIMARY KEY ("ID"))'
        ]

        # Create table with composite primary key
        self.snowflake.executed_queries = []
        self.snowflake.create_table(
            target_schema='test_schema',
            table_name='TABLE with SPACE',
            columns=[
                '"ID" INTEGER',
                '"NUM" INTEGER',
                '"COLUMN WITH SPACE" CHARACTER VARYING',
            ],
            primary_key=['"ID", "NUM"'],
        )
        assert self.snowflake.executed_queries == [
            'CREATE OR REPLACE TABLE test_schema."TABLE WITH SPACE" ('
            '"ID" INTEGER,"NUM" INTEGER,"COLUMN WITH SPACE" CHARACTER VARYING,'
            '_SDC_EXTRACTED_AT TIMESTAMP_NTZ,'
            '_SDC_BATCHED_AT TIMESTAMP_NTZ,'
            '_SDC_DELETED_AT VARCHAR'
            ', PRIMARY KEY ("ID", "NUM"))'
        ]

        # Create table with no primary key
        self.snowflake.executed_queries = []
        self.snowflake.create_table(
            target_schema='test_schema',
            table_name='test_table_no_pk',
            columns=['"ID" INTEGER', '"TXT" CHARACTER VARYING'],
            primary_key=None,
        )
        assert self.snowflake.executed_queries == [
            'CREATE OR REPLACE TABLE test_schema."TEST_TABLE_NO_PK" ('
            '"ID" INTEGER,"TXT" CHARACTER VARYING,'
            '_SDC_EXTRACTED_AT TIMESTAMP_NTZ,'
            '_SDC_BATCHED_AT TIMESTAMP_NTZ,'
            '_SDC_DELETED_AT VARCHAR)'
        ]

    def test_copy_to_table(self):
        """Validate if COPY command generated correctly"""
        # COPY table with standard table and column names
        self.snowflake.executed_queries = []
        self.snowflake.copy_to_table(
            s3_key='s3_key',
            target_schema='test_schema',
            table_name='test_table',
            size_bytes=1000,
            is_temporary=False,
            skip_csv_header=False,
        )
        assert self.snowflake.executed_queries == [
            'COPY INTO test_schema."TEST_TABLE" FROM \'@dummy_stage/s3_key\''
            ' FILE_FORMAT = (type=CSV escape=\'\\x1e\' escape_unenclosed_field=\'\\x1e\''
            ' field_optionally_enclosed_by=\'\"\' skip_header=0'
            ' compression=GZIP binary_format=HEX)'
        ]

        # COPY table with reserved word in table and column names in temp table
        self.snowflake.executed_queries = []
        self.snowflake.copy_to_table(
            s3_key='s3_key',
            target_schema='test_schema',
            table_name='full',
            size_bytes=1000,
            is_temporary=True,
            skip_csv_header=False,
        )
        assert self.snowflake.executed_queries == [
            'COPY INTO test_schema."FULL_TEMP" FROM \'@dummy_stage/s3_key\''
            ' FILE_FORMAT = (type=CSV escape=\'\\x1e\' escape_unenclosed_field=\'\\x1e\''
            ' field_optionally_enclosed_by=\'\"\' skip_header=0'
            ' compression=GZIP binary_format=HEX)'
        ]

        # COPY table with space and uppercase in table name and s3 key
        self.snowflake.executed_queries = []
        self.snowflake.copy_to_table(
            s3_key='s3 key with space',
            target_schema='test_schema',
            table_name='table with SPACE and UPPERCASE',
            size_bytes=1000,
            is_temporary=True,
            skip_csv_header=False,
        )
        assert self.snowflake.executed_queries == [
            'COPY INTO test_schema."TABLE WITH SPACE AND UPPERCASE_TEMP" FROM \'@dummy_stage/s3 key with space\''
            ' FILE_FORMAT = (type=CSV escape=\'\\x1e\' escape_unenclosed_field=\'\\x1e\''
            ' field_optionally_enclosed_by=\'\"\' skip_header=0'
            ' compression=GZIP binary_format=HEX)'
        ]

    def test_grant_select_on_table(self):
        """Validate if GRANT command generated correctly"""
        # GRANT table with standard table and column names
        self.snowflake.executed_queries = []
        self.snowflake.grant_select_on_table(
            target_schema='test_schema',
            table_name='test_table',
            role='test_role',
            is_temporary=False,
        )
        assert self.snowflake.executed_queries == [
            'GRANT SELECT ON test_schema."TEST_TABLE" TO ROLE test_role'
        ]

        # GRANT table with reserved word in table and column names in temp table
        self.snowflake.executed_queries = []
        self.snowflake.grant_select_on_table(
            target_schema='test_schema',
            table_name='full',
            role='test_role',
            is_temporary=False,
        )
        assert self.snowflake.executed_queries == [
            'GRANT SELECT ON test_schema."FULL" TO ROLE test_role'
        ]

        # GRANT table with with space and uppercase in table name and s3 key
        self.snowflake.executed_queries = []
        self.snowflake.grant_select_on_table(
            target_schema='test_schema',
            table_name='table with SPACE and UPPERCASE',
            role='test_role',
            is_temporary=False,
        )
        assert self.snowflake.executed_queries == [
            'GRANT SELECT ON test_schema."TABLE WITH SPACE AND UPPERCASE" TO ROLE test_role'
        ]

    def test_grant_usage_on_schema(self):
        """Validate if GRANT command generated correctly"""
        self.snowflake.executed_queries = []
        self.snowflake.grant_usage_on_schema(
            target_schema='test_schema', role='test_role'
        )
        assert self.snowflake.executed_queries == [
            'GRANT USAGE ON SCHEMA test_schema TO ROLE test_role'
        ]

    def test_grant_select_on_schema(self):
        """Validate if GRANT command generated correctly"""
        self.snowflake.executed_queries = []
        self.snowflake.grant_select_on_schema(
            target_schema='test_schema', role='test_role'
        )
        assert self.snowflake.executed_queries == [
            'GRANT SELECT ON ALL TABLES IN SCHEMA test_schema TO ROLE test_role'
        ]

    def test_swap_tables(self):
        """Validate if swap table commands generated correctly"""
        # Swap tables with standard table and column names
        self.snowflake.executed_queries = []
        self.snowflake.swap_tables(schema='test_schema', table_name='test_table')
        assert self.snowflake.executed_queries == [
            'ALTER TABLE test_schema."TEST_TABLE_TEMP" SWAP WITH test_schema."TEST_TABLE"',
            'DROP TABLE IF EXISTS test_schema."TEST_TABLE_TEMP"',
        ]

        # Swap tables with reserved word in table and column names in temp table
        self.snowflake.executed_queries = []
        self.snowflake.swap_tables(schema='test_schema', table_name='full')
        assert self.snowflake.executed_queries == [
            'ALTER TABLE test_schema."FULL_TEMP" SWAP WITH test_schema."FULL"',
            'DROP TABLE IF EXISTS test_schema."FULL_TEMP"',
        ]

        # Swap tables with with space and uppercase in table name and s3 key
        self.snowflake.executed_queries = []
        self.snowflake.swap_tables(
            schema='test_schema', table_name='table with SPACE and UPPERCASE'
        )
        assert self.snowflake.executed_queries == [
            'ALTER TABLE test_schema."TABLE WITH SPACE AND UPPERCASE_TEMP" '
            'SWAP WITH test_schema."TABLE WITH SPACE AND UPPERCASE"',
            'DROP TABLE IF EXISTS test_schema."TABLE WITH SPACE AND UPPERCASE_TEMP"',
        ]

    def test_create_query_tag(self):
        """Validate if query tag generated correctly"""
        self.snowflake.connection_config['dbname'] = 'fake_db'

        # not passing query_tag_props
        assert json.loads(self.snowflake.create_query_tag()) == {
            'ppw_component': 'fastsync',
            'tap_id': None,
            'database': 'fake_db',
            'schema': None,
            'table': None,
        }

        # passing invalid query_tag_props (string)
        assert json.loads(self.snowflake.create_query_tag('invalid_query_props')) == {
            'ppw_component': 'fastsync',
            'tap_id': None,
            'database': 'fake_db',
            'schema': None,
            'table': None,
        }

        # passing invalid query_tag_props (number)
        self.snowflake.connection_config['tap_id'] = 'fake_tap'
        assert json.loads(self.snowflake.create_query_tag(1234567890)) == {
            'ppw_component': 'fastsync',
            'tap_id': 'fake_tap',
            'database': 'fake_db',
            'schema': None,
            'table': None,
        }

        # passing invalid query_tag_props (array)
        self.snowflake.connection_config['tap_id'] = 'fake_tap'
        assert json.loads(self.snowflake.create_query_tag([1, 2, 3])) == {
            'ppw_component': 'fastsync',
            'tap_id': 'fake_tap',
            'database': 'fake_db',
            'schema': None,
            'table': None,
        }

        # passing invalid query_tag_props
        self.snowflake.connection_config['tap_id'] = 'fake_tap'
        assert json.loads(self.snowflake.create_query_tag()) == {
            'ppw_component': 'fastsync',
            'tap_id': 'fake_tap',
            'database': 'fake_db',
            'schema': None,
            'table': None,
        }

        # passing valid query_props
        self.snowflake.connection_config['tap_id'] = 'fake_tap'
        assert json.loads(
            self.snowflake.create_query_tag(
                {'schema': 'fake_schema', 'table': 'fake_table'}
            )
        ) == {
            'ppw_component': 'fastsync',
            'tap_id': 'fake_tap',
            'database': 'fake_db',
            'schema': 'fake_schema',
            'table': 'fake_table',
        }

        # passing partial query_props
        self.snowflake.connection_config['tap_id'] = 'fake_tap'
        assert json.loads(
            self.snowflake.create_query_tag({'schema': 'fake_schema'})
        ) == {
            'ppw_component': 'fastsync',
            'tap_id': 'fake_tap',
            'database': 'fake_db',
            'schema': 'fake_schema',
            'table': None,
        }

        # passing partial query_props
        self.snowflake.connection_config['tap_id'] = 'fake_tap'
        assert json.loads(self.snowflake.create_query_tag({'table': 'fake_table'})) == {
            'ppw_component': 'fastsync',
            'tap_id': 'fake_tap',
            'database': 'fake_db',
            'schema': None,
            'table': 'fake_table',
        }

        # passing not supported query_props
        self.snowflake.connection_config['tap_id'] = 'fake_tap'
        assert json.loads(
            self.snowflake.create_query_tag({'fake_prop': 'fake_value'})
        ) == {
            'ppw_component': 'fastsync',
            'tap_id': 'fake_tap',
            'database': 'fake_db',
            'schema': None,
            'table': None,
        }

    def test_obfuscate_columns_case1(self):
        """
        Test obfuscation where given transformations are emtpy
        Test should pass with no executed queries
        """
        target_schema = 'my_schema'
        table_name = 'public.my_table'

        self.snowflake.transformation_config = {}

        self.snowflake.obfuscate_columns(target_schema, table_name)
        self.assertFalse(self.snowflake.executed_queries)

    def test_obfuscate_columns_case2(self):
        """
        Test obfuscation where given transformations has an unsupported transformation type
        Test should fail
        """
        target_schema = 'my_schema'
        table_name = 'public.my_table'

        self.snowflake.transformation_config = {
            'transformations': [
                {
                    'field_id': 'col_7',
                    'tap_stream_name': 'public-my_table',
                    'type': 'RANDOM',
                }
            ]
        }

        with self.assertRaises(ValueError):
            self.snowflake.obfuscate_columns(target_schema, table_name)

        self.assertFalse(self.snowflake.executed_queries)

    def test_obfuscate_columns_case3(self):
        """
        Test obfuscation where given transformations have no conditions
        Test should pass
        """
        target_schema = 'my_schema'
        table_name = 'public.my_table'

        self.snowflake.transformation_config = {
            'transformations': [
                {
                    'field_id': 'col_1',
                    'tap_stream_name': 'public-my_table',
                    'type': 'SET-NULL',
                },
                {
                    'field_id': 'col_2',
                    'tap_stream_name': 'public-my_table',
                    'type': 'MASK-HIDDEN',
                },
                {
                    'field_id': 'col_3',
                    'tap_stream_name': 'public-my_table',
                    'type': 'MASK-DATE',
                },
                {
                    'field_id': 'col_4',
                    'tap_stream_name': 'public-my_table',
                    'safe_field_id': '"COL_4"',
                    'type': 'MASK-NUMBER',
                },
                {
                    'field_id': 'col_5',
                    'tap_stream_name': 'public-my_table',
                    'type': 'HASH',
                },
                {
                    'field_id': 'col_6',
                    'tap_stream_name': 'public-my_table',
                    'type': 'HASH-SKIP-FIRST-5',
                },
                {
                    'field_id': 'col_7',
                    'tap_stream_name': 'public-my_table',
                    'type': 'MASK-STRING-SKIP-ENDS-3',
                },
            ]
        }

        self.snowflake.obfuscate_columns(target_schema, table_name)

        self.assertListEqual(
            self.snowflake.executed_queries,
            [
                'UPDATE "MY_SCHEMA"."MY_TABLE_TEMP" SET '
                '"COL_1" = NULL, '
                '"COL_2" = \'hidden\', '
                '"COL_3" = TIMESTAMP_NTZ_FROM_PARTS(DATE_FROM_PARTS(YEAR("COL_3"), 1, 1),TO_TIME("COL_3")), '
                '"COL_4" = 0, '
                '"COL_5" = SHA2("COL_5", 256), '
                '"COL_6" = CONCAT(SUBSTRING("COL_6", 1, 5), SHA2(SUBSTRING("COL_6", 5 + 1), 256)), '
                '"COL_7" = CASE WHEN LENGTH("COL_7") > 2 * 3 THEN '
                'CONCAT(SUBSTRING("COL_7", 1, 3), REPEAT(\'*\', LENGTH("COL_7")-(2 * 3)), '
                'SUBSTRING("COL_7", LENGTH("COL_7")-3+1, 3)) '
                'ELSE "COL_7" END;'
            ],
        )

    def test_obfuscate_columns_case4(self):
        """
        Test obfuscation where given transformations have conditions
        Test should pass
        """
        target_schema = 'my_schema'
        table_name = 'public.my_table'

        self.snowflake.transformation_config = {
            'transformations': [
                {
                    'field_id': 'col_1',
                    'tap_stream_name': 'public-my_table',
                    'type': 'SET-NULL',
                },
                {
                    'field_id': 'col_2',
                    'tap_stream_name': 'public-my_table',
                    'type': 'MASK-HIDDEN',
                    'when': [
                        {'column': 'col_4', 'safe_column': '"COL_4"', 'equals': None},
                        {
                            'column': 'col_1',
                        },
                    ],
                },
                {
                    'field_id': 'col_3',
                    'tap_stream_name': 'public-my_table',
                    'type': 'MASK-DATE',
                    'when': [{'column': 'col_5', 'equals': 'some_value'}],
                },
                {
                    'field_id': 'col_4',
                    'tap_stream_name': 'public-my_table',
                    'type': 'MASK-NUMBER',
                },
                {
                    'field_id': 'col_5',
                    'tap_stream_name': 'public-my_table',
                    'type': 'HASH',
                },
                {
                    'field_id': 'col_6',
                    'tap_stream_name': 'public-my_table',
                    'type': 'HASH-SKIP-FIRST-5',
                    'when': [
                        {'column': 'col_1', 'equals': 30},
                        {'column': 'col_2', 'regex_match': r'[0-9]{3}\.[0-9]{3}'},
                    ],
                },
                {
                    'field_id': 'col_7',
                    'tap_stream_name': 'public-my_table',
                    'type': 'MASK-STRING-SKIP-ENDS-3',
                    'when': [
                        {'column': 'col_1', 'equals': 30},
                        {'column': 'col_2', 'regex_match': r'[0-9]{3}\.[0-9]{3}'},
                        {'column': 'col_4', 'equals': None},
                    ],
                },
            ]
        }

        self.snowflake.obfuscate_columns(target_schema, table_name)

        self.assertListEqual(
            self.snowflake.executed_queries,
            [
                'UPDATE "MY_SCHEMA"."MY_TABLE_TEMP" SET "COL_2" = \'hidden\' WHERE ("COL_4" IS NULL);',
                'UPDATE "MY_SCHEMA"."MY_TABLE_TEMP" SET "COL_3" = TIMESTAMP_NTZ_FROM_PARTS('
                'DATE_FROM_PARTS(YEAR("COL_3"), 1, 1),TO_TIME("COL_3")) WHERE ("COL_5" = \'some_value\');',
                'UPDATE "MY_SCHEMA"."MY_TABLE_TEMP" SET '
                '"COL_6" = CONCAT(SUBSTRING("COL_6", 1, 5), SHA2(SUBSTRING("COL_6", 5 + 1), 256)) '
                'WHERE ("COL_1" = 30) AND ("COL_2" '
                'REGEXP \'[0-9]{3}\.[0-9]{3}\');',  # pylint: disable=W1401  # noqa: W605
                'UPDATE "MY_SCHEMA"."MY_TABLE_TEMP" SET '
                '"COL_7" = CASE WHEN LENGTH("COL_7") > 2 * 3 THEN '
                'CONCAT(SUBSTRING("COL_7", 1, 3), REPEAT(\'*\', LENGTH("COL_7")-(2 * 3)), '
                'SUBSTRING("COL_7", LENGTH("COL_7")-3+1, 3)) '
                'ELSE "COL_7" END WHERE ("COL_1" = 30) AND ("COL_2" '
                'REGEXP \'[0-9]{3}\.[0-9]{3}\') AND ("COL_4" IS NULL);',  # pylint: disable=W1401  # noqa: W605
                'UPDATE "MY_SCHEMA"."MY_TABLE_TEMP" SET "COL_1" = NULL, "COL_4" = 0, "COL_5" = SHA2("COL_5", 256);',
            ],
        )

    # pylint: disable=invalid-name
    def test_default_archive_destination(self):
        """
        Validate parameters passed to s3 copy_object method when custom s3 bucket and folder are not defined
        """
        mock_copy_object = MagicMock()
        self.snowflake.s3.copy_object = mock_copy_object
        self.snowflake.connection_config['s3_bucket'] = 'some_bucket'
        self.snowflake.copy_to_archive(
            'snowflake-import/ppw_20210615115603_fastsync.csv.gz',
            'some-tap',
            'some_schema.some_table',
        )

        mock_copy_object.assert_called_with(
            Bucket='some_bucket',
            CopySource='some_bucket/snowflake-import/ppw_20210615115603_fastsync.csv.gz',
            Key='archive/some-tap/some_table/ppw_20210615115603_fastsync.csv.gz',
            Metadata={
                'tap': 'some-tap',
                'schema': 'some_schema',
                'table': 'some_table',
                'archived-by': 'pipelinewise_fastsync_postgres_to_snowflake',
            },
            MetadataDirective='REPLACE',
        )

    # pylint: disable=invalid-name
    def test_custom_archive_destination(self):
        """
        Validate parameters passed to s3 copy_object method when using custom s3 bucket and folder
        """
        mock_copy_object = MagicMock()
        self.snowflake.s3.copy_object = mock_copy_object
        self.snowflake.connection_config['s3_bucket'] = 'some_bucket'
        self.snowflake.connection_config[
            'archive_load_files_s3_bucket'
        ] = 'archive_bucket'
        self.snowflake.connection_config[
            'archive_load_files_s3_prefix'
        ] = 'archive_folder'
        self.snowflake.copy_to_archive(
            'snowflake-import/ppw_20210615115603_fastsync.csv.gz',
            'some-tap',
            'some_schema.some_table',
        )

        mock_copy_object.assert_called_with(
            Bucket='archive_bucket',
            CopySource='some_bucket/snowflake-import/ppw_20210615115603_fastsync.csv.gz',
            Key='archive_folder/some-tap/some_table/ppw_20210615115603_fastsync.csv.gz',
            Metadata={
                'tap': 'some-tap',
                'schema': 'some_schema',
                'table': 'some_table',
                'archived-by': 'pipelinewise_fastsync_postgres_to_snowflake',
            },
            MetadataDirective='REPLACE',
        )

    # pylint: disable=invalid-name
    def test_copied_archive_metadata(self):
        """
        Validate parameters passed to s3 copy_object method when custom s3 bucket and folder are not defined
        """
        mock_head_object = MagicMock()
        mock_head_object.return_value = {
            'Metadata': {'copied-old-key': 'copied-old-value'}
        }
        mock_copy_object = MagicMock()
        self.snowflake.s3.copy_object = mock_copy_object
        self.snowflake.s3.head_object = mock_head_object
        self.snowflake.connection_config['s3_bucket'] = 'some_bucket'
        self.snowflake.copy_to_archive(
            'snowflake-import/ppw_20210615115603_fastsync.csv.gz',
            'some-tap',
            'some_schema.some_table',
        )

        mock_copy_object.assert_called_with(
            Bucket='some_bucket',
            CopySource='some_bucket/snowflake-import/ppw_20210615115603_fastsync.csv.gz',
            Key='archive/some-tap/some_table/ppw_20210615115603_fastsync.csv.gz',
            Metadata={
                'copied-old-key': 'copied-old-value',
                'tap': 'some-tap',
                'schema': 'some_schema',
                'table': 'some_table',
                'archived-by': 'pipelinewise_fastsync_postgres_to_snowflake',
            },
            MetadataDirective='REPLACE',
        )
