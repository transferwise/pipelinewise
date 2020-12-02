import json
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


class FastSyncTargetSnowflakeMock(FastSyncTargetSnowflake):
    """
    Mocked FastSyncTargetPostgres class
    """
    def __init__(self, connection_config, transformation_config=None):
        super().__init__(connection_config, transformation_config)

        self.executed_queries = []
        self.s3 = S3Mock()

    def query(self, query, params=None, query_tag_props=None):
        self.executed_queries.append(query)
        return []


# pylint: disable=attribute-defined-outside-init
class TestFastSyncTargetSnowflake:
    """
    Unit tests for fastsync target snowflake
    """
    def setup_method(self):
        """Initialise test FastSyncTargetPostgres object"""
        self.snowflake = FastSyncTargetSnowflakeMock(connection_config={'s3_bucket': 'dummy_bucket',
                                                                        'stage': 'dummy_stage'},
                                                     transformation_config={})

    def test_create_schema(self):
        """Validate if create schema queries generated correctly"""
        self.snowflake.create_schema('new_schema')
        assert self.snowflake.executed_queries == ['CREATE SCHEMA IF NOT EXISTS new_schema']

    def test_drop_table(self):
        """Validate if drop table queries generated correctly"""
        self.snowflake.drop_table('test_schema', 'test_table')
        self.snowflake.drop_table('test_schema', 'test_table', is_temporary=True)
        self.snowflake.drop_table('test_schema', 'UPPERCASE_TABLE')
        self.snowflake.drop_table('test_schema', 'UPPERCASE_TABLE', is_temporary=True)
        self.snowflake.drop_table('test_schema', 'test table with space')
        self.snowflake.drop_table('test_schema', 'test table with space', is_temporary=True)
        assert self.snowflake.executed_queries == [
            'DROP TABLE IF EXISTS test_schema."TEST_TABLE"',
            'DROP TABLE IF EXISTS test_schema."TEST_TABLE_TEMP"',
            'DROP TABLE IF EXISTS test_schema."UPPERCASE_TABLE"',
            'DROP TABLE IF EXISTS test_schema."UPPERCASE_TABLE_TEMP"',
            'DROP TABLE IF EXISTS test_schema."TEST TABLE WITH SPACE"',
            'DROP TABLE IF EXISTS test_schema."TEST TABLE WITH SPACE_TEMP"']

    def test_create_table(self):
        """Validate if create table queries generated correctly"""
        # Create table with standard table and column names
        self.snowflake.executed_queries = []
        self.snowflake.create_table(target_schema='test_schema',
                                    table_name='test_table',
                                    columns=['"ID" INTEGER',
                                             '"TXT" VARCHAR'],
                                    primary_key=['"ID"'])
        assert self.snowflake.executed_queries == [
            'CREATE OR REPLACE TABLE test_schema."TEST_TABLE" ('
            '"ID" INTEGER,"TXT" VARCHAR,'
            '_SDC_EXTRACTED_AT TIMESTAMP_NTZ,'
            '_SDC_BATCHED_AT TIMESTAMP_NTZ,'
            '_SDC_DELETED_AT VARCHAR'
            ', PRIMARY KEY ("ID"))']

        # Create table with reserved words in table and column names
        self.snowflake.executed_queries = []
        self.snowflake.create_table(target_schema='test_schema',
                                    table_name='order',
                                    columns=['"ID" INTEGER',
                                             '"TXT" VARCHAR',
                                             '"SELECT" VARCHAR'],
                                    primary_key=['"ID"'])
        assert self.snowflake.executed_queries == [
            'CREATE OR REPLACE TABLE test_schema."ORDER" ('
            '"ID" INTEGER,"TXT" VARCHAR,"SELECT" VARCHAR,'
            '_SDC_EXTRACTED_AT TIMESTAMP_NTZ,'
            '_SDC_BATCHED_AT TIMESTAMP_NTZ,'
            '_SDC_DELETED_AT VARCHAR'
            ', PRIMARY KEY ("ID"))']

        # Create table with mixed lower and uppercase and space characters
        self.snowflake.executed_queries = []
        self.snowflake.create_table(target_schema='test_schema',
                                    table_name='TABLE with SPACE',
                                    columns=['"ID" INTEGER',
                                             '"COLUMN WITH SPACE" CHARACTER VARYING'],
                                    primary_key=['"ID"'])
        assert self.snowflake.executed_queries == [
            'CREATE OR REPLACE TABLE test_schema."TABLE WITH SPACE" ('
            '"ID" INTEGER,"COLUMN WITH SPACE" CHARACTER VARYING,'
            '_SDC_EXTRACTED_AT TIMESTAMP_NTZ,'
            '_SDC_BATCHED_AT TIMESTAMP_NTZ,'
            '_SDC_DELETED_AT VARCHAR'
            ', PRIMARY KEY ("ID"))']

        # Create table with composite primary key
        self.snowflake.executed_queries = []
        self.snowflake.create_table(target_schema='test_schema',
                                    table_name='TABLE with SPACE',
                                    columns=['"ID" INTEGER',
                                             '"NUM" INTEGER',
                                             '"COLUMN WITH SPACE" CHARACTER VARYING'],
                                    primary_key=['"ID", "NUM"'])
        assert self.snowflake.executed_queries == [
            'CREATE OR REPLACE TABLE test_schema."TABLE WITH SPACE" ('
            '"ID" INTEGER,"NUM" INTEGER,"COLUMN WITH SPACE" CHARACTER VARYING,'
            '_SDC_EXTRACTED_AT TIMESTAMP_NTZ,'
            '_SDC_BATCHED_AT TIMESTAMP_NTZ,'
            '_SDC_DELETED_AT VARCHAR'
            ', PRIMARY KEY ("ID", "NUM"))']

        # Create table with no primary key
        self.snowflake.executed_queries = []
        self.snowflake.create_table(target_schema='test_schema',
                                    table_name='test_table_no_pk',
                                    columns=['"ID" INTEGER',
                                             '"TXT" CHARACTER VARYING'],
                                    primary_key=None)
        assert self.snowflake.executed_queries == [
            'CREATE OR REPLACE TABLE test_schema."TEST_TABLE_NO_PK" ('
            '"ID" INTEGER,"TXT" CHARACTER VARYING,'
            '_SDC_EXTRACTED_AT TIMESTAMP_NTZ,'
            '_SDC_BATCHED_AT TIMESTAMP_NTZ,'
            '_SDC_DELETED_AT VARCHAR)']

    def test_copy_to_table(self):
        """Validate if COPY command generated correctly"""
        # COPY table with standard table and column names
        self.snowflake.executed_queries = []
        self.snowflake.copy_to_table(s3_key='s3_key',
                                     target_schema='test_schema',
                                     table_name='test_table',
                                     size_bytes=1000,
                                     is_temporary=False,
                                     skip_csv_header=False)
        assert self.snowflake.executed_queries == [
            'COPY INTO test_schema."TEST_TABLE" FROM \'@dummy_stage/s3_key\''
            ' FILE_FORMAT = (type=CSV escape=\'\\x1e\' escape_unenclosed_field=\'\\x1e\''
            ' field_optionally_enclosed_by=\'\"\' skip_header=0'
            ' compression=GZIP binary_format=HEX)']

        # COPY table with reserved word in table and column names in temp table
        self.snowflake.executed_queries = []
        self.snowflake.copy_to_table(s3_key='s3_key',
                                     target_schema='test_schema',
                                     table_name='full',
                                     size_bytes=1000,
                                     is_temporary=True,
                                     skip_csv_header=False)
        assert self.snowflake.executed_queries == [
            'COPY INTO test_schema."FULL_TEMP" FROM \'@dummy_stage/s3_key\''
            ' FILE_FORMAT = (type=CSV escape=\'\\x1e\' escape_unenclosed_field=\'\\x1e\''
            ' field_optionally_enclosed_by=\'\"\' skip_header=0'
            ' compression=GZIP binary_format=HEX)']

        # COPY table with space and uppercase in table name and s3 key
        self.snowflake.executed_queries = []
        self.snowflake.copy_to_table(s3_key='s3 key with space',
                                     target_schema='test_schema',
                                     table_name='table with SPACE and UPPERCASE',
                                     size_bytes=1000,
                                     is_temporary=True,
                                     skip_csv_header=False)
        assert self.snowflake.executed_queries == [
            'COPY INTO test_schema."TABLE WITH SPACE AND UPPERCASE_TEMP" FROM \'@dummy_stage/s3 key with space\''
            ' FILE_FORMAT = (type=CSV escape=\'\\x1e\' escape_unenclosed_field=\'\\x1e\''
            ' field_optionally_enclosed_by=\'\"\' skip_header=0'
            ' compression=GZIP binary_format=HEX)']

    def test_grant_select_on_table(self):
        """Validate if GRANT command generated correctly"""
        # GRANT table with standard table and column names
        self.snowflake.executed_queries = []
        self.snowflake.grant_select_on_table(target_schema='test_schema',
                                             table_name='test_table',
                                             role='test_role',
                                             is_temporary=False)
        assert self.snowflake.executed_queries == [
            'GRANT SELECT ON test_schema."TEST_TABLE" TO ROLE test_role']

        # GRANT table with reserved word in table and column names in temp table
        self.snowflake.executed_queries = []
        self.snowflake.grant_select_on_table(target_schema='test_schema',
                                             table_name='full',
                                             role='test_role',
                                             is_temporary=False)
        assert self.snowflake.executed_queries == [
            'GRANT SELECT ON test_schema."FULL" TO ROLE test_role']

        # GRANT table with with space and uppercase in table name and s3 key
        self.snowflake.executed_queries = []
        self.snowflake.grant_select_on_table(target_schema='test_schema',
                                             table_name='table with SPACE and UPPERCASE',
                                             role='test_role',
                                             is_temporary=False)
        assert self.snowflake.executed_queries == [
            'GRANT SELECT ON test_schema."TABLE WITH SPACE AND UPPERCASE" TO ROLE test_role']

    def test_grant_usage_on_schema(self):
        """Validate if GRANT command generated correctly"""
        self.snowflake.executed_queries = []
        self.snowflake.grant_usage_on_schema(target_schema='test_schema',
                                             role='test_role')
        assert self.snowflake.executed_queries == [
            'GRANT USAGE ON SCHEMA test_schema TO ROLE test_role']

    def test_grant_select_on_schema(self):
        """Validate if GRANT command generated correctly"""
        self.snowflake.executed_queries = []
        self.snowflake.grant_select_on_schema(target_schema='test_schema',
                                              role='test_role')
        assert self.snowflake.executed_queries == [
            'GRANT SELECT ON ALL TABLES IN SCHEMA test_schema TO ROLE test_role']

    def test_swap_tables(self):
        """Validate if swap table commands generated correctly"""
        # Swap tables with standard table and column names
        self.snowflake.executed_queries = []
        self.snowflake.swap_tables(schema='test_schema',
                                   table_name='test_table')
        assert self.snowflake.executed_queries == [
            'ALTER TABLE test_schema."TEST_TABLE_TEMP" SWAP WITH test_schema."TEST_TABLE"',
            'DROP TABLE IF EXISTS test_schema."TEST_TABLE_TEMP"']

        # Swap tables with reserved word in table and column names in temp table
        self.snowflake.executed_queries = []
        self.snowflake.swap_tables(schema='test_schema',
                                   table_name='full')
        assert self.snowflake.executed_queries == [
            'ALTER TABLE test_schema."FULL_TEMP" SWAP WITH test_schema."FULL"',
            'DROP TABLE IF EXISTS test_schema."FULL_TEMP"']

        # Swap tables with with space and uppercase in table name and s3 key
        self.snowflake.executed_queries = []
        self.snowflake.swap_tables(schema='test_schema',
                                   table_name='table with SPACE and UPPERCASE')
        assert self.snowflake.executed_queries == [
            'ALTER TABLE test_schema."TABLE WITH SPACE AND UPPERCASE_TEMP" '
            'SWAP WITH test_schema."TABLE WITH SPACE AND UPPERCASE"',
            'DROP TABLE IF EXISTS test_schema."TABLE WITH SPACE AND UPPERCASE_TEMP"']

    def test_create_query_tag(self):
        """Validate if query tag genrated correctly"""
        # not passing query_tag_props
        assert json.loads(self.snowflake.create_query_tag()) == {
            'ppw_component': 'fastsync',
            'tap_id': None,
            'schema': None,
            'table': None
        }

        # passing invalid query_tag_props (string)
        assert json.loads(self.snowflake.create_query_tag('invalid_query_props')) == {
            'ppw_component': 'fastsync',
            'tap_id': None,
            'schema': None,
            'table': None
        }

        # passing invalid query_tag_props (number)
        self.snowflake.connection_config['tap_id'] = 'fake_tap'
        assert json.loads(self.snowflake.create_query_tag(1234567890)) == {
            'ppw_component': 'fastsync',
            'tap_id': 'fake_tap',
            'schema': None,
            'table': None
        }

        # passing invalid query_tag_props (array)
        self.snowflake.connection_config['tap_id'] = 'fake_tap'
        assert json.loads(self.snowflake.create_query_tag([1, 2, 3])) == {
            'ppw_component': 'fastsync',
            'tap_id': 'fake_tap',
            'schema': None,
            'table': None
        }

        # passing invalid query_tag_props
        self.snowflake.connection_config['tap_id'] = 'fake_tap'
        assert json.loads(self.snowflake.create_query_tag()) == {
            'ppw_component': 'fastsync',
            'tap_id': 'fake_tap',
            'schema': None,
            'table': None
        }

        # passing valid query_props
        self.snowflake.connection_config['tap_id'] = 'fake_tap'
        assert json.loads(self.snowflake.create_query_tag({'schema': 'fake_schema',
                                                           'table': 'fake_table'})) == {
            'ppw_component': 'fastsync',
            'tap_id': 'fake_tap',
            'schema': 'fake_schema',
            'table': 'fake_table'
        }

        # passing partial query_props
        self.snowflake.connection_config['tap_id'] = 'fake_tap'
        assert json.loads(self.snowflake.create_query_tag({'schema': 'fake_schema'})) == {
            'ppw_component': 'fastsync',
            'tap_id': 'fake_tap',
            'schema': 'fake_schema',
            'table': None
        }

        # passing partial query_props
        self.snowflake.connection_config['tap_id'] = 'fake_tap'
        assert json.loads(self.snowflake.create_query_tag({'table': 'fake_table'})) == {
            'ppw_component': 'fastsync',
            'tap_id': 'fake_tap',
            'schema': None,
            'table': 'fake_table'
        }

        # passing not supported query_props
        self.snowflake.connection_config['tap_id'] = 'fake_tap'
        assert json.loads(self.snowflake.create_query_tag({'fake_prop': 'fake_value'})) == {
            'ppw_component': 'fastsync',
            'tap_id': 'fake_tap',
            'schema': None,
            'table': None
        }
