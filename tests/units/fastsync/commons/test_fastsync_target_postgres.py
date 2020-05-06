from pipelinewise.fastsync.commons.target_postgres import FastSyncTargetPostgres


class FastSyncTargetPostgresMock(FastSyncTargetPostgres):
    """
    Mocked FastSyncTargetPostgres class
    """
    def __init__(self, connection_config, transformation_config=None):
        super().__init__(connection_config, transformation_config)

        self.executed_queries = []

    def query(self, query, params=None):
        self.executed_queries.append(query)


# pylint: disable=attribute-defined-outside-init
class TestFastSyncTargetPostgres:
    """
    Unit tests for fastsync target postgres
    """
    def setup_method(self):
        """Initialise test FastSyncTargetPostgres object"""
        self.postgres = FastSyncTargetPostgresMock(connection_config={}, transformation_config={})

    def test_create_schema(self):
        """Validate if create schema queries generated correctly"""
        self.postgres.create_schema('new_schema')
        assert self.postgres.executed_queries == ['CREATE SCHEMA IF NOT EXISTS new_schema']

    def test_drop_table(self):
        """Validate if drop table queries generated correctly"""
        self.postgres.drop_table('test_schema', 'test_table')
        self.postgres.drop_table('test_schema', 'test_table', is_temporary=True)
        self.postgres.drop_table('test_schema', 'UPPERCASE_TABLE')
        self.postgres.drop_table('test_schema', 'UPPERCASE_TABLE', is_temporary=True)
        self.postgres.drop_table('test_schema', 'test table with space')
        self.postgres.drop_table('test_schema', 'test table with space', is_temporary=True)
        assert self.postgres.executed_queries == [
            'DROP TABLE IF EXISTS test_schema."test_table"',
            'DROP TABLE IF EXISTS test_schema."test_table_temp"',
            'DROP TABLE IF EXISTS test_schema."uppercase_table"',
            'DROP TABLE IF EXISTS test_schema."uppercase_table_temp"',
            'DROP TABLE IF EXISTS test_schema."test table with space"',
            'DROP TABLE IF EXISTS test_schema."test table with space_temp"']

    def test_create_table(self):
        """Validate if create table queries generated correctly"""
        # Create table with standard table and column names
        self.postgres.executed_queries = []
        self.postgres.create_table(target_schema='test_schema',
                                   table_name='test_table',
                                   columns=['"id" INTEGER',
                                            '"txt" CHARACTER VARYING'],
                                   primary_key=['"id"'])
        assert self.postgres.executed_queries == [
            'CREATE TABLE IF NOT EXISTS test_schema."test_table" ('
            '"id" integer,"txt" character varying,'
            '_sdc_extracted_at timestamp without time zone,'
            '_sdc_batched_at timestamp without time zone,'
            '_sdc_deleted_at character varying'
            ', PRIMARY KEY ("id"))']

        # Create table with reserved words in table and column names
        self.postgres.executed_queries = []
        self.postgres.create_table(target_schema='test_schema',
                                   table_name='ORDER',
                                   columns=['"id" INTEGER',
                                            '"txt" CHARACTER VARYING',
                                            '"SELECT" CHARACTER VARYING'],
                                   primary_key=['"id"'])
        assert self.postgres.executed_queries == [
            'CREATE TABLE IF NOT EXISTS test_schema."order" ('
            '"id" integer,"txt" character varying,"select" character varying,'
            '_sdc_extracted_at timestamp without time zone,'
            '_sdc_batched_at timestamp without time zone,'
            '_sdc_deleted_at character varying'
            ', PRIMARY KEY ("id"))']

        # Create table with mixed lower and uppercase and space characters
        self.postgres.executed_queries = []
        self.postgres.create_table(target_schema='test_schema',
                                   table_name='TABLE with SPACE',
                                   columns=['"id" INTEGER',
                                            '"column_with space" CHARACTER VARYING'],
                                   primary_key=['"id"'])
        assert self.postgres.executed_queries == [
            'CREATE TABLE IF NOT EXISTS test_schema."table with space" ('
            '"id" integer,"column_with space" character varying,'
            '_sdc_extracted_at timestamp without time zone,'
            '_sdc_batched_at timestamp without time zone,'
            '_sdc_deleted_at character varying'
            ', PRIMARY KEY ("id"))']

        # Create table with composite primary key
        self.postgres.executed_queries = []
        self.postgres.create_table(target_schema='test_schema',
                                   table_name='TABLE with SPACE',
                                   columns=['"id" INTEGER',
                                            '"num" INTEGER',
                                            '"column_with space" CHARACTER VARYING'],
                                   primary_key=['"id"', '"num"'])
        assert self.postgres.executed_queries == [
            'CREATE TABLE IF NOT EXISTS test_schema."table with space" ('
            '"id" integer,"num" integer,"column_with space" character varying,'
            '_sdc_extracted_at timestamp without time zone,'
            '_sdc_batched_at timestamp without time zone,'
            '_sdc_deleted_at character varying'
            ', PRIMARY KEY ("id","num"))']

        # Create table with no primary key
        self.postgres.executed_queries = []
        self.postgres.create_table(target_schema='test_schema',
                                   table_name='test_table_no_pk',
                                   columns=['"id" INTEGER',
                                            '"txt" CHARACTER VARYING'],
                                   primary_key=None)
        assert self.postgres.executed_queries == [
            'CREATE TABLE IF NOT EXISTS test_schema."test_table_no_pk" ('
            '"id" integer,"txt" character varying,'
            '_sdc_extracted_at timestamp without time zone,'
            '_sdc_batched_at timestamp without time zone,'
            '_sdc_deleted_at character varying)']

    def test_grant_select_on_table(self):
        """Validate if GRANT command generated correctly"""
        # GRANT table with standard table and column names
        self.postgres.executed_queries = []
        self.postgres.grant_select_on_table(target_schema='test_schema',
                                            table_name='test_table',
                                            role='test_role',
                                            is_temporary=False)
        assert self.postgres.executed_queries == [
            'GRANT SELECT ON test_schema."test_table" TO GROUP test_role']

        # GRANT table with reserved word in table and column names in temp table
        self.postgres.executed_queries = []
        self.postgres.grant_select_on_table(target_schema='test_schema',
                                            table_name='full',
                                            role='test_role',
                                            is_temporary=False)
        assert self.postgres.executed_queries == [
            'GRANT SELECT ON test_schema."full" TO GROUP test_role']

        # GRANT table with with space and uppercase in table name and s3 key
        self.postgres.executed_queries = []
        self.postgres.grant_select_on_table(target_schema='test_schema',
                                            table_name='table with SPACE and UPPERCASE',
                                            role='test_role',
                                            is_temporary=False)
        assert self.postgres.executed_queries == [
            'GRANT SELECT ON test_schema."table with space and uppercase" TO GROUP test_role']

    def test_grant_usage_on_schema(self):
        """Validate if GRANT command generated correctly"""
        self.postgres.executed_queries = []
        self.postgres.grant_usage_on_schema(target_schema='test_schema',
                                            role='test_role')
        assert self.postgres.executed_queries == [
            'GRANT USAGE ON SCHEMA test_schema TO GROUP test_role']

    def test_grant_select_on_schema(self):
        """Validate if GRANT command generated correctly"""
        self.postgres.executed_queries = []
        self.postgres.grant_select_on_schema(target_schema='test_schema',
                                             role='test_role')
        assert self.postgres.executed_queries == [
            'GRANT SELECT ON ALL TABLES IN SCHEMA test_schema TO GROUP test_role']

    def test_swap_tables(self):
        """Validate if swap table commands generated correctly"""
        # Swap tables with standard table and column names
        self.postgres.executed_queries = []
        self.postgres.swap_tables(schema='test_schema',
                                  table_name='test_table')
        assert self.postgres.executed_queries == [
            'DROP TABLE IF EXISTS test_schema."test_table"',
            'ALTER TABLE test_schema."test_table_temp" RENAME TO "test_table"']

        # Swap tables with reserved word in table and column names in temp table
        self.postgres.executed_queries = []
        self.postgres.swap_tables(schema='test_schema',
                                  table_name='full')
        assert self.postgres.executed_queries == [
            'DROP TABLE IF EXISTS test_schema."full"',
            'ALTER TABLE test_schema."full_temp" RENAME TO "full"']

        # Swap tables with with space and uppercase in table name
        self.postgres.executed_queries = []
        self.postgres.swap_tables(schema='test_schema',
                                  table_name='table with SPACE and UPPERCASE')
        assert self.postgres.executed_queries == [
            'DROP TABLE IF EXISTS test_schema."table with space and uppercase"',
            'ALTER TABLE test_schema."table with space and uppercase_temp" '
            'RENAME TO "table with space and uppercase"']
