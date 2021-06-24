import pytest
from unittest.mock import Mock, patch, ANY, mock_open
from google.cloud import bigquery
from pipelinewise.fastsync.commons.target_bigquery import FastSyncTargetBigquery

@pytest.fixture
def query_result():
    """
    Mocked Bigquery Run Query Results
    """
    qr = Mock()
    qr.return_value = []
    return qr

@pytest.fixture
def bigquery_job(query_result):
    """
    Mocked Bigquery Job Query
    """
    qj = Mock()
    qj.output_rows = 0
    qj.job_id = 1
    qj.result().total_rows = 0
    return qj

@pytest.fixture
def bigquery_job_config():
    """
    Mocked Bigquery Job Config
    """
    qc = Mock()
    return qc

class FastSyncTargetBigqueryMock(FastSyncTargetBigquery):
    """
    Mocked FastSyncTargetBigquery class
    """
    def __init__(self, connection_config, transformation_config=None):
        super().__init__(connection_config, transformation_config)


# pylint: disable=attribute-defined-outside-init
class TestFastSyncTargetBigquery:
    """
    Unit tests for fastsync target bigquery
    """
    def setup_method(self):
        """Initialise test FastSyncTargetPostgres object"""
        self.bigquery = FastSyncTargetBigqueryMock(connection_config={'project_id': 'dummy-project'},
                                                   transformation_config={})

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_create_schema(self, Client, bigquery_job):
        """Validate if create schema queries generated correctly"""
        Client().query.return_value = bigquery_job
        self.bigquery.create_schema('new_schema')
        Client().create_dataset.assert_called_with('new_schema', exists_ok=True)

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_drop_table(self, Client, bigquery_job):
        """Validate if drop table queries generated correctly"""
        Client().query.return_value = bigquery_job

        self.bigquery.drop_table('test_schema', 'test_table')
        Client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`test_table`', job_config=ANY)

        self.bigquery.drop_table('test_schema', 'test_table', is_temporary=True)
        Client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`test_table_temp`', job_config=ANY)

        self.bigquery.drop_table('test_schema', 'UPPERCASE_TABLE')
        Client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`uppercase_table`', job_config=ANY)

        self.bigquery.drop_table('test_schema', 'UPPERCASE_TABLE', is_temporary=True)
        Client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`uppercase_table_temp`', job_config=ANY)

        self.bigquery.drop_table('test_schema', 'test_table_with_space')
        Client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`test_table_with_space`', job_config=ANY)

        self.bigquery.drop_table('test_schema', 'test table with space', is_temporary=True)
        Client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`test_table_with_space_temp`', job_config=ANY)

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_create_table(self, Client, bigquery_job):
        """Validate if create table queries generated correctly"""
        Client().query.return_value = bigquery_job

        # Create table with standard table and column names
        self.bigquery.create_table(target_schema='test_schema',
                                    table_name='test_table',
                                    columns=['`id` INTEGER',
                                             '`txt` STRING'])
        Client().query.assert_called_with(
            'CREATE OR REPLACE TABLE test_schema.`test_table` ('
            '`id` integer,`txt` string,'
            '_sdc_extracted_at timestamp,'
            '_sdc_batched_at timestamp,'
            '_sdc_deleted_at timestamp)',
            job_config=ANY)

        # Create table with reserved words in table and column names
        self.bigquery.create_table(target_schema='test_schema',
                                    table_name='order',
                                    columns=['`id` INTEGER',
                                             '`txt` STRING',
                                             '`select` STRING'])
        Client().query.assert_called_with(
            'CREATE OR REPLACE TABLE test_schema.`order` ('
            '`id` integer,`txt` string,`select` string,'
            '_sdc_extracted_at timestamp,'
            '_sdc_batched_at timestamp,'
            '_sdc_deleted_at timestamp)',
            job_config=ANY)

        # Create table with mixed lower and uppercase and space characters
        self.bigquery.create_table(target_schema='test_schema',
                                    table_name='TABLE with SPACE',
                                    columns=['`ID` INTEGER',
                                             '`COLUMN WITH SPACE` STRING'])
        Client().query.assert_called_with(
            'CREATE OR REPLACE TABLE test_schema.`table_with_space` ('
            '`id` integer,`column with space` string,'
            '_sdc_extracted_at timestamp,'
            '_sdc_batched_at timestamp,'
            '_sdc_deleted_at timestamp)',
            job_config=ANY)

        # Create table with no primary key
        self.bigquery.create_table(target_schema='test_schema',
                                    table_name='test_table_no_pk',
                                    columns=['`ID` INTEGER',
                                             '`TXT` STRING'])
        Client().query.assert_called_with(
            'CREATE OR REPLACE TABLE test_schema.`test_table_no_pk` ('
            '`id` integer,`txt` string,'
            '_sdc_extracted_at timestamp,'
            '_sdc_batched_at timestamp,'
            '_sdc_deleted_at timestamp)',
            job_config=ANY)

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.LoadJobConfig')
    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_copy_to_table(self, Client, LoadJobConfig, bigquery_job_config, bigquery_job):
        """Validate if COPY command generated correctly"""
        # COPY table with standard table and column names
        Client().load_table_from_file.return_value = bigquery_job
        LoadJobConfig.return_value = bigquery_job_config
        m = mock_open()
        with patch('pipelinewise.fastsync.commons.target_bigquery.open', m):
            self.bigquery.copy_to_table(filepath='/path/to/dummy-file.csv.gz',
                                         target_schema='test_schema',
                                         table_name='test_table',
                                         size_bytes=1000,
                                         is_temporary=False,
                                         skip_csv_header=False)
        m.assert_called_with('/path/to/dummy-file.csv.gz', 'rb')
        assert bigquery_job_config.source_format == bigquery.SourceFormat.CSV
        assert bigquery_job_config.write_disposition == 'WRITE_TRUNCATE'
        assert bigquery_job_config.allow_quoted_newlines == True
        assert bigquery_job_config.skip_leading_rows == 0
        Client().dataset.assert_called_with('test_schema')
        Client().dataset().table.assert_called_with('test_table')
        assert Client().load_table_from_file.call_count == 1

        # COPY table with reserved word in table and column names in temp table
        with patch('pipelinewise.fastsync.commons.target_bigquery.open', m):
            self.bigquery.copy_to_table(filepath='/path/to/full-file.csv.gz',
                                         target_schema='test_schema',
                                         table_name='full',
                                         size_bytes=1000,
                                         is_temporary=True,
                                         skip_csv_header=False)
        m.assert_called_with('/path/to/full-file.csv.gz', 'rb')
        assert bigquery_job_config.source_format == bigquery.SourceFormat.CSV
        assert bigquery_job_config.write_disposition == 'WRITE_TRUNCATE'
        assert bigquery_job_config.allow_quoted_newlines == True
        assert bigquery_job_config.skip_leading_rows == 0
        Client().dataset.assert_called_with('test_schema')
        Client().dataset().table.assert_called_with('full_temp')
        assert Client().load_table_from_file.call_count == 2

        # COPY table with space and uppercase in table name and s3 key
        with patch('pipelinewise.fastsync.commons.target_bigquery.open', m):
            self.bigquery.copy_to_table(filepath='/path/to/file with space.csv.gz',
                                         target_schema='test_schema',
                                         table_name='table with SPACE and UPPERCASE',
                                         size_bytes=1000,
                                         is_temporary=True,
                                         skip_csv_header=False)
        m.assert_called_with('/path/to/file with space.csv.gz', 'rb')
        assert bigquery_job_config.source_format == bigquery.SourceFormat.CSV
        assert bigquery_job_config.write_disposition == 'WRITE_TRUNCATE'
        assert bigquery_job_config.allow_quoted_newlines == True
        assert bigquery_job_config.skip_leading_rows == 0
        Client().dataset.assert_called_with('test_schema')
        Client().dataset().table.assert_called_with('table_with_space_and_uppercase_temp')
        assert Client().load_table_from_file.call_count == 3

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_grant_select_on_table(self, Client, bigquery_job):
        """Validate if GRANT command generated correctly"""
        # GRANT table with standard table and column names
        Client().query.return_value = bigquery_job
        self.bigquery.grant_select_on_table(target_schema='test_schema',
                                             table_name='test_table',
                                             role='test_role',
                                             is_temporary=False)
        Client().query.assert_called_with(
            'GRANT SELECT ON test_schema.`test_table` TO ROLE test_role', job_config=ANY)

        # GRANT table with reserved word in table and column names in temp table
        self.bigquery.grant_select_on_table(target_schema='test_schema',
                                             table_name='full',
                                             role='test_role',
                                             is_temporary=False)
        Client().query.assert_called_with(
            'GRANT SELECT ON test_schema.`full` TO ROLE test_role', job_config=ANY)

        # GRANT table with with space and uppercase in table name and s3 key
        self.bigquery.grant_select_on_table(target_schema='test_schema',
                                             table_name='table with SPACE and UPPERCASE',
                                             role='test_role',
                                             is_temporary=False)
        Client().query.assert_called_with(
            'GRANT SELECT ON test_schema.`table_with_space_and_uppercase` TO ROLE test_role', job_config=ANY)

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_grant_usage_on_schema(self, Client, bigquery_job):
        """Validate if GRANT command generated correctly"""
        self.bigquery.grant_usage_on_schema(target_schema='test_schema',
                                             role='test_role')
        Client().query.assert_called_with(
            'GRANT USAGE ON SCHEMA test_schema TO ROLE test_role', job_config=ANY)

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_grant_select_on_schema(self, Client, bigquery_job):
        """Validate if GRANT command generated correctly"""
        self.bigquery.grant_select_on_schema(target_schema='test_schema',
                                              role='test_role')
        Client().query.assert_called_with(
            'GRANT SELECT ON ALL TABLES IN SCHEMA test_schema TO ROLE test_role', job_config=ANY)

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.CopyJobConfig')
    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_swap_tables(self, Client, CopyJobConfig, bigquery_job_config, bigquery_job):
        """Validate if swap table commands generated correctly"""
        # Swap tables with standard table and column names
        Client().copy_table.return_value = bigquery_job
        CopyJobConfig.return_value = bigquery_job_config
        self.bigquery.swap_tables(schema='test_schema',
                                   table_name='test_table')
        assert bigquery_job_config.write_disposition == 'WRITE_TRUNCATE'
        Client().copy_table.assert_called_with(
            'dummy-project.test_schema.test_table_temp',
            'dummy-project.test_schema.test_table',
            job_config=ANY)
        Client().delete_table.assert_called_with('dummy-project.test_schema.test_table_temp')

        # Swap tables with reserved word in table and column names in temp table
        self.bigquery.swap_tables(schema='test_schema',
                                   table_name='full')
        assert bigquery_job_config.write_disposition == 'WRITE_TRUNCATE'
        Client().copy_table.assert_called_with(
            'dummy-project.test_schema.full_temp',
            'dummy-project.test_schema.full',
            job_config=ANY)
        Client().delete_table.assert_called_with('dummy-project.test_schema.full_temp')

        # Swap tables with with space and uppercase in table name and s3 key
        self.bigquery.swap_tables(schema='test_schema',
                                   table_name='table with SPACE and UPPERCASE')
        assert bigquery_job_config.write_disposition == 'WRITE_TRUNCATE'
        Client().copy_table.assert_called_with(
            'dummy-project.test_schema.table_with_space_and_uppercase_temp',
            'dummy-project.test_schema.table_with_space_and_uppercase',
            job_config=ANY)
        Client().delete_table.assert_called_with('dummy-project.test_schema.table_with_space_and_uppercase_temp')
