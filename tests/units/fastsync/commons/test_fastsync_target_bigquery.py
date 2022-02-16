import pytest
from unittest.mock import Mock, call, patch, ANY
from google.cloud import bigquery
from pipelinewise.fastsync.commons.target_bigquery import FastSyncTargetBigquery


@pytest.fixture(name='query_result')
def fixture_query_result():
    """
    Mocked Bigquery Run Query Results
    """
    mocked_qr = Mock()
    mocked_qr.return_value = []
    return mocked_qr


# pylint: disable=W0613
@pytest.fixture(name='bigquery_job')
def fixture_bigquery_job(query_result):
    """
    Mocked Bigquery Job Query
    """
    mocked_qj = Mock()
    mocked_qj.output_rows = 0
    mocked_qj.job_id = 1
    mocked_qj.result().total_rows = 0
    return mocked_qj


@pytest.fixture(name='bigquery_job_config')
def fixture_bigquery_job_config():
    """
    Mocked Bigquery Job Config
    """
    mocked_qc = Mock()
    return mocked_qc


class FastSyncTargetBigqueryMock(FastSyncTargetBigquery):
    """
    Mocked FastSyncTargetBigquery class
    """

    def __init__(self, connection_config, transformation_config=None):
        super().__init__(connection_config, transformation_config)
        self.executed_queries = []


# pylint: disable=attribute-defined-outside-init
class TestFastSyncTargetBigquery:
    """
    Unit tests for fastsync target bigquery
    """

    def setup_method(self):
        """Initialise test FastSyncTargetPostgres object"""
        self.bigquery = FastSyncTargetBigqueryMock(
            connection_config={'project_id': 'dummy-project', 'location': 'EU'},
            transformation_config={},
        )

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_open_connection(self, client):
        """Validate if create schema queries generated correctly"""
        self.bigquery.open_connection()
        client.assert_called_with(project='dummy-project', location='EU')

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_create_schema(self, client):
        """Validate if create schema queries generated correctly"""
        self.bigquery.create_schema('new_schema')
        client().create_dataset.assert_called_with('new_schema', exists_ok=True)

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_drop_table(self, client):
        """Validate if drop table queries generated correctly"""
        self.bigquery.drop_table('test_schema', 'test_table')
        client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`test_table`', job_config=ANY
        )

        self.bigquery.drop_table('test_schema', 'test_table', is_temporary=True)
        client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`test_table_temp`', job_config=ANY
        )

        self.bigquery.drop_table('test_schema', 'UPPERCASE_TABLE')
        client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`uppercase_table`', job_config=ANY
        )

        self.bigquery.drop_table('test_schema', 'UPPERCASE_TABLE', is_temporary=True)
        client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`uppercase_table_temp`', job_config=ANY
        )

        self.bigquery.drop_table('test_schema', 'test_table_with_space')
        client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`test_table_with_space`', job_config=ANY
        )

        self.bigquery.drop_table(
            'test_schema', 'test table with space', is_temporary=True
        )
        client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`test_table_with_space_temp`',
            job_config=ANY,
        )

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_create_table(self, client):
        """Validate if create table queries generated correctly"""
        # Create table with standard table and column names
        self.bigquery.create_table(
            target_schema='test_schema',
            table_name='test_table',
            columns=['`id` INTEGER', '`txt` STRING'],
        )
        client().query.assert_called_with(
            'CREATE OR REPLACE TABLE test_schema.`test_table` ('
            '`id` integer,`txt` string,'
            '_sdc_extracted_at timestamp,'
            '_sdc_batched_at timestamp,'
            '_sdc_deleted_at timestamp)',
            job_config=ANY,
        )

        # Create table with reserved words in table and column names
        self.bigquery.create_table(
            target_schema='test_schema',
            table_name='order',
            columns=['`id` INTEGER', '`txt` STRING', '`select` STRING'],
        )
        client().query.assert_called_with(
            'CREATE OR REPLACE TABLE test_schema.`order` ('
            '`id` integer,`txt` string,`select` string,'
            '_sdc_extracted_at timestamp,'
            '_sdc_batched_at timestamp,'
            '_sdc_deleted_at timestamp)',
            job_config=ANY,
        )

        # Create table with mixed lower and uppercase and space characters
        self.bigquery.create_table(
            target_schema='test_schema',
            table_name='TABLE with SPACE',
            columns=['`ID` INTEGER', '`COLUMN WITH SPACE` STRING'],
        )
        client().query.assert_called_with(
            'CREATE OR REPLACE TABLE test_schema.`table_with_space` ('
            '`id` integer,`column with space` string,'
            '_sdc_extracted_at timestamp,'
            '_sdc_batched_at timestamp,'
            '_sdc_deleted_at timestamp)',
            job_config=ANY,
        )

        # Create table with no primary key
        self.bigquery.create_table(
            target_schema='test_schema',
            table_name='test_table_no_pk',
            columns=['`ID` INTEGER', '`TXT` STRING'],
        )
        client().query.assert_called_with(
            'CREATE OR REPLACE TABLE test_schema.`test_table_no_pk` ('
            '`id` integer,`txt` string,'
            '_sdc_extracted_at timestamp,'
            '_sdc_batched_at timestamp,'
            '_sdc_deleted_at timestamp)',
            job_config=ANY,
        )

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.LoadJobConfig')
    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_copy_to_table(
        self, client, load_job_config, bigquery_job_config, bigquery_job
    ):
        """Validate if COPY command generated correctly"""
        # COPY table with standard table and column names
        client().load_table_from_uri.return_value = bigquery_job
        load_job_config.return_value = bigquery_job_config

        mock_bucket = Mock()
        mock_bucket.name = 'some-bucket'
        mock_blob = Mock()
        mock_blob.bucket = mock_bucket
        mock_blob.name = '/path/to/dummy-file.csv.gz'

        self.bigquery.copy_to_table(
            blobs=[mock_blob],
            target_schema='test_schema',
            table_name='test_table',
            size_bytes=1000,
            is_temporary=False,
            skip_csv_header=False,
        )
        assert bigquery_job_config.source_format == bigquery.SourceFormat.CSV
        assert bigquery_job_config.write_disposition == 'WRITE_TRUNCATE'
        assert bigquery_job_config.allow_quoted_newlines is True
        assert bigquery_job_config.skip_leading_rows == 0
        client().dataset.assert_called_with('test_schema')
        client().dataset().table.assert_called_with('test_table')
        client().load_table_from_uri.assert_called_once()
        client().load_table_from_uri.reset_mock()

        # COPY table with reserved word in table and column names in temp table
        self.bigquery.copy_to_table(
            blobs=[mock_blob],
            target_schema='test_schema',
            table_name='full',
            size_bytes=1000,
            is_temporary=True,
            skip_csv_header=True,
            write_truncate=False,
            allow_quoted_newlines=False,
        )
        assert bigquery_job_config.source_format == bigquery.SourceFormat.CSV
        assert bigquery_job_config.write_disposition == 'WRITE_APPEND'
        assert bigquery_job_config.allow_quoted_newlines is False
        assert bigquery_job_config.skip_leading_rows == 1
        client().dataset.assert_called_with('test_schema')
        client().dataset().table.assert_called_with('full_temp')
        client().load_table_from_uri.assert_called_once()
        client().load_table_from_uri.reset_mock()

        # COPY table with space and uppercase in table name
        self.bigquery.copy_to_table(
            blobs=[mock_blob],
            target_schema='test_schema',
            table_name='table with SPACE and UPPERCASE',
            size_bytes=1000,
            is_temporary=True,
            skip_csv_header=False,
        )
        assert bigquery_job_config.source_format == bigquery.SourceFormat.CSV
        assert bigquery_job_config.write_disposition == 'WRITE_TRUNCATE'
        assert bigquery_job_config.allow_quoted_newlines is True
        assert bigquery_job_config.skip_leading_rows == 0
        client().dataset.assert_called_with('test_schema')
        client().dataset().table.assert_called_with(
            'table_with_space_and_uppercase_temp'
        )
        client().load_table_from_uri.assert_called_once()
        client().load_table_from_uri.reset_mock()

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_grant_select_on_table(self, client, bigquery_job):
        """Validate if GRANT command generated correctly"""
        # GRANT table with standard table and column names
        client().query.return_value = bigquery_job
        self.bigquery.grant_select_on_table(
            target_schema='test_schema',
            table_name='test_table',
            role='test_role',
            is_temporary=False,
        )
        client().query.assert_called_with(
            'GRANT SELECT ON test_schema.`test_table` TO ROLE test_role', job_config=ANY
        )

        # GRANT table with reserved word in table and column names in temp table
        self.bigquery.grant_select_on_table(
            target_schema='test_schema',
            table_name='full',
            role='test_role',
            is_temporary=False,
        )
        client().query.assert_called_with(
            'GRANT SELECT ON test_schema.`full` TO ROLE test_role', job_config=ANY
        )

        # GRANT table with with space and uppercase in table name and s3 key
        self.bigquery.grant_select_on_table(
            target_schema='test_schema',
            table_name='table with SPACE and UPPERCASE',
            role='test_role',
            is_temporary=False,
        )
        client().query.assert_called_with(
            'GRANT SELECT ON test_schema.`table_with_space_and_uppercase` TO ROLE test_role',
            job_config=ANY,
        )

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_grant_usage_on_schema(self, client):
        """Validate if GRANT command generated correctly"""
        self.bigquery.grant_usage_on_schema(
            target_schema='test_schema', role='test_role'
        )
        client().query.assert_called_with(
            'GRANT USAGE ON SCHEMA test_schema TO ROLE test_role', job_config=ANY
        )

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_grant_select_on_schema(self, client):
        """Validate if GRANT command generated correctly"""
        self.bigquery.grant_select_on_schema(
            target_schema='test_schema', role='test_role'
        )
        client().query.assert_called_with(
            'GRANT SELECT ON ALL TABLES IN SCHEMA test_schema TO ROLE test_role',
            job_config=ANY,
        )

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.CopyJobConfig')
    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_swap_tables(
        self, client, copy_job_config, bigquery_job_config, bigquery_job
    ):
        """Validate if swap table commands generated correctly"""
        # Swap tables with standard table and column names
        client().copy_table.return_value = bigquery_job
        copy_job_config.return_value = bigquery_job_config
        self.bigquery.swap_tables(schema='test_schema', table_name='test_table')
        assert bigquery_job_config.write_disposition == 'WRITE_TRUNCATE'
        client().copy_table.assert_called_with(
            'dummy-project.test_schema.test_table_temp',
            'dummy-project.test_schema.test_table',
            job_config=ANY,
        )
        client().delete_table.assert_called_with(
            'dummy-project.test_schema.test_table_temp'
        )

        # Swap tables with reserved word in table and column names in temp table
        self.bigquery.swap_tables(schema='test_schema', table_name='full')
        assert bigquery_job_config.write_disposition == 'WRITE_TRUNCATE'
        client().copy_table.assert_called_with(
            'dummy-project.test_schema.full_temp',
            'dummy-project.test_schema.full',
            job_config=ANY,
        )
        client().delete_table.assert_called_with('dummy-project.test_schema.full_temp')

        # Swap tables with with space and uppercase in table name and s3 key
        self.bigquery.swap_tables(
            schema='test_schema', table_name='table with SPACE and UPPERCASE'
        )
        assert bigquery_job_config.write_disposition == 'WRITE_TRUNCATE'
        client().copy_table.assert_called_with(
            'dummy-project.test_schema.table_with_space_and_uppercase_temp',
            'dummy-project.test_schema.table_with_space_and_uppercase',
            job_config=ANY,
        )
        client().delete_table.assert_called_with(
            'dummy-project.test_schema.table_with_space_and_uppercase_temp'
        )

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_obfuscate_columns_case1(self, client):
        """
        Test obfuscation where given transformations are emtpy
        Test should pass with no executed queries
        """
        target_schema = 'my_schema'
        table_name = 'public.my_table'

        self.bigquery.transformation_config = {}

        self.bigquery.obfuscate_columns(target_schema, table_name)
        client().query.assert_not_called()

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_obfuscate_columns_case2(self, client):
        """
        Test obfuscation where given transformations has an unsupported transformation type
        Test should fail
        """
        target_schema = 'my_schema'
        table_name = 'public.my_table'

        self.bigquery.transformation_config = {
            'transformations': [
                {
                    'field_id': 'col_7',
                    'tap_stream_name': 'public-my_table',
                    'type': 'RANDOM',
                }
            ]
        }

        with pytest.raises(ValueError):
            self.bigquery.obfuscate_columns(target_schema, table_name)

        client().query.assert_not_called()

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_obfuscate_columns_case3(self, client):
        """
        Test obfuscation where given transformations have no conditions
        Test should pass
        """
        target_schema = 'my_schema'
        table_name = 'public.my_table'

        self.bigquery.transformation_config = {
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

        self.bigquery.obfuscate_columns(target_schema, table_name)

        client().query.assert_called_with(
            "UPDATE `my_schema`.`my_table_temp` SET `col_1` = NULL, `col_2` = 'hidden', "
            '`col_3` = TIMESTAMP(DATETIME(DATE(EXTRACT(YEAR FROM `col_3`), 1, '
            '1),TIME(`col_3`))), `col_4` = 0, `col_5` = TO_BASE64(SHA256(`col_5`)), '
            '`col_6` = CONCAT(SUBSTRING(`col_6`, 1, 5), '
            'TO_BASE64(SHA256(SUBSTRING(`col_6`, 5 + 1)))), `col_7` = CASE WHEN '
            "LENGTH(`col_7`) > 2 * 3 THEN CONCAT(SUBSTRING(`col_7`, 1, 3), REPEAT('*', "
            'LENGTH(`col_7`)-(2 * 3)), SUBSTRING(`col_7`, LENGTH(`col_7`)-3+1, 3)) ELSE '
            "REPEAT('*', LENGTH(`col_7`)) END WHERE true;",
            job_config=ANY,
        )

    @patch('pipelinewise.fastsync.commons.target_bigquery.bigquery.Client')
    def test_obfuscate_columns_case4(self, client):
        """
        Test obfuscation where given transformations have conditions
        Test should pass
        """
        target_schema = 'my_schema'
        table_name = 'public.my_table'

        self.bigquery.transformation_config = {
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

        self.bigquery.obfuscate_columns(target_schema, table_name)

        expected_sql = [
            "UPDATE `my_schema`.`my_table_temp` SET `col_2` = 'hidden' WHERE (`col_4` IS NULL);",
            "UPDATE `my_schema`.`my_table_temp` SET `col_3` = TIMESTAMP(DATETIME(DATE(EXTRACT(YEAR FROM `col_3`), 1, 1),TIME(`col_3`))) WHERE (`col_5` = 'some_value');",
            "UPDATE `my_schema`.`my_table_temp` SET `col_6` = CONCAT(SUBSTRING(`col_6`, 1, 5), TO_BASE64(SHA256(SUBSTRING(`col_6`, 5 + 1)))) WHERE (`col_1` = 30) AND REGEXP_CONTAINS(`col_2`, '[0-9]{3}\\.[0-9]{3}');",
            "UPDATE `my_schema`.`my_table_temp` SET `col_7` = CASE WHEN LENGTH(`col_7`) > 2 * 3 THEN CONCAT(SUBSTRING(`col_7`, 1, 3), REPEAT('*', LENGTH(`col_7`)-(2 * 3)), SUBSTRING(`col_7`, LENGTH(`col_7`)-3+1, 3)) ELSE REPEAT('*', LENGTH(`col_7`)) END WHERE (`col_1` = 30) AND REGEXP_CONTAINS(`col_2`, '[0-9]{3}\\.[0-9]{3}') AND (`col_4` IS NULL);",
            "UPDATE `my_schema`.`my_table_temp` SET `col_1` = NULL, `col_4` = 0, `col_5` = TO_BASE64(SHA256(`col_5`)) WHERE true;",
        ]

        expected_calls = []
        for sql in expected_sql:
            expected_calls.append(call(sql, job_config=ANY))
            expected_calls.append(call().result())

        client().query.assert_has_calls(expected_calls)

    # pylint: disable=invalid-name
    def test_default_archive_destination(self):
        """
        Validate parameters passed to gcs copy_blob method when custom GCS bucket and folder are not defined
        """
        mock_source_bucket = Mock()
        mock_source_bucket.name = 'some_bucket'

        mock_buckets = {mock_source_bucket.name: mock_source_bucket}

        mock_blob = Mock()
        mock_blob.bucket = mock_source_bucket
        mock_blob.name = 'some_bucket/snowflake-import/ppw_20210615115603_fastsync.csv.gz'

        mock_gcs_client = Mock()
        mock_gcs_client.get_bucket.side_effect = lambda x: mock_buckets.get(x)

        self.bigquery.gcs = mock_gcs_client

        self.bigquery.connection_config['gcs_bucket'] = mock_source_bucket.name

        archive_blob = self.bigquery.copy_to_archive(
            mock_blob,
            'some-tap',
            'some_schema.some_table',
        )

        mock_source_bucket.copy_blob.assert_called_with(
            mock_blob,
            mock_source_bucket,
            new_name='archive/some-tap/some_table/ppw_20210615115603_fastsync.csv.gz',
        )
        archive_blob.metadata.update.assert_called_with(
            {
                'tap': 'some-tap',
                'schema': 'some_schema',
                'table': 'some_table',
                'archived-by': 'pipelinewise_fastsync',
            }
        )

    # pylint: disable=invalid-name
    def test_custom_archive_destination(self):
        """
        Validate parameters passed to s3 copy_object method when using custom s3 bucket and folder
        """
        mock_source_bucket = Mock()
        mock_source_bucket.name = 'some_bucket'
        mock_archive_bucket = Mock()
        mock_archive_bucket.name = 'archive_bucket'

        mock_buckets = {
            mock_source_bucket.name: mock_source_bucket,
            mock_archive_bucket.name: mock_archive_bucket,
        }

        mock_blob = Mock()
        mock_blob.bucket = mock_source_bucket
        mock_blob.name = 'some_bucket/snowflake-import/ppw_20210615115603_fastsync.csv.gz'

        mock_gcs_client = Mock()
        mock_gcs_client.get_bucket.side_effect = lambda x: mock_buckets.get(x)

        self.bigquery.gcs = mock_gcs_client

        self.bigquery.connection_config['gcs_bucket'] = 'some_bucket'
        self.bigquery.connection_config['archive_load_files_gcs_bucket'] = 'archive_bucket'
        self.bigquery.connection_config['archive_load_files_gcs_prefix'] = 'archive_folder'

        archive_blob = self.bigquery.copy_to_archive(
            mock_blob,
            'some-tap',
            'some_schema.some_table',
        )

        mock_source_bucket.copy_blob.assert_called_with(
            mock_blob,
            mock_archive_bucket,
            new_name='archive_folder/some-tap/some_table/ppw_20210615115603_fastsync.csv.gz',
        )
        archive_blob.metadata.update.assert_called_with(
            {
                'tap': 'some-tap',
                'schema': 'some_schema',
                'table': 'some_table',
                'archived-by': 'pipelinewise_fastsync',
            }
        )

    # pylint: disable=invalid-name
    def test_copied_archive_metadata(self):
        """
        Validate parameters passed to s3 copy_object method when custom s3 bucket and folder are not defined
        """
        mock_source_bucket = Mock()
        mock_source_bucket.name = 'some_bucket'

        mock_buckets = {mock_source_bucket.name: mock_source_bucket}

        mock_blob = Mock()
        mock_blob.bucket = mock_source_bucket
        mock_blob.name = 'some_bucket/snowflake-import/ppw_20210615115603_fastsync.csv.gz'
        mock_blob.metadata = {'copied-old-key': 'copied-old-value'}

        mock_gcs_client = Mock()
        mock_gcs_client.get_bucket.side_effect = lambda x: mock_buckets.get(x)

        self.bigquery.gcs = mock_gcs_client

        self.bigquery.connection_config['s3_bucket'] = 'some_bucket'

        archive_blob = self.bigquery.copy_to_archive(
            mock_blob,
            'some-tap',
            'some_schema.some_table',
        )

        mock_source_bucket.copy_blob.assert_called_with(
            mock_blob,
            mock_source_bucket,
            new_name='archive/some-tap/some_table/ppw_20210615115603_fastsync.csv.gz',
        )
        assert archive_blob.metadata == {
            'copied-old-key': 'copied-old-value',
            'tap': 'some-tap',
            'schema': 'some_schema',
            'table': 'some_table',
            'archived-by': 'pipelinewise_fastsync',
        }
