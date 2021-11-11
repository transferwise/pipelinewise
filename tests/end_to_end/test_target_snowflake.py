import decimal
import gzip
import os
import tempfile
import uuid
from datetime import datetime
from random import randint

import bson
import pytest
from bson import Timestamp
from pipelinewise.fastsync import mysql_to_snowflake

from .helpers import tasks
from .helpers import assertions
from .helpers.env import E2EEnv

DIR = os.path.dirname(__file__)
TAP_MARIADB_ID = 'mariadb_to_sf'
TAP_MARIADB_SPLIT_LARGE_FILES_ID = 'mariadb_to_sf_split_large_files'
TAP_MARIADB_BUFFERED_STREAM_ID = 'mariadb_to_sf_buffered_stream'
TAP_POSTGRES_ID = 'postgres_to_sf'
TAP_POSTGRES_SPLIT_LARGE_FILES_ID = 'postgres_to_sf_split_large_files'
TAP_POSTGRES_ARCHIVE_LOAD_FILES_ID = 'postgres_to_sf_archive_load_files'
TAP_MONGODB_ID = 'mongo_to_sf'
TAP_S3_CSV_ID = 's3_csv_to_sf'
TARGET_ID = 'snowflake'


# pylint: disable=attribute-defined-outside-init
class TestTargetSnowflake:
    """
    End to end tests for Target Snowflake
    """

    def setup_method(self):
        """Initialise test project by generating YAML files from
        templates for all the configured connectors"""
        self.project_dir = os.path.join(DIR, 'test-project')

        # Init query runner methods
        self.e2e = E2EEnv(self.project_dir)
        self.run_query_tap_mysql = self.e2e.run_query_tap_mysql
        self.run_query_tap_postgres = self.e2e.run_query_tap_postgres
        self.run_query_target_snowflake = self.e2e.run_query_target_snowflake
        self.mongodb_con = self.e2e.get_tap_mongodb_connection()
        self.snowflake_schema_postfix = '_TEST'

    def teardown_method(self):
        """Delete test directories and database objects"""

    '''
    @pytest.mark.dependency(name='import_config')
    def test_import_project(self):
        """Import the YAML project with taps and target and do discovery mode
        to write the JSON files for singer connectors"""

        # Skip every target_postgres related test if required env vars not provided
        if not self.e2e.env['TARGET_SNOWFLAKE']['is_configured']:
            pytest.skip('Target Snowflake environment variables are not provided')

        # Setup and clean source and target databases
        self.e2e.setup_tap_mysql()
        self.e2e.setup_tap_postgres()
        if self.e2e.env['TAP_S3_CSV']['is_configured']:
            self.e2e.setup_tap_s3_csv()
        self.e2e.setup_tap_mongodb()
        self.e2e.setup_target_snowflake()

        # Import project
        [return_code, stdout, stderr] = tasks.run_command(
            f'pipelinewise import_config --dir {self.project_dir}'
        )
        assertions.assert_command_success(return_code, stdout, stderr)

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_mariadb_to_sf(self, tap_mariadb_id=TAP_MARIADB_ID):
        """Replicate data from MariaDB to Snowflake"""
        # 1. Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(
            tap_mariadb_id, TARGET_ID, ['fastsync', 'singer']
        )
        assertions.assert_row_counts_equal(
            self.run_query_tap_mysql, self.run_query_target_snowflake, self.snowflake_schema_postfix
        )
        assertions.assert_all_columns_exist(
            self.run_query_tap_mysql,
            self.e2e.run_query_target_snowflake,
            mysql_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.snowflake_schema_postfix
        )

        # 2. Make changes in MariaDB source database
        #  LOG_BASED
        self.run_query_tap_mysql(
            'UPDATE weight_unit SET isactive = 0 WHERE weight_unit_id IN (2, 3, 4)'
        )
        self.run_query_tap_mysql(
            'INSERT INTO edgydata (c_varchar, `group`, `case`, cjson, c_time) VALUES'
            '(\'Lorem ipsum dolor sit amet\', 10, \'A\', \'[]\', \'00:00:00\'),'
            '(\'Thai: แผ่นดินฮั่นเสื่อมโทรมแสนสังเวช\', 20, \'A\', \'{}\', \'12:00:59\'),'
            '(\'Chinese: 和毛泽东 <<重上井冈山>>. 严永欣, 一九八八年.\', null,\'B\', '
            '\'[{"key": "ValueOne", "actions": []}, {"key": "ValueTwo", "actions": []}]\','
            ' \'9:1:00\'),'
            '(\'Special Characters: [\"\\,'
            '!@£$%^&*()]\\\\\', null, \'B\', '
            'null, \'12:00:00\'),'
            '(\'	\', 20, \'B\', null, \'15:36:10\'),'
            '(CONCAT(CHAR(0x0000 using utf16), \'<- null char\'), 20, \'B\', null, \'15:36:10\')'
        )

        self.run_query_tap_mysql('UPDATE all_datatypes SET c_point = NULL')

        #  INCREMENTAL
        self.run_query_tap_mysql(
            'INSERT INTO address(isactive, street_number, date_created, date_updated,'
            ' supplier_supplier_id, zip_code_zip_code_id)'
            'VALUES (1, 1234, NOW(), NOW(), 0, 1234)'
        )
        self.run_query_tap_mysql(
            'UPDATE address SET street_number = 9999, date_updated = NOW()'
            ' WHERE address_id = 1'
        )
        #  FULL_TABLE
        self.run_query_tap_mysql('DELETE FROM no_pk_table WHERE id > 10')

        # 3. Run tap second time - both fastsync and a singer should be triggered, there are some FULL_TABLE
        assertions.assert_run_tap_success(
            tap_mariadb_id, TARGET_ID, ['fastsync', 'singer']
        )
        assertions.assert_row_counts_equal(
            self.run_query_tap_mysql, self.run_query_target_snowflake, schema_postfix=self.snowflake_schema_postfix
        )
        assertions.assert_all_columns_exist(
            self.run_query_tap_mysql,
            self.e2e.run_query_target_snowflake,
            mysql_to_snowflake.tap_type_to_target_type,
            {'blob_col'},
            schema_postfix=self.snowflake_schema_postfix
        )

        # Checking if mask-date transformation is working
        result = self.run_query_target_snowflake(
            f'SELECT count(1) FROM ppw_e2e_tap_mysql{self.snowflake_schema_postfix}.address '
            f'where MONTH(date_created) != 1 or DAY(date_created)::int != 1;'
        )[0][0]

        assert result == 0

        # Checking if conditional MASK-NUMBER transformation is working
        result = self.run_query_target_snowflake(
            f'SELECT count(1) FROM ppw_e2e_tap_mysql{self.snowflake_schema_postfix}.address '
            f'where zip_code_zip_code_id != 0 and street_number REGEXP \'[801]\';'
        )[0][0]

        assert result == 0

        # Checking if conditional SET-NULL transformation is working
        result = self.run_query_target_snowflake(
            f'SELECT count(1) FROM ppw_e2e_tap_mysql{self.snowflake_schema_postfix}.edgydata '
            f'where "GROUP" is not null and "CASE" = \'B\';'
        )[0][0]

        assert result == 0

    @pytest.mark.dependency(depends=['import_config'])
    def test_resync_mariadb_to_sf(self, tap_mariadb_id=TAP_MARIADB_ID):
        """Resync tables from MariaDB to Snowflake"""
        assertions.assert_resync_tables_success(
            tap_mariadb_id, TARGET_ID, profiling=True
        )
        assertions.assert_row_counts_equal(
            self.run_query_tap_mysql, self.run_query_target_snowflake, schema_postfix=self.snowflake_schema_postfix
        )
        assertions.assert_all_columns_exist(
            self.run_query_tap_mysql,
            self.run_query_target_snowflake,
            mysql_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.snowflake_schema_postfix
        )

    # pylint: disable=invalid-name
    @pytest.mark.dependency(depends=['import_config'])
    def test_resync_mariadb_to_sf_with_split_large_files(
        self, tap_mariadb_id=TAP_MARIADB_SPLIT_LARGE_FILES_ID
    ):
        """Resync tables from MariaDB to Snowflake using splitting large files option"""
        assertions.assert_resync_tables_success(
            tap_mariadb_id, TARGET_ID, profiling=True
        )
        assertions.assert_row_counts_equal(
            self.run_query_tap_mysql, self.run_query_target_snowflake, schema_postfix=self.snowflake_schema_postfix
        )
        assertions.assert_all_columns_exist(
            self.run_query_tap_mysql,
            self.run_query_target_snowflake,
            mysql_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.snowflake_schema_postfix
        )

    # pylint: disable=invalid-name
    @pytest.mark.dependency(depends=['import_config'])
    def test_resync_pg_to_sf_with_split_large_files(
        self, tap_postgres_id=TAP_POSTGRES_SPLIT_LARGE_FILES_ID
    ):
        """Resync tables from Postgres to Snowflake using splitting large files option"""
        assertions.assert_resync_tables_success(
            tap_postgres_id, TARGET_ID, profiling=True
        )
        assertions.assert_row_counts_equal(
            self.run_query_tap_mysql, self.run_query_target_snowflake, schema_postfix=self.snowflake_schema_postfix
        )
        assertions.assert_all_columns_exist(
            self.run_query_tap_mysql,
            self.run_query_target_snowflake,
            mysql_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.snowflake_schema_postfix
        )

    # pylint: disable=invalid-name
    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_mariadb_to_sf_with_custom_buffer_size(self):
        """Replicate data from MariaDB to Snowflake with custom buffer size
        Same tests cases as test_replicate_mariadb_to_sf but using another tap with custom stream buffer size"""
        self.test_replicate_mariadb_to_sf(tap_mariadb_id=TAP_MARIADB_BUFFERED_STREAM_ID)

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_pg_to_sf(self):
        """Replicate data from Postgres to Snowflake"""
        # Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(
            TAP_POSTGRES_ID, TARGET_ID, ['fastsync', 'singer']
        )
        assertions.assert_row_counts_equal(
            self.run_query_tap_postgres, self.run_query_target_snowflake, schema_postfix=self.snowflake_schema_postfix
        )
        assertions.assert_all_columns_exist(
            self.run_query_tap_postgres, self.run_query_target_snowflake, schema_postfix=self.snowflake_schema_postfix
        )
        assertions.assert_date_column_naive_in_target(
            self.run_query_target_snowflake,
            'updated_at',
            f'ppw_e2e_tap_postgres{self.snowflake_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE"',
        )

        result = self.run_query_target_snowflake(
            f'SELECT updated_at FROM '
            f'ppw_e2e_tap_postgres{self.snowflake_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE" '
            f'where cvarchar=\'H\';'
        )[0][0]

        assert result == datetime(9999, 12, 31, 23, 59, 59, 998993)

        result = self.run_query_target_snowflake(
            f'SELECT updated_at FROM '
            f'ppw_e2e_tap_postgres{self.snowflake_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE" '
            f'where cvarchar=\'I\';'
        )[0][0]

        assert result == datetime(9999, 12, 31, 23, 59, 59, 998993)

        # 2. Make changes in PG source database
        #  LOG_BASED
        self.run_query_tap_postgres(
            'insert into public."table_with_space and UPPERCase" (cvarchar, updated_at) values '
            "('X', '2020-01-01 08:53:56.8+10'),"
            "('Y', '2020-12-31 12:59:00.148+00'),"
            "('faaaar future', '15000-05-23 12:40:00.148'),"
            "('BC', '2020-01-23 01:40:00 BC'),"
            "('Z', null),"
            "('W', '2020-03-03 12:30:00');"
        )
        #  INCREMENTAL
        self.run_query_tap_postgres(
            'INSERT INTO public.city (id, name, countrycode, district, population) '
            "VALUES (4080, 'Bath', 'GBR', 'England', 88859)"
        )
        self.run_query_tap_postgres(
            'UPDATE public.edgydata SET '
            "cjson = json '{\"data\": 1234}', "
            "cjsonb = jsonb '{\"data\": 2345}', "
            "cvarchar = 'Liewe Maatjies UPDATED' WHERE cid = 23"
        )
        #  FULL_TABLE
        self.run_query_tap_postgres("DELETE FROM public.country WHERE code = 'UMI'")

        # 3. Run tap second time - both fastsync and a singer should be triggered, there are some FULL_TABLE
        assertions.assert_run_tap_success(
            TAP_POSTGRES_ID, TARGET_ID, ['fastsync', 'singer'], profiling=True
        )
        assertions.assert_row_counts_equal(
            self.run_query_tap_postgres, self.run_query_target_snowflake, schema_postfix=self.snowflake_schema_postfix
        )
        assertions.assert_all_columns_exist(
            self.run_query_tap_postgres, self.run_query_target_snowflake, schema_postfix=self.snowflake_schema_postfix
        )
        assertions.assert_date_column_naive_in_target(
            self.run_query_target_snowflake,
            'updated_at',
            f'ppw_e2e_tap_postgres{self.snowflake_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE"',
        )

        result = self.run_query_target_snowflake(
            f'SELECT updated_at FROM '
            f'ppw_e2e_tap_postgres{self.snowflake_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE"'
            f' where cvarchar=\'X\';'
        )[0][0]

        assert result == datetime(2019, 12, 31, 22, 53, 56, 800000)

        result = self.run_query_target_snowflake(
            f'SELECT updated_at FROM '
            f'ppw_e2e_tap_postgres{self.snowflake_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE" '
            f'where cvarchar=\'faaaar future\';'
        )[0][0]

        assert result == datetime(9999, 12, 31, 23, 59, 59, 998993)

        result = self.run_query_target_snowflake(
            f'SELECT updated_at FROM '
            f'ppw_e2e_tap_postgres{self.snowflake_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE" '
            f'where cvarchar=\'BC\';'
        )[0][0]

        assert result == datetime(9999, 12, 31, 23, 59, 59, 998993)

    # pylint: disable=invalid-name,too-many-locals
    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_pg_to_sf_with_archive_load_files(self):
        """Fastsync tables from Postgres to Snowflake with archive load files enabled"""
        if not self.e2e.env['TARGET_SNOWFLAKE']['is_configured']:
            pytest.skip('Target Snowflake environment variables are not provided')

        archive_s3_prefix = 'archive_folder'  # Custom s3 prefix defined in TAP_POSTGRES_ARCHIVE_LOAD_FILES_ID

        s3_bucket = os.environ.get('TARGET_SNOWFLAKE_S3_BUCKET')
        s3_client = self.e2e.get_aws_session().client('s3')

        # Delete any dangling files from archive
        files_in_s3_archive = s3_client.list_objects(
            Bucket=s3_bucket,
            Prefix='{}/postgres_to_sf_archive_load_files/'.format(archive_s3_prefix),
        ).get('Contents', [])
        for file_in_archive in files_in_s3_archive:
            s3_client.delete_object(Bucket=s3_bucket, Key=(file_in_archive['Key']))

        # Run tap
        assertions.assert_run_tap_success(
            TAP_POSTGRES_ARCHIVE_LOAD_FILES_ID, TARGET_ID, ['fastsync', 'singer']
        )

        # Assert expected files in archive folder
        expected_archive_files_count = {
            'public.city': 2,  # INCREMENTAL: fastsync and singer
            'public.country': 1,  # FULL_TABLE : fastsync only
            'public2.wearehere': 1,  # FULL_TABLE : fastsync only
        }
        for (
            schema_table,
            expected_archive_files,
        ) in expected_archive_files_count.items():
            schema, table = schema_table.split('.')
            files_in_s3_archive = s3_client.list_objects(
                Bucket=s3_bucket,
                Prefix=(
                    '{}/postgres_to_sf_archive_load_files/{}'.format(
                        archive_s3_prefix, table
                    )
                ),
            ).get('Contents')

            if (
                files_in_s3_archive is None
                or len(files_in_s3_archive) != expected_archive_files
            ):
                raise Exception(
                    'files_in_archive for {} is {}. Expected archive files count: {}'.format(
                        table, files_in_s3_archive, expected_archive_files
                    )
                )

            # Assert expected metadata
            archive_metadata = s3_client.head_object(
                Bucket=s3_bucket, Key=(files_in_s3_archive[0]['Key'])
            )['Metadata']

            expected_metadata = {
                'tap': 'postgres_to_sf_archive_load_files',
                'schema': schema,
                'table': table,
                'archived-by': 'pipelinewise_fastsync_postgres_to_snowflake',
            }

            if archive_metadata != expected_metadata:
                raise Exception(
                    'archive_metadata for {} is {}'.format(table, archive_metadata)
                )

            # Assert expected file contents
            with tempfile.NamedTemporaryFile() as tmpfile:

                with open(tmpfile.name, 'wb') as f:
                    s3_client.download_fileobj(
                        s3_bucket, files_in_s3_archive[0]['Key'], f
                    )

                with gzip.open(tmpfile, 'rt') as gzipfile:
                    rows_in_csv = len(gzipfile.readlines())

            rows_in_table = self.run_query_tap_postgres(
                'SELECT COUNT(1) FROM {}'.format(schema_table)
            )[0][0]

            if rows_in_csv != rows_in_table:
                raise Exception(
                    'Rows in csv and db differ: {} vs {}'.format(
                        rows_in_csv, rows_in_table
                    )
                )

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_s3_to_sf(self):
        """Replicate csv files from s3 to Snowflake, check if return code is zero and success log file created"""
        # Skip tap_s3_csv related test if required env vars not provided
        if not self.e2e.env['TAP_S3_CSV']['is_configured']:
            pytest.skip('Tap S3 CSV environment variables are not provided')

        def assert_columns_exist():
            """Helper inner function to test if every table and column exists in target snowflake"""
            assertions.assert_cols_in_table(
                self.run_query_target_snowflake,
                'ppw_e2e_tap_s3_csv',
                'countries',
                ['CITY', 'COUNTRY', 'CURRENCY', 'ID', 'LANGUAGE'],
                schema_postfix=self.snowflake_schema_postfix
            )
            assertions.assert_cols_in_table(
                self.run_query_target_snowflake,
                f'ppw_e2e_tap_s3_csv',
                'people',
                [
                    'BIRTH_DATE',
                    'EMAIL',
                    'FIRST_NAME',
                    'GENDER',
                    'GROUP',
                    'ID',
                    'IP_ADDRESS',
                    'IS_PENSIONEER',
                    'LAST_NAME',
                ],
                schema_postfix=self.snowflake_schema_postfix
            )

        # 1. Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(
            TAP_S3_CSV_ID, TARGET_ID, ['fastsync', 'singer']
        )
        assert_columns_exist()

        # 2. Run tap second time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(
            TAP_S3_CSV_ID, TARGET_ID, ['fastsync', 'singer']
        )
        assert_columns_exist()
    '''
    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_mongodb_to_sf(self):
        """Replicate mongodb to Snowflake"""

        def assert_columns_exist(table):
            """Helper inner function to test if every table and column exists in the target"""
            assertions.assert_cols_in_table(
                self.run_query_target_snowflake,
                'ppw_e2e_tap_mongodb',
                table,
                [
                    '_ID',
                    'DOCUMENT',
                    '_SDC_EXTRACTED_AT',
                    '_SDC_BATCHED_AT',
                    '_SDC_DELETED_AT',
                ],
                schema_postfix=self.snowflake_schema_postfix,
            )

        def assert_row_counts_equal(target_schema, table, count_in_source):
            assert (
                count_in_source
                == self.run_query_target_snowflake(
                    f'select count(_id) from {target_schema}.{table}'
                )[0][0]
            )

        # Run tap first time - fastsync and singer should be triggered
        assertions.assert_run_tap_success(
            TAP_MONGODB_ID, TARGET_ID, ['fastsync', 'singer']
        )
        assert_columns_exist('listings')
        assert_columns_exist('my_collection')
        assert_columns_exist('all_datatypes')

        listing_count = self.mongodb_con['listings'].count_documents({})
        my_coll_count = self.mongodb_con['my_collection'].count_documents({})
        all_datatypes_count = self.mongodb_con['all_datatypes'].count_documents({})

        assert_row_counts_equal(f'ppw_e2e_tap_mongodb{self.snowflake_schema_postfix}', 'listings', listing_count)
        assert_row_counts_equal(f'ppw_e2e_tap_mongodb{self.snowflake_schema_postfix}', 'my_collection', my_coll_count)
        assert_row_counts_equal(
            f'ppw_e2e_tap_mongodb{self.snowflake_schema_postfix}', 'all_datatypes', all_datatypes_count
        )

        result_insert = self.mongodb_con.my_collection.insert_many(
            [
                {
                    'age': randint(10, 30),
                    'id': 1001,
                    'uuid': uuid.uuid4(),
                    'ts': Timestamp(12030, 500),
                },
                {
                    'date': datetime.utcnow(),
                    'id': 1002,
                    'uuid': uuid.uuid4(),
                    'regex': bson.Regex(r'^[A-Z]\\w\\d{2,6}.*$'),
                },
                {
                    'uuid': uuid.uuid4(),
                    'id': 1003,
                    'decimal': bson.Decimal128(
                        decimal.Decimal('5.64547548425446546546644')
                    ),
                    'nested_json': {
                        'a': 1,
                        'b': 3,
                        'c': {'key': bson.datetime.datetime(2020, 5, 3, 10, 0, 0)},
                    },
                },
            ]
        )
        my_coll_count += len(result_insert.inserted_ids)

        result_del = self.mongodb_con.my_collection.delete_one(
            {'_id': result_insert.inserted_ids[0]}
        )
        my_coll_count -= result_del.deleted_count

        result_update = self.mongodb_con.my_collection.update_many(
            {}, {'$set': {'id': 0}}
        )

        assertions.assert_run_tap_success(TAP_MONGODB_ID, TARGET_ID, ['singer'])

        assert (
            result_update.modified_count
            == self.run_query_target_snowflake(
                f'select count(_id) from ppw_e2e_tap_mongodb{self.snowflake_schema_postfix}.my_collection'
                f' where document:id = 0'
            )[0][0]
        )

        assert_row_counts_equal(f'ppw_e2e_tap_mongodb_{self.snowflake_schema_postfix}', 'my_collection', my_coll_count)
