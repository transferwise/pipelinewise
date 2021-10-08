import decimal
import os
import uuid
import bson
import pytest

from datetime import datetime
from random import randint
from bson import Timestamp

from pipelinewise.fastsync.commons.type_mapping import MYSQL_TO_POSTGRES_MAPPER

from .helpers import tasks
from .helpers import assertions
from .helpers.env import E2EEnv

DIR = os.path.dirname(__file__)
TAP_MARIADB_ID = 'mariadb_to_pg'
TAP_MARIADB_BUFFERED_STREAM_ID = 'mariadb_to_pg_buffered_stream'
TAP_MONGODB_ID = 'mongo_to_pg'
TAP_POSTGRES_ID = 'postgres_to_pg'
TAP_S3_CSV_ID = 's3_csv_to_pg'
TARGET_ID = 'postgres_dwh'


# pylint: disable=attribute-defined-outside-init
class TestTargetPostgres:
    """
    End to end tests for Target Postgres
    """

    def setup_method(self):
        """Initialise test project by generating YAML files from
        templates for all the configured connectors"""
        self.project_dir = os.path.join(DIR, 'test-project')

        # Init query runner methods
        self.e2e = E2EEnv(self.project_dir)
        self.run_query_tap_mysql = self.e2e.run_query_tap_mysql
        self.run_query_tap_postgres = self.e2e.run_query_tap_postgres
        self.run_query_target_postgres = self.e2e.run_query_target_postgres
        self.mongodb_con = self.e2e.get_tap_mongodb_connection()

    def teardown_method(self):
        """Delete test directories and database objects"""

    @pytest.mark.dependency(name='import_config')
    def test_import_project(self):
        """Import the YAML project with taps and target and do discovery mode
        to write the JSON files for singer connectors"""

        # Skip every target_postgres related test if required env vars not provided
        if not self.e2e.env['TARGET_POSTGRES']['is_configured']:
            pytest.skip('Target Postgres environment variables are not provided')

        # Setup and clean source and target databases
        self.e2e.setup_tap_mysql()
        self.e2e.setup_tap_postgres()
        if self.e2e.env['TAP_S3_CSV']['is_configured']:
            self.e2e.setup_tap_s3_csv()
        self.e2e.setup_tap_mongodb()
        self.e2e.setup_target_postgres()

        # Import project
        [return_code, stdout, stderr] = tasks.run_command(
            f'pipelinewise import_config --dir {self.project_dir} --profiler'
        )

        assertions.assert_command_success(return_code, stdout, stderr)
        assertions.assert_profiling_stats_files_created(stdout, 'import_project')

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_mariadb_to_pg(self, tap_mariadb_id=TAP_MARIADB_ID):
        """Replicate data from MariaDB to Postgres DWH"""
        # 1. Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(
            tap_mariadb_id, TARGET_ID, ['fastsync', 'singer'], profiling=True
        )
        assertions.assert_row_counts_equal(
            self.run_query_tap_mysql, self.run_query_target_postgres
        )
        assertions.assert_all_columns_exist(
            self.run_query_tap_mysql,
            self.run_query_target_postgres,
            MYSQL_TO_POSTGRES_MAPPER,
        )

        # 2. Make changes in MariaDB source database
        #  LOG_BASED
        self.run_query_tap_mysql(
            'UPDATE weight_unit SET isactive = 0 WHERE weight_unit_id IN (2, 3, 4)'
        )
        self.run_query_tap_mysql('ALTER table weight_unit add column bool_col bool;')
        self.run_query_tap_mysql(
            'INSERT into weight_unit(weight_unit_name, isActive, original_date_created, bool_col) '
            'values (\'Oz\', false, \'2020-07-23 10:00:00\', true);'
        )
        self.run_query_tap_mysql('ALTER table weight_unit add column blob_col blob;')
        self.run_query_tap_mysql(
            'INSERT into weight_unit(weight_unit_name, isActive, original_date_created, blob_col) '
            'values (\'Oz\', false, \'2020-07-23 10:00:00\', \'blob content\');'
        )
        self.run_query_tap_mysql(
            'ALTER table weight_unit change column bool_col is_imperial bool;'
        )
        self.run_query_tap_mysql(
            'UPDATE weight_unit SET is_imperial = false WHERE weight_unit_name like \'%oz%\''
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
            tap_mariadb_id, TARGET_ID, ['fastsync', 'singer'], profiling=True
        )
        assertions.assert_row_counts_equal(
            self.run_query_tap_mysql, self.run_query_target_postgres
        )
        assertions.assert_all_columns_exist(
            self.run_query_tap_mysql,
            self.run_query_target_postgres,
            MYSQL_TO_POSTGRES_MAPPER,
            {'blob_col'},
        )

        # Checking if mask-date transformation is working
        result = self.run_query_target_postgres(
            'SELECT count(1) FROM ppw_e2e_tap_mysql."address" '
            'where date_part(\'month\',date_created)::int != 1 or '
            'date_part(\'day\', date_created)::int != 1;'
        )[0][0]

        assert result == 0

        # Checking if conditional MASK-NUMBER transformation is working
        result = self.run_query_target_postgres(
            'SELECT count(1) FROM ppw_e2e_tap_mysql."address" '
            'where zip_code_zip_code_id != 0 and street_number ~ \'[801]\';'
        )[0][0]

        assert result == 0

        # Checking if conditional SET-NULL transformation is working
        result = self.run_query_target_postgres(
            'SELECT count(1) FROM ppw_e2e_tap_mysql."edgydata" '
            'where "group" is not null and "case" = \'B\';'
        )[0][0]

        assert result == 0

    @pytest.mark.dependency(depends=['import_config'])
    def test_resync_mariadb_to_pg(self, tap_mariadb_id=TAP_MARIADB_ID):
        """Resync tables from MariaDB to Postgres DWH"""
        # 1. Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_resync_tables_success(tap_mariadb_id, TARGET_ID)
        assertions.assert_row_counts_equal(
            self.run_query_tap_mysql, self.run_query_target_postgres
        )
        assertions.assert_all_columns_exist(
            self.run_query_tap_mysql,
            self.run_query_target_postgres,
            MYSQL_TO_POSTGRES_MAPPER,
        )

    # pylint: disable=invalid-name
    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_mariadb_to_pg_with_custom_buffer_size(self):
        """Replicate data from MariaDB to Postgres DWH with custom buffer size
        Same tests cases as test_replicate_mariadb_to_pg but using another tap with custom stream buffer size"""
        self.test_resync_mariadb_to_pg(tap_mariadb_id=TAP_MARIADB_BUFFERED_STREAM_ID)

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_pg_to_pg(self):
        """Replicate data from Postgres to Postgres DWH"""
        # 1. Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(
            TAP_POSTGRES_ID, TARGET_ID, ['fastsync', 'singer']
        )
        assertions.assert_row_counts_equal(
            self.run_query_tap_postgres, self.run_query_target_postgres
        )
        assertions.assert_all_columns_exist(
            self.run_query_tap_postgres, self.run_query_target_postgres
        )
        assertions.assert_date_column_naive_in_target(
            self.run_query_target_postgres,
            'updated_at',
            'ppw_e2e_tap_postgres."table_with_space and uppercase"',
        )

        result = self.run_query_target_postgres(
            'SELECT updated_at FROM '
            'ppw_e2e_tap_postgres."table_with_space and uppercase" '
            'where cvarchar=\'H\';'
        )[0][0]

        assert result == datetime(9999, 12, 31, 23, 59, 59, 999000)

        result = self.run_query_target_postgres(
            'SELECT updated_at FROM '
            'ppw_e2e_tap_postgres."table_with_space and uppercase" '
            'where cvarchar=\'I\';'
        )[0][0]

        assert result == datetime(9999, 12, 31, 23, 59, 59, 999000)

        # 2. Make changes in pg source database
        #  LOG_BASED
        self.run_query_tap_postgres(
            'insert into public."table_with_space and UPPERCase" (cvarchar, updated_at) values '
            "('M', '2020-01-01 08:53:56.8+10'),"
            "('N', '2020-12-31 12:59:00.148+00'),"
            "('Year in the faaaar future', '20000-05-23 12:40:00.148'),"
            "('Year in the BC', '2020-01-23 01:40:00 BC'),"
            "('O', null),"
            "('P', '2020-03-03 12:30:00');"
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

        #  LOG_BASED
        self.run_query_tap_postgres(
            'ALTER TABLE logical1.logical1_table1 ADD COLUMN bool_col bool;'
        )
        self.run_query_tap_postgres(
            'ALTER TABLE logical1.logical1_table1 RENAME COLUMN cvarchar2 to varchar_col;'
        )
        self.run_query_tap_postgres(
            'INSERT INTO logical1.logical1_table1 (cvarchar, varchar_col, bool_col) values '
            '(\'insert after alter table\', \'this is renamed column\', true);'
        )

        # 3. Run tap second time - both fastsync and a singer should be triggered, there are some FULL_TABLE
        assertions.assert_run_tap_success(
            TAP_POSTGRES_ID, TARGET_ID, ['fastsync', 'singer']
        )
        assertions.assert_row_counts_equal(
            self.run_query_tap_postgres, self.run_query_target_postgres
        )
        assertions.assert_all_columns_exist(
            self.run_query_tap_postgres, self.run_query_target_postgres
        )
        assertions.assert_date_column_naive_in_target(
            self.run_query_target_postgres,
            'updated_at',
            'ppw_e2e_tap_postgres."table_with_space and uppercase"',
        )

        result = self.run_query_target_postgres(
            'SELECT updated_at FROM ppw_e2e_tap_postgres."table_with_space and uppercase" where cvarchar=\'M\';'
        )[0][0]

        assert result == datetime(2019, 12, 31, 22, 53, 56, 800000)

        result = self.run_query_target_postgres(
            'SELECT updated_at FROM '
            'ppw_e2e_tap_postgres."table_with_space and uppercase" '
            'where cvarchar=\'Year in the faaaar future\';'
        )[0][0]

        assert result == datetime(9999, 12, 31, 23, 59, 59, 999000)

        result = self.run_query_target_postgres(
            'SELECT updated_at FROM '
            'ppw_e2e_tap_postgres."table_with_space and uppercase" '
            'where cvarchar=\'Year in the BC\';'
        )[0][0]

        assert result == datetime(9999, 12, 31, 23, 59, 59, 999000)

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_s3_to_pg(self):
        """Replicate csv files from s3 to Postgres"""
        # Skip tap_s3_csv related test if required env vars not provided
        if not self.e2e.env['TAP_S3_CSV']['is_configured']:
            pytest.skip('Tap S3 CSV environment variables are not provided')

        def assert_columns_exist():
            """Helper inner function to test if every table and column exists in target snowflake"""
            assertions.assert_cols_in_table(
                self.run_query_target_postgres,
                'ppw_e2e_tap_s3_csv',
                'countries',
                ['city', 'country', 'currency', 'id', 'language'],
            )
            assertions.assert_cols_in_table(
                self.run_query_target_postgres,
                'ppw_e2e_tap_s3_csv',
                'people',
                [
                    'birth_date',
                    'email',
                    'first_name',
                    'gender',
                    'group',
                    'id',
                    'ip_address',
                    'is_pensioneer',
                    'last_name',
                ],
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

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_mongodb_to_pg(self):
        """Replicate mongodb to Postgres"""

        def assert_columns_exist(table):
            """Helper inner function to test if every table and column exists in the target"""
            assertions.assert_cols_in_table(
                self.run_query_target_postgres,
                'ppw_e2e_tap_mongodb',
                table,
                [
                    '_id',
                    'document',
                    '_sdc_extracted_at',
                    '_sdc_batched_at',
                    '_sdc_deleted_at',
                ],
            )

        def assert_row_counts_equal(target_schema, table, count_in_source):
            assert (
                count_in_source
                == self.run_query_target_postgres(
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

        assert_row_counts_equal('ppw_e2e_tap_mongodb', 'listings', listing_count)
        assert_row_counts_equal('ppw_e2e_tap_mongodb', 'my_collection', my_coll_count)
        assert_row_counts_equal(
            'ppw_e2e_tap_mongodb', 'all_datatypes', all_datatypes_count
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
            == self.run_query_target_postgres(
                'select count(_id) from ppw_e2e_tap_mongodb.my_collection where cast(document->>\'id\' as int) = 0'
            )[0][0]
        )

        assert_row_counts_equal('ppw_e2e_tap_mongodb', 'my_collection', my_coll_count)
