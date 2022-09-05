from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB
from datetime import datetime


class TestPartialSyncMariaDBToSF(TapMariaDB):
    """
    Test cases for Partial sync table from MariaDB to Snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self):
        self.table = 'weight_unit'
        self.column = 'weight_unit_id'
        super().setUp(tap_id='mariadb_to_sf', target_id='snowflake')
        # It should be ran one time before for partial sync
        assertions.assert_resync_tables_success(self.tap_id, self.target_id, profiling=False)
        self.tap_parameters = {
            'env': self.e2e_env,
            'tap': self.tap_id,
            'tap_type': 'mysql',
            'target': self.target_id,
            'source_db': self.e2e_env.get_conn_env_var('TAP_MYSQL', 'DB'),
            'table': self.table,
            'column': self.column
        }

    def test_partial_sync_mariadb_to_sf(self):
        """
        Test partial sync table from MariaDB to Snowflake
        """

        assertions.assert_partial_sync_table_success(
            self.tap_parameters,
            start_value=4,
            end_value=6,
            min_pk_value_for_target_missed_records=1
        )

        # for this test, all records with id > 1 are deleted from the target and then will do a partial sync
        expected_records_for_column = [1, 4, 5, 6]
        column_to_check = primary_key = self.column

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'mysql', self.table, column_to_check, primary_key, expected_records_for_column
        )

    def test_partial_sync_if_there_is_additional_column_in_source(self):
        """
        Test partial sync table from MariaDB to Snowflake if there are additional columns in source
        """

        additional_column = 'FOO_NEW_COLUMN_SOURCE'
        additional_column_value = 345
        assertions.assert_partial_sync_table_with_source_additional_columns(
            self.tap_parameters,
            additional_column={'name': additional_column, 'value': additional_column_value},
            start_value=4,
            end_value=6,
            min_pk_value_for_target_missed_records=1
        )

        # for this test, all records with id > 1 are deleted from the target and then will do a partial sync
        # records start_value to end_value will be with the value same as source because
        # out of this range wont be touched and they will have None

        expected_records_for_column = [None, additional_column_value, additional_column_value, additional_column_value]
        primary_key = self.column
        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'mysql', self.table, additional_column, primary_key, expected_records_for_column
        )

    def test_partial_sync_if_there_is_additional_column_in_target(self):
        """
        Test partial sync table from MariaDB to Snowflake if there are additional columns in target
        """

        additional_column_value = 567
        additional_column = 'FOO_NEW_COLUMN_TARGET'
        assertions.assert_partial_sync_table_with_target_additional_columns(
            self.tap_parameters,
            additional_column={'name': additional_column, 'value': additional_column_value},
            start_value=4,
            end_value=6,
            min_pk_value_for_target_missed_records=1
        )

        # for this test, all records with id > 1 are deleted from the target and then will do a partial sync
        # records start_value to end_value should be None because these columns do not exist in the source and records
        # out of this range wont be touched and they will have their original value
        expected_records_for_column = [additional_column_value, None, None, None]
        primary_key = self.column

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'mysql', self.table, additional_column, primary_key, expected_records_for_column
        )

    def test_partial_sync_if_record_is_deleted_from_the_source_and_hard_delete(self):
        """
        Test partial sync table from MariaDB to SF if hard delete is selected and a record is deleted from the source
        """
        self.e2e_env.delete_record_from_source('mysql', self.table, 'WHERE weight_unit_id=5')

        assertions.assert_partial_sync_table_success(
            self.tap_parameters,
            start_value=4,
            end_value=6,
            min_pk_value_for_target_missed_records=1
        )

        # for this test, all records with id > 1 are deleted from the target and then will do a partial sync
        expected_records_for_column = [1, 4, 6]
        column_to_check = primary_key = self.column

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'mysql', self.table, column_to_check, primary_key, expected_records_for_column
        )


class TestPartialSyncMariaDBToSFSoftDelete(TapMariaDB):
    """
    Test cases for Partial sync table from MariaDB to Snowflake if set to soft delete
    """

    # pylint: disable=arguments-differ
    def setUp(self):
        self.table = 'weight_unit'
        self.column = 'weight_unit_id'
        super().setUp(tap_id='mariadb_to_sf_soft_delete', target_id='snowflake')
        # It should be ran one time before for partial sync
        assertions.assert_resync_tables_success(self.tap_id, self.target_id, profiling=False)
        self.tap_parameters = {
            'env': self.e2e_env,
            'tap': self.tap_id,
            'tap_type': 'mysql',
            'target': self.target_id,
            'source_db': self.e2e_env.get_conn_env_var('TAP_MYSQL', 'DB'),
            'table': self.table,
            'column': self.column
        }

    def test_partial_sync_if_record_is_deleted_from_the_source_and_soft_delete(self):
        """
        Test partial sync table from MariaDB to SF if soft delete is selected and a record is deleted from the source
        """
        self.e2e_env.delete_record_from_source('mysql', self.table, 'WHERE weight_unit_id=5')

        assertions.assert_partial_sync_table_success(
            self.tap_parameters,
            start_value=4,
            end_value=6,
            min_pk_value_for_target_missed_records=5
        )

        # for this test, all records with id > 1 are deleted from the target and then will do a partial sync
        expected_records_for_column = [1, 2, 3, 4, 5, 6]
        column_to_check = primary_key = 'weight_unit_id'

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'mysql', self.table, column_to_check, primary_key, expected_records_for_column
        )

        expected_metadata = [None, None, None, None, 'TIME_STAMP', None]

        records = self.e2e_env.get_records_from_target_snowflake(
            tap_type='mysql', table=self.table, column='_SDC_DELETED_AT', primary_key=primary_key
        )
        list_of_column_values = [column[0] for column in records]

        *first_part, sdc_delete, end_part = list_of_column_values
        self.assertListEqual(first_part, expected_metadata[:4])
        self.assertEqual(end_part, expected_metadata[-1])
        with assertions.assert_not_raises(ValueError):
            datetime.strptime(sdc_delete[:19], '%Y-%m-%d %H:%M:%S')
