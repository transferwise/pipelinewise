from datetime import datetime
from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_postgres import TapPostgres


class TestPartialSyncPGToSF(TapPostgres):
    """
    Test cases for Partial sync table from Postgres to Snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self):
        self.table = 'edgydata'
        self.column = 'cid'
        super().setUp(tap_id='postgres_to_sf', target_id='snowflake')

        self.tap_parameters = {
            'env': self.e2e_env,
            'tap': self.tap_id,
            'tap_type': 'postgres',
            'target': self.target_id,
            'source_db': 'public',
            'table': self.table,
            'column': self.column,
        }
        # It should be ran one time before for partial sync
        while True:
            # Repeating resync until it is successful and there are records in target
            assertions.assert_resync_tables_success(self.tap_id, self.target_id)
            records = self.e2e_env.get_records_from_target_snowflake(
                tap_type='postgres', table=self.table, column=self.column, primary_key=self.column
            )
            if records:
                break

    def test_partial_sync_pg_to_sf(self):
        """
        Test partial sync table from MariaDB to Snowflake
        """
        # Deleting all records from the target with primary key greater than 1
        self.e2e_env.delete_record_from_target_snowflake(
            tap_type=self.tap_parameters['tap_type'],
            table=self.tap_parameters['table'],
            where_clause=f'WHERE {self.column} > 1'
        )

        assertions.assert_partial_sync_table_success(
            self.tap_parameters,
            start_value=3,
            end_value=7,
        )

        # for this test, all records with id > 1 are deleted from the target and then will do a partial sync
        expected_records_for_column = [1, 3, 4, 5, 6, 7]
        column_to_check = primary_key = self.column

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'postgres', self.table, column_to_check, primary_key, expected_records_for_column
        )

    def test_partial_sync_if_there_is_additional_column_in_source(self):
        """
        Test partial sync table from PG to Snowflake if there are additional columns in source
        """
        additional_column = 'FOO_NEW_COLUMN_SOURCE'
        additional_column_value = 567

        # Deleting all records from the target with primary key greater than 2
        self.e2e_env.delete_record_from_target_snowflake(
            tap_type=self.tap_parameters['tap_type'],
            table=self.tap_parameters['table'],
            where_clause=f'WHERE {self.column} > 2'
        )

        assertions.assert_partial_sync_table_with_source_additional_columns(
            self.tap_parameters,
            additional_column={'name': additional_column, 'value': additional_column_value},
            start_value=4,
            end_value=6,
        )

        # for this test, all records with id > 2 are deleted from the target and then will do a partial sync
        # It is expected records 4 to 6 be with the value same as source
        # out of this range should have None in target
        expected_records_for_column = [
            None, None, additional_column_value, additional_column_value, additional_column_value
        ]
        primary_key = self.column
        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'postgres', self.table, additional_column, primary_key, expected_records_for_column
        )

    def test_partial_sync_if_there_is_additional_column_in_target(self):
        """
        Test partial sync table from PG to Snowflake if there are additional columns in target
        """
        additional_column_value = 987
        additional_column = 'FOO_NEW_COLUMN_TARGET'

        # Deleting all records from the target with primary key greater than 2
        self.e2e_env.delete_record_from_target_snowflake(
            tap_type=self.tap_parameters['tap_type'],
            table=self.tap_parameters['table'],
            where_clause=f'WHERE {self.column} > 2'
        )
        assertions.assert_partial_sync_table_with_target_additional_columns(
            self.tap_parameters,
            additional_column={'name': additional_column, 'value': additional_column_value},
            start_value=4,
            end_value=7,
        )

        # for this test, all records with id > 2 are deleted from the target and then will do a partial sync
        # It is expected records 4 to 7 be None value because this column does not exist in the source and records
        # out of this range wont be touched and they will have their original value
        expected_records_for_column = [additional_column_value, additional_column_value, None, None, None, None]
        primary_key = self.column

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'postgres', self.table, additional_column, primary_key, expected_records_for_column
        )

    def test_partial_sync_if_record_is_deleted_from_the_source_and_hard_delete(self):
        """
        Test partial sync table from PG to Snowflake if hard delete is selected and a record is deleted from the source
        """
        self.e2e_env.delete_record_from_source('postgres', self.table, 'WHERE cid=5')

        # Deleting all records from the target with primary key greater than 1
        self.e2e_env.delete_record_from_target_snowflake(
            tap_type=self.tap_parameters['tap_type'],
            table=self.tap_parameters['table'],
            where_clause=f'WHERE {self.column} > 1'
        )

        assertions.assert_partial_sync_table_success(
            self.tap_parameters,
            start_value=4,
            end_value=6,
        )

        # for this test, all records with id > 1 are deleted from the target and then will do a partial sync
        expected_records_for_column = [1, 4, 6]
        column_to_check = primary_key = self.column

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'postgres', self.table, column_to_check, primary_key, expected_records_for_column
        )

    def test_partial_sync_if_table_does_not_exist_in_target(self):
        """Test partial sync if table does not exist in target"""

        # Dropping the table
        self.e2e_env.run_query_target_snowflake(
            f'DROP TABLE ppw_e2e_tap_{self.tap_parameters["tap_type"]}{self.e2e_env.sf_schema_postfix}.{self.table}')

        assertions.assert_partial_sync_table_success(
            self.tap_parameters,
            start_value=4,
            end_value=6,
        )

        expected_records_for_column = [4, 5, 6]
        column_to_check = primary_key = self.column

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'postgres', self.table, column_to_check, primary_key, expected_records_for_column
        )


class TestPartialSyncPGToSFSoftDelete(TapPostgres):
    """
    Test cases for Partial sync table from Postgres to Snowflake if set to soft delete
    """
    # pylint: disable=arguments-differ
    def setUp(self):
        self.table = 'edgydata'
        self.column = 'cid'
        super().setUp(tap_id='postgres_to_sf_soft_delete', target_id='snowflake')
        self.tap_parameters = {
            'env': self.e2e_env,
            'tap': self.tap_id,
            'tap_type': 'postgres',
            'target': self.target_id,
            'source_db': 'public',
            'table': self.table,
            'column': self.column,
        }
        # It should be ran one time before for partial sync
        while True:
            assertions.assert_resync_tables_success(self.tap_id, self.target_id)
            records = self.e2e_env.get_records_from_target_snowflake(
                tap_type='postgres', table=self.table, column=self.column, primary_key=self.column
            )
            if records:
                break

    def test_partial_sync_if_record_is_deleted_from_the_source_and_soft_delete(self):
        """
        Test partial sync table from PG to Snowflake if soft delete is selected and a record is deleted from the source
        """
        self.e2e_env.delete_record_from_source('postgres', self.table, 'WHERE cid=5')

        # Deleting all records from the target with primary key greater than 5
        self.e2e_env.delete_record_from_target_snowflake(
            tap_type=self.tap_parameters['tap_type'],
            table=self.tap_parameters['table'],
            where_clause=f'WHERE {self.column} > 5'
        )
        assertions.assert_partial_sync_table_success(
            self.tap_parameters,
            start_value=4,
            end_value=6,
        )

        # for this test, all records with id > 3 are deleted from the target and then will do a partial sync
        expected_records_for_column = [1, 2, 3, 4, 5, 6]
        column_to_check = primary_key = self.column

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'postgres', self.table, column_to_check, primary_key, expected_records_for_column
        )

        expected_metadata = [None, None, None, None, 'TIME_STAMP', None]

        records = self.e2e_env.get_records_from_target_snowflake(
            tap_type='postgres', table=self.table, column='_SDC_DELETED_AT', primary_key=primary_key
        )
        list_of_column_values = [column[0] for column in records]

        *first_part, sdc_delete, end_part = list_of_column_values
        self.assertListEqual(first_part, expected_metadata[:4])
        self.assertEqual(end_part, expected_metadata[-1])
        with assertions.assert_not_raises(ValueError):
            datetime.strptime(sdc_delete[:19], '%Y-%m-%d %H:%M:%S')
