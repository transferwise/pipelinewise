from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_postgres import TapPostgres


POSTGRES_FASTSYNC_COMPARISON_COLUMNS = [
    {
        'name': 'cid',
        'source_expression': '"cid"',
        'target_expression': '"CID"',
        'normalizer': 'integer',
    },
    {
        'name': 'ctimentz',
        'source_expression': (
            'replace("ctimentz"::varchar,\'24:00:00\',\'00:00:00\')'
        ),
        'target_expression': 'TO_VARCHAR("CTIMENTZ", \'HH24:MI:SS\')',
        'normalizer': 'text',
    },
    {
        'name': 'ctimetz',
        'source_expression': (
            'replace(("ctimetz" at time zone \'UTC\')::time::varchar,'
            '\'24:00:00\',\'00:00:00\')'
        ),
        'target_expression': 'TO_VARCHAR("CTIMETZ", \'HH24:MI:SS\')',
        'normalizer': 'text',
    },
    {
        'name': 'cjson',
        'source_expression': '"cjson"',
        'target_expression': '"CJSON"',
        'normalizer': 'json',
    },
    {
        'name': 'cjsonb',
        'source_expression': '"cjsonb"',
        'target_expression': '"CJSONB"',
        'normalizer': 'json',
    },
    {
        'name': 'cvarchar',
        'source_expression': '"cvarchar"',
        'target_expression': '"CVARCHAR"',
        'normalizer': 'text',
        'source_normalizer': 'hash_skip_first_3',
    },
    {
        'name': 'date',
        'source_expression': (
            '(CASE WHEN "date" < DATE \'0001-01-01\' '
            'OR "date" > DATE \'9999-12-31\' '
            'THEN DATE \'9999-12-31\' ELSE "date" END)::text'
        ),
        'target_expression': 'TO_VARCHAR("DATE", \'YYYY-MM-DD\')',
        'normalizer': 'text',
    },
]


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
            'comparison_columns': POSTGRES_FASTSYNC_COMPARISON_COLUMNS,
        }
        assertions.assert_resync_populates_target(
            self.tap_parameters, primary_key=self.column
        )

    def test_partial_sync_pg_to_sf(self):
        """
        Test partial sync table from PG to Snowflake
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
        assertions.assert_source_target_rows_equal(
            self.tap_parameters,
            primary_key=self.column,
            where_clause=(
                f'WHERE {self.column} = 1 '
                f'OR {self.column} BETWEEN 3 AND 7'
            ),
            operation='PartialSync',
        )

        # for this test, all records with id > 1 are deleted from the target and then will do a partial sync
        expected_records_for_column = [1, 3, 4, 5, 6, 7]
        column_to_check = primary_key = self.column

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'postgres', self.table, column_to_check, primary_key, expected_records_for_column
        )

    def test_unbounded_empty_range_removes_target_only_row(self):
        """Publish an empty source range so its target-only row is deleted."""
        source_max = self.e2e_env.run_query_tap_postgres(
            f'SELECT MAX({self.column}) FROM public.{self.table}'
        )[0][0]
        target_only_key = int(source_max) + 1

        self.e2e_env.delete_record_from_target_snowflake(
            tap_type=self.tap_parameters['tap_type'],
            table=self.table,
            where_clause=f'WHERE {self.column} > 1',
        )
        self.e2e_env.run_query_target_snowflake(
            f'UPDATE ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}.{self.table} '
            f'SET "{self.column.upper()}" = {target_only_key} '
            f'WHERE "{self.column.upper()}" = 1'
        )
        source_rows = self.e2e_env.get_rows_from_source(
            tap_type='postgres',
            source_db=self.tap_parameters['source_db'],
            table=self.table,
            columns=[self.column],
            primary_key=self.column,
            where_clause=f'WHERE {self.column} >= {target_only_key}',
        )
        target_rows = self.e2e_env.get_rows_from_target_snowflake(
            tap_type='postgres',
            table=self.table,
            columns=[f'"{self.column.upper()}"'],
            primary_key=self.column,
        )
        self.assertEqual(source_rows, [])
        self.assertEqual(target_rows, [(target_only_key,)])

        assertions.assert_partial_sync_table_success(
            self.tap_parameters,
            start_value=target_only_key,
        )

        target_rows = self.e2e_env.get_rows_from_target_snowflake(
            tap_type='postgres',
            table=self.table,
            columns=[f'"{self.column.upper()}"'],
            primary_key=self.column,
        )
        self.assertEqual(target_rows, [])

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
        assertions.assert_source_target_rows_equal(
            self.tap_parameters,
            primary_key=self.column,
            where_clause=(
                f'WHERE {self.column} <= 2 '
                f'OR {self.column} BETWEEN 4 AND 6'
            ),
            operation='PartialSync',
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
        assertions.assert_source_target_rows_equal(
            self.tap_parameters,
            primary_key=self.column,
            where_clause=(
                f'WHERE {self.column} <= 2 '
                f'OR {self.column} BETWEEN 4 AND 7'
            ),
            operation='PartialSync',
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
        assertions.assert_source_target_rows_equal(
            self.tap_parameters,
            primary_key=self.column,
            where_clause=(
                f'WHERE {self.column} = 1 '
                f'OR {self.column} BETWEEN 4 AND 6'
            ),
            operation='PartialSync',
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
        assertions.assert_source_target_rows_equal(
            self.tap_parameters,
            primary_key=self.column,
            where_clause=f'WHERE {self.column} BETWEEN 4 AND 6',
            operation='PartialSync',
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
            'comparison_columns': POSTGRES_FASTSYNC_COMPARISON_COLUMNS,
        }
        assertions.assert_resync_populates_target(
            self.tap_parameters, primary_key=self.column
        )

    def _get_deleted_row_state(self):
        target_columns = [
            column['target_expression']
            for column in self.tap_parameters['comparison_columns']
        ]
        return self.e2e_env.get_rows_from_target_snowflake(
            tap_type=self.tap_parameters['tap_type'],
            table=self.table,
            columns=[
                *target_columns,
                'TRY_TO_TIMESTAMP_TZ("_SDC_DELETED_AT")',
            ],
            primary_key=self.column,
            where_clause=f'WHERE "{self.column.upper()}" = 5',
        )

    def _snowflake_current_timestamp(self):
        return self.e2e_env.run_query_target_snowflake(
            'SELECT CURRENT_TIMESTAMP()'
        )[0][0]

    def test_partial_sync_if_record_is_deleted_from_the_source_and_soft_delete(self):
        """
        Test partial sync table from PG to Snowflake if soft delete is selected and a record is deleted from the source
        """
        row_before = self._get_deleted_row_state()
        self.assertEqual(len(row_before), 1)
        self.assertIsNone(row_before[0][-1])

        self.e2e_env.delete_record_from_source(
            'postgres', self.table, 'WHERE cid=5'
        )

        # Deleting all records from the target with primary key greater than 5
        self.e2e_env.delete_record_from_target_snowflake(
            tap_type=self.tap_parameters['tap_type'],
            table=self.tap_parameters['table'],
            where_clause=f'WHERE {self.column} > 5'
        )
        started_at = self._snowflake_current_timestamp()
        assertions.assert_partial_sync_table_success(
            self.tap_parameters,
            start_value=4,
            end_value=6,
        )
        finished_at = self._snowflake_current_timestamp()

        assertions.assert_source_target_rows_equal(
            self.tap_parameters,
            primary_key=self.column,
            where_clause=f'WHERE {self.column} <= 6 AND {self.column} <> 5',
            operation='PartialSync',
        )

        # for this test, all records with id > 3 are deleted from the target and then will do a partial sync
        expected_records_for_column = [1, 2, 3, 4, 5, 6]
        column_to_check = primary_key = self.column

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'postgres', self.table, column_to_check, primary_key, expected_records_for_column
        )

        row_after = self._get_deleted_row_state()
        self.assertEqual(len(row_after), 1)
        self.assertEqual(row_after[0][:-1], row_before[0][:-1])

        records = self.e2e_env.get_rows_from_target_snowflake(
            tap_type='postgres',
            table=self.table,
            columns=[
                '"CID"',
                'TRY_TO_TIMESTAMP_TZ("_SDC_DELETED_AT")',
            ],
            primary_key=primary_key,
        )
        self.assertEqual([record[0] for record in records], expected_records_for_column)
        self.assertTrue(all(
            deleted_at is None
            for record_id, deleted_at in records
            if record_id != 5
        ))

        deleted_at = dict(records)[5]
        self.assertIsNotNone(deleted_at)
        self.assertEqual(deleted_at, row_after[0][-1])
        self.assertLessEqual(started_at, deleted_at)
        self.assertLessEqual(deleted_at, finished_at)
