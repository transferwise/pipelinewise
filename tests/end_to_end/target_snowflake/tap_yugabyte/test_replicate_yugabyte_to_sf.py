from pipelinewise.fastsync import yugabyte_to_snowflake
from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_yugabyte import (
    TapYugabyte,
    yugabyte_initial_state_expectations,
    yugabyte_recurring_state_expectations,
)

TAP_ID = 'yugabyte_to_sf'
TARGET_ID = 'snowflake'


class TestReplicateYugabyteToSF(TapYugabyte):
    """
    Replicate data from YugabyteDB to Snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)

    def test_replicate_yugabyte_to_sf(self):
        """Replicate data from YugabyteDB to Snowflake"""
        # 1. Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(
            self.tap_id,
            self.target_id,
            ['fastsync', 'singer'],
            expected_state_streams=yugabyte_initial_state_expectations(),
        )
        assertions.assert_row_counts_equal(
            self.e2e_env.run_query_tap_yugabyte,
            self.e2e_env.run_query_target_snowflake,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )
        assertions.assert_all_columns_exist(
            self.e2e_env.run_query_tap_yugabyte,
            self.e2e_env.run_query_target_snowflake,
            yugabyte_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )

        # 2. Make changes in the YugabyteDB source database
        #  INCREMENTAL
        self.e2e_env.run_query_tap_yugabyte(
            'INSERT INTO public.city (id, name, countrycode, district, population) '
            "VALUES (4080, 'Bath', 'GBR', 'England', 88859)"
        )
        self.e2e_env.run_query_tap_yugabyte(
            'UPDATE public.edgydata SET '
            "cjson = json '{\"data\": 1234}', "
            "cjsonb = jsonb '{\"data\": 2345}', "
            "cvarchar = 'Liewe Maatjies UPDATED' WHERE cid = 23"
        )
        #  FULL_TABLE (now via FastSync bulk-copy)
        self.e2e_env.run_query_tap_yugabyte(
            "DELETE FROM public.country WHERE code = 'UMI'"
        )

        #  LOG_BASED - DDL first, DML last: on YugabyteDB a DML immediately followed
        #  by a DDL statement on the same session can silently drop the DML's wal2json
        #  change event, so schema changes on a CDC-streamed table must never precede
        #  the DML that depends on them within the same connection.
        self.e2e_env.run_query_tap_yugabyte(
            'ALTER TABLE logical1.logical1_table1 ADD COLUMN bool_col bool;'
        )
        self.e2e_env.run_query_tap_yugabyte(
            'ALTER TABLE logical1.logical1_table1 RENAME COLUMN cvarchar2 to varchar_col;'
        )
        self.e2e_env.run_query_tap_yugabyte(
            'INSERT INTO logical1.logical1_table1 (cvarchar, varchar_col, bool_col) values '
            '(\'insert after alter table\', \'this is renamed column\', true);'
        )

        # 3. Run tap second time - both fastsync and a singer should be triggered, there are some FULL_TABLE
        assertions.assert_run_tap_success(
            self.tap_id,
            self.target_id,
            ['fastsync', 'singer'],
            expected_state_streams=yugabyte_recurring_state_expectations(),
        )
        assertions.assert_row_counts_equal(
            self.e2e_env.run_query_tap_yugabyte,
            self.e2e_env.run_query_target_snowflake,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )
        assertions.assert_all_columns_exist(
            self.e2e_env.run_query_tap_yugabyte,
            self.e2e_env.run_query_target_snowflake,
            yugabyte_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )

        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT "NAME" FROM ppw_e2e_tap_yugabyte{self.e2e_env.sf_schema_postfix}.city '
            'WHERE "ID" = 4080;'
        )[0][0]
        self.assertEqual(result, 'Bath')

        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT count(1) FROM ppw_e2e_tap_yugabyte{self.e2e_env.sf_schema_postfix}.country '
            "WHERE \"CODE\" = 'UMI';"
        )[0][0]
        self.assertEqual(result, 0)

        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT "VARCHAR_COL", "BOOL_COL" FROM '
            f'ppw_e2e_tap_yugabyte_logical1{self.e2e_env.sf_schema_postfix}.logical1_table1 '
            "WHERE \"CVARCHAR\" = 'insert after alter table';"
        )[0]
        self.assertEqual(result, ('this is renamed column', True))
