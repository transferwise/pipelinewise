from datetime import datetime

from pipelinewise.fastsync import postgres_to_snowflake
from tests.end_to_end.helpers import assertions
from tests.end_to_end.helpers.json_fixtures import (
    assert_ticket_20155_json_metadata,
    ticket_20155_json_metadata,
)
from tests.end_to_end.target_snowflake.tap_postgres import (
    TapPostgres,
    postgres_initial_state_expectations,
    postgres_recurring_state_expectations,
)

TAP_ID = 'postgres_to_sf'
TARGET_ID = 'snowflake'
LOG_BASED_SEED_MARKER = 'Ticket 20155 before unchanged TOAST update'
LOG_BASED_UPDATED_MARKER = 'Ticket 20155 after unchanged TOAST update'
LOG_BASED_INSERT_MARKER = 'Ticket 20155 LOG_BASED insert before update'
LOG_BASED_INSERT_UPDATED_MARKER = 'Ticket 20155 LOG_BASED insert after update'
LOG_BASED_NULL_SEED_MARKER = 'Ticket 20155 before explicit NULL update'
LOG_BASED_NULL_UPDATED_MARKER = 'Ticket 20155 after explicit NULL update'


class TestReplicatePGToSF(TapPostgres):
    """
    Resync tables from Postgres to Snowflake using splitting large files option.
    """

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)

    def _insert_incremental_ticket_json_metadata(self, row_id):
        self.e2e_env.run_query_tap_postgres(
            'INSERT INTO public.city '
            '(id, name, countrycode, district, population) '
            'VALUES (%s, %s, %s, %s, %s)',
            (row_id, ticket_20155_json_metadata(), 'GBR', 'Ticket 20155', 0),
        )

    def _insert_log_based_ticket_json_metadata(self, marker):
        return self.e2e_env.run_query_tap_postgres(
            'INSERT INTO public."table_with_space and UPPERCase" '
            '(cvarchar, updated_at, json_metadata) VALUES (%s, %s, %s) '
            'RETURNING id',
            (marker, '2020-03-03 12:30:00+00', ticket_20155_json_metadata()),
        )[0][0]

    def _seed_log_based_ticket_json_metadata(self, marker, route):
        row_id = self._insert_log_based_ticket_json_metadata(marker)
        self._assert_log_based_source_ticket_json_metadata(row_id, marker, route)
        return row_id

    def _assert_incremental_source_ticket_json_metadata(self, row_id, route):
        rows = self.e2e_env.run_query_tap_postgres(
            'SELECT name FROM public.city WHERE id = %s',
            (row_id,),
        )
        assert_ticket_20155_json_metadata(
            rows, f'PostgreSQL {route} source row {row_id}'
        )

    def _assert_incremental_target_ticket_json_metadata(self, row_id, route):
        rows = self.e2e_env.run_query_target_snowflake(
            f'SELECT "NAME" FROM ppw_e2e_tap_postgres'
            f'{self.e2e_env.sf_schema_postfix}.city '
            f'WHERE "ID" = {row_id}'
        )
        assert_ticket_20155_json_metadata(
            rows, f'PostgreSQL {route} Snowflake row {row_id}'
        )

    def _assert_log_based_source_ticket_json_metadata(
        self, row_id, expected_marker, route, expect_null=False
    ):
        rows = self.e2e_env.run_query_tap_postgres(
            'SELECT cvarchar, json_metadata '
            'FROM public."table_with_space and UPPERCase" WHERE id = %s',
            (row_id,),
        )
        self.assertEqual(len(rows), 1, f'{route} source row {row_id} is missing')
        self.assertEqual(rows[0][0], expected_marker)
        if expect_null:
            self.assertIsNone(rows[0][1])
        else:
            assert_ticket_20155_json_metadata(
                [(rows[0][1],)], f'PostgreSQL {route} source row {row_id}'
            )

    def _assert_log_based_target_ticket_json_metadata(
        self, row_id, expected_marker, route, expect_null=False
    ):
        rows = self.e2e_env.run_query_target_snowflake(
            f'SELECT "CVARCHAR", "JSON_METADATA" FROM ppw_e2e_tap_postgres'
            f'{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE" '
            f'WHERE "ID" = {row_id}'
        )
        self.assertEqual(len(rows), 1, f'{route} Snowflake row {row_id} is missing')
        self.assertEqual(rows[0][0], expected_marker)
        if expect_null:
            self.assertIsNone(rows[0][1])
        else:
            assert_ticket_20155_json_metadata(
                [(rows[0][1],)], f'PostgreSQL {route} Snowflake row {row_id}'
            )

    def test_replicate_pg_to_sf(self):
        """
        Resync tables from Postgres to Snowflake using splitting large files option.
        """

        fastsync_json_row_id = self.e2e_env.run_query_tap_postgres(
            'SELECT max(id) + 1 FROM public.city'
        )[0][0]
        self._insert_incremental_ticket_json_metadata(fastsync_json_row_id)
        self._assert_incremental_source_ticket_json_metadata(
            fastsync_json_row_id, 'FastSync'
        )
        assertions.assert_resync_tables_success(
            self.tap_id,
            self.target_id,
            tables='public.city',
            expected_state_streams={
                'fastsync': {'public-city': True}
            },
        )
        self._assert_incremental_target_ticket_json_metadata(
            fastsync_json_row_id, 'FastSync'
        )

        singer_json_row_id = fastsync_json_row_id + 1
        self._insert_incremental_ticket_json_metadata(singer_json_row_id)
        self._assert_incremental_source_ticket_json_metadata(
            singer_json_row_id, 'Singer INCREMENTAL'
        )

        log_based_update_row_id = self._seed_log_based_ticket_json_metadata(
            LOG_BASED_SEED_MARKER,
            'FastSync seed for LOG_BASED update',
        )
        log_based_null_row_id = self._seed_log_based_ticket_json_metadata(
            LOG_BASED_NULL_SEED_MARKER,
            'FastSync seed for explicit NULL update',
        )

        initial_state_expectations = postgres_initial_state_expectations()
        initial_state_expectations['fastsync'].pop('public-city')
        assertions.assert_run_tap_success(
            self.tap_id,
            self.target_id,
            ['fastsync', 'singer'],
            expected_state_streams=initial_state_expectations,
        )

        assertions.assert_row_counts_equal(
            self.e2e_env.run_query_tap_postgres,
            self.e2e_env.run_query_target_snowflake,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )

        assertions.assert_all_columns_exist(
            self.e2e_env.run_query_tap_postgres,
            self.e2e_env.run_query_target_snowflake,
            postgres_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )
        self._assert_incremental_target_ticket_json_metadata(
            singer_json_row_id, 'Singer INCREMENTAL'
        )
        self._assert_log_based_target_ticket_json_metadata(
            log_based_update_row_id,
            LOG_BASED_SEED_MARKER,
            'FastSync seed for LOG_BASED update',
        )
        self._assert_log_based_target_ticket_json_metadata(
            log_based_null_row_id,
            LOG_BASED_NULL_SEED_MARKER,
            'FastSync seed for explicit NULL update',
        )

        assertions.assert_date_column_naive_in_target(
            self.e2e_env.run_query_target_snowflake,
            'updated_at',
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE"',
        )

        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT updated_at FROM '
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE" '
            f"where cvarchar='H';"
        )[0][0]

        self.assertEqual(result, datetime(9999, 12, 31, 23, 59, 59, 999000))

        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT updated_at FROM '
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE" '
            f"where cvarchar='I';"
        )[0][0]

        self.assertEqual(result, datetime(9999, 12, 31, 23, 59, 59, 999000))

        # 2. Make changes in PG source database
        #  LOG_BASED
        self.e2e_env.run_query_tap_postgres(
            'insert into public."table_with_space and UPPERCase" (cvarchar, updated_at) values '
            "('X', '2020-01-01 08:53:56.8+10'),"
            "('Y', '2020-12-31 12:59:00.148+00'),"
            "('faaaar future', '15000-05-23 12:40:00.148'),"
            "('BC', '2020-01-23 01:40:00 BC'),"
            "('Z', null),"
            "('W', '2020-03-03 12:30:00');"
        )
        # Control for payload size/escaping, then reproduce an unchanged TOAST
        # value omitted from an otherwise valid LOG_BASED update.
        log_based_insert_row_id = self._insert_log_based_ticket_json_metadata(
            LOG_BASED_INSERT_MARKER
        )
        self.e2e_env.run_query_tap_postgres(
            'UPDATE public."table_with_space and UPPERCase" '
            'SET cvarchar = %s WHERE id = %s',
            (LOG_BASED_INSERT_UPDATED_MARKER, log_based_insert_row_id),
        )
        self.e2e_env.run_query_tap_postgres(
            'UPDATE public."table_with_space and UPPERCase" '
            'SET cvarchar = %s WHERE id = %s',
            (LOG_BASED_UPDATED_MARKER, log_based_update_row_id),
        )
        self.e2e_env.run_query_tap_postgres(
            'UPDATE public."table_with_space and UPPERCase" '
            'SET cvarchar = %s, json_metadata = NULL WHERE id = %s',
            (LOG_BASED_NULL_UPDATED_MARKER, log_based_null_row_id),
        )
        self._assert_log_based_source_ticket_json_metadata(
            log_based_insert_row_id,
            LOG_BASED_INSERT_UPDATED_MARKER,
            'LOG_BASED insert followed by sparse update',
        )
        self._assert_log_based_source_ticket_json_metadata(
            log_based_update_row_id,
            LOG_BASED_UPDATED_MARKER,
            'LOG_BASED unchanged TOAST update',
        )
        self._assert_log_based_source_ticket_json_metadata(
            log_based_null_row_id,
            LOG_BASED_NULL_UPDATED_MARKER,
            'LOG_BASED explicit NULL update',
            expect_null=True,
        )

        #  INCREMENTAL
        last_id = self.e2e_env.run_query_tap_postgres(
            'SELECT max(id) from public.city'
        )[0][0]
        self.e2e_env.run_query_tap_postgres(
            'INSERT INTO public.city (id, name, countrycode, district, population) '
            f"VALUES ({last_id+1}, 'Bath', 'GBR', 'England', 88859)"
        )

        self.e2e_env.run_query_tap_postgres(
            'UPDATE public.edgydata SET '
            "cjson = json '{\"data\": 1234}', "
            "cjsonb = jsonb '{\"data\": 2345}', "
            "cvarchar = 'Liewe Maatjies UPDATED' WHERE cid = 23"
        )

        #  FULL_TABLE
        self.e2e_env.run_query_tap_postgres(
            "DELETE FROM public.country WHERE code = 'UMI'"
        )

        # 3. Run tap second time - both fastsync and a singer should be triggered, there are some FULL_TABLE
        assertions.assert_run_tap_success(
            self.tap_id,
            self.target_id,
            ['fastsync', 'singer'],
            profiling=True,
            expected_state_streams=postgres_recurring_state_expectations(),
        )

        assertions.assert_row_counts_equal(
            self.e2e_env.run_query_tap_postgres,
            self.e2e_env.run_query_target_snowflake,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )

        assertions.assert_all_columns_exist(
            self.e2e_env.run_query_tap_postgres,
            self.e2e_env.run_query_target_snowflake,
            postgres_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )

        self._assert_log_based_target_ticket_json_metadata(
            log_based_insert_row_id,
            LOG_BASED_INSERT_UPDATED_MARKER,
            'Singer LOG_BASED insert followed by sparse update',
        )
        self._assert_log_based_target_ticket_json_metadata(
            log_based_update_row_id,
            LOG_BASED_UPDATED_MARKER,
            'Singer LOG_BASED unchanged TOAST update',
        )
        self._assert_log_based_target_ticket_json_metadata(
            log_based_null_row_id,
            LOG_BASED_NULL_UPDATED_MARKER,
            'Singer LOG_BASED explicit NULL update',
            expect_null=True,
        )

        assertions.assert_date_column_naive_in_target(
            self.e2e_env.run_query_target_snowflake,
            'updated_at',
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE"',
        )

        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT updated_at FROM '
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE"'
            f" where cvarchar='X';"
        )[0][0]

        self.assertEqual(result, datetime(2019, 12, 31, 22, 53, 56, 800000))

        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT updated_at FROM '
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE" '
            f"where cvarchar='faaaar future';"
        )[0][0]

        self.assertEqual(result, datetime(9999, 12, 31, 23, 59, 59, 999000))

        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT updated_at FROM '
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE" '
            f"where cvarchar='BC';"
        )[0][0]

        self.assertEqual(result, datetime(9999, 12, 31, 23, 59, 59, 999000))
