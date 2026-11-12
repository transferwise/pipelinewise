from datetime import datetime
from pathlib import Path
import re

from pipelinewise.fastsync import postgres_to_snowflake
from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres
from pipelinewise.fastsync.commons import utils
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
EXPECTED_LOG_BASED_STREAMS = {'public-table_with_space and UPPERCase'}
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

    def _assert_final_postgres_feedback(self, previous_singer_logs: set[Path]) -> None:
        """Verify the completed managed run confirmed its durable target checkpoint on the source."""
        state = assertions._load_persisted_state(self.target_id, self.tap_id)
        self.assertIsInstance(state, dict)
        bookmarks = state['bookmarks']
        self.assertTrue(EXPECTED_LOG_BASED_STREAMS)
        self.assertTrue(EXPECTED_LOG_BASED_STREAMS.issubset(bookmarks))
        checkpoint_lsns = [bookmarks[stream_id]['lsn'] for stream_id in EXPECTED_LOG_BASED_STREAMS]
        self.assertTrue(all(type(lsn) is int and lsn > 0 for lsn in checkpoint_lsns))
        minimum_lsn = min(checkpoint_lsns)
        boundary = f'{minimum_lsn >> 32:X}/{minimum_lsn & 0xFFFFFFFF:X}'
        slot_name = utils.generate_replication_slot_name(
            self.e2e_env.get_conn_env_var('TAP_POSTGRES', 'DB'), self.tap_id,
        )
        rows = self.e2e_env.run_query_tap_postgres(
            'SELECT confirmed_flush_lsn::text, confirmed_flush_lsn >= %s::pg_lsn '
            'FROM pg_replication_slots WHERE slot_name = %s',
            (boundary, slot_name),
        )
        self.assertEqual(len(rows), 1, f'Expected managed logical slot {slot_name}')
        self.assertIs(rows[0][1], True, f'Slot confirmed {rows[0][0]} below durable target checkpoint {boundary}')

        log_dir = assertions._state_file_path(self.target_id, self.tap_id).parent / 'log'
        current_logs = set(log_dir.glob('*.singer.log.success')) - previous_singer_logs
        self.assertEqual(len(current_logs), 1, 'Expected one new successful managed Singer run')
        log_content = current_logs.pop().read_text(encoding='utf-8')
        confirmations = [
            line for line in log_content.splitlines()
            if 'Confirmed final PostgreSQL feedback at ' in line and f'(slot {slot_name})' in line
        ]
        self.assertTrue(
            any(re.search(r'\bINFO\b', line) for line in confirmations),
            'The recurring run must confirm managed final feedback at INFO rather than use a fallback stop',
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
        log_dir = assertions._state_file_path(self.target_id, self.tap_id).parent / 'log'
        previous_singer_logs = set(log_dir.glob('*.singer.log.success'))
        assertions.assert_run_tap_success(
            self.tap_id,
            self.target_id,
            ['fastsync', 'singer'],
            profiling=True,
            expected_state_streams=postgres_recurring_state_expectations(),
        )
        self._assert_final_postgres_feedback(previous_singer_logs)

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
