import json

from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.multiline_values import (
    exercise_multiline_fastsync,
    prepare_mysql_multiline_table,
)
from tests.end_to_end.target_snowflake.tap_mysql import (
    TapMySQL,
    mysql_initial_state_expectations,
    mysql_recurring_state_expectations,
)


TAP_ID = 'mysql_to_sf_iceberg'
TARGET_ID = 'snowflake'


class TestIcebergV3MySQLToSnowflake(TapMySQL):
    """Exercise genuine MySQL Singer, FullSync, and PartialSync into Iceberg."""

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)
        self.source_db = self.e2e_env.get_conn_env_var('TAP_ORACLE_MYSQL', 'DB')
        self.target_schema = (
            f'PPW_E2E_TAP_ORACLE_MYSQL{self.e2e_env.sf_schema_postfix}'
        ).upper()
        self.initial_s3_keys = self.iceberg_fastsync_s3_keys()

    def prepare_source(self):
        """Create the multiline table before catalog discovery."""
        super().prepare_source()
        self.addCleanup(
            self.e2e_env.run_query_tap_oracle_mysql,
            'DROP TABLE IF EXISTS multiline_values',
        )
        prepare_mysql_multiline_table(
            self.e2e_env.run_query_tap_oracle_mysql
        )

    def _assert_managed_v3(self, table_name):
        rows = self.e2e_env.run_query_target_snowflake(
            f'SHOW ICEBERG TABLES IN SCHEMA "{self.target_schema}" '
            f'STARTS WITH \'{table_name.upper()}\''
        )
        exact_rows = [row for row in rows if row[1] == table_name.upper()]
        self.assertEqual(len(exact_rows), 1)

        version_rows = self.e2e_env.run_query_target_snowflake(
            "SHOW PARAMETERS LIKE 'ICEBERG_VERSION' IN TABLE "
            f'"{self.target_schema}"."{table_name.upper()}"'
        )
        self.assertEqual(len(version_rows), 1)
        self.assertEqual(str(version_rows[0][1]), '3')
        merge_on_read_rows = self.e2e_env.run_query_target_snowflake(
            "SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR' IN TABLE "
            f'"{self.target_schema}"."{table_name.upper()}"'
        )
        self.assertEqual(len(merge_on_read_rows), 1)
        self.assertEqual(str(merge_on_read_rows[0][1]).upper(), 'DISABLED')
        self.assertEqual(str(merge_on_read_rows[0][3]).upper(), 'TABLE')

    def _target_event_rows(self):
        return self.e2e_env.run_query_target_snowflake(
            'SELECT "ID", TO_JSON("PAYLOAD"), "BODY", '
            'HEX_ENCODE("BINARY_VALUE"), "UNSIGNED_VALUE" '
            f'FROM "{self.target_schema}"."ICEBERG_EVENTS" ORDER BY "ID"'
        )

    def _target_value_rows(self, table_name):
        return self.e2e_env.run_query_target_snowflake(
            'SELECT "ID", "VALUE_TEXT" '
            f'FROM "{self.target_schema}"."{table_name.upper()}" ORDER BY "ID"'
        )

    def test_fullsync_hands_over_to_singer_on_managed_iceberg_v3(self):
        """Initial FastSync and later Singer writes preserve exact MySQL values."""
        assertions.assert_run_tap_success(
            self.tap_id,
            self.target_id,
            ['fastsync', 'singer'],
            expected_state_streams=mysql_initial_state_expectations(self.source_db),
        )

        for table_name in (
            'iceberg_events',
            'iceberg_incremental',
            'iceberg_full_reload',
            'multiline_values',
        ):
            self._assert_managed_v3(table_name)

        rows = self._target_event_rows()
        self.assertEqual([row[0] for row in rows], [1, 2, 3])
        self.assertEqual(json.loads(rows[0][1])['nested']['value'], '初')
        self.assertEqual(json.loads(rows[0][1])['empty'], [])
        self.assertEqual(
            rows[0][2:],
            ('first', '0001FF', 18446744073709551615),
        )
        self.assertGreater(len(json.loads(rows[1][1])['large']), 64 * 1024)
        self.assertGreater(len(rows[1][2]), 64 * 1024)
        self.assertEqual(rows[1][3], 'CAFE')
        self.assertEqual(rows[1][4], 0)
        self.assertEqual(rows[2][1:], (None, '', None, None))
        self.assertEqual(
            self._target_value_rows('iceberg_incremental'),
            [(1, 'incremental-one'), (2, 'incremental-two')],
        )
        self.assertEqual(
            self._target_value_rows('iceberg_full_reload'),
            [(1, 'full-one'), (2, 'full-two')],
        )
        self.assert_iceberg_fastsync_cleanup(
            self.target_schema,
            self.initial_s3_keys,
        )

        self.e2e_env.run_query_tap_oracle_mysql(
            'ALTER TABLE iceberg_events ADD COLUMN singer_added_text LONGTEXT NULL'
        )
        self.e2e_env.run_query_tap_oracle_mysql(
            'INSERT INTO iceberg_events '
            '(id, payload, body, binary_value, unsigned_value, updated_at, '
            'singer_added_text) '
            "VALUES (4, JSON_OBJECT('singer', JSON_ARRAY(NULL, JSON_OBJECT())), "
            "'singer-row', UNHEX('ABCD'), 18446744073709551615, "
            "'2026-08-19 10:00:03.000004', 'added-by-singer')"
        )
        self.e2e_env.run_query_tap_oracle_mysql(
            'DELETE FROM iceberg_events WHERE id = 3'
        )
        self.e2e_env.run_query_tap_oracle_mysql(
            "UPDATE iceberg_incremental SET value_text = 'incremental-two-updated', "
            "updated_at = '2026-08-19 10:00:04.000005' WHERE id = 2"
        )
        self.e2e_env.run_query_tap_oracle_mysql(
            'INSERT INTO iceberg_incremental (id, value_text, updated_at) '
            "VALUES (3, 'incremental-three', '2026-08-19 10:00:05.000006')"
        )
        self.e2e_env.run_query_tap_oracle_mysql(
            "UPDATE iceberg_full_reload SET value_text = 'full-two-updated' "
            'WHERE id = 2'
        )
        self.e2e_env.run_query_tap_oracle_mysql(
            'DELETE FROM iceberg_full_reload WHERE id = 1'
        )
        self.e2e_env.run_query_tap_oracle_mysql(
            "INSERT INTO iceberg_full_reload VALUES (3, 'full-three')"
        )
        assertions.assert_run_tap_success(
            self.tap_id,
            self.target_id,
            ['fastsync', 'singer'],
            expected_state_streams=mysql_recurring_state_expectations(self.source_db),
        )

        rows = self._target_event_rows()
        self.assertEqual([row[0] for row in rows], [1, 2, 4])
        self.assertEqual(json.loads(rows[-1][1]), {'singer': [None, {}]})
        self.assertEqual(
            rows[-1][2:],
            ('singer-row', 'ABCD', 18446744073709551615),
        )
        self.assertEqual(
            self.e2e_env.run_query_target_snowflake(
                'SELECT "ID", "SINGER_ADDED_TEXT" '
                f'FROM "{self.target_schema}"."ICEBERG_EVENTS" '
                'WHERE "ID" = 4'
            ),
            [(4, 'added-by-singer')],
        )
        self._assert_managed_v3('iceberg_events')
        self.assertEqual(
            self._target_value_rows('iceberg_incremental'),
            [
                (1, 'incremental-one'),
                (2, 'incremental-two-updated'),
                (3, 'incremental-three'),
            ],
        )
        self.assertEqual(
            self._target_value_rows('iceberg_full_reload'),
            [(2, 'full-two-updated'), (3, 'full-three')],
        )
        self.assert_iceberg_fastsync_cleanup(
            self.target_schema,
            self.initial_s3_keys,
        )

    def test_full_and_bounded_partial_sync_preserve_multiline_bytes(self):
        """MySQL FastSync retains controls, escapes, text, empty, and NULL."""
        exercise_multiline_fastsync(
            self,
            self.e2e_env.run_query_tap_oracle_mysql,
            self.source_db,
            'oracle_mysql',
            self.target_schema,
            lambda: self._assert_managed_v3('multiline_values'),
        )
        self.assert_iceberg_fastsync_cleanup(
            self.target_schema,
            self.initial_s3_keys,
        )

    def test_partial_sync_merges_a_bounded_range_into_managed_iceberg_v3(self):
        """PartialSync updates only the requested MySQL key range."""
        assertions.assert_resync_tables_success(
            self.tap_id,
            self.target_id,
            tables=f'{self.source_db}.iceberg_events',
            expected_state_streams={
                'fastsync': {f'{self.source_db}-iceberg_events': True}
            },
        )
        self._assert_managed_v3('iceberg_events')
        self.assert_iceberg_fastsync_cleanup(
            self.target_schema,
            self.initial_s3_keys,
        )
        sentinel_before = self.e2e_env.run_query_target_snowflake(
            'SELECT "BODY" '
            f'FROM "{self.target_schema}"."ICEBERG_EVENTS" WHERE "ID" = 1'
        )
        self.assertEqual(sentinel_before, [('first',)])

        self.e2e_env.run_query_tap_oracle_mysql(
            "UPDATE iceberg_events SET body = 'source-outside-range' WHERE id = 1"
        )
        self.e2e_env.run_query_tap_oracle_mysql(
            "UPDATE iceberg_events SET body = 'partial-updated', "
            "payload = JSON_OBJECT('partial', TRUE) WHERE id = 2"
        )
        self.e2e_env.run_query_tap_oracle_mysql(
            'INSERT INTO iceberg_events '
            '(id, payload, body, binary_value, unsigned_value, updated_at) '
            "VALUES (4, JSON_OBJECT('partial', 'insert'), 'partial-insert', "
            "UNHEX('1020'), 18446744073709551615, "
            "'2026-08-19 10:00:03.000004')"
        )

        assertions.assert_partial_sync_table_success(
            {
                'env': self.e2e_env,
                'tap': self.tap_id,
                'tap_type': 'oracle_mysql',
                'target': self.target_id,
                'source_db': self.source_db,
                'table': 'iceberg_events',
                'column': 'id',
            },
            start_value=2,
            end_value=4,
        )

        rows = self._target_event_rows()
        self.assertEqual([row[0] for row in rows], [1, 2, 3, 4])
        self.assertEqual(json.loads(rows[1][1]), {'partial': True})
        self.assertEqual(rows[1][2], 'partial-updated')
        self.assertEqual(json.loads(rows[-1][1]), {'partial': 'insert'})
        self.assertEqual(
            rows[-1][2:],
            ('partial-insert', '1020', 18446744073709551615),
        )
        self.assertEqual(rows[0][2], 'first')
        self.assert_iceberg_fastsync_cleanup(
            self.target_schema,
            self.initial_s3_keys,
        )
