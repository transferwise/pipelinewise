from tests.end_to_end.target_snowflake import TargetSnowflake
from tests.end_to_end.helpers import assertions


MYSQL_FASTSYNC_TABLES = {
    'iceberg_events': True,
    'iceberg_incremental': True,
    'iceberg_full_reload': False,
    'multiline_values': True,
    'replication_audit': True,
}


def mysql_initial_state_expectations(source_db='mysql8_source_db'):
    """Return exact first-run stream bookmarks for genuine MySQL."""
    return {
        'fastsync': {
            f'{source_db}-{table}': requires_progress
            for table, requires_progress in MYSQL_FASTSYNC_TABLES.items()
        }
    }


def mysql_recurring_state_expectations(source_db='mysql8_source_db'):
    """Return recurring FullSync streams for genuine MySQL."""
    return {
        'fastsync': {
            f'{source_db}-{table}': False
            for table, requires_progress in MYSQL_FASTSYNC_TABLES.items()
            if not requires_progress
        }
    }


def exercise_mysql_replication_audit(test_case, managed_iceberg):
    """Verify key identity and exact UTF-8 across FastSync and Singer handover."""
    query_source = test_case.e2e_env.run_query_tap_oracle_mysql
    target_table = f'"{test_case.target_schema}"."REPLICATION_AUDIT"'
    expected_state = mysql_initial_state_expectations(test_case.source_db) if managed_iceberg else {
        'fastsync': {
            f'{test_case.source_db}-replication_audit': True,
            f'{test_case.source_db}-multiline_values': True,
        },
    }

    def target_rows():
        return test_case.e2e_env.run_query_target_snowflake(
            f'SELECT "KEY_LEFT", "KEY_RIGHT", "ROW_ID", HEX_ENCODE("VALUE_TEXT") '
            f'FROM {target_table} ORDER BY "ROW_ID"'
        )

    def assert_source_matches_target():
        source_rows = list(query_source(
            'SELECT key_left, key_right, row_id, HEX(value_text) FROM replication_audit ORDER BY row_id'
        ))
        test_case.assertTrue(source_rows)
        test_case.assertEqual(target_rows(), source_rows)

    def run_singer():
        assertions.assert_run_tap_success(
            test_case.tap_id, test_case.target_id,
            ['fastsync', 'singer'] if managed_iceberg else ['singer'],
            expected_state_streams=mysql_recurring_state_expectations(test_case.source_db) if managed_iceberg else None,
        )
        assert_source_matches_target()

    assertions.assert_resync_tables_success(
        test_case.tap_id, test_case.target_id, expected_state_streams=expected_state,
    )
    assert_source_matches_target()
    test_case.assertEqual(
        [bytes.fromhex(row[-1]).decode('utf-8') for row in target_rows()],
        ['FullSync 🚀', 'FullSync 🌍'],
    )
    table_format = test_case.e2e_env.run_query_target_snowflake(
        'SELECT IS_ICEBERG FROM INFORMATION_SCHEMA.TABLES '
        f"WHERE TABLE_SCHEMA = '{test_case.target_schema}' AND TABLE_NAME = 'REPLICATION_AUDIT'"
    )
    test_case.assertEqual(table_format, [('YES' if managed_iceberg else 'NO',)])

    query_source('UPDATE replication_audit SET value_text = %s WHERE row_id = 1', ('source-only 🌏',))
    query_source('UPDATE replication_audit SET value_text = %s WHERE row_id = 2', ('PartialSync 🧪',))
    assertions.assert_partial_sync_table_success(
        {
            'env': test_case.e2e_env, 'tap': test_case.tap_id, 'target': test_case.target_id,
            'tap_type': 'oracle_mysql', 'source_db': test_case.source_db,
            'table': 'replication_audit', 'column': 'row_id',
        },
        start_value=2, end_value=2,
    )
    test_case.assertEqual(
        [bytes.fromhex(row[-1]).decode('utf-8') for row in target_rows()],
        ['FullSync 🚀', 'PartialSync 🧪'],
    )

    # These distinct composite keys used to collapse to the same target buffer key.
    query_source(
        'INSERT INTO replication_audit VALUES (%s, %s, %s, %s), (%s, %s, %s, %s), (%s, %s, %s, %s)',
        ('a,b', 'c', 10, 'Singer 🍀', 'a', 'b,c', 11, 'Singer 🌈', '', '', 12, 'empty 🔑'),
    )
    query_source('UPDATE replication_audit SET value_text = %s WHERE row_id = 12', ('empty updated 🔐',))
    query_source('UPDATE replication_audit SET key_left = %s WHERE row_id = 1', ('renamed 🚀',))
    run_singer()
    test_case.assertEqual([row[2] for row in target_rows()], [1, 2, 10, 11, 12])

    query_source('UPDATE replication_audit SET value_text = %s WHERE row_id = 10', ('updated 🍀',))
    query_source('DELETE FROM replication_audit WHERE row_id = 11')
    query_source('UPDATE replication_audit SET key_right = %s WHERE row_id = 2', ('renamed 🌍',))
    run_singer()
    test_case.assertEqual([row[2] for row in target_rows()], [1, 2, 10, 12])


class TapMySQL(TargetSnowflake):
    """Base class for genuine MySQL-to-Snowflake E2E tests."""

    def setUp(self, tap_id: str, target_id: str):
        super().setUp(
            tap_id=tap_id,
            target_id=target_id,
            tap_type='TAP_ORACLE_MYSQL',
        )

    def prepare_source(self):
        """Reset genuine MySQL before validate/import discovers its schema."""
        self.e2e_env.setup_tap_oracle_mysql()
