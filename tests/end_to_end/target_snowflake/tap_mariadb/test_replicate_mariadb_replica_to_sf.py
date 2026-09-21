import os
import time

import pymysql

from pipelinewise.fastsync import mysql_to_snowflake
from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_mariadb import (
    TapMariaDB,
    mariadb_initial_state_expectations,
)

TAP_ID = 'mariadb_replica_to_sf'
TARGET_ID = 'snowflake'


class TestReplicateMariaDBReplicaToSF(TapMariaDB):
    """
    Test Replicate data from MariaDB to Snowflake
    """

    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)

    def test_replicate_mariadb_replica_to_sf(self):
        """
        Test Replicate data from MariaDB to Snowflake
        """

        with pymysql.connect(
            host=os.environ['TAP_MYSQL_REPLICA_HOST'],
            port=int(os.environ['TAP_MYSQL_REPLICA_PORT']),
            user=os.environ['TAP_MYSQL_REPLICA_USER'],
            password=os.environ['TAP_MYSQL_REPLICA_PASSWORD'],
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        ) as replica:
            with replica.cursor() as cursor:
                self._wait_for_replica_snapshot(cursor)
                cursor.execute('STOP SLAVE SQL_THREAD')
                try:
                    self.e2e_env.run_query_tap_mysql_2(
                        "INSERT INTO weight_unit (weight_unit_id, weight_unit_name) VALUES (100, 'replica lag')"
                    )
                    self._wait_for_received_change(cursor)
                    assertions.assert_run_tap_success(
                        self.tap_id,
                        self.target_id,
                        ['fastsync', 'singer'],
                        expected_state_streams=mariadb_initial_state_expectations(
                            'mysql_source_db_2'
                        ),
                    )
                finally:
                    cursor.execute('START SLAVE SQL_THREAD')
        assertions.assert_row_counts_equal(
            self.e2e_env.run_query_tap_mysql_2,
            self.e2e_env.run_query_target_snowflake,
            self.e2e_env.sf_schema_postfix,
        )
        assertions.assert_all_columns_exist(
            self.e2e_env.run_query_tap_mysql_2,
            self.e2e_env.run_query_target_snowflake,
            mysql_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )

    def _wait_for_replica_snapshot(self, cursor):
        """Make the initial fixture complete before deliberately pausing apply."""
        status = self.e2e_env.run_query_tap_mysql_2('SHOW MASTER STATUS')[0]
        cursor.execute('SELECT MASTER_POS_WAIT(%s, %s, 30)', status[:2])
        result = next(iter(cursor.fetchone().values()))
        self.assertIsNotNone(result, 'Replica SQL thread is not running')
        self.assertGreaterEqual(result, 0, 'Replica did not catch up to the fixture')

    def _wait_for_received_change(self, cursor):
        """Prove the receiver is ahead while the snapshot still lacks the row."""
        log_file, log_pos = self.e2e_env.run_query_tap_mysql_2('SHOW MASTER STATUS')[0][:2]
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            cursor.execute('SHOW SLAVE STATUS')
            status = cursor.fetchone()
            if (status['Master_Log_File'], status['Read_Master_Log_Pos']) >= (log_file, log_pos):
                self.assertLess(
                    (status['Relay_Master_Log_File'], status['Exec_Master_Log_Pos']),
                    (log_file, log_pos),
                )
                return
            time.sleep(0.1)
        self.fail('Replica receiver did not read the unapplied source change')
