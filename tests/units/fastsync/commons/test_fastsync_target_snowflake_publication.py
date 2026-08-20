import json
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from tests.units.fastsync.commons.test_fastsync_target_snowflake import (
    FastSyncTargetSnowflakeMock,
)


class TestFastSyncTargetSnowflakePartialSync(TestCase):
    """Unit tests for atomic PartialSync publication."""

    def setUp(self) -> None:
        self.snowflake = FastSyncTargetSnowflakeMock(
            connection_config={'s3_bucket': 'dummy_bucket', 'stage': 'dummy_stage'},
            transformation_config={},
        )

    @patch('pipelinewise.fastsync.commons.target_snowflake.pem2der', return_value=b'private-key')
    @patch('pipelinewise.fastsync.commons.target_snowflake.snowflake.connector.connect')
    def test_open_connection_disables_autocommit_for_transactions(self, mocked_connect, _mocked_pem2der):
        """The transaction connection must disable Snowflake autocommit."""
        self.snowflake.connection_config.update({
            'user': 'test_user',
            'private_key': 'private-key.pem',
            'account': 'test_account',
            'dbname': 'test_database',
            'warehouse': 'test_warehouse',
            'role': 'test_role',
            'tap_id': 'test_tap',
        })

        self.snowflake.open_connection(
            {'schema': 'test_schema', 'table': 'test_table'}, autocommit=False
        )

        mocked_connect.assert_called_once_with(
            user='test_user',
            private_key=b'private-key',
            account='test_account',
            database='test_database',
            warehouse='test_warehouse',
            role='test_role',
            authenticator='SNOWFLAKE_JWT',
            autocommit=False,
            session_parameters={
                'QUOTED_IDENTIFIERS_IGNORE_CASE': 'FALSE',
                'QUERY_TAG': json.dumps({
                    'ppw_component': 'fastsync',
                    'tap_id': 'test_tap',
                    'database': 'test_database',
                    'schema': 'test_schema',
                    'table': 'test_table',
                }),
            },
        )

    def test_partial_hard_delete(self):
        """Hard delete is limited to the selected, marked range."""
        self.snowflake.partial_hard_delete(
            'test_schema', 'target_table', ' WHERE updated_at >= \'2024-01-01\''
        )

        self.assertListEqual(self.snowflake.executed_queries, [
            'DELETE FROM test_schema."TARGET_TABLE" WHERE updated_at >= \'2024-01-01\''
            ' AND _SDC_DELETED_AT IS NOT NULL'
        ])

    def test_publish_partial_sync_executes_one_atomic_transaction(self):
        """Marker, merge, and hard delete share one transaction."""
        self.snowflake.execute_transaction = MagicMock()
        self.snowflake.publish_partial_sync(
            'test_schema',
            'source_table',
            'target_table',
            ['id', 'value'],
            ['id'],
            ' WHERE updated_at >= \'2024-01-01\'',
            hard_delete=True,
        )

        self.snowflake.execute_transaction.assert_called_once_with(
            [
                'UPDATE test_schema."TARGET_TABLE" SET _SDC_DELETED_AT = CURRENT_TIMESTAMP()'
                ' WHERE updated_at >= \'2024-01-01\' AND _SDC_DELETED_AT IS NULL',
                'MERGE INTO test_schema."TARGET_TABLE" USING test_schema."SOURCE_TABLE"'
                ' ON "SOURCE_TABLE".ID = "TARGET_TABLE".ID'
                ' WHEN MATCHED THEN UPDATE SET "TARGET_TABLE".ID = "SOURCE_TABLE".ID, '
                '"TARGET_TABLE".VALUE = "SOURCE_TABLE".VALUE'
                ' WHEN NOT MATCHED THEN INSERT (ID, VALUE)'
                ' VALUES ("SOURCE_TABLE".ID, "SOURCE_TABLE".VALUE)',
                'DELETE FROM test_schema."TARGET_TABLE" WHERE updated_at >= \'2024-01-01\''
                ' AND _SDC_DELETED_AT IS NOT NULL',
            ],
            query_tag_props={'schema': 'test_schema', 'table': 'target_table'},
        )

    def test_publish_partial_sync_preserves_soft_deleted_rows(self):
        """Soft-delete publication does not issue a physical delete."""
        self.snowflake.execute_transaction = MagicMock()
        self.snowflake.publish_partial_sync(
            'test_schema', 'source_table', 'target_table', ['id'], ['id'],
            ' WHERE updated_at >= \'2024-01-01\'', hard_delete=False,
        )

        transaction_queries = self.snowflake.execute_transaction.call_args.args[0]
        self.assertEqual(len(transaction_queries), 2)
        self.assertFalse(any(query.startswith('DELETE') for query in transaction_queries))

    def test_execute_transaction_commits_all_queries(self):
        """A successful transaction commits exactly once."""
        connection = MagicMock()
        cursor = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        self.snowflake.open_connection = MagicMock(return_value=connection)

        self.snowflake.execute_transaction(['UPDATE one', 'MERGE two'])

        self.snowflake.open_connection.assert_called_once_with(None, autocommit=False)
        self.assertListEqual(cursor.method_calls, [
            call.execute('UPDATE one'),
            call.execute('MERGE two'),
        ])
        connection.commit.assert_called_once_with()
        connection.rollback.assert_not_called()
        connection.close.assert_called_once_with()

    def test_execute_transaction_rolls_back_publication_failures(self):
        """Every publication-statement failure rolls back the transaction."""
        queries = ['UPDATE marker', 'MERGE rows', 'DELETE stale']
        for failed_query_index, error_message in (
            (0, 'marker update failed'),
            (1, 'merge failed'),
            (2, 'delete failed'),
        ):
            with self.subTest(error_message=error_message):
                connection = MagicMock()
                cursor = MagicMock()
                cursor.execute.side_effect = [
                    *([None] * failed_query_index),
                    RuntimeError(error_message),
                ]
                connection.cursor.return_value.__enter__.return_value = cursor
                self.snowflake.open_connection = MagicMock(return_value=connection)

                with self.assertRaisesRegex(RuntimeError, error_message):
                    self.snowflake.execute_transaction(queries)

                self.assertListEqual(
                    cursor.method_calls,
                    [call.execute(query) for query in queries[:failed_query_index + 1]],
                )
                connection.rollback.assert_called_once_with()
                connection.commit.assert_not_called()
                connection.close.assert_called_once_with()

    def test_execute_transaction_rolls_back_and_propagates_commit_failure(self):
        """A failed commit is reported after a best-effort rollback."""
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = MagicMock()
        connection.commit.side_effect = RuntimeError('commit failed')
        self.snowflake.open_connection = MagicMock(return_value=connection)

        with self.assertRaisesRegex(RuntimeError, 'commit failed'):
            self.snowflake.execute_transaction(['MERGE rows'])

        connection.commit.assert_called_once_with()
        connection.rollback.assert_called_once_with()
        connection.close.assert_called_once_with()

    def test_execute_transaction_ignores_close_failure_after_commit(self):
        """A close failure cannot make a committed publication look unsuccessful."""
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = MagicMock()
        connection.close.side_effect = RuntimeError('close failed')
        self.snowflake.open_connection = MagicMock(return_value=connection)

        with self.assertLogs(
            'pipelinewise.fastsync.commons.target_snowflake', level='WARNING'
        ) as logs:
            self.snowflake.execute_transaction(['MERGE rows'])

        connection.commit.assert_called_once_with()
        connection.close.assert_called_once_with()
        self.assertIn('Failed to close Snowflake publication connection', logs.output[0])

    def test_execute_transaction_preserves_query_failure_when_cleanup_fails(self):
        """Rollback and close failures do not mask the publication error."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.execute.side_effect = RuntimeError('merge failed')
        connection.cursor.return_value.__enter__.return_value = cursor
        connection.rollback.side_effect = RuntimeError('rollback failed')
        connection.close.side_effect = RuntimeError('close failed')
        self.snowflake.open_connection = MagicMock(return_value=connection)

        with self.assertLogs(
            'pipelinewise.fastsync.commons.target_snowflake', level='WARNING'
        ) as logs:
            with self.assertRaisesRegex(RuntimeError, 'merge failed'):
                self.snowflake.execute_transaction(['MERGE rows'])

        connection.rollback.assert_called_once_with()
        connection.close.assert_called_once_with()
        self.assertEqual(len(logs.output), 2)
        self.assertIn('Failed to roll back Snowflake publication transaction', logs.output[0])
        self.assertIn('Failed to close Snowflake publication connection', logs.output[1])


class TestFastSyncTargetSnowflakePublication(TestCase):
    """Failure-boundary and staging-cleanup tests for publication."""

    def setUp(self) -> None:
        self.snowflake = FastSyncTargetSnowflakeMock(
            connection_config={'s3_bucket': 'dummy_bucket', 'stage': 'dummy_stage'},
            transformation_config={},
        )

    def test_swap_can_defer_cleanup_to_post_publication_finalizer(self):
        """FullSync can separate committed cutover from cleanup and grants."""
        self.snowflake.query = MagicMock()

        self.snowflake.swap_tables(
            schema='test_schema',
            table_name='test_table',
            cleanup_old_table=False,
        )

        self.snowflake.query.assert_called_once_with(
            'ALTER TABLE test_schema."TEST_TABLE_TEMP" '
            'SWAP WITH test_schema."TEST_TABLE"',
            query_tag_props={'schema': 'test_schema', 'table': 'test_table'},
        )

    def test_cleanup_is_retried_without_repeating_the_committed_swap(self):
        """A transient DROP failure retries cleanup but never the committed swap."""
        self.snowflake.query = MagicMock(
            side_effect=[None, RuntimeError('cleanup failed'), None]
        )

        with self.assertLogs(
            'pipelinewise.fastsync.commons.target_snowflake', level='WARNING'
        ) as logs:
            self.snowflake.swap_tables(schema='test_schema', table_name='test_table')

        self.assertEqual(
            self.snowflake.query.call_args_list,
            [
                call(
                    'ALTER TABLE test_schema."TEST_TABLE_TEMP" '
                    'SWAP WITH test_schema."TEST_TABLE"',
                    query_tag_props={'schema': 'test_schema', 'table': 'test_table'},
                ),
                call(
                    'DROP TABLE IF EXISTS test_schema."TEST_TABLE_TEMP"',
                    query_tag_props={'schema': 'test_schema', 'table': 'test_table'},
                ),
                call(
                    'DROP TABLE IF EXISTS test_schema."TEST_TABLE_TEMP"',
                    query_tag_props={'schema': 'test_schema', 'table': 'test_table'},
                ),
            ],
        )
        self.assertIn('cleanup failed', logs.output[0])

    def test_cleanup_exhaustion_is_propagated_after_one_committed_swap(self):
        """State must be withheld when the old table cannot be removed."""
        cleanup_error = RuntimeError('cleanup failed')
        self.snowflake.query = MagicMock(
            side_effect=[None, cleanup_error, cleanup_error, cleanup_error]
        )

        with self.assertRaisesRegex(RuntimeError, 'cleanup failed'):
            self.snowflake.swap_tables(schema='test_schema', table_name='test_table')

        self.assertEqual(
            self.snowflake.query.call_args_list[0],
            call(
                'ALTER TABLE test_schema."TEST_TABLE_TEMP" '
                'SWAP WITH test_schema."TEST_TABLE"',
                query_tag_props={'schema': 'test_schema', 'table': 'test_table'},
            ),
        )
        self.assertEqual(
            self.snowflake.query.call_args_list[1:],
            [
                call(
                    'DROP TABLE IF EXISTS test_schema."TEST_TABLE_TEMP"',
                    query_tag_props={'schema': 'test_schema', 'table': 'test_table'},
                )
            ] * 3,
        )

    def test_publication_failure_is_propagated(self):
        """The staging DROP is non-fatal only after the table swap succeeds."""
        self.snowflake.query = MagicMock(side_effect=RuntimeError('swap failed'))

        with self.assertRaisesRegex(RuntimeError, 'swap failed'):
            self.snowflake.swap_tables(schema='test_schema', table_name='test_table')

        self.snowflake.query.assert_called_once_with(
            'ALTER TABLE test_schema."TEST_TABLE_TEMP" SWAP WITH test_schema."TEST_TABLE"',
            query_tag_props={'schema': 'test_schema', 'table': 'test_table'},
        )

    @patch('pipelinewise.fastsync.commons.target_snowflake.os.remove')
    @patch('pipelinewise.fastsync.commons.target_snowflake.SnowflakeEncryptionUtil.encrypt_file')
    def test_encrypted_upload_failure_removes_temporary_file(self, encrypt_file, remove):
        """An S3 failure removes the encrypted staging copy."""
        metadata = MagicMock(key='encrypted-key', iv='initial-vector')
        encrypt_file.return_value = (metadata, '/tmp/encrypted-part')
        self.snowflake.connection_config['client_side_encryption_master_key'] = 'master-key'
        self.snowflake.s3 = MagicMock()
        self.snowflake.s3.upload_file.side_effect = RuntimeError('upload failed')

        with self.assertRaisesRegex(RuntimeError, 'upload failed'):
            self.snowflake.upload_to_s3('/tmp/plain-part', tmp_dir='/tmp')

        remove.assert_called_once_with('/tmp/encrypted-part')

    @patch(
        'pipelinewise.fastsync.commons.target_snowflake.os.remove',
        side_effect=OSError('cleanup failed'),
    )
    @patch('pipelinewise.fastsync.commons.target_snowflake.SnowflakeEncryptionUtil.encrypt_file')
    def test_encrypted_cleanup_failure_returns_uploaded_key(self, encrypt_file, remove):
        """Local cleanup cannot hide a successfully uploaded key."""
        metadata = MagicMock(key='encrypted-key', iv='initial-vector')
        encrypt_file.return_value = (metadata, '/tmp/encrypted-part')
        self.snowflake.connection_config['client_side_encryption_master_key'] = 'master-key'
        self.snowflake.s3 = MagicMock()

        with self.assertLogs(
            'pipelinewise.fastsync.commons.target_snowflake', level='WARNING'
        ) as logs:
            s3_key = self.snowflake.upload_to_s3('/tmp/plain-part', tmp_dir='/tmp')

        self.assertEqual(s3_key, 'plain-part')
        remove.assert_called_once_with('/tmp/encrypted-part')
        self.assertIn('Failed to remove encrypted staging file', logs.output[0])

    def test_create_swap_target_does_not_mutate_existing_primary_keys(self):
        """A FullSync swap placeholder does not alter the live table."""
        self.snowflake.create_table(
            target_schema='test_schema',
            table_name='test_table',
            columns=['"ID" INTEGER', '"TXT" VARCHAR'],
            primary_key=['"ID"'],
            allow_replace_table=False,
            normalize_primary_keys=False,
        )

        self.assertListEqual(self.snowflake.executed_queries, [
            'CREATE TABLE IF NOT EXISTS "TEST_SCHEMA"."TEST_TABLE" ('
            '"ID" INTEGER,"TXT" VARCHAR,'
            '_SDC_EXTRACTED_AT TIMESTAMP_NTZ,'
            '_SDC_BATCHED_AT TIMESTAMP_NTZ,'
            '_SDC_DELETED_AT VARCHAR'
            ', PRIMARY KEY ("ID"))'
        ])
