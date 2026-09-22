"""Unit tests for bucketed keyset SQL generation and the read-retry policy."""

import psycopg2
import pytest

from tap_yugabyte import keyset
from tap_yugabyte.retry import PermanentSourceError, is_permanent, retry_read


class TestBucketExpression:
    def test_modulo_is_literal_without_parameters(self):
        # the index DDL and the max-key probe run with no parameter sequence, so
        # psycopg2 never interpolates and the operator must stay single
        assert keyset.bucket_expr(['id'], 3) == '(yb_hash_code("id") % 3)'

    def test_modulo_is_doubled_for_parameterised_statements(self):
        # a single % here is consumed as a placeholder and raises IndexError at
        # execute() time, long after the SQL looks correct in a log line
        assert keyset.bucket_expr(['id'], 3, escape_percent=True) == \
            '(yb_hash_code("id") %% 3)'

    def test_composite_key_hashes_the_whole_tuple(self):
        assert keyset.bucket_expr(['tenant', 'id'], 8) == \
            '(yb_hash_code("tenant", "id") % 8)'


class TestOrdering:
    def test_order_by_lists_columns_individually(self):
        # a ROW() expression is opaque to the planner and forces a blocking sort
        # even when the index could have supplied the order
        assert keyset.order_by_sql(['tenant', 'id']) == '"tenant" ASC, "id" ASC'

    def test_tuple_sql_is_a_row_constructor(self):
        # correct and index-usable in WHERE, unlike in ORDER BY
        assert keyset.tuple_sql(['tenant', 'id']) == '("tenant", "id")'


class TestIndexDefinition:
    def test_ddl_hash_shards_the_bucket_and_range_orders_the_key(self):
        ddl = keyset.index_ddl('s.orders', 'orders', ['tenant', 'id'], 8, tablets=8)
        assert 'CREATE UNIQUE INDEX orders_pw_keyset ON s.orders' in ddl
        assert '((yb_hash_code("tenant", "id") % 8)) HASH' in ddl
        assert '"tenant" ASC, "id" ASC' in ddl
        assert ddl.endswith('SPLIT INTO 8 TABLETS')

    @pytest.mark.parametrize('indexdef,expected', [
        ('CREATE UNIQUE INDEX t_pw_keyset ON s.t USING lsm '
         '(((yb_hash_code(id) % 3)) HASH, id ASC)', 3),
        ('CREATE UNIQUE INDEX t_pw_keyset ON s.t USING lsm '
         '(((yb_hash_code(a, b) % 16)) HASH, a ASC, b ASC)', 16),
        ('CREATE UNIQUE INDEX other ON s.t USING lsm (id HASH)', None),
        ('', None),
        (None, None),
    ])
    def test_bucket_count_round_trips_from_a_live_definition(self, indexdef, expected):
        assert keyset.parse_index_buckets(indexdef) == expected


class TestReplicationKeyWarnings:
    def test_sequence_key_is_flagged_for_per_connection_caching(self):
        candidate = {'column': 'id', 'kind': 'sequence', 'unique': True,
                     'not_null': True, 'tiebreaker_required': False}
        warnings = keyset.replication_key_warnings(candidate, ['id'],
                                                   sequence_cache_minval=100)
        assert len(warnings) == 1
        assert 'commit order does not follow id order' in warnings[0]

    def test_created_timestamp_is_flagged_for_tiebreaker_and_transaction_time(self):
        candidate = {'column': 'created_at', 'kind': 'created_timestamp',
                     'unique': False, 'not_null': True, 'tiebreaker_required': True}
        warnings = keyset.replication_key_warnings(candidate, ['order_id'])
        assert any('tiebreaker (order_id)' in w for w in warnings)
        assert any('transaction start time' in w for w in warnings)

    def test_nullable_key_is_flagged_as_permanently_resyncing(self):
        candidate = {'column': 'updated_at', 'kind': 'created_timestamp',
                     'unique': False, 'not_null': False, 'tiebreaker_required': True}
        warnings = keyset.replication_key_warnings(candidate, ['id'])
        assert any('re-sync on every run' in w for w in warnings)


class SqlStateError(psycopg2.Error):
    """A psycopg2 error carrying a chosen SQLSTATE.

    psycopg2.Error.pgcode is a read-only C member, so a test cannot assign one;
    a subclass overriding it with a property is the way to stand one up.
    """

    def __init__(self, sqlstate, message='boom'):
        super().__init__(message)
        self._sqlstate = sqlstate

    @property
    def pgcode(self):
        return self._sqlstate


class TestRetryPolicy:
    @staticmethod
    def _error(sqlstate):
        return SqlStateError(sqlstate)

    @pytest.mark.parametrize('sqlstate', ['42501', '42P01', '42703', '25001', '28P01'])
    def test_configuration_errors_are_permanent(self, sqlstate):
        assert is_permanent(self._error(sqlstate))

    @pytest.mark.parametrize('sqlstate', ['40001', '57014', '08006', 'XX000', '53300'])
    def test_distributed_transients_are_retried(self, sqlstate):
        assert not is_permanent(self._error(sqlstate))

    def test_error_without_a_sqlstate_is_treated_as_transient(self):
        # it never reached the server's classifier -- a dropped socket, a DNS
        # failure -- which is exactly the case worth retrying
        assert not is_permanent(psycopg2.OperationalError('connection lost'))

    def test_read_restart_is_retried_until_it_succeeds(self):
        attempts = []

        def flaky():
            attempts.append(len(attempts))
            if len(attempts) < 3:
                raise self._error('40001')
            return 'done'

        assert retry_read(flaky, 'scan', initial_backoff=0,
                          max_backoff=0) == 'done'
        assert len(attempts) == 3

    def test_permanent_error_fails_immediately_without_retrying(self):
        attempts = []

        def denied():
            attempts.append(len(attempts))
            raise self._error('42501')

        with pytest.raises(PermanentSourceError, match='42501'):
            retry_read(denied, 'pin snapshot', initial_backoff=0, max_backoff=0)
        assert len(attempts) == 1

    def test_on_retry_hook_runs_between_attempts(self):
        rebuilt = []

        def flaky():
            if not rebuilt:
                raise self._error('40001')
            return 'ok'

        retry_read(flaky, 'scan', initial_backoff=0, max_backoff=0,
                   on_retry=lambda attempt, exc: rebuilt.append(attempt))
        assert rebuilt == [1]

    def test_gives_up_after_max_attempts_and_reraises_the_last_error(self):
        def always_fails():
            raise self._error('40001')

        with pytest.raises(psycopg2.Error):
            retry_read(always_fails, 'scan', max_attempts=3, initial_backoff=0,
                       max_backoff=0)


class TestMergeScan:
    def test_in_list_covers_every_bucket(self):
        assert keyset.bucket_in_sql(['id'], 3) == \
            '(yb_hash_code("id") % 3) IN (0, 1, 2)'

    def test_in_list_escapes_the_modulo_for_parameterised_statements(self):
        assert keyset.bucket_in_sql(['id'], 3, escape_percent=True) == \
            '(yb_hash_code("id") %% 3) IN (0, 1, 2)'

    def test_guc_covers_the_bucket_count(self):
        # below the bucket count the planner cannot merge every stream and falls
        # back to sorting the whole result
        assert keyset.merge_scan_guc_sql(16) == 'SET yb_max_merge_scan_streams = 16'

    def test_guc_has_a_floor_for_small_bucket_counts(self):
        assert keyset.merge_scan_guc_sql(3) == 'SET yb_max_merge_scan_streams = 8'


class TestLeadingColumnOrdering:
    """Whether a column leads an index in sorted order decides if it can bound a range."""

    @pytest.mark.parametrize('indexdef,column,expected', [
        # range-sharded primary key: the column leads it, sorted
        ('CREATE UNIQUE INDEX t_pkey ON s.t USING lsm (id ASC)', 'id', True),
        ('CREATE UNIQUE INDEX t_pkey ON s.t USING lsm (id DESC)', 'id', True),
        ('CREATE UNIQUE INDEX i ON s.t USING lsm (created_at ASC, id ASC)',
         'created_at', True),
        # hash-sharded: ordered by hash, which says nothing about the column's order
        ('CREATE UNIQUE INDEX t_pkey ON s.t USING lsm (id HASH)', 'id', False),
        ('CREATE UNIQUE INDEX t_pkey ON s.t USING lsm (tenant HASH, id ASC)',
         'tenant', False),
        # the column is present but does not lead
        ('CREATE UNIQUE INDEX i ON s.t USING lsm (tenant ASC, id ASC)', 'id', False),
        # a bucket index leads with the discriminator, never with the key
        ('CREATE UNIQUE INDEX t_pw_keyset ON s.t USING lsm '
         '(((yb_hash_code(id) % 3)) HASH, id ASC)', 'id', False),
    ])
    def test_leading_column_detection(self, indexdef, column, expected):
        assert keyset._leading_column_is_ordered(indexdef, column) is expected


class TestStrategyNames:
    def test_strategies_are_distinct(self):
        names = {keyset.STRATEGY_PK_RANGE, keyset.STRATEGY_BUCKET_INDEX,
                 keyset.STRATEGY_PLAIN_SCAN}
        assert len(names) == 3
