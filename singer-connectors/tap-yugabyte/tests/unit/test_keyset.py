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


class TestKeysetPredicate:
    """The resume comparison. A row constructor reads as `Index Cond` and then
    rechecks every remaining entry in the bucket -- measured, 9,221 index rows to
    return 5 against 22 for the expanded form -- because the bucket leads the
    index, not the key."""

    def test_single_column_needs_no_expansion(self):
        sql, order = keyset.keyset_predicate(['id'])
        assert sql == '"id" > %s'
        assert keyset.keyset_params([7], order) == [7]

    def test_two_columns_bound_the_leading_one_then_filter(self):
        sql, order = keyset.keyset_predicate(['tenant', 'id'])
        assert sql == '"tenant" >= %s AND ("tenant" > %s OR "id" > %s)'
        assert keyset.keyset_params(['t', 9], order) == ['t', 't', 9]

    def test_three_columns_chain_the_equalities(self):
        sql, order = keyset.keyset_predicate(['a', 'b', 'c'])
        assert sql == ('"a" >= %s AND ("a" > %s OR "b" >= %s) '
                       'AND ("a" > %s OR "b" > %s OR "c" > %s)')
        assert keyset.keyset_params([1, 2, 3], order) == [1, 1, 2, 1, 2, 3]

    def test_upper_bound_mirrors_the_lower_one(self):
        sql, order = keyset.keyset_predicate(['tenant', 'id'], after=False)
        assert sql == '"tenant" <= %s AND ("tenant" < %s OR "id" <= %s)'
        assert keyset.keyset_params(['t', 9], order) == ['t', 't', 9]

    def test_no_row_constructor_anywhere(self):
        for columns in (['id'], ['a', 'b'], ['a', 'b', 'c']):
            sql, _ = keyset.keyset_predicate(columns)
            assert 'ROW(' not in sql
            assert ') >' not in sql


class TestIndexDefinition:
    def test_ddl_hash_shards_the_bucket_and_range_orders_the_key(self):
        ddl = keyset.index_ddl('s.orders', 'orders', ['tenant', 'id'], 8, tablets=8)
        assert 'CREATE UNIQUE INDEX orders_pw_keyset ON s.orders' in ddl
        assert '((yb_hash_code("tenant", "id") % 8)) ASC' in ddl
        assert '"tenant" ASC, "id" ASC' in ddl
        assert ddl.endswith('SPLIT AT VALUES ((1), (2), (3), (4), (5), (6), (7))')

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


class TestTheFourIndexShapes:
    """INDEXES.md defines four indexes. They are one shape: the primary key
    hashed into N buckets ASC, the ordering column, then the primary key, UNIQUE,
    split one bucket per tablet."""

    def test_shape_1_and_2_are_the_same_index(self):
        # shape 2 is shape 1 plus a property of the data, not a second index
        assert (keyset.index_ddl('s.t', 't', ['id'], 3)
                == keyset.index_ddl('s.t', 't', ['id'], 3))
        ddl = keyset.index_ddl('s.t', 't', ['id'], 3)
        assert ddl == ('CREATE UNIQUE INDEX t_pw_keyset ON s.t '
                       '(((yb_hash_code("id") % 3)) ASC, "id" ASC) '
                       'SPLIT AT VALUES ((1), (2))')

    def test_shape_3_hashes_the_primary_key_not_the_timestamp(self):
        # both are constructible and both plan the same; primary keys are
        # distinct by construction, whereas every row written in one transaction
        # shares a timestamp -- measured, a 9,000-row transaction hashed on the
        # timestamp put 9000/0/0 into three buckets, against 2960/2955/3085 when
        # hashed on the key
        ddl = keyset.replication_key_index_ddl('s.t', 't', 'created_at', ['id'], 3)
        assert 'yb_hash_code("id")' in ddl
        assert 'yb_hash_code("created_at")' not in ddl

    def test_shape_3_ends_in_the_primary_key_and_is_unique(self):
        ddl = keyset.replication_key_index_ddl('s.t', 't', 'created_at', ['id'], 3)
        assert ddl == ('CREATE UNIQUE INDEX t_created_at_pw_keyset ON s.t '
                       '(((yb_hash_code("id") % 3)) ASC, "created_at" ASC, "id" ASC) '
                       'SPLIT AT VALUES ((1), (2))')

    def test_shape_4_is_shape_3_with_a_different_column(self):
        created = keyset.replication_key_index_ddl('s.t', 't', 'created_at', ['id'], 3)
        updated = keyset.replication_key_index_ddl('s.t', 't', 'updated_at', ['id'], 3)
        assert created.replace('created_at', 'X') == updated.replace('updated_at', 'X')

    def test_a_composite_primary_key_trails_in_key_order(self):
        ddl = keyset.replication_key_index_ddl('s.t', 't', 'updated_at',
                                               ['tenant', 'id'], 3)
        assert '"updated_at" ASC, "tenant" ASC, "id" ASC' in ddl

    def test_every_shape_splits_one_bucket_per_tablet(self):
        for ddl in (keyset.index_ddl('s.t', 't', ['id'], 4),
                    keyset.replication_key_index_ddl('s.t', 't', 'created_at',
                                                     ['id'], 4)):
            assert ddl.endswith('SPLIT AT VALUES ((1), (2), (3))')

    def test_replication_key_that_is_the_primary_key_reuses_shape_1(self):
        # a hint naming an index nobody created is silently dropped, and the scan
        # falls back to a sequential scan and an external sort
        assert keyset.index_for_replication_key('t', 'id', ['id']) == 't_pw_keyset'
        assert keyset.replication_key_hint('t', 'id', ['id']) \
            == '/*+ IndexScan(t t_pw_keyset) */'

    def test_replication_key_that_is_not_the_primary_key_gets_its_own(self):
        assert (keyset.index_for_replication_key('t', 'created_at', ['id'])
                == 't_created_at_pw_keyset')
        assert (keyset.index_for_replication_key('t', 'updated_at', ['tenant', 'id'])
                == 't_updated_at_pw_keyset')


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

    def test_settings_cover_the_bucket_count(self):
        # below the bucket count the planner cannot merge every stream and falls
        # back to sorting the whole result
        assert keyset.scan_settings_sql(16)[0] == 'SET yb_max_merge_scan_streams = 16'

    def test_settings_have_a_floor_for_small_bucket_counts(self):
        assert keyset.scan_settings_sql(3)[0] == 'SET yb_max_merge_scan_streams = 8'

    def test_sequential_scan_is_deprioritised(self):
        # the index hint alone does not get the streaming plan; this does
        assert 'SET enable_seqscan = off' in keyset.scan_settings_sql(3)


class TestLeadingColumnOrdering:
    """Whether a column leads an index in sorted order decides if it can bound a
    range. Read from pg_index.indoption, not from pg_get_indexdef's text.

    Every value below was observed on YugabyteDB 2026.1.1.1 by creating the index
    and reading the catalog back."""

    @pytest.mark.parametrize('indoption,expected,shape', [
        (0, True,  'a ASC'),
        (3, True,  'b DESC -- DESC sets NULLS FIRST too, neither is the hash bit'),
        (2, True,  'a ASC NULLS FIRST'),
        (4, False, 'c HASH'),
    ])
    def test_leading_column_detection(self, indoption, expected, shape):
        assert keyset._column_is_ordered(indoption) is expected, shape

    def test_only_the_hash_bit_matters(self):
        # a composite hash index reads `4 4 0 3` for ((a,b) HASH, c ASC, d DESC)
        assert [keyset._column_is_ordered(o) for o in (4, 4, 0, 3)] == \
            [False, False, True, True]


class TestStrategyNames:
    def test_strategies_are_distinct(self):
        names = {keyset.STRATEGY_PK_RANGE, keyset.STRATEGY_BUCKET_INDEX,
                 keyset.STRATEGY_PLAIN_SCAN}
        assert len(names) == 3


class FakeCursor:
    """Returns queued rows, so the key checks can be exercised without a server."""

    def __init__(self, rows):
        self._rows = list(rows)
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def fetchall(self):
        """Next queued row is a list of rows; absent or None means no rows."""
        nxt = self._rows.pop(0) if self._rows else None
        return nxt or []


class TestRequireMonotonicKey:
    """(not_null, is_unique, owned_sequence, data_type, is_identity, column_default)
    then (seqcache,)"""

    def test_unique_not_null_sequence_key_is_usable(self):
        cur = FakeCursor([(True, True, 's.orders_id_seq', 'bigint', False, None), (1,)])
        v = keyset.require_monotonic_key(cur, 's', 'orders', 'id', ['id'])
        assert v['usable'] is True
        assert v['tiebreaker'] is None
        assert v['risks'] == []          # cache of 1 keeps commit order with key order

    def test_cached_sequence_is_flagged_with_its_real_block_size(self):
        cur = FakeCursor([(True, True, 's.orders_id_seq', 'bigint', False, None), (100,)])
        v = keyset.require_monotonic_key(cur, 's', 'orders', 'id', ['id'])
        assert v['usable'] is True       # usable, but not safe on its own
        assert 'caches 100 values per connection' in v['risks'][0]

    def test_nullable_key_is_a_hard_failure(self):
        cur = FakeCursor([(False, True, None, 'timestamptz', False, None), []])
        v = keyset.require_monotonic_key(cur, 's', 'orders', 'updated_at', ['id'])
        assert v['usable'] is False
        assert 're-sync on every run' in v['hard_failures'][0]

    def test_non_unique_key_borrows_the_primary_key_as_tiebreaker(self):
        cur = FakeCursor([(True, False, None, 'timestamptz', False, None)])
        v = keyset.require_monotonic_key(cur, 's', 'orders', 'created_at', ['id'])
        assert v['usable'] is True
        assert v['tiebreaker'] == ['id']

    def test_non_unique_key_with_no_primary_key_cannot_be_made_deterministic(self):
        cur = FakeCursor([(True, False, None, 'timestamptz', False, None)])
        v = keyset.require_monotonic_key(cur, 's', 'orders', 'created_at', [])
        assert v['usable'] is False
        assert 'no primary key to break' in v['hard_failures'][0]

    def test_timestamp_key_is_flagged_for_transaction_start_time(self):
        cur = FakeCursor([(True, True, None, 'timestamptz', False, None)])
        v = keyset.require_monotonic_key(cur, 's', 'orders', 'created_at', ['id'])
        assert 'transaction start time' in v['risks'][0]

    def test_key_with_no_increasing_guarantee_at_all_is_flagged(self):
        cur = FakeCursor([(True, True, None, 'numeric', False, None)])
        v = keyset.require_monotonic_key(cur, 's', 'orders', 'amount', ['id'])
        assert 'nothing\n' not in v['risks'][0]
        assert 'guarantees it increases' in v['risks'][0]

    def test_missing_column_is_reported_not_crashed(self):
        v = keyset.require_monotonic_key(FakeCursor([]), 's', 'orders', 'nope', ['id'])
        assert v['usable'] is False
        assert 'does not exist' in v['hard_failures'][0]


class TestSequenceFromDefault:
    """A sequence the column does not own is still driving the column."""

    @pytest.mark.parametrize('default,expected', [
        ("nextval('pwtest.loose_seq'::regclass)", 'pwtest.loose_seq'),
        ("nextval('trap_a_bigserial_seq'::regclass)", 'trap_a_bigserial_seq'),
        ("nextval('s.q'::regclass)", 's.q'),
        ('now()', None),
        ("'x'::text", None),
        (None, None),
        ('', None),
    ])
    def test_sequence_name_recovered_from_a_plain_default(self, default, expected):
        assert keyset._sequence_from_default(default) == expected

    def test_unowned_sequence_still_reports_its_cache(self):
        # (not_null, is_unique, owned_sequence, data_type, is_identity, column_default)
        cur = FakeCursor([
            (True, True, None, 'bigint', False, "nextval('s.loose'::regclass)"),
            (100,),
        ])
        v = keyset.require_monotonic_key(cur, 's', 't', 'c', ['id'])
        assert v['sequence'] == 's.loose'
        # the cache warning is the one that would have been lost entirely
        assert any('caches 100 values per connection' in r for r in v['risks'])
        assert any('rather than owning it' in r for r in v['risks'])

    def test_owned_sequence_is_not_flagged_for_ownership(self):
        cur = FakeCursor([
            (True, True, 's.t_c_seq', 'bigint', False, "nextval('s.t_c_seq'::regclass)"),
            (1,),
        ])
        v = keyset.require_monotonic_key(cur, 's', 't', 'c', ['id'])
        assert v['sequence'] == 's.t_c_seq'
        assert v['risks'] == []


class TestTemporalNameRanking:
    """Names order candidates; they never decide whether one is a candidate."""

    @pytest.mark.parametrize('name,hints,expected', [
        ('updated_at', keyset._MODIFIED_NAME_HINTS, 0),
        ('last_modified', keyset._MODIFIED_NAME_HINTS, 3),   # 'modified' hits before the exact hint
        ('created_at', keyset._CREATED_NAME_HINTS, 0),
        ('createdat', keyset._CREATED_NAME_HINTS, 1),        # no underscore, 'created' still hits
        ('date_created', keyset._CREATED_NAME_HINTS, 1),
        ('event_time', keyset._CREATED_NAME_HINTS, None),    # no hint: still a candidate, just last
        ('ts_insert', keyset._MODIFIED_NAME_HINTS, None),
    ])
    def test_name_rank(self, name, hints, expected):
        assert keyset._name_rank(name, hints) == expected


class TestModificationTimestampIsAClaim:
    """A name saying 'updated' is not a mechanism that updates anything."""

    def test_no_update_trigger_means_nothing_maintains_it(self):
        # facts row, then an empty trigger lookup
        cur = FakeCursor([(True, True, None, 'timestamptz', False, 'now()'), []])
        v = keyset.require_monotonic_key(cur, 's', 't', 'updated_at', ['id'])
        assert any('no row-level UPDATE trigger' in r for r in v['risks'])
        assert any('never moves again' in r for r in v['risks'])

    def test_existing_update_trigger_is_reported_but_not_trusted(self):
        cur = FakeCursor([(True, True, None, 'timestamptz', False, None),
                          [('touch_updated',)]])
        v = keyset.require_monotonic_key(cur, 's', 't', 'updated_at', ['id'])
        assert any('may be maintained by touch_updated' in r for r in v['risks'])

    def test_creation_named_column_is_not_asked_about_triggers(self):
        cur = FakeCursor([(True, True, None, 'timestamptz', False, 'now()')])
        v = keyset.require_monotonic_key(cur, 's', 't', 'created_at', ['id'])
        # only the transaction-start-time risk; no trigger question is relevant
        assert len(v['risks']) == 1
        assert 'transaction start time' in v['risks'][0]

    def test_neither_kind_outranks_the_other(self):
        # both sit in the same rank group: they fail differently, not worse
        assert keyset._name_rank('updated_at', keyset._MODIFIED_NAME_HINTS) is not None
        assert keyset._name_rank('created_at', keyset._CREATED_NAME_HINTS) is not None


class TestMaxPkValuesSql:
    """The probe reads N index entries either way; only the wording differs."""

    def test_merge_scan_form_is_one_statement(self):
        sql = keyset.max_pk_values_sql('s.t', 't', ['id'], 3, merge_scan=True)
        assert 'UNION ALL' not in sql
        assert '(yb_hash_code("id") % 3) IN (0, 1, 2)' in sql
        assert sql.endswith('ORDER BY "id" DESC LIMIT 1')

    def test_fallback_form_carries_a_limit_per_bucket(self):
        sql = keyset.max_pk_values_sql('s.t', 't', ['id'], 3, merge_scan=False)
        # the per-branch LIMIT is what makes each branch read a single entry;
        # the IN-list form without merge scan reads the whole index instead
        assert sql.count('UNION ALL') == 2
        assert sql.count('ORDER BY "id" DESC LIMIT 1') == 4   # 3 branches + the outer
        assert sql.count('/*+ IndexScan(t t_pw_keyset) */') == 3

    def test_fallback_form_names_every_bucket_exactly_once(self):
        sql = keyset.max_pk_values_sql('s.t', 't', ['id'], 4, merge_scan=False)
        for bucket in range(4):
            assert f'% 4) = {bucket}' in sql

    def test_composite_key_orders_columns_individually_in_both_forms(self):
        for merge_scan in (True, False):
            sql = keyset.max_pk_values_sql('s.t', 't', ['tenant', 'id'], 2, merge_scan)
            assert '"tenant" DESC, "id" DESC' in sql
            assert 'ROW(' not in sql


class TestMergeScanAvailability:
    def test_present_when_pg_settings_has_the_parameter(self):
        assert keyset.merge_scan_available(FakeCursor([(1,)])) is True

    def test_absent_on_a_build_without_it(self):
        assert keyset.merge_scan_available(FakeCursor([(0,)])) is False


class TestUnknownGucIsPermanent:
    def test_unrecognised_parameter_is_not_retried(self):
        # a GUC this build lacks can never appear; retrying it just delays the failure
        assert is_permanent(SqlStateError('42704'))


class TestIndexHint:
    def test_hint_names_the_table_and_its_keyset_index(self):
        assert keyset.index_hint('orders') == '/*+ IndexScan(orders orders_pw_keyset) */'

    def test_merge_form_carries_the_hint(self):
        sql = keyset.max_pk_values_sql('s.orders', 'orders', ['id'], 3, merge_scan=True)
        # the plan is otherwise a cost decision, and the cost model prefers the
        # sequential scan and the spill it brings
        assert sql.startswith('/*+ IndexScan(orders orders_pw_keyset) */ SELECT')


class TestReplicationKeyThatIsThePrimaryKey:
    """When the replication key IS the primary key, the primary-key index
    already is the replication-key index. Asking for a separate one produced
    `(bucket ASC, "id" ASC, "id" ASC)` under a name nothing hints at -- the
    server accepts it, so a service owner running it builds a second copy of an
    index they already have and nothing ever reads it."""

    def test_ddl_is_the_primary_key_index(self):
        assert (keyset.replication_key_index_ddl('s.t', 't', 'id', ['id'], 3)
                == keyset.index_ddl('s.t', 't', ['id'], 3))

    def test_the_key_column_is_not_repeated(self):
        ddl = keyset.replication_key_index_ddl('s.t', 't', 'id', ['id'], 3)
        assert '"id" ASC, "id" ASC' not in ddl

    def test_the_ddl_matches_the_index_the_scan_hints(self):
        ddl = keyset.replication_key_index_ddl('s.t', 't', 'id', ['id'], 3)
        assert keyset.index_for_replication_key('t', 'id', ['id']) in ddl

    def test_a_distinct_replication_key_still_gets_its_own(self):
        ddl = keyset.replication_key_index_ddl('s.t', 't', 'created_at', ['id'], 3)
        assert '"created_at" ASC, "id" ASC' in ddl
        assert 't_created_at_pw_keyset' in ddl
