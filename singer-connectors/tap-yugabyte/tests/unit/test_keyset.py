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


class TestKeysetBranches:
    """The resume, laddered into one statement per equality prefix.

    The conjunctive expansion above bounds only the leading column, so a resume
    costs the size of the leading-value group: measured 135 index rows to
    return 5 with 250 distinct leading values, 662 with a three-column key, and
    33,220 -- the whole bucket -- with a constant leading column. Stating the
    prefix as an equality is what gets the whole prefix into the Index Cond.
    """

    def test_single_column_is_one_branch_and_unchanged(self):
        # the single-column path must not regress: one statement, and the same
        # statement keyset_predicate has always produced
        branches = keyset.keyset_branches(['id'])
        assert branches == [('"id" > %s', [0])]
        assert branches[0][0] == keyset.keyset_predicate(['id'])[0]
        assert keyset.keyset_params([7], branches[0][1]) == [7]

    def test_two_columns_ladder_most_specific_first(self):
        branches = keyset.keyset_branches(['tenant', 'id'])
        assert [sql for sql, _ in branches] == [
            '"tenant" = %s AND "id" > %s',
            '"tenant" > %s',
        ]
        assert [keyset.keyset_params(['t', 9], order) for _, order in branches] == [
            ['t', 9], ['t'],
        ]

    def test_three_columns_ladder_most_specific_first(self):
        branches = keyset.keyset_branches(['a', 'b', 'c'])
        assert [sql for sql, _ in branches] == [
            '"a" = %s AND "b" = %s AND "c" > %s',
            '"a" = %s AND "b" > %s',
            '"a" > %s',
        ]
        assert [keyset.keyset_params([1, 2, 3], order) for _, order in branches] == [
            [1, 2, 3], [1, 2], [1],
        ]

    @pytest.mark.parametrize('columns', [['id'], ['a', 'b'], ['a', 'b', 'c'],
                                         ['a', 'b', 'c', 'd']])
    def test_one_branch_per_key_column(self, columns):
        assert len(keyset.keyset_branches(columns)) == len(columns)

    @pytest.mark.parametrize('columns', [['id'], ['a', 'b'], ['a', 'b', 'c']])
    def test_every_branch_ends_in_the_only_inequality(self, columns):
        # exactly one open end per branch is what makes it a seek: everything
        # before it is pinned, so the Index Cond can position on the prefix
        for sql, _ in keyset.keyset_branches(columns):
            assert sql.count('>') == 1
            assert sql.endswith('> %s')
            assert '<' not in sql
            assert 'OR' not in sql
            assert 'ROW(' not in sql

    @pytest.mark.parametrize('columns', [['id'], ['a', 'b'], ['a', 'b', 'c']])
    def test_branches_partition_every_key_above_the_bound(self, columns):
        """Disjoint, exhaustive, and in ascending order -- the three properties
        the sequential loop and the bookmark both rest on."""
        width = len(columns)
        bound = tuple([2] * width)
        universe = [tuple(t) for t in _tuples(width, range(1, 5))]

        def matched(order, values):
            prefix = len(order) - 1
            return [key for key in universe
                    if key[:prefix] == values[:prefix] and key[prefix] > values[prefix]]

        selections = [matched(order, bound) for _, order in keyset.keyset_branches(columns)]

        # exhaustive: together they are exactly the keys above the bound
        assert sorted(k for sel in selections for k in sel) == \
            sorted(k for k in universe if k > bound)
        # disjoint: no key is selected twice
        flat = [k for sel in selections for k in sel]
        assert len(flat) == len(set(flat))
        # ascending: concatenating the branches in order, each sorted within
        # itself, is already globally sorted -- which is what lets the tap's own
        # loop supply the ordering that a UNION ALL's Append would not
        assert [k for sel in selections for k in sorted(sel)] == sorted(flat)


def _tuples(width, values):
    if width == 0:
        yield []
        return
    for head in values:
        for tail in _tuples(width - 1, values):
            yield [head] + tail


class TestIndexDefinition:
    def test_ddl_hash_shards_the_bucket_and_range_orders_the_key(self):
        ddl = keyset.index_ddl('s.orders', 'orders', ['tenant', 'id'], 8, tablets=8)
        assert 'CREATE UNIQUE INDEX "orders_pw_keyset" ON s.orders' in ddl
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
        assert ddl == ('CREATE UNIQUE INDEX "t_pw_keyset" ON s.t '
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
        assert ddl == ('CREATE UNIQUE INDEX "t_created_at_pw_keyset" ON s.t '
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
        # naming an index nobody created means the DDL the preflight prints
        # builds a second copy of an index the table already has
        assert keyset.index_for_replication_key('t', 'id', ['id']) == 't_pw_keyset'

    def test_the_incremental_scan_no_longer_renders_a_hint(self):
        # replication_key_hint is gone -- not because the hint was ignored (a
        # nested hint IS applied: unhinted the subquery plans an Index Only
        # Scan, and `/*+ SeqScan(healthy) */` inside it plans a Seq Scan) but
        # because it does not help and can hurt. See bucket_branches_sql.
        assert not hasattr(keyset, 'replication_key_hint')

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


class TestBucketBranches:
    """The branch form, which is what a NAMED cursor needs.

    The storage-level merge behind bucket_in_sql is not performed under
    `DECLARE CURSOR`: the per-bucket streams come back concatenated, and both
    plans read `Merge Streams: 3`. Measured on rt.healthy, 40,000 rows, only the
    cursor varying -- plain: 0 ORDER BY violations, 26,712 bucket changes along
    the stream; named: 2 violations, 2 bucket changes. `Merge Append` is a plan
    node, so the cursor honours it: 0 violations named and plain.
    """

    ORDER = '"updated_at" ASC, "id" ASC'

    def _sql(self, buckets=3, **kwargs):
        return keyset.bucket_branches_sql('"s"."t"', ['id'], buckets,
                                          self.ORDER, **kwargs)

    def test_one_branch_per_bucket(self):
        sql = self._sql()
        assert sql.count('UNION ALL') == 2
        assert sql.count('SELECT * FROM "s"."t"') == 3

    def test_each_branch_states_its_bucket_as_an_equality(self):
        # an equality is the one access pattern a hash-sharded discriminator
        # serves; the IN-list reaches the index too, but only the equality
        # survives into a per-branch Index Cond
        sql = self._sql()
        for bucket in range(3):
            assert f'(yb_hash_code("id") % 3) = {bucket}' in sql
        assert 'IN (0, 1, 2)' not in sql

    def test_every_branch_carries_its_own_ordering(self):
        # without it the branches feed a blocking Sort rather than a Merge
        # Append -- measured, peak memory 5,624 kB against 161 kB
        assert self._sql().count(f'ORDER BY {self.ORDER}') == 3

    def test_the_bookmark_rides_inside_every_branch(self):
        # so bucket equality and bookmark reach the SAME Index Cond, which is
        # what terminates early under a LIMIT: 4,096 index rows per branch for
        # LIMIT 10000 over three buckets, not a third of the table
        sql = self._sql(extra_predicate='"updated_at" >= \'X\'::timestamptz')
        assert sql.count('"updated_at" >= \'X\'::timestamptz') == 3
        for bucket in range(3):
            assert (f'WHERE (yb_hash_code("id") % 3) = {bucket} '
                    f'AND "updated_at" >= \'X\'::timestamptz') in sql

    def test_no_bookmark_leaves_the_bucket_predicate_alone(self):
        sql = self._sql()
        assert 'WHERE (yb_hash_code("id") % 3) = 0 ORDER BY' in sql
        assert 'AND' not in sql

    def test_no_index_hint_is_emitted(self):
        # one leading hint on the bare table name reaches exactly ONE branch,
        # because every branch writes the same relation name, and N hints on the
        # per-branch aliases reach all of them and make it worse -- IndexScan
        # replaces the Index Only Scan the planner picks unaided, adding a heap
        # fetch per row: 80,000 storage rows scanned against 40,000
        assert 'IndexScan' not in self._sql()
        assert '/*+' not in self._sql()

    def test_the_modulo_is_escaped_only_for_parameterised_statements(self):
        # this statement interpolates its bookmark and passes no parameters, so
        # the default is the undoubled form. The doubled one here is not a slow
        # query, it is a malformed statement
        assert '% 3' in self._sql()
        assert '%%' not in self._sql()
        assert '%% 3' in self._sql(escape_percent=True)

    def test_the_shape_holds_at_larger_bucket_counts(self):
        # confirmed against the server at N = 3, 8, 16, 32, 64 and 128: Merge
        # Append every time, 0 ORDER BY violations through the named cursor, no
        # point at which it degrades to a Sort
        sql = self._sql(buckets=16)
        assert sql.count('UNION ALL') == 15
        assert '(yb_hash_code("id") % 16) = 15' in sql

    def test_a_single_bucket_is_one_branch_and_no_union(self):
        sql = self._sql(buckets=1)
        assert 'UNION ALL' not in sql
        assert '(yb_hash_code("id") % 1) = 0' in sql

    def test_composite_primary_key_hashes_the_whole_tuple(self):
        sql = keyset.bucket_branches_sql('"s"."t"', ['tenant', 'id'], 2,
                                         '"tenant" ASC, "id" ASC')
        assert '(yb_hash_code("tenant", "id") % 2) = 0' in sql
        assert '(yb_hash_code("tenant", "id") % 2) = 1' in sql

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


class TestIndexValidityAndShape:
    """Checks added after a live cluster produced each of these silently.

    An index whose backfill never completed exists, is named correctly, and has
    exactly the right definition -- and the planner refuses it. A failed
    CREATE INDEX leaves one behind permanently, so this is not a transient
    window: the tap reported the requirement satisfied forever while every scan
    read the whole table."""

    def test_key_columns_survive_a_composite_bucket_expression(self):
        # the comma inside yb_hash_code(tenant, id) is not a key separator
        assert keyset._index_key_columns(
            'CREATE UNIQUE INDEX i ON s.t USING lsm '
            '(((yb_hash_code(tenant, id) % 3)) ASC, created_at ASC, tenant ASC, id ASC)'
        ) == ['((yb_hash_code(tenant, id) % 3))', 'created_at', 'tenant', 'id']

    def test_key_columns_survive_a_quoted_column_name(self):
        # pg_get_indexdef quotes any name that needs it
        assert keyset._index_key_columns(
            'CREATE UNIQUE INDEX i ON s.t USING lsm '
            '(((yb_hash_code(id) % 3)) ASC, "pct%done" ASC, id ASC)'
        ) == ['((yb_hash_code(id) % 3))', 'pct%done', 'id']

    def test_a_single_column_key_still_parses(self):
        assert keyset._index_key_columns(
            'CREATE UNIQUE INDEX i ON s.t USING lsm (((yb_hash_code(id) % 3)) ASC, id ASC)'
        ) == ['((yb_hash_code(id) % 3))', 'id']


class TestDeterministicErrorsAreNotRetried:
    """A syntax error does not become correct on the eighth attempt; retrying
    one just delays the failure by the whole backoff schedule."""

    @pytest.mark.parametrize('sqlstate', ['42601', '42P02', '42804', '42846'])
    def test_never_transient_states_are_permanent(self, sqlstate):
        from tap_yugabyte import retry
        assert sqlstate in retry.PERMANENT_SQLSTATES

    def test_a_tablet_move_is_still_retried(self):
        from tap_yugabyte import retry
        assert '40001' not in retry.PERMANENT_SQLSTATES
        assert '40P01' not in retry.PERMANENT_SQLSTATES


class TestRuntimeGateMatchesThePreflight:
    """validate_index is the gate full_table.sync_table calls before taking the
    bucketed parallel path; check_index is what the preflight tool calls. They
    used to be separate implementations, and the runtime one was weaker -- it
    checked existence and bucket count only. An index whose backfill never
    completed passed it, and all N workers then sequentially scanned the whole
    table with enable_seqscan off and the hint ignored."""

    def _cursor(self, row):
        class Cur:
            def execute(self_inner, *_a, **_k):
                pass

            def fetchone(self_inner):
                return row
        return Cur()

    def _row(self, **over):
        shape = dict(
            indexdef='CREATE UNIQUE INDEX t_pw_keyset ON s.t USING lsm '
                     '(((yb_hash_code(id) % 3)) ASC, id ASC)',
            unique=True, tablets=3, valid=True, ready=True, indoption=0)
        shape.update(over)
        return (shape['indexdef'], shape['unique'], shape['tablets'],
                shape['valid'], shape['ready'], shape['indoption'])

    def test_a_correct_index_passes(self):
        usable, reason = keyset.validate_index(
            self._cursor(self._row()), 's', 't', ['id'], 3)
        assert usable is True and reason is None

    def test_an_invalid_index_is_refused(self):
        usable, reason = keyset.validate_index(
            self._cursor(self._row(valid=False, ready=False)), 's', 't', ['id'], 3)
        assert usable is False
        assert 'not valid' in reason

    def test_a_hashed_bucket_is_refused(self):
        usable, reason = keyset.validate_index(
            self._cursor(self._row(indoption=keyset.INDOPTION_HASH)), 's', 't', ['id'], 3)
        assert usable is False
        assert 'HASH' in reason

    def test_a_single_tablet_is_refused(self):
        usable, reason = keyset.validate_index(
            self._cursor(self._row(tablets=1)), 's', 't', ['id'], 3)
        assert usable is False
        assert 'tablet' in reason

    def test_a_missing_index_still_reports_the_ddl_to_run(self):
        usable, reason = keyset.validate_index(
            self._cursor(None), 's', 't', ['id'], 3)
        assert usable is False
        assert 'CREATE UNIQUE INDEX "t_pw_keyset"' in reason


class PlanCursor:
    """Enough of a cursor for plan_keyset_strategy: one sharding row, then one
    yb_hash_code probe per key column."""

    class _Connection:
        autocommit = True          # so _hashable skips the savepoint dance

    def __init__(self, sharding, hashable=True):
        self.connection = self._Connection()
        self.sharding = sharding
        self.hashable = hashable
        self.executed = []
        self._next = None

    def execute(self, sql, params=None):
        self.executed.append(sql)
        if 'yb_hash_code(NULL' in sql:
            if not self.hashable:
                raise psycopg2.Error('yb_hash_code does not accept this type')
            self._next = (1,)
        else:
            self._next = self.sharding

    def fetchone(self):
        return self._next


class TestPlanKeysetStrategy:
    """Shape 1 is required for every table with a primary key, whatever the
    sharding. The tap pages on yb_hash_code(pk) % N, which no table can order
    by, so a range-sharded key is no more scannable by this code than a hashed
    one -- and reporting it as needing nothing made the preflight print OK for a
    table full_table.sync_table then refused, dropping it to a single
    non-resumable pass."""

    HASH_PK = ('r', 1, 3)        # relkind, num_hash_key_columns, num_tablets
    RANGE_PK = ('r', 0, 1)

    def _plan(self, sharding, pk=('id',), types=('bigint',), hashable=True):
        return keyset.plan_keyset_strategy(
            PlanCursor(sharding, hashable), 's', 't', list(pk), list(types), 3)

    def test_a_range_sharded_key_still_requires_the_index(self):
        plan = self._plan(self.RANGE_PK)
        assert plan['strategy'] == keyset.STRATEGY_PK_RANGE
        assert plan['index_required'] is True
        assert plan['blockers'] == []

    def test_a_range_sharded_key_reports_the_ddl_that_satisfies_it(self):
        plan = self._plan(self.RANGE_PK)
        assert plan['index_ddl'] == keyset.index_ddl('"s"."t"', 't', ['id'], 3)
        assert 'CREATE UNIQUE INDEX "t_pw_keyset"' in plan['index_ddl']

    def test_both_shardings_ask_for_the_same_index(self):
        # the index is a function of the key and the bucket count, not of how
        # the table happens to be sharded
        assert self._plan(self.RANGE_PK)['index_ddl'] == \
            self._plan(self.HASH_PK)['index_ddl']

    def test_the_range_note_says_why_the_order_it_has_is_not_enough(self):
        note = self._plan(self.RANGE_PK)['note']
        assert 'range-sharded' in note
        assert 'yb_hash_code(id) % 3' in note

    def test_a_hash_sharded_key_needs_the_bucket_index(self):
        plan = self._plan(self.HASH_PK)
        assert plan['strategy'] == keyset.STRATEGY_BUCKET_INDEX
        assert plan['index_required'] is True

    def test_no_primary_key_is_not_indexable(self):
        plan = self._plan(self.HASH_PK, pk=(), types=())
        assert plan['strategy'] == keyset.STRATEGY_PLAIN_SCAN
        assert plan['index_required'] is False
        assert plan['index_ddl'] is None
        assert 'no primary key' in plan['blockers'][0]

    def test_a_view_is_not_indexable(self):
        plan = self._plan(('v', 0, 1))
        assert plan['strategy'] == keyset.STRATEGY_PLAIN_SCAN
        assert plan['index_required'] is False

    def test_a_missing_relation_is_not_indexable(self):
        plan = keyset.plan_keyset_strategy(
            PlanCursor(None), 's', 't', ['id'], ['bigint'], 3)
        assert plan['strategy'] == keyset.STRATEGY_PLAIN_SCAN
        assert plan['index_required'] is False

    def test_an_unhashable_range_key_cannot_be_bucketed_either(self):
        # the probe has to run BEFORE the sharding is considered: promising
        # pk_range here would promise a parallel scan whose index cannot be built
        plan = self._plan(self.RANGE_PK, types=('point',), hashable=False)
        assert plan['strategy'] == keyset.STRATEGY_PLAIN_SCAN
        assert plan['index_required'] is False
        assert 'yb_hash_code does not accept' in plan['blockers'][0]

    def test_an_unhashable_hash_key_is_still_refused(self):
        plan = self._plan(self.HASH_PK, types=('point',), hashable=False)
        assert plan['strategy'] == keyset.STRATEGY_PLAIN_SCAN
        assert plan['index_required'] is False


class TestMeasureTieGroups:
    """(reltuples, analyzed, null_frac, n_distinct, top_freq) from ANALYZE's
    stored statistics -- no table read.

    The numbers in the first two cases are the ones YugabyteDB 2026.1.1.1
    actually reported for a 40,050-row table whose first 40,000 rows were
    inserted in one transaction, and for a 40,000-row table with distinct
    timestamps."""

    BULK_LOADED = (40050.0, True, 0.0, 42.0, 0.9986333)
    ALL_DISTINCT = (40000.0, True, 0.0, -1.0, None)

    def _measure(self, row, run_limit=None):
        return keyset.measure_tie_groups(FakeCursor([row]), 's', 't',
                                         'updated_at', run_limit)

    def test_the_mcv_list_recovers_the_group_size(self):
        m = self._measure(self.BULK_LOADED)
        assert m['largest_tie_group'] == 39995        # 40,000 actual
        assert m['source'] == 'most_common_vals'

    def test_a_group_at_or_above_the_limit_is_a_proven_livelock(self):
        m = self._measure(self.BULK_LOADED, run_limit=10000)
        assert m['severity'] == 'livelock'
        assert 'cannot advance past this value, ever' in m['risks'][0]

    def test_a_group_below_the_limit_is_not_a_livelock(self):
        # the run drains the group and reaches the next value, so it advances
        m = self._measure(self.BULK_LOADED, run_limit=50000)
        assert m['severity'] != 'livelock'

    def test_a_large_group_with_no_limit_configured_is_a_risk_not_a_verdict(self):
        # with no LIMIT a run that completes always advances; the failure needs
        # the run to be killed, which is a wall-clock question this cannot answer
        m = self._measure(self.BULK_LOADED)
        assert m['severity'] == 'risk'
        assert 'drain the whole group' in m['risks'][0]

    def test_a_key_with_no_ties_is_silent(self):
        m = self._measure(self.ALL_DISTINCT)
        assert m['largest_tie_group'] == 1
        assert m['severity'] is None
        assert m['risks'] == []

    def test_a_small_table_is_not_warned_about_on_the_fraction_alone(self):
        # 90 of 100 rows share a value, but 90 rows drain instantly -- below
        # TIE_GROUP_MIN_ROWS nothing is reported without a limit to compare to
        m = self._measure((100.0, True, 0.0, 2.0, 0.9))
        assert m['largest_tie_group'] == 90
        assert m['severity'] is None

    def test_a_small_table_is_still_a_livelock_against_a_smaller_limit(self):
        m = self._measure((100.0, True, 0.0, 2.0, 0.9), run_limit=50)
        assert m['severity'] == 'livelock'

    def test_a_big_group_in_a_much_bigger_table_is_not_warned_about(self):
        # 20,000 rows share a value in 100M -- over the absolute floor, which is
        # exactly why the floor alone is not the signal
        m = self._measure((100_000_000.0, True, 0.0, 5000.0, 0.0002))
        assert m['largest_tie_group'] == 20000
        assert m['severity'] is None

    def test_no_statistics_is_reported_as_unknown_rather_than_as_fine(self):
        m = keyset.measure_tie_groups(
            FakeCursor([(-1.0, False, None, None, None)]), 's', 't', 'updated_at')
        assert m['severity'] == 'unknown'
        assert 'Run ANALYZE' in m['risks'][0]

    def test_a_missing_column_statistic_is_unknown_too(self):
        m = self._measure((40000.0, False, None, None, None))
        assert m['severity'] == 'unknown'

    def test_n_distinct_supplies_an_average_when_there_is_no_mcv_list(self):
        m = self._measure((1000.0, True, 0.0, 10.0, None))
        assert m['largest_tie_group'] == 100
        assert 'average' in m['source']

    def test_a_negative_n_distinct_is_a_ratio_not_a_count(self):
        # -0.25 means distinct values are a quarter of the rows: groups of four
        m = self._measure((40000.0, True, 0.0, -0.25, None))
        assert m['largest_tie_group'] == 4

    def test_a_missing_table_is_unknown(self):
        m = keyset.measure_tie_groups(FakeCursor([]), 's', 't', 'updated_at')
        assert m['severity'] == 'unknown'
        assert 'not found' in m['risks'][0]


class TestAttemptCap:
    """A SQLSTATE that covers two failures deserving different treatment gets a
    cap. 57014 is the case: the server answers it both for a cancel, which the
    next attempt succeeds through, and for a statement_timeout shorter than one
    FETCH, where all eight attempts fail identically after ~45s of backoff."""

    def _err(self, sqlstate):
        class E(psycopg2.Error):
            @property
            def pgcode(self_inner):
                return sqlstate
        return E('boom')

    def test_57014_stops_at_two_attempts(self):
        from tap_yugabyte import retry
        assert retry.attempt_cap(self._err('57014'), 8) == 2

    def test_an_ordinary_transient_state_keeps_the_full_budget(self):
        from tap_yugabyte import retry
        assert retry.attempt_cap(self._err('40001'), 8) == 8

    def test_a_fault_with_no_sqlstate_keeps_the_full_budget(self):
        # every connection-level fault measured arrived with pgcode None --
        # a terminated backend, a YSQL restart -- so this is the common path
        from tap_yugabyte import retry
        assert retry.attempt_cap(psycopg2.OperationalError('server closed'), 8) == 8

    def test_the_cap_never_raises_the_budget(self):
        from tap_yugabyte import retry
        assert retry.attempt_cap(self._err('57014'), 1) == 1

    def test_both_retry_policies_agree(self):
        """The tap and FastSync classify faults identically.

        They drifted once already -- 42704 was missing from FastSync's set,
        which meant a GUC the server does not have was retried eight times on
        the export path and failed fast on the tap's. Loaded by path rather
        than imported, so this runs in the tap's own suite without pulling in
        the pipelinewise package.
        """
        import importlib.util
        import pathlib
        from tap_yugabyte import retry as tap_retry

        path = (pathlib.Path(__file__).resolve().parents[4]
                / 'pipelinewise' / 'fastsync' / 'commons' / 'yb_retry.py')
        if not path.exists():
            pytest.skip('fastsync retry module not present in this checkout')
        spec = importlib.util.spec_from_file_location('_yb_retry', path)
        yb_retry = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(yb_retry)

        assert tap_retry.PERMANENT_SQLSTATES == yb_retry.PERMANENT_SQLSTATES
        assert tap_retry.MAX_ATTEMPTS_BY_SQLSTATE == yb_retry.MAX_ATTEMPTS_BY_SQLSTATE
