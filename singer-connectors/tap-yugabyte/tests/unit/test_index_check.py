import pytest

from tap_yugabyte import index_check, keyset
from tap_yugabyte.index_check import ACTION, BLOCK, OK

# pg_get_indexdef output captured from YugabyteDB 2026.1 for the DDL index_ddl emits
ORDER_LINE_DEF = ('CREATE UNIQUE INDEX "OrderLine_pw_keyset" ON ixprobe."OrderLine" USING lsm '
                  '(((yb_hash_code("OrderId") % 3)) ASC, "OrderId" ASC)')
SPACED_DEF = ('CREATE UNIQUE INDEX sp_pw_keyset ON ixprobe.sp USING lsm '
              '(((yb_hash_code("order id", "user") % 3)) ASC, "order id" ASC, "user" ASC)')
DESC_DEF = ('CREATE UNIQUE INDEX dsc_pw_keyset ON ixprobe.dsc USING lsm '
            '(((yb_hash_code(id) % 3)) ASC, id DESC)')


def pk_def(table='t', buckets=3):
    return (f'CREATE UNIQUE INDEX {table}_pw_keyset ON s.{table} USING lsm '
            f'(((yb_hash_code(id) % {buckets})) ASC, id ASC)')


def shape(indexdef, unique=True, tablets=3, valid=True, ready=True, indoption=0, colocated=False):
    return (indexdef, unique, tablets, valid, ready, indoption, colocated)


class ScriptedCursor:
    """Answers each catalog query the index check issues from a fixed script."""

    def __init__(self, pk=(('id', 'bigint'),), sharding=('r', 1, 3, False), indexes=None,
                 hashable=True):
        self.pk = list(pk)
        self.sharding = sharding
        self.indexes = indexes or {}
        self.hashable = hashable
        self.connection = type('Conn', (), {'autocommit': True})()
        self._one, self._all = None, []

    def execute(self, sql, params=None):
        if 'x.indisprimary' in sql:
            self._all = self.pk
        elif 'yb_hash_code(NULL' in sql:
            if not self.hashable:
                raise keyset.psycopg2.Error('not hashable')
            self._one = (1,)
        elif 'pg_get_indexdef(i.oid), x.indisunique' in sql:
            self._one = self.indexes.get(params[2])
        elif 'yb_table_properties(c.oid)' in sql:
            self._one = self.sharding
        else:
            raise AssertionError(f'unscripted query: {sql}')

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


def key_facts(hard_failures=(), risks=(), tiebreaker=None):
    return {'usable': not hard_failures, 'hard_failures': list(hard_failures),
            'risks': list(risks), 'tiebreaker': tiebreaker}


@pytest.fixture
def stub_key_checks(monkeypatch):
    """require_monotonic_key and measure_tie_groups are covered in test_keyset;
    here only the index check's decisions on their verdicts matter."""
    state = {'key': key_facts(), 'ties': {'risks': [], 'severity': None}}
    monkeypatch.setattr(keyset, 'require_monotonic_key', lambda *a, **k: state['key'])
    monkeypatch.setattr(keyset, 'measure_tie_groups', lambda *a, **k: state['ties'])
    return state


class TestRouting:
    def test_a_table_without_a_method_is_checked_as_log_based(self):
        # PipelineWise defaults tap-yugabyte tables to LOG_BASED; reporting them
        # as an unknown method blocked every table that relied on the default
        status, notes, ddl = index_check.check_table(None, 's', {'table_name': 't'}, 3, fastsync=True)
        assert (status, ddl) == (OK, [])
        assert 'initial snapshot' in notes[0]

    @pytest.mark.parametrize('method', ['FULL_TABLE', 'LOG_BASED', 'full_table', 'log_based'])
    def test_fastsync_routes_need_no_keyset_index(self, method):
        """FastSync copies these tables with a plain COPY, so no catalog query runs and no DDL is asked for."""
        status, notes, ddl = index_check.check_table(
            None, 's', {'table_name': 't', 'replication_method': method}, 3, fastsync=True)
        assert (status, ddl) == (OK, [])
        assert 'plain COPY' in notes[0]

    def test_log_based_blocks_without_fastsync(self):
        """Only FastSync creates the replication slot, and PipelineWise rejects LOG_BASED without it."""
        status, notes, ddl = index_check.check_table(
            None, 's', {'table_name': 't', 'replication_method': 'LOG_BASED'}, 3)
        assert (status, ddl) == (BLOCK, [])
        assert 'needs FastSync' in notes[0]

    def test_full_table_without_fastsync_checks_the_keyset_index(self, monkeypatch):
        seen = []
        monkeypatch.setattr(index_check, 'check_full_table',
                            lambda cur, s, t, b: seen.append(t) or (ACTION, [], ['ddl']))
        assert index_check.check_table(
            None, 's', {'table_name': 't', 'replication_method': 'full_table'}, 3)[0] == ACTION
        assert seen == ['t']

    def test_incremental_passes_its_key_and_limit(self, monkeypatch):
        seen = []
        monkeypatch.setattr(index_check, 'check_incremental',
                            lambda cur, s, t, key, b, limit: seen.append((key, limit)) or (OK, [], []))
        index_check.check_table(None, 's', {'table_name': 't', 'replication_method': 'INCREMENTAL',
                                            'replication_key': 'updated_at'}, 3, 500)
        assert seen == [('updated_at', 500)]

    def test_an_unknown_method_blocks(self):
        status, notes, ddl = index_check.check_table(
            None, 's', {'table_name': 't', 'replication_method': 'CDC'}, 3)
        assert status == BLOCK and ddl == []
        assert "Unknown replication_method 'CDC'" in notes[0]


class TestPrimaryKeyIndex:
    def test_a_correct_index_is_ok(self):
        cur = ScriptedCursor(indexes={'t_pw_keyset': shape(pk_def())})
        status, notes, ddl = index_check.check_full_table(cur, 's', 't', 3)
        assert status == OK and ddl == []
        assert '3 buckets over 3 tablets' in notes[0]

    def test_a_missing_index_prints_only_the_create(self):
        status, notes, ddl = index_check.check_full_table(ScriptedCursor(), 's', 't', 3)
        assert status == ACTION
        assert ddl == [keyset.index_ddl('"s"."t"', 't', ['id'], 3) + ';']
        assert any('does not exist' in n for n in notes)

    def test_a_wrong_index_is_dropped_before_it_is_recreated(self):
        # a bare CREATE fails with "already exists" while the wrong one is there
        cur = ScriptedCursor(indexes={'t_pw_keyset': shape(pk_def(buckets=5))})
        status, notes, ddl = index_check.check_full_table(cur, 's', 't', 3)
        assert status == ACTION
        assert ddl[0] == 'DROP INDEX "s"."t_pw_keyset";'
        assert ddl[1].startswith('CREATE UNIQUE INDEX "t_pw_keyset"')
        assert any('built with 5 buckets' in n for n in notes)

    def test_a_table_without_a_primary_key_blocks_with_no_ddl(self):
        status, notes, ddl = index_check.check_full_table(ScriptedCursor(pk=()), 's', 't', 3)
        assert status == BLOCK and ddl == []
        assert 'no primary key' in notes[0]

    def test_an_unhashable_key_blocks(self):
        status, _, ddl = index_check.check_full_table(
            ScriptedCursor(hashable=False), 's', 't', 3)
        assert status == BLOCK and ddl == []

    def test_a_colocated_table_is_ok_on_one_tablet(self):
        cur = ScriptedCursor(sharding=('r', 0, 1, True),
                             indexes={'t_pw_keyset': shape(pk_def(), tablets=1, colocated=True)})
        assert index_check.check_full_table(cur, 's', 't', 3)[0] == OK

    def test_a_colocated_table_is_asked_for_an_index_it_can_create(self):
        _, _, ddl = index_check.check_full_table(
            ScriptedCursor(sharding=('r', 0, 1, True)), 's', 't', 3)
        assert 'SPLIT AT' not in ddl[0]


class TestIncremental:
    def test_no_replication_key_blocks(self, stub_key_checks):
        status, notes, _ = index_check.check_incremental(ScriptedCursor(), 's', 't', None, 3)
        assert status == BLOCK and 'no replication_key' in notes[0]

    def test_no_primary_key_blocks(self, stub_key_checks):
        status, notes, _ = index_check.check_incremental(
            ScriptedCursor(pk=()), 's', 't', 'updated_at', 3)
        assert status == BLOCK and 'no primary key' in notes[0]

    def test_a_missing_timestamp_index_asks_for_index_b(self, stub_key_checks):
        status, _, ddl = index_check.check_incremental(
            ScriptedCursor(), 's', 't', 'updated_at', 3)
        assert status == ACTION
        assert ddl == [keyset.replication_key_index_ddl(
            '"s"."t"', 't', 'updated_at', ['id'], 3) + ';']

    def test_a_correct_timestamp_index_is_ok(self, stub_key_checks):
        indexdef = ('CREATE UNIQUE INDEX t_updated_at_pw_keyset ON s.t USING lsm '
                    '(((yb_hash_code(id) % 3)) ASC, updated_at ASC, id ASC)')
        cur = ScriptedCursor(indexes={'t_updated_at_pw_keyset': shape(indexdef)})
        status, notes, ddl = index_check.check_incremental(cur, 's', 't', 'updated_at', 3)
        assert status == OK and ddl == []
        assert any('rereads every row that shares the bookmarked value' in n for n in notes)

    def test_a_hard_failure_is_not_downgraded_by_missing_index_ddl(self, stub_key_checks):
        stub_key_checks['key'] = key_facts(hard_failures=['updated_at is nullable.'])
        status, notes, ddl = index_check.check_incremental(
            ScriptedCursor(), 's', 't', 'updated_at', 3)
        assert status == BLOCK
        assert 'updated_at is nullable.' in notes
        assert ddl, 'the index DDL is still reported alongside the block'

    def test_key_risks_are_reported_without_blocking(self, stub_key_checks):
        stub_key_checks['key'] = key_facts(risks=['no UPDATE trigger'])
        status, notes, _ = index_check.check_incremental(
            ScriptedCursor(), 's', 't', 'updated_at', 3)
        assert status == ACTION and 'no UPDATE trigger' in notes

    def test_a_tie_group_livelock_blocks(self, stub_key_checks):
        stub_key_checks['ties'] = {'risks': ['cannot advance'], 'severity': 'livelock'}
        status, notes, _ = index_check.check_incremental(
            ScriptedCursor(), 's', 't', 'updated_at', 3, run_limit=10000)
        assert status == BLOCK and 'cannot advance' in notes

    def test_a_tie_group_risk_without_a_limit_is_a_note(self, stub_key_checks):
        stub_key_checks['ties'] = {'risks': ['large group'], 'severity': 'risk'}
        status, notes, _ = index_check.check_incremental(
            ScriptedCursor(), 's', 't', 'updated_at', 3)
        assert status == ACTION and 'large group' in notes

    def test_a_primary_key_watermark_reuses_index_a(self, stub_key_checks):
        status, notes, ddl = index_check.check_incremental(ScriptedCursor(), 's', 't', 'id', 3)
        assert status == ACTION
        assert ddl == [keyset.index_ddl('"s"."t"', 't', ['id'], 3) + ';']
        assert any('No second index' in n for n in notes)

    def test_a_primary_key_watermark_with_index_a_is_ok(self, stub_key_checks):
        cur = ScriptedCursor(indexes={'t_pw_keyset': shape(pk_def())})
        assert index_check.check_incremental(cur, 's', 't', 'id', 3)[0] == OK

    def test_a_primary_key_watermark_keeps_a_key_block(self, stub_key_checks):
        stub_key_checks['key'] = key_facts(hard_failures=['id is nullable.'])
        cur = ScriptedCursor(indexes={'t_pw_keyset': shape(pk_def())})
        assert index_check.check_incremental(cur, 's', 't', 'id', 3)[0] == BLOCK

    def test_a_non_unique_key_explains_its_tiebreaker(self, stub_key_checks):
        stub_key_checks['key'] = key_facts(tiebreaker=['id'])
        _, notes, _ = index_check.check_incremental(ScriptedCursor(), 's', 't', 'updated_at', 3)
        assert any('updated_at is not unique; the index ends in id' in n for n in notes)

    def test_a_colocated_table_gets_an_unsplit_index_b(self, stub_key_checks):
        _, _, ddl = index_check.check_incremental(
            ScriptedCursor(sharding=('r', 0, 1, True)), 's', 't', 'updated_at', 3)
        assert 'SPLIT AT' not in ddl[0]


class TestTapSettings:
    def test_defaults_without_config(self):
        assert index_check.tap_settings({}) == (keyset.BUCKETS_DEFAULT, None)

    def test_reads_what_the_tap_will_run_with(self):
        config = {'db_conn': {'keyset_buckets': '8', 'limit': 10000}}
        assert index_check.tap_settings(config) == (8, 10000)

    def test_flags_win_over_the_yaml(self):
        config = {'db_conn': {'keyset_buckets': 8, 'limit': 10000}}
        assert index_check.tap_settings(config, buckets=4, limit=500) == (4, 500)

    def test_an_invalid_bucket_count_is_refused(self):
        with pytest.raises(ValueError, match='keyset_buckets'):
            index_check.tap_settings({'db_conn': {'keyset_buckets': 1}})


class TestWorstStatus:
    @pytest.mark.parametrize('current,status,expected', [
        (OK, OK, OK), (OK, ACTION, ACTION), (ACTION, OK, ACTION),
        (ACTION, BLOCK, BLOCK), (BLOCK, ACTION, BLOCK), (BLOCK, OK, BLOCK),
    ])
    def test_block_beats_action_beats_ok(self, current, status, expected):
        assert index_check.worse(current, status) == expected


class TestMain:
    def _run(self, monkeypatch, tmp_path, yaml_text, cursor, argv=()):
        config = tmp_path / 'tap.yml'
        config.write_text(yaml_text)
        closed = []

        class Conn:
            autocommit = False

            def cursor(self):
                class Ctx:
                    def __enter__(self_inner):
                        return cursor

                    def __exit__(self_inner, *exc):
                        return False
                return Ctx()

            def close(self):
                closed.append(True)

        monkeypatch.setattr(index_check.psycopg2, 'connect', lambda **kwargs: Conn())
        code = index_check.main([str(config), '--host', 'h', '--user', 'u', '--dbname', 'd',
                                 *argv])
        assert closed == [True]
        return code

    def test_all_ok_exits_zero_without_a_ddl_block(self, monkeypatch, tmp_path, capsys):
        code = self._run(monkeypatch, tmp_path,
                         'schemas:\n  - source_schema: s\n    tables:\n      - table_name: t\n', ScriptedCursor())
        out = capsys.readouterr().out
        assert code == 0
        assert 'OK     s.t  [LOG_BASED]' in out
        assert 'All DDL' not in out
        assert 'Worst status: OK' in out

    def test_any_action_exits_one_and_collects_the_ddl(self, monkeypatch, tmp_path, capsys):
        code = self._run(monkeypatch, tmp_path,
                         'schemas:\n  - source_schema: s\n    tables:\n'
                         '      - {table_name: t, replication_method: FULL_TABLE}\n',
                         ScriptedCursor(), ['--target-type', 'target-s3-csv'])
        out = capsys.readouterr().out
        assert code == 1
        assert 'All DDL, to hand to the service owner:' in out
        assert 'Worst status: ACTION' in out

    @pytest.mark.parametrize('argv', [[], ['--target-type', 'target-postgres'],
                                      ['--target-type', 'target-snowflake']])
    def test_fastsync_targets_ask_for_no_full_table_index(self, monkeypatch, tmp_path, capsys, argv):
        code = self._run(monkeypatch, tmp_path,
                         'schemas:\n  - source_schema: s\n    tables:\n'
                         '      - {table_name: t, replication_method: FULL_TABLE}\n',
                         ScriptedCursor(), argv)
        assert code == 0, capsys.readouterr().out

    def test_managed_iceberg_blocks_every_table(self, monkeypatch, tmp_path, capsys):
        code = self._run(monkeypatch, tmp_path,
                         'target_table_format: iceberg\niceberg_version: 3\n'
                         'schemas:\n  - source_schema: s\n    tables:\n'
                         '      - {table_name: t, replication_method: FULL_TABLE}\n',
                         ScriptedCursor())
        out = capsys.readouterr().out
        assert code == 1
        assert 'BLOCK  s.t  [FULL_TABLE]' in out
        assert 'managed Iceberg' in out

    def test_an_unknown_target_type_is_refused(self, monkeypatch, tmp_path):
        with pytest.raises(SystemExit):
            index_check.main(['tap.yml', '--host', 'h', '--user', 'u', '--dbname', 'd',
                              '--target-type', 'target-bigquery'])

    def test_bucket_count_comes_from_the_yaml(self, monkeypatch, tmp_path, capsys):
        cur = ScriptedCursor(indexes={'t_pw_keyset': shape(pk_def(buckets=8), tablets=8)})
        code = self._run(monkeypatch, tmp_path,
                         'db_conn: {keyset_buckets: 8}\n'
                         'schemas:\n  - source_schema: s\n    tables:\n'
                         '      - {table_name: t, replication_method: FULL_TABLE}\n',
                         cur, ['--target-type', 'target-s3-csv'])
        assert code == 0, capsys.readouterr().out

    def test_the_connection_is_closed_when_a_check_fails(self, monkeypatch, tmp_path):
        class Exploding(ScriptedCursor):
            def execute(self, sql, params=None):
                raise RuntimeError('lost connection')

        with pytest.raises(RuntimeError):
            self._run(monkeypatch, tmp_path,
                      'schemas:\n  - source_schema: s\n    tables:\n'
                      '      - {table_name: t, replication_method: FULL_TABLE}\n',
                      Exploding(), ['--target-type', 'target-s3-csv'])


class TestDescribeRealIndexDefinitions:
    """Definitions as YugabyteDB renders them, not as the DDL spells them."""

    def _describe(self, indexdef, name):
        return keyset.describe_index(ScriptedCursor(indexes={name: shape(indexdef)}),
                                     's', 't', name)

    def test_quoted_mixed_case_names(self):
        found = self._describe(ORDER_LINE_DEF, 'OrderLine_pw_keyset')
        assert found['bucket_columns'] == ['OrderId']
        assert found['trailing'] == ['OrderId']
        assert found['buckets'] == 3

    def test_names_with_spaces_and_reserved_words(self):
        found = self._describe(SPACED_DEF, 'sp_pw_keyset')
        assert found['bucket_columns'] == ['order id', 'user']
        assert found['trailing'] == ['order id', 'user']

    def test_a_desc_trailing_column_is_accepted(self):
        # the planner serves it with Index Scan Backward, so the shape is usable
        cur = ScriptedCursor(indexes={'dsc_pw_keyset': shape(DESC_DEF)})
        found, problems = keyset.check_index(cur, 's', 'dsc', 'dsc_pw_keyset', ['id'], ['id'], 3)
        assert found['trailing'] == ['id'] and problems == []

    def test_a_missing_index_describes_as_none(self):
        assert keyset.describe_index(ScriptedCursor(), 's', 't', 't_pw_keyset') is None


class TestCheckIndexShape:
    """Each shape fault check_index exists to catch, one at a time."""

    def _problems(self, indexdef, trailing=('id',), **over):
        cur = ScriptedCursor(indexes={'t_pw_keyset': shape(indexdef, **over)})
        return keyset.check_index(cur, 's', 't', 't_pw_keyset', ['id'], list(trailing), 3)[1]

    def test_a_hashed_bucket_is_named_as_the_real_fault(self):
        problems = self._problems(pk_def(), indoption=keyset.INDOPTION_HASH, tablets=1)
        assert any('declares the bucket HASH' in p for p in problems)
        assert any('not legal on a hashed bucket' in p for p in problems)

    def test_an_index_that_is_not_bucketed(self):
        problems = self._problems('CREATE UNIQUE INDEX t_pw_keyset ON s.t USING lsm (id ASC)')
        assert any('is not a bucket index' in p for p in problems)

    def test_a_bucket_on_the_wrong_column(self):
        indexdef = ('CREATE UNIQUE INDEX t_pw_keyset ON s.t USING lsm '
                    '(((yb_hash_code(updated_at) % 3)) ASC, id ASC)')
        assert any('hashes updated_at but the scan hashes id' in p
                   for p in self._problems(indexdef))

    def test_trailing_columns_in_the_wrong_order(self):
        indexdef = ('CREATE UNIQUE INDEX t_pw_keyset ON s.t USING lsm '
                    '(((yb_hash_code(id) % 3)) ASC, id ASC, updated_at ASC)')
        problems = self._problems(indexdef, trailing=('updated_at', 'id'))
        assert any('orders by id, updated_at but the scan orders by updated_at, id' in p
                   for p in problems)

    def test_a_non_unique_index(self):
        assert self._problems(pk_def(), unique=False) == ['t_pw_keyset is not UNIQUE.']

    def test_an_unfinished_backfill(self):
        problems = self._problems(pk_def(), valid=False, ready=True)
        assert any('backfill did not complete' in p for p in problems)


class TestHashableProbe:
    def test_a_rejected_type_rolls_back_to_its_savepoint_inside_a_transaction(self):
        class TxCursor:
            connection = type('Conn', (), {'autocommit': False})()

            def __init__(self):
                self.executed = []

            def execute(self, sql, params=None):
                self.executed.append(sql)
                if 'yb_hash_code(NULL' in sql:
                    raise keyset.psycopg2.Error('not hashable')

        cur = TxCursor()
        assert keyset._hashable(cur, 'point') is False
        assert cur.executed[0] == 'SAVEPOINT yb_hash_probe'
        assert cur.executed[-1] == 'ROLLBACK TO SAVEPOINT yb_hash_probe'

    def test_an_accepted_type_releases_its_savepoint_inside_a_transaction(self):
        class TxCursor:
            connection = type('Conn', (), {'autocommit': False})()

            def __init__(self):
                self.executed = []

            def execute(self, sql, params=None):
                self.executed.append(sql)

            def fetchone(self):
                return (1,)

        cur = TxCursor()
        assert keyset._hashable(cur, 'bigint') is True
        assert cur.executed[-1] == 'RELEASE SAVEPOINT yb_hash_probe'
