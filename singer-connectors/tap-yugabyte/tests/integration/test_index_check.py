"""The index check against a live YugabyteDB.

Every DDL statement the check prints must be accepted by the server, must turn
the verdict to OK, and must give the tap a scan that actually uses the index --
an OK the tap cannot exploit is the failure this suite exists to catch.
"""

import contextlib
import os
import subprocess
import sys
import uuid
from unittest.mock import patch

import psycopg2
import pytest
import singer
from singer import metadata

import tap_yugabyte
from tap_yugabyte import db as yb_db
from tap_yugabyte import index_check, keyset
from tap_yugabyte.index_check import ACTION, BLOCK, OK
from tap_yugabyte.sync_strategies import full_table, incremental

from tests.integration.test_full_table import _desired_columns
from tests.utils import get_test_connection_config

ROWS = 300
NAMEDATALEN = 63


class Interrupted(Exception):
    """Stands in for a crash mid-scan; retry_read only retries database errors."""


@contextlib.contextmanager
def cursor(dbname=None):
    cfg = get_test_connection_config()
    conn = psycopg2.connect(host=cfg['host'], port=cfg['port'], user=cfg['user'],
                            password=cfg['password'], dbname=dbname or cfg['dbname'])
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            yield cur
    finally:
        conn.close()


def run_sql(*statements, dbname=None):
    with cursor(dbname) as cur:
        for statement in statements:
            cur.execute(statement)


def fq(schema, table):
    return yb_db.fully_qualified_table_name(schema, table)


def stored_name(name):
    """The name the server keeps: identifiers are truncated to 63 bytes."""
    return name.encode()[:NAMEDATALEN].decode()


def check(schema, table, method='FULL_TABLE', key=None, buckets=keyset.BUCKETS_DEFAULT,
          limit=None, dbname=None):
    spec = {'table_name': table, 'replication_method': method}
    if key:
        spec['replication_key'] = key
    with cursor(dbname) as cur:
        return index_check.check_table(cur, schema, spec, buckets, limit)


def apply(ddl, dbname=None):
    run_sql(*(statement.rstrip(';') for statement in ddl), dbname=dbname)


def conn_config(schema, buckets=keyset.BUCKETS_DEFAULT, dbname=None):
    cfg = get_test_connection_config()
    cfg.update(filter_schemas=schema, keyset_buckets=buckets)
    if dbname:
        cfg['dbname'] = dbname
    return cfg


def discover(cfg, table, method=None, key=None):
    with patch('tap_yugabyte.dump_catalog'):
        streams = tap_yugabyte.do_discovery(cfg)
    stream, = (s for s in streams if s['table_name'] == table)
    if method:
        for entry in stream['metadata']:
            if not entry['breadcrumb']:
                entry['metadata'].update({'selected': True, 'replication-method': method})
                if key:
                    entry['metadata']['replication-key'] = key
    return stream


def run_sync(sync, cfg, stream, state, fail_after=None, records=None):
    """Run one sync and return the records it delivered, raising Interrupted
    instead of delivering record number `fail_after`. Records land in `records`
    as they arrive, so an interrupted run still reports what it delivered."""
    md_map = metadata.to_map(stream['metadata'])
    records = [] if records is None else records

    def write(message):
        if isinstance(message, singer.RecordMessage):
            if fail_after is not None and len(records) >= fail_after:
                raise Interrupted()
            records.append(message.record)

    with patch('singer.write_message', write):
        sync(cfg, stream, state, _desired_columns(stream, md_map), md_map)
    return records


def sync_full_table(cfg, table, state=None, fail_after=None, records=None):
    stream = discover(cfg, table)
    # a usable index must never reach the plain-scan fallback
    with patch.object(full_table, '_sync_table_without_pk',
                      side_effect=AssertionError('fell back to a plain scan')):
        return run_sync(tap_yugabyte.do_sync_full_table, cfg, stream,
                        {} if state is None else state, fail_after, records)


def plan_text(sql, buckets, dbname=None):
    with cursor(dbname) as cur:
        for setting in keyset.scan_settings_sql(buckets):
            cur.execute(setting)
        cur.execute('EXPLAIN ' + sql)
        return '\n'.join(row[0] for row in cur.fetchall())


def bucket_scan_plan(schema, table, pk, buckets, dbname=None):
    """EXPLAIN of the statement full_table._scan_bucket issues for bucket 0."""
    sql = (f'{keyset.index_hint(table)} SELECT * FROM {fq(schema, table)} '
           f'WHERE {keyset.bucket_expr(pk, buckets)} = 0 ORDER BY {keyset.order_by_sql(pk)}')
    return plan_text(sql, buckets, dbname)


def keys_of(records, pk):
    return [tuple(record[c] for c in pk) for record in records]


@pytest.fixture(scope='module')
def schema():
    # capitals on purpose: every case also exercises schema quoting
    name = f'PwIxCheck_{uuid.uuid4().hex[:8]}'
    run_sql(f'CREATE SCHEMA "{name}"')
    yield name
    run_sql(f'DROP SCHEMA "{name}" CASCADE')


@pytest.fixture(scope='module')
def colocated_db():
    name = f'pw_ixcheck_coloc_{uuid.uuid4().hex[:8]}'
    run_sql(f'CREATE DATABASE {name} WITH colocation = true')
    yield name
    run_sql(f'DROP DATABASE {name}')


def assert_index_becomes_ok(schema, table, method='FULL_TABLE', key=None,
                            buckets=keyset.BUCKETS_DEFAULT, dbname=None):
    status, notes, ddl = check(schema, table, method, key, buckets, dbname=dbname)
    assert status == ACTION, notes
    assert len(ddl) == 1 and ddl[0].startswith('CREATE UNIQUE INDEX'), ddl
    apply(ddl, dbname)
    status, notes, ddl = check(schema, table, method, key, buckets, dbname=dbname)
    assert (status, ddl) == (OK, []), notes


# ---------------------------------------------------------------- index A

PK_CASES = [
    pytest.param('ix_bigint', 'id bigint PRIMARY KEY, v text', 'g, $$v$$ || g', ['id'],
                 id='bigint-hash'),
    pytest.param('ix_uuid', 'id uuid PRIMARY KEY, v int', 'gen_random_uuid(), g', ['id'],
                 id='uuid'),
    pytest.param('ix_text', 'k text PRIMARY KEY, v int', '$$k$$ || g, g', ['k'], id='text'),
    pytest.param('ix_numeric', 'id numeric(20,0) PRIMARY KEY',
                 '10000000000000000000::numeric + g', ['id'], id='numeric-20-digits'),
    pytest.param('ix_tstz', 'ts timestamptz PRIMARY KEY',
                 "$$2026-01-01$$::timestamptz + g * interval '1 minute'", ['ts'], id='timestamptz'),
    pytest.param('ix_date', 'd date PRIMARY KEY', "$$2000-01-01$$::date + g", ['d'], id='date'),
    pytest.param('ix_tenant', 'tenant_id int, id bigint, PRIMARY KEY ((tenant_id) HASH, id ASC)',
                 'g % 5, g', ['tenant_id', 'id'], id='composite-hash-range'),
    pytest.param('ix_allhash', 'a int, b text, PRIMARY KEY ((a, b) HASH)', 'g % 7, $$b$$ || g',
                 ['a', 'b'], id='composite-all-hash'),
    pytest.param('ix_range', 'id bigint, PRIMARY KEY (id ASC)', 'g', ['id'], id='range-sharded'),
    pytest.param('OrderLine', '"OrderId" bigint PRIMARY KEY, v int', 'g, g', ['OrderId'],
                 id='mixed-case'),
    pytest.param('ix_spaced', '"order id" bigint, "user" text, PRIMARY KEY ("order id", "user")',
                 'g, $$u$$ || g', ['order id', 'user'], id='quoted-columns'),
    pytest.param('ix_collated', 'k text COLLATE "C" PRIMARY KEY', '$$k$$ || g', ['k'],
                 id='collated'),
    pytest.param('ix_' + 'long' * 13, 'id bigint PRIMARY KEY', 'g', ['id'],
                 id='index-name-over-63-chars'),
]


@pytest.mark.parametrize('table,columns,values,pk', PK_CASES)
def test_full_table_index_is_created_validated_and_used(schema, table, columns, values, pk):
    run_sql(f'CREATE TABLE {fq(schema, table)} ({columns})',
            f'INSERT INTO {fq(schema, table)} SELECT {values} FROM generate_series(1, {ROWS}) g',
            f'ANALYZE {fq(schema, table)}')

    assert_index_becomes_ok(schema, table)
    with cursor() as cur:
        assert keyset.validate_index(cur, schema, table, pk, keyset.BUCKETS_DEFAULT) == (True, None)

    records = sync_full_table(conn_config(schema), table)
    keys = keys_of(records, pk)
    assert len(keys) == ROWS and len(set(keys)) == ROWS

    plan = bucket_scan_plan(schema, table, pk, keyset.BUCKETS_DEFAULT)
    assert 'Index Scan' in plan and stored_name(keyset.index_name(table)) in plan, plan


@pytest.mark.parametrize('buckets', [2, 8])
def test_bucket_count_is_honoured_end_to_end(schema, buckets):
    table = f'ix_buckets_{buckets}'
    run_sql(f'CREATE TABLE {fq(schema, table)} (id bigint PRIMARY KEY)',
            f'INSERT INTO {fq(schema, table)} SELECT g FROM generate_series(1, {ROWS}) g')

    assert_index_becomes_ok(schema, table, buckets=buckets)
    # the same index under the default count names buckets it does not have
    assert check(schema, table)[0] == ACTION
    records = sync_full_table(conn_config(schema, buckets), table)
    assert sorted(keys_of(records, ['id'])) == [(i,) for i in range(1, ROWS + 1)]


@pytest.mark.parametrize('table,columns,values,pk', [
    pytest.param('rs_bigint', 'id bigint PRIMARY KEY', 'g', ['id'], id='single-column'),
    pytest.param('rs_tenant', 'tenant_id int, id bigint, PRIMARY KEY ((tenant_id) HASH, id ASC)',
                 'g % 3, g', ['tenant_id', 'id'], id='composite'),
])
def test_an_interrupted_sync_resumes_without_loss_or_repeats(schema, table, columns, values, pk):
    run_sql(f'CREATE TABLE {fq(schema, table)} ({columns})',
            f'INSERT INTO {fq(schema, table)} SELECT {values} FROM generate_series(1, {ROWS}) g')
    assert_index_becomes_ok(schema, table)
    cfg, state, delivered = conn_config(schema), {}, []

    with pytest.raises(Interrupted):
        sync_full_table(cfg, table, state, fail_after=ROWS // 3, records=delivered)
    resumed = sync_full_table(cfg, table, state)

    assert len(delivered) == ROWS // 3
    keys = keys_of(delivered, pk) + keys_of(resumed, pk)
    assert len(keys) == ROWS, 'a resume must neither replay nor skip a row'
    assert len(set(keys)) == ROWS


def test_log_based_needs_fastsync_and_no_keyset_index(schema):
    table = 'ix_log_based'
    run_sql(f'CREATE TABLE {fq(schema, table)} (id bigint PRIMARY KEY)')
    status, notes, ddl = check(schema, table, method='LOG_BASED')
    assert (status, ddl) == (index_check.BLOCK, [])
    assert 'needs FastSync' in notes[0]
    with cursor() as cur:
        status, notes, ddl = index_check.check_table(
            cur, schema, {'table_name': table, 'replication_method': 'LOG_BASED'}, keyset.BUCKETS_DEFAULT,
            fastsync=True)
    assert (status, ddl) == (index_check.OK, [])
    assert any('initial snapshot' in note for note in notes)


def test_partitioned_parent_is_checked_and_synced_through_its_partitions(schema):
    table = 'ix_parted'
    run_sql(f'CREATE TABLE {fq(schema, table)} (id bigint, created date, PRIMARY KEY (id, created)) '
            f'PARTITION BY RANGE (created)',
            f'CREATE TABLE {fq(schema, table + "_2026")} PARTITION OF {fq(schema, table)} '
            f"FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
            f'INSERT INTO {fq(schema, table)} SELECT g, $$2026-01-01$$::date + (g % 300) '
            f'FROM generate_series(1, {ROWS}) g')

    assert_index_becomes_ok(schema, table)
    records = sync_full_table(conn_config(schema), table)
    assert len(set(keys_of(records, ['id', 'created']))) == ROWS


def test_colocated_table_gets_an_index_it_can_create_and_use(colocated_db):
    table = 'ix_coloc'
    run_sql(f'CREATE TABLE public.{table} (id bigint PRIMARY KEY)',
            f'INSERT INTO public.{table} SELECT g FROM generate_series(1, {ROWS}) g',
            dbname=colocated_db)

    status, _, ddl = check('public', table, dbname=colocated_db)
    assert status == ACTION and 'SPLIT AT' not in ddl[0]
    apply(ddl, colocated_db)
    assert check('public', table, dbname=colocated_db)[0] == OK
    with cursor(colocated_db) as cur:
        assert keyset.validate_index(cur, 'public', table, ['id'], 3) == (True, None)

    records = sync_full_table(conn_config('public', dbname=colocated_db), table)
    assert len(set(keys_of(records, ['id']))) == ROWS


@pytest.mark.parametrize('ddl,reason', [
    pytest.param('CREATE TABLE {t} (v int)', 'no primary key', id='no-primary-key'),
    pytest.param('CREATE VIEW {t} AS SELECT 1 AS id', 'view', id='view'),
])
def test_tables_that_cannot_be_paged_block(schema, ddl, reason):
    table = f'ix_block_{uuid.uuid4().hex[:6]}'
    run_sql(ddl.format(t=fq(schema, table)))
    status, notes, statements = check(schema, table)
    assert status == BLOCK and statements == []
    assert any(reason in note for note in notes), notes


# ---------------------------------------------------------- broken indexes

BROKEN_INDEXES = [
    pytest.param('(((yb_hash_code(id) % 3)) HASH, id ASC)', 'UNIQUE', '', 'declares the bucket HASH',
                 id='hashed-bucket'),
    pytest.param('(((yb_hash_code(id) % 5)) ASC, id ASC)', 'UNIQUE',
                 ' SPLIT AT VALUES ((1), (2), (3), (4))', 'built with 5 buckets', id='wrong-n'),
    pytest.param('(((yb_hash_code(id) % 3)) ASC, id ASC)', 'UNIQUE', '', '1 tablet(s) for 3 buckets',
                 id='no-split'),
    pytest.param('(((yb_hash_code(id) % 3)) ASC, id ASC)', '', ' SPLIT AT VALUES ((1), (2))',
                 'is not UNIQUE', id='not-unique'),
    pytest.param('(((yb_hash_code(v) % 3)) ASC, id ASC)', 'UNIQUE', ' SPLIT AT VALUES ((1), (2))',
                 'hashes v but the scan hashes id', id='wrong-bucket-column'),
]


@pytest.mark.parametrize('keys,unique,split,problem', BROKEN_INDEXES)
def test_a_broken_primary_key_index_is_dropped_and_recreated(schema, keys, unique, split, problem):
    table = f'ix_broken_{uuid.uuid4().hex[:6]}'
    run_sql(f'CREATE TABLE {fq(schema, table)} (id bigint PRIMARY KEY, v int)',
            f'INSERT INTO {fq(schema, table)} SELECT g, g FROM generate_series(1, 50) g',
            f'CREATE {unique} INDEX "{table}_pw_keyset" ON {fq(schema, table)} {keys}{split}')

    status, notes, ddl = check(schema, table)
    assert status == ACTION and any(problem in note for note in notes), notes
    assert ddl[0] == f'DROP INDEX "{schema}"."{table}_pw_keyset";'
    with cursor() as cur:
        assert keyset.validate_index(cur, schema, table, ['id'], 3)[0] is False

    apply(ddl)
    assert check(schema, table)[0] == OK


def test_an_index_left_invalid_by_a_failed_build_is_replaced(schema):
    table = 'ix_invalid'
    run_sql(f'CREATE TABLE {fq(schema, table)} (id bigint PRIMARY KEY, ts timestamptz NOT NULL)',
            f"INSERT INTO {fq(schema, table)} SELECT g, '2026-01-01' FROM generate_series(1, 50) g")
    # a unique build over duplicates fails and leaves the index behind, invalid
    with pytest.raises(psycopg2.Error):
        run_sql(f'CREATE UNIQUE INDEX "{table}_ts_pw_keyset" ON {fq(schema, table)} '
                f'(((yb_hash_code(id) % 3)) ASC, ts ASC) SPLIT AT VALUES ((1), (2))')

    status, notes, ddl = check(schema, table, 'INCREMENTAL', 'ts')
    assert status == ACTION
    assert any('backfill did not complete' in note for note in notes), notes
    apply(ddl)
    assert check(schema, table, 'INCREMENTAL', 'ts')[0] == OK


@pytest.mark.parametrize('keys,problem', [
    pytest.param('id ASC, updated_at ASC', 'orders by id, updated_at', id='wrong-order'),
    pytest.param('updated_at ASC', 'orders by updated_at but the scan orders by updated_at, id',
                 id='missing-trailing-pk'),
])
def test_a_broken_replication_key_index_is_dropped_and_recreated(schema, keys, problem):
    table = f'ix_rk_{uuid.uuid4().hex[:6]}'
    run_sql(f'CREATE TABLE {fq(schema, table)} (id bigint PRIMARY KEY, updated_at timestamptz NOT NULL)',
            f'CREATE INDEX "{table}_updated_at_pw_keyset" ON {fq(schema, table)} '
            f'(((yb_hash_code(id) % 3)) ASC, {keys}) SPLIT AT VALUES ((1), (2))')

    status, notes, ddl = check(schema, table, 'INCREMENTAL', 'updated_at')
    assert status == ACTION and any(problem in note for note in notes), notes
    assert ddl[0].startswith('DROP INDEX')
    apply(ddl)
    assert check(schema, table, 'INCREMENTAL', 'updated_at')[0] == OK


# ---------------------------------------------------------------- index B

def sync_incremental(cfg, table, key, limit=None):
    stream = discover(cfg, table, 'INCREMENTAL', key)
    cfg = dict(cfg, limit=limit)
    return run_sync(tap_yugabyte.do_sync_incremental, cfg, stream,
                    {'bookmarks': {stream['tap_stream_id']: {}}})


def incremental_plan(schema, table, key, pk, buckets=keyset.BUCKETS_DEFAULT):
    sql = incremental._get_select_sql({
        'escaped_columns': ['*'], 'replication_key': key, 'replication_key_sql_datatype': None,
        'replication_key_value': None, 'schema_name': schema, 'table_name': table,
        'limit': None, 'pk_columns': pk, 'keyset_buckets': buckets})
    return plan_text(sql.strip().rstrip(';'), buckets)


@pytest.mark.parametrize('table,columns,values,key,pk,expected_note', [
    pytest.param('rk_trigger', 'id bigint PRIMARY KEY, updated_at timestamptz NOT NULL',
                 "g, $$2026-01-01$$::timestamptz + g * interval '1 second'", 'updated_at', ['id'],
                 'may be maintained by', id='updated-at-with-trigger'),
    pytest.param('rk_notrigger', 'id bigint PRIMARY KEY, updated_at timestamptz NOT NULL',
                 "g, $$2026-01-01$$::timestamptz + g * interval '1 second'", 'updated_at', ['id'],
                 'no row-level UPDATE trigger', id='updated-at-without-trigger'),
    pytest.param('rk_composite', 'tenant_id int, id bigint, updated_at timestamptz NOT NULL, '
                 'PRIMARY KEY ((tenant_id) HASH, id ASC)',
                 "g % 4, g, $$2026-01-01$$::timestamptz + g * interval '1 second'", 'updated_at',
                 ['tenant_id', 'id'], 'transaction start time', id='composite-primary-key'),
    pytest.param('rk_nonunique', 'id bigint PRIMARY KEY, created_at timestamptz NOT NULL',
                 "g, $$2026-01-01$$::timestamptz + (g / 10) * interval '1 second'", 'created_at',
                 ['id'], 'is not unique; the index ends in id', id='non-unique-key'),
])
def test_replication_key_index_is_created_and_used(schema, table, columns, values, key, pk,
                                                   expected_note):
    run_sql(f'CREATE TABLE {fq(schema, table)} ({columns})',
            f'INSERT INTO {fq(schema, table)} SELECT {values} FROM generate_series(1, {ROWS}) g',
            f'ANALYZE {fq(schema, table)}')
    if table == 'rk_trigger':
        run_sql(f'CREATE FUNCTION {fq(schema, "touch")}() RETURNS trigger AS '
                f'$$BEGIN NEW.updated_at = now(); RETURN NEW; END$$ LANGUAGE plpgsql',
                f'CREATE TRIGGER rk_touch BEFORE UPDATE ON {fq(schema, table)} '
                f'FOR EACH ROW EXECUTE FUNCTION {fq(schema, "touch")}()')

    assert_index_becomes_ok(schema, table, 'INCREMENTAL', key)
    _, notes, _ = check(schema, table, 'INCREMENTAL', key)
    assert any(expected_note in note for note in notes), notes

    records = sync_incremental(conn_config(schema), table, key)
    assert len(set(keys_of(records, pk))) == ROWS

    plan = incremental_plan(schema, table, key, pk)
    assert 'Merge Append' in plan, plan
    assert stored_name(keyset.replication_key_index_name(table, key)) in plan, plan


def test_a_nullable_replication_key_blocks(schema):
    run_sql(f'CREATE TABLE {fq(schema, "rk_nullable")} (id bigint PRIMARY KEY, updated_at timestamptz)')
    status, notes, _ = check(schema, 'rk_nullable', 'INCREMENTAL', 'updated_at')
    assert status == BLOCK and any('is nullable' in note for note in notes), notes


@pytest.mark.parametrize('cache,warned', [(100, True), (1, False)])
def test_a_sequence_key_reports_its_cache(schema, cache, warned):
    table = f'rk_seq_{cache}'
    run_sql(f'CREATE SEQUENCE {fq(schema, table + "_seq")} CACHE {cache}',
            f'CREATE TABLE {fq(schema, table)} (id bigint PRIMARY KEY '
            f"DEFAULT nextval('{fq(schema, table + '_seq')}'), v int)")
    assert_index_becomes_ok(schema, table, 'INCREMENTAL', 'id')
    _, notes, _ = check(schema, table, 'INCREMENTAL', 'id')
    assert any(f'caches {cache} values' in note for note in notes) is warned, notes
    assert any('No second index' in note for note in notes)


def test_an_identity_key_reuses_the_primary_key_index(schema):
    table = 'rk_identity'
    run_sql(f'CREATE TABLE {fq(schema, table)} (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v int)')
    assert_index_becomes_ok(schema, table, 'INCREMENTAL', 'id')
    with cursor() as cur:
        cur.execute(f'INSERT INTO {fq(schema, table)} (v) SELECT g FROM generate_series(1, {ROWS}) g')
    assert len(sync_incremental(conn_config(schema), table, 'id')) == ROWS


def test_a_tie_group_larger_than_the_limit_blocks(schema):
    table = 'rk_ties'
    run_sql(f'CREATE TABLE {fq(schema, table)} (id bigint PRIMARY KEY, updated_at timestamptz NOT NULL)',
            f"INSERT INTO {fq(schema, table)} SELECT g, '2026-01-01' FROM generate_series(1, 20000) g",
            f'ANALYZE {fq(schema, table)}')
    assert check(schema, table, 'INCREMENTAL', 'updated_at', limit=10000)[0] == BLOCK
    status, notes, _ = check(schema, table, 'INCREMENTAL', 'updated_at')
    assert status == ACTION
    assert any('updated_at' in note and 'share one' in note for note in notes), notes


# --------------------------------------------------------------------- CLI

def test_the_command_exits_one_until_its_own_ddl_is_applied(schema, tmp_path):
    command = [os.path.join(os.path.dirname(sys.executable), 'tap-yugabyte-index-check')]
    table = f'cli_{uuid.uuid4().hex[:6]}'
    run_sql(f'CREATE TABLE {fq(schema, table)} (id bigint PRIMARY KEY, updated_at timestamptz NOT NULL)')
    config = tmp_path / 'tap.yml'
    config.write_text(
        f'db_conn: {{keyset_buckets: 3}}\n'
        f'schemas:\n  - source_schema: {schema}\n    tables:\n'
        f'      - {{table_name: {table}, replication_method: FULL_TABLE}}\n'
        f'      - {{table_name: {table}, replication_method: INCREMENTAL, replication_key: updated_at}}\n')
    cfg = get_test_connection_config()
    args = [*command, str(config), '--host', cfg['host'], '--port', str(cfg['port']),
            '--user', cfg['user'], '--dbname', cfg['dbname'], '--target-type', 'target-s3-csv']
    env = dict(os.environ, PGPASSWORD=cfg['password'])

    first = subprocess.run(args, env=env, capture_output=True, text=True, check=False)
    assert first.returncode == 1, first.stdout + first.stderr
    ddl = [line.strip() for line in first.stdout.split('All DDL, to hand to the service owner:')[1]
           .splitlines() if line.strip().startswith('CREATE')]
    assert len(ddl) == 2
    apply(ddl)

    second = subprocess.run(args, env=env, capture_output=True, text=True, check=False)
    assert second.returncode == 0, second.stdout + second.stderr
    assert 'Worst status: OK' in second.stdout
