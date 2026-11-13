"""Cut a tap over from tap-postgres to tap-yugabyte without a resync.

The tap keeps its id and target. The target already holds every PostgreSQL change and YugabyteDB holds the same
data. LOG_BASED bookmarks are pointed at a new YugabyteDB slot instead of re-copying the tables. INCREMENTAL
bookmarks hold a value from the data, so they carry over unchanged. FULL_TABLE tables have no bookmark: every run
copies them, from YugabyteDB once the tap has switched.
"""

import ast
import contextlib
import datetime
import decimal
import glob
import json
import os
import re
import shutil
import sys

from dataclasses import dataclass, field
from pathlib import Path

import yaml

# fastsync.commons.utils imports the CLI, which imports the taps; loading it first avoids that import cycle.
from pipelinewise.fastsync.commons import utils as _fastsync_utils  # noqa: F401
from pipelinewise.cli import VENV_DIR
from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres

from .env import E2EEnv
from .pipeline_process import read_state, run_cleanups, run_success
from .yugabyte_recovery import (
    KeyAllocator,
    acknowledge_source_changes,
    assert_slot_behind_state,
    commit_each,
    drop_slot,
    mixed_changes,
    slot_name,
    slot_status,
)


LOG_BASED, INCREMENTAL, FULL_TABLE = 'LOG_BASED', 'INCREMENTAL', 'FULL_TABLE'
INDEX_BUCKETS = 3


@dataclass(frozen=True)
class Table:
    """One replicated table, created identically in PostgreSQL and YugabyteDB."""

    name: str
    method: str
    columns: tuple
    array_types: tuple
    definition: str
    seed: str
    key: str = None


LEDGER = Table(
    'ledger_entries', LOG_BASED,
    ('id', 'payload', 'version', 'amount', 'updated_at', 'flag'),
    ('int', 'text', 'int', 'numeric', 'timestamptz', 'bool'),
    'id integer PRIMARY KEY, payload text NOT NULL, version integer NOT NULL, '
    'amount numeric(12,2), updated_at timestamptz, flag boolean',
    "SELECT k, md5(k::text), 0, k * 1.25, timestamptz '2026-01-01 00:00:00+00' + k * interval '1 minute', "
    'mod(k, 2) = 0 FROM generate_series(1, 500) AS k',
)
# Replication key values carry microseconds and six decimal places, so a truncated or rounded bookmark shows.
KEY_VALUE_SQL = {
    'updated_at': "timestamptz '2026-03-01 00:00:00+00' + {k} * interval '1 second' "
                  "+ (mod({k} * 7919, 999999) + 1) * interval '1 microsecond'",
    'change_seq': '1000000 + {k} + (mod({k}, 997) + 1) * 0.000001',
}
INCREMENTAL_SEED_ROWS = 200
EVENTS_BY_TIME = Table(
    'events_by_time', INCREMENTAL,
    ('id', 'payload', 'updated_at'), ('int', 'text', 'timestamptz'),
    'id integer PRIMARY KEY, payload text NOT NULL, updated_at timestamptz NOT NULL',
    f"SELECT k, md5(k::text), {KEY_VALUE_SQL['updated_at'].format(k='k')} "
    f'FROM generate_series(1, {INCREMENTAL_SEED_ROWS}) AS k',
    key='updated_at',
)
EVENTS_BY_SEQ = Table(
    'events_by_seq', INCREMENTAL,
    ('id', 'payload', 'change_seq'), ('int', 'text', 'numeric'),
    'id integer PRIMARY KEY, payload text NOT NULL, change_seq numeric(20,6) NOT NULL',
    f"SELECT k, md5(k::text), {KEY_VALUE_SQL['change_seq'].format(k='k')} "
    f'FROM generate_series(1, {INCREMENTAL_SEED_ROWS}) AS k',
    key='change_seq',
)
LOOKUP_CODES = Table(
    'lookup_codes', FULL_TABLE,
    ('id', 'label', 'weight'), ('int', 'text', 'numeric'),
    'id integer PRIMARY KEY, label text NOT NULL, weight numeric(10,2)',
    "SELECT k, 'code ' || k, k * 0.5 FROM generate_series(1, 50) AS k",
)
LOG_BASED_TABLES = (LEDGER,)
MIXED_TABLES = (LEDGER, EVENTS_BY_TIME, EVENTS_BY_SEQ, LOOKUP_CODES)
# FULL_TABLE is copied on every run, so on a slow target it is left to the targets where it is cheap: the switch
# changes only the source it is copied from.
MIXED_TABLES_WITHOUT_FULL_TABLE = (LEDGER, EVENTS_BY_TIME, EVENTS_BY_SEQ)


def _utc(value):
    """Compare timestamps by instant: targets may return naive UTC or zone-aware values."""
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value.astimezone(datetime.timezone.utc)
    return value


def normalise(rows):
    return [tuple(_utc(value) for value in row) for row in rows]


def bookmark_value(table, value):
    """Read an INCREMENTAL bookmark as the value it stands for, failing on a bookmark tap-yugabyte would reject."""
    if table.key == 'updated_at':
        return _utc(datetime.datetime.fromisoformat(value))
    assert isinstance(value, str), f'decimal bookmark {value!r} is a {type(value).__name__}, not an exact string'
    return decimal.Decimal(value)


def sync_selection(run_tap_output):
    """Return the streams one `run_tap` sent to FastSync and to Singer, from its log."""
    selected = {}
    for route in ('fastsync/partialsync', 'singer'):
        match = re.search(rf'selected to sync by {route}: (\[.*?\])', run_tap_output)
        selected[route] = set(ast.literal_eval(match.group(1))) if match else set()
    return selected['fastsync/partialsync'], selected['singer']


class PostgresTarget:
    """target-postgres needs every column of an update, so YugabyteDB tables use REPLICA IDENTITY DEFAULT."""

    name = 'pg'
    target_id = 'yb_migration_pg_dwh'
    replica_identity = 'DEFAULT'

    def __init__(self, e2e):
        self.e2e = e2e

    def schema(self, tap_id):
        return f'ppw_e2e_{tap_id}_target'

    def rows(self, tap_id, table):
        return normalise(self.e2e.run_query_target_postgres(
            f'SELECT {", ".join(table.columns)} FROM {self.schema(tap_id)}.{table.name} ORDER BY id'
        ))

    def deleted_marker_count(self, tap_id, table):
        return self.e2e.run_query_target_postgres(
            f'SELECT COUNT(*) FROM {self.schema(tap_id)}.{table.name} WHERE _sdc_deleted_at IS NOT NULL'
        )[0][0]

    def column_types(self, tap_id, table):
        return self.e2e.run_query_target_postgres(
            'SELECT column_name, data_type, numeric_precision, numeric_scale FROM information_schema.columns '
            f"WHERE table_schema = '{self.schema(tap_id)}' AND table_name = '{table.name}' ORDER BY column_name"
        )

    def drop_schema(self, tap_id):
        self.e2e.run_query_target_postgres(f'DROP SCHEMA IF EXISTS {self.schema(tap_id)} CASCADE')


class SnowflakeTarget:
    """Snowflake keeps columns a CHANGE-identity update omits, so YugabyteDB keeps its default replica identity."""

    name = 'sf'
    target_id = 'yb_migration_sf_dwh'
    replica_identity = None

    def __init__(self, e2e):
        self.e2e = e2e

    def schema(self, tap_id):
        return f'ppw_e2e_{tap_id}{self.e2e.sf_schema_postfix}'

    def _table(self, tap_id, table):
        return f'"{self.schema(tap_id).upper()}"."{table.name.upper()}"'

    def rows(self, tap_id, table):
        columns = ', '.join(f'"{column.upper()}"' for column in table.columns)
        return normalise(self.e2e.run_query_target_snowflake(
            f'SELECT {columns} FROM {self._table(tap_id, table)} ORDER BY "ID"'
        ))

    def deleted_marker_count(self, tap_id, table):
        return self.e2e.run_query_target_snowflake(
            f'SELECT COUNT(*) FROM {self._table(tap_id, table)} WHERE "_SDC_DELETED_AT" IS NOT NULL'
        )[0][0]

    def column_types(self, tap_id, table):
        return self.e2e.run_query_target_snowflake(
            'SELECT column_name, data_type, numeric_precision, numeric_scale FROM information_schema.columns '
            f"WHERE table_schema = '{self.schema(tap_id).upper()}' AND table_name = '{table.name.upper()}' "
            'ORDER BY column_name'
        )

    def drop_schema(self, tap_id):
        self.e2e.run_query_target_snowflake(f'DROP SCHEMA IF EXISTS "{self.schema(tap_id).upper()}" CASCADE')


@dataclass
class MigrationScenario:
    """One tap that starts on PostgreSQL and moves to YugabyteDB under the same id and target."""

    e2e: object
    target: object
    tap_id: str
    project_dir: Path
    config_dir: Path
    command_env: dict
    tables: tuple = LOG_BASED_TABLES
    keys: KeyAllocator = field(default_factory=KeyAllocator)
    key_counters: dict = field(default_factory=dict)

    @property
    def source_schema(self):
        return f'ppw_e2e_{self.tap_id}'

    def source_table(self, table):
        return f'{self.source_schema}.{table.name}'

    def stream_id(self, table):
        return f'{self.source_schema}-{table.name}'

    def streams(self, *methods):
        return {self.stream_id(table) for table in self.tables if table.method in methods}

    def tables_of(self, method):
        return [table for table in self.tables if table.method == method]

    @property
    def tap_dir(self):
        return self.config_dir / self.target.target_id / self.tap_id

    @property
    def state_path(self):
        return self.tap_dir / 'state.json'

    @property
    def yugabyte_slot(self):
        return slot_name(self.e2e, self.tap_id)

    # ---- PipelineWise --------------------------------------------------------------------------------------------

    def pipelinewise(self, *arguments):
        return run_success(['pipelinewise', *arguments], self.command_env, timeout=600)

    def run_tap(self):
        return self.pipelinewise('run_tap', '--tap', self.tap_id, '--target', self.target.target_id)

    def fast_sync(self):
        return self.pipelinewise('fast_sync', '--tap', self.tap_id, '--target', self.target.target_id)

    def import_tap(self, source):
        """Write the tap YAML for `source` ('postgres' or 'yugabyte') over the previous one and import it."""
        (self.project_dir / f'tap_{self.tap_id}.yml').write_text(
            yaml.safe_dump(self._tap_config(source), sort_keys=False), encoding='utf-8'
        )
        self.pipelinewise('validate', '--dir', str(self.project_dir))
        self.pipelinewise('import_config', '--dir', str(self.project_dir))

    def _tap_config(self, source):
        connector = {'postgres': 'TAP_POSTGRES', 'yugabyte': 'TAP_YUGABYTE'}[source]
        db_conn = {
            key.lower(): self.e2e.get_conn_env_var(connector, key)
            for key in ('HOST', 'PORT', 'USER', 'PASSWORD', 'DB')
        }
        db_conn['dbname'] = db_conn.pop('db')
        db_conn['port'] = int(db_conn['port'])
        db_conn['fastsync_parallelism'] = len(self.tables)
        if source == 'postgres':
            db_conn['logical_poll_total_seconds'] = 10
        else:
            db_conn.update({'logical_poll_total_seconds': 20, 'break_at_end_lsn': False, 'max_run_seconds': 90})
        tables = []
        for table in self.tables:
            entry = {'table_name': table.name, 'replication_method': table.method}
            if table.key:
                entry['replication_key'] = table.key
            tables.append(entry)
        return {
            'id': self.tap_id,
            'name': f'YugabyteDB migration {self.tap_id}',
            'type': f'tap-{source}',
            'owner': 'test-runner',
            'db_conn': db_conn,
            'target': self.target.target_id,
            'batch_size_rows': 1000,
            'stream_buffer_size': 0,
            'schemas': [{
                'source_schema': self.source_schema,
                'target_schema': self.target.schema(self.tap_id),
                'tables': tables,
            }],
        }

    def check_yugabyte_indexes(self):
        """Run tap-yugabyte-index-check on the tap as it will be after the switch; every table must be OK."""
        config_path = self.project_dir.parent / 'index-check.yml'
        config_path.write_text(yaml.safe_dump(self._tap_config('yugabyte'), sort_keys=False), encoding='utf-8')
        output = run_success([
            os.path.join(VENV_DIR, 'tap-yugabyte', 'bin', 'tap-yugabyte-index-check'), str(config_path),
            '--host', self.e2e.get_conn_env_var('TAP_YUGABYTE', 'HOST'),
            '--port', str(self.e2e.get_conn_env_var('TAP_YUGABYTE', 'PORT')),
            '--user', self.e2e.get_conn_env_var('TAP_YUGABYTE', 'USER'),
            '--dbname', self.e2e.get_conn_env_var('TAP_YUGABYTE', 'DB'),
        ], {**self.command_env, 'PGPASSWORD': self.e2e.get_conn_env_var('TAP_YUGABYTE', 'PASSWORD')})
        for table in self.tables:
            assert re.search(rf'^OK\s+{re.escape(self.source_table(table))}\s', output, re.MULTILINE), output

    def fastsync_logs(self):
        log_dir = self.tap_dir / 'log'
        return sorted(glob.glob(str(log_dir / '*.fastsync.log*')))

    def bookmarks(self):
        return read_state(self.state_path)['bookmarks']

    def bookmark(self, table):
        return self.bookmarks()[self.stream_id(table)]

    # ---- Sources -------------------------------------------------------------------------------------------------

    def postgres_connection_config(self):
        return {
            'host': self.e2e.get_conn_env_var('TAP_POSTGRES', 'HOST'),
            'port': self.e2e.get_conn_env_var('TAP_POSTGRES', 'PORT'),
            'user': self.e2e.get_conn_env_var('TAP_POSTGRES', 'USER'),
            'password': self.e2e.get_conn_env_var('TAP_POSTGRES', 'PASSWORD'),
            'dbname': self.e2e.get_conn_env_var('TAP_POSTGRES', 'DB'),
            'tap_id': self.tap_id,
        }

    def drop_postgres_slot(self):
        """Drop the old PostgreSQL slot by hand: PipelineWise keeps it because the tap id did not change."""
        FastSyncTapPostgres.drop_slot(self.postgres_connection_config())

    def postgres_slot_exists(self):
        return bool(self.e2e.run_query_tap_postgres(
            'SELECT 1 FROM pg_replication_slots WHERE slot_name = %s',
            (FastSyncTapPostgres.generate_replication_slot_name(
                self.e2e.get_conn_env_var('TAP_POSTGRES', 'DB'), self.tap_id),),
        ))

    def create_source_tables(self):
        """Create every table in both databases, seed PostgreSQL, and index YugabyteDB's INCREMENTAL keys."""
        for run_query in (self.e2e.run_query_tap_postgres, self.e2e.run_query_tap_yugabyte):
            run_query(f'DROP SCHEMA IF EXISTS {self.source_schema} CASCADE')
            run_query(f'CREATE SCHEMA {self.source_schema}')
            for table in self.tables:
                run_query(f'CREATE TABLE {self.source_table(table)} ({table.definition})')
        for table in self.tables:
            self.e2e.run_query_tap_postgres(
                f'INSERT INTO {self.source_table(table)} ({", ".join(table.columns)}) {table.seed}'
            )
            if table.key:
                self.key_counters[table.name] = INCREMENTAL_SEED_ROWS + 1
                # The replication-key index tap-yugabyte needs to read INCREMENTAL tables without full scans.
                splits = ', '.join(f'({bucket})' for bucket in range(1, INDEX_BUCKETS))
                self.e2e.run_query_tap_yugabyte(
                    f'CREATE UNIQUE INDEX {table.name}_{table.key}_pw_keyset ON {self.source_table(table)} '
                    f'(((yb_hash_code(id) % {INDEX_BUCKETS})) ASC, {table.key} ASC, id ASC) '
                    f'SPLIT AT VALUES ({splits})'
                )
        if self.target.replica_identity:
            # The identity is fixed when the slot is created, so it must be set before the slot exists.
            for table in self.tables_of(LOG_BASED):
                self.e2e.run_query_tap_yugabyte(
                    f'ALTER TABLE {self.source_table(table)} REPLICA IDENTITY {self.target.replica_identity}'
                )

    def drop_source_schemas(self):
        self.e2e.run_query_tap_postgres(f'DROP SCHEMA IF EXISTS {self.source_schema} CASCADE')
        self.e2e.run_query_tap_yugabyte(f'DROP SCHEMA IF EXISTS {self.source_schema} CASCADE')

    def source_rows(self, run_query, table):
        return normalise(run_query(
            f'SELECT {", ".join(table.columns)} FROM {self.source_table(table)} ORDER BY id'
        ))

    def max_key(self, run_query, table):
        return _utc(run_query(f'SELECT max({table.key}) FROM {self.source_table(table)}')[0][0])

    def copy_postgres_to_yugabyte(self):
        """Stand in for the data migration: make YugabyteDB hold exactly PostgreSQL's rows."""
        for table in self.tables:
            columns = list(zip(*self.source_rows(self.e2e.run_query_tap_postgres, table)))
            casts = ', '.join(f'%s::{array_type}[]' for array_type in table.array_types)
            self.e2e.run_query_tap_yugabyte(f'DELETE FROM {self.source_table(table)}')
            self.e2e.run_query_tap_yugabyte(
                f'INSERT INTO {self.source_table(table)} ({", ".join(table.columns)}) SELECT * FROM unnest({casts})',
                tuple(list(column) for column in columns),
            )

    def postgres_wal_position(self):
        return int(self.e2e.run_query_tap_postgres("SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint")[0][0])

    def create_yugabyte_slot(self):
        """Create the slot by hand, as FastSync would: wal2json with HYBRID_TIME LSNs."""
        self.e2e.run_query_tap_yugabyte(
            "SELECT * FROM pg_create_logical_replication_slot(%s, 'wal2json', false, false, 'HYBRID_TIME')",
            (self.yugabyte_slot,),
        )
        plugin, lsn_type, restart_commit_ht = self.e2e.run_query_tap_yugabyte(
            'SELECT plugin, yb_lsn_type, yb_restart_commit_ht FROM pg_replication_slots WHERE slot_name = %s',
            (self.yugabyte_slot,),
        )[0]
        assert (plugin, lsn_type) == ('wal2json', 'HYBRID_TIME')
        return int(restart_commit_ht)

    def rewrite_bookmarks(self, restart_commit_ht):
        """Set every LOG_BASED bookmark to the slot's restart HybridTime and keep `version`, as FastSync would."""
        backup = self.state_path.with_name('state.json.before-yb-migration.bak')
        backup.write_text(self.state_path.read_text(encoding='utf-8'), encoding='utf-8')
        state = json.loads(backup.read_text(encoding='utf-8'))
        for bookmark in state['bookmarks'].values():
            if 'lsn' in bookmark:
                bookmark['lsn'] = restart_commit_ht
        self.state_path.write_text(json.dumps(state, indent=4), encoding='utf-8')

    # ---- Changes -------------------------------------------------------------------------------------------------

    def _next_key_sql(self, table):
        k = self.key_counters[table.name]
        self.key_counters[table.name] += 1
        return KEY_VALUE_SQL[table.key].format(k=k)

    def plan_changes(self, phase, *, on_yugabyte=False):
        """Plan one batch of changes for every table, to be committed on one or more sources.

        LOG_BASED gets the full mix of change shapes, except primary key changes while tap-postgres replicates it:
        tap-postgres ignores the old key of an update. INCREMENTAL gets inserts and updates with increasing keys, as
        it never captures deletes; on YugabyteDB one row is also updated to exactly the bookmark value, which both
        taps must read again. FULL_TABLE gets inserts, updates and deletes.
        """
        statements = []
        for table in self.tables:
            source_table = self.source_table(table)
            if table.method == LOG_BASED:
                changes = mixed_changes(source_table, self.keys)
                if not on_yugabyte:
                    changes = without_key_changes(changes)
                statements += [changes.seed, *changes.statements]
            elif table.method == INCREMENTAL:
                if on_yugabyte:
                    statements.append((
                        f'UPDATE {source_table} SET payload = %s, {table.key} = '
                        f'(SELECT max({table.key}) FROM {source_table}) WHERE id = %s',
                        ('updated to the bookmark value', 5),
                    ))
                for new_id in self.keys.take(2):
                    statements.append((
                        f'INSERT INTO {source_table} (id, payload, {table.key}) '
                        f'VALUES (%s, %s, {self._next_key_sql(table)})',
                        (new_id, f'inserted in phase {phase}'),
                    ))
                for existing_id in (10 * phase + 1, 10 * phase + 2):
                    statements.append((
                        f'UPDATE {source_table} SET payload = %s, {table.key} = {self._next_key_sql(table)} '
                        'WHERE id = %s',
                        (f'updated in phase {phase}', existing_id),
                    ))
            else:
                new_id, = self.keys.take(1)
                statements += [
                    (f'INSERT INTO {source_table} (id, label, weight) VALUES (%s, %s, 1.5)',
                     (new_id, f'inserted in phase {phase}')),
                    (f'UPDATE {source_table} SET label = %s WHERE id = %s', (f'updated in phase {phase}', phase)),
                    (f'DELETE FROM {source_table} WHERE id = %s', (40 + phase,)),
                ]
        return statements

    # ---- Assertions ----------------------------------------------------------------------------------------------

    def assert_target_matches(self, run_query):
        for table in self.tables:
            source_rows = self.source_rows(run_query, table)
            target_rows = self.target.rows(self.tap_id, table)
            assert source_rows, f'{table.name} has no source rows to compare'
            assert target_rows == source_rows, (
                f'{table.name}: {len(source_rows)} source rows, {len(target_rows)} target rows; '
                f'missing {sorted(set(source_rows) - set(target_rows))[:5]}, '
                f'unexpected {sorted(set(target_rows) - set(source_rows))[:5]}'
            )
            if table.method == LOG_BASED:
                assert self.target.deleted_marker_count(self.tap_id, table) == 0

    def assert_incremental_bookmarks_at_max(self, run_query):
        for table in self.tables_of(INCREMENTAL):
            stored = self.bookmark(table)['replication_key_value']
            assert bookmark_value(table, stored) == self.max_key(run_query, table), (
                f'{table.name} bookmark {stored!r} is not the largest {table.key} in the source'
            )

    def column_types(self):
        return {table.name: self.target.column_types(self.tap_id, table) for table in self.tables}


def without_key_changes(changes):
    """Drop primary key changes from a plan that tap-postgres replicates.

    tap-postgres ignores the old key wal2json sends with an update, so a key change leaves the old row in the target.
    That predates this cutover and tap-yugabyte does not share it, so only YugabyteDB changes include key changes.
    """
    changes.statements = [(sql, params) for sql, params in changes.statements if ' SET id = ' not in sql]
    return changes


def run_cutover(scenario, *, slot_created_early):
    """Replicate from PostgreSQL, cut over to YugabyteDB, and prove only FULL_TABLE tables were copied again.

    With `slot_created_early`, the slot is created while YugabyteDB is still being kept in sync from PostgreSQL, so
    the first tap-yugabyte run replays changes the target already has.
    """
    e2e = scenario.e2e
    on_postgres, on_yugabyte = e2e.run_query_tap_postgres, e2e.run_query_tap_yugabyte

    # Before the switch: an ordinary tap-postgres tap with an initial load and some changes.
    scenario.create_source_tables()
    scenario.import_tap('postgres')
    scenario.fast_sync()
    assert scenario.fastsync_logs(), 'the initial load did not go through FastSync'
    commit_each(e2e, scenario.plan_changes(1), run_query=on_postgres)
    scenario.run_tap()
    scenario.assert_target_matches(on_postgres)
    column_types = scenario.column_types()

    restart_commit_ht = None
    final = scenario.plan_changes(2)
    if slot_created_early:
        # YugabyteDB is kept in sync from PostgreSQL, and the slot exists before the last synced batch.
        scenario.copy_postgres_to_yugabyte()
        restart_commit_ht = scenario.create_yugabyte_slot()
        for run_query in (on_postgres, on_yugabyte):
            commit_each(e2e, final, run_query=run_query)
    else:
        commit_each(e2e, final, run_query=on_postgres)

    # Writes are frozen; drain PostgreSQL past the position noted at the freeze.
    frozen_at = scenario.postgres_wal_position()
    scenario.run_tap()
    postgres_bookmarks = scenario.bookmarks()
    for table in scenario.tables_of(LOG_BASED):
        assert scenario.bookmark(table)['lsn'] >= frozen_at, (
            f'{table.name} bookmark does not cover the WAL position noted at the freeze'
        )
    scenario.assert_incremental_bookmarks_at_max(on_postgres)
    scenario.assert_target_matches(on_postgres)

    # YugabyteDB holds PostgreSQL's data, and the slot exists before the service writes to it.
    if not slot_created_early:
        scenario.copy_postgres_to_yugabyte()
        restart_commit_ht = scenario.create_yugabyte_slot()
    for table in scenario.tables:
        assert scenario.source_rows(on_yugabyte, table) == scenario.source_rows(on_postgres, table)
    scenario.check_yugabyte_indexes()

    # The service writes to YugabyteDB.
    commit_each(e2e, scenario.plan_changes(3, on_yugabyte=True), run_query=on_yugabyte)

    # Same tap id, new type and connection. State and the PostgreSQL slot survive the import.
    scenario.import_tap('yugabyte')
    tap_config = json.loads((scenario.tap_dir / 'config.json').read_text(encoding='utf-8'))
    assert tap_config['host'] == e2e.get_conn_env_var('TAP_YUGABYTE', 'HOST')
    assert scenario.bookmarks() == postgres_bookmarks
    assert scenario.postgres_slot_exists()

    # Point the LOG_BASED bookmarks at the new slot; every other bookmark stays as tap-postgres left it.
    scenario.rewrite_bookmarks(restart_commit_ht)
    for stream_id, bookmark in scenario.bookmarks().items():
        before = postgres_bookmarks[stream_id]
        assert bookmark == ({**before, 'lsn': restart_commit_ht} if 'lsn' in before else before)

    outputs = []
    acknowledge_source_changes(e2e, scenario.source_table(LEDGER), lambda: outputs.append(scenario.run_tap()))

    scenario.assert_target_matches(on_yugabyte)
    for output in outputs:
        assert sync_selection(output) == (scenario.streams(FULL_TABLE), scenario.streams(LOG_BASED, INCREMENTAL)), (
            'a LOG_BASED or INCREMENTAL table was copied again, or a FULL_TABLE table was not'
        )
    assert scenario.column_types() == column_types
    for table in scenario.tables_of(INCREMENTAL):
        before = bookmark_value(table, postgres_bookmarks[scenario.stream_id(table)]['replication_key_value'])
        assert bookmark_value(table, scenario.bookmark(table)['replication_key_value']) > before
    scenario.assert_incremental_bookmarks_at_max(on_yugabyte)
    slot = assert_slot_behind_state(e2e, scenario.yugabyte_slot, [scenario.bookmark(LEDGER)['lsn']])
    assert slot['confirmed_flush_lsn'] > 0

    # Drop the PostgreSQL slot by hand.
    scenario.drop_postgres_slot()
    assert not scenario.postgres_slot_exists()
    return slot_status(e2e, scenario.yugabyte_slot)


@contextlib.contextmanager
def migration_scenarios(tmp_path_factory, template_dir, target_class):
    """Yield a factory of isolated scenarios; drop their slots and schemas once the module is done.

    Each scenario has its own tap, slots, schemas and PipelineWise config directory. YugabyteDB slots are dropped at
    module end, not per test: a slot reads as active for minutes after its consumer leaves, so by then only the
    last scenario's slot is still waiting out that window.
    """
    scenarios = []
    rendered = tmp_path_factory.mktemp('yb_migration_templates')
    shutil.copytree(template_dir, rendered, dirs_exist_ok=True)
    e2e = E2EEnv(rendered)
    target = target_class(e2e)

    def make(tap_id, tables=LOG_BASED_TABLES):
        root = tmp_path_factory.mktemp(tap_id)
        project_dir = root / 'project'
        shutil.copytree(rendered, project_dir)
        config_dir = root / 'pipelinewise-config'
        config_dir.mkdir()
        command_env = os.environ.copy()
        command_env['PIPELINEWISE_CONFIG_DIRECTORY'] = str(config_dir)
        scenario = MigrationScenario(e2e, target, tap_id, project_dir, config_dir, command_env, tables)
        drop_slot(e2e, tap_id)
        if scenario.postgres_slot_exists():
            scenario.drop_postgres_slot()
        target.drop_schema(tap_id)
        scenarios.append(scenario)
        return scenario

    try:
        yield e2e, make
    finally:
        cleanups = []
        for scenario in scenarios:
            cleanups += [
                (f'drop {scenario.tap_id} PostgreSQL slot',
                 lambda s=scenario: s.postgres_slot_exists() and s.drop_postgres_slot()),
                (f'drop {scenario.tap_id} YugabyteDB slot', lambda s=scenario: drop_slot(e2e, s.tap_id)),
                (f'drop {scenario.tap_id} source schemas', scenario.drop_source_schemas),
                (f'drop {scenario.tap_id} target schema', lambda s=scenario: target.drop_schema(s.tap_id)),
            ]
        run_cleanups(sys.exception(), cleanups)
