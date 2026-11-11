"""Cut a LOG_BASED tap over from tap-postgres to tap-yugabyte without a resync.

The tap keeps its id and target. Snowflake (or PostgreSQL) already holds every PostgreSQL change, YugabyteDB holds
the same data, and the tap's bookmarks are pointed at a new YugabyteDB slot instead of re-copying the tables.
"""

import contextlib
import datetime
import glob
import json
import os
import shutil
import sys

from dataclasses import dataclass, field
from pathlib import Path

import yaml

# fastsync.commons.utils imports the CLI, which imports the taps; loading it first avoids that import cycle.
from pipelinewise.fastsync.commons import utils as _fastsync_utils  # noqa: F401
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


TABLE = 'ledger_entries'
COLUMNS = ('id', 'payload', 'version', 'amount', 'updated_at', 'flag')
INITIAL_ROWS = 500
TABLE_DDL = (
    'CREATE TABLE {table} ('
    'id integer PRIMARY KEY, payload text NOT NULL, version integer NOT NULL, '
    'amount numeric(12,2), updated_at timestamptz, flag boolean)'
)
SEED_ROWS = (
    "INSERT INTO {table} (id, payload, version, amount, updated_at, flag) "
    "SELECT k, md5(k::text), 0, k * 1.25, "
    "timestamptz '2026-01-01 00:00:00+00' + k * interval '1 minute', mod(k, 2) = 0 "
    "FROM generate_series(1, %s) AS k"
)


def _utc(value):
    """Compare timestamps by instant: targets may return naive UTC or zone-aware values."""
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value.astimezone(datetime.timezone.utc)
    return value


def normalise(rows):
    return [tuple(_utc(value) for value in row) for row in rows]


class PostgresTarget:
    """target-postgres needs every column of an update, so YugabyteDB tables use REPLICA IDENTITY DEFAULT."""

    name = 'pg'
    target_id = 'yb_migration_pg_dwh'
    replica_identity = 'DEFAULT'

    def __init__(self, e2e):
        self.e2e = e2e

    def schema(self, tap_id):
        return f'ppw_e2e_{tap_id}_target'

    def rows(self, tap_id):
        return normalise(self.e2e.run_query_target_postgres(
            f'SELECT {", ".join(COLUMNS)} FROM {self.schema(tap_id)}.{TABLE} ORDER BY id'
        ))

    def deleted_marker_count(self, tap_id):
        return self.e2e.run_query_target_postgres(
            f'SELECT COUNT(*) FROM {self.schema(tap_id)}.{TABLE} WHERE _sdc_deleted_at IS NOT NULL'
        )[0][0]

    def column_types(self, tap_id):
        return self.e2e.run_query_target_postgres(
            'SELECT column_name, data_type, numeric_precision, numeric_scale FROM information_schema.columns '
            f"WHERE table_schema = '{self.schema(tap_id)}' AND table_name = '{TABLE}' ORDER BY column_name"
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

    def _table(self, tap_id):
        return f'"{self.schema(tap_id).upper()}"."{TABLE.upper()}"'

    def rows(self, tap_id):
        columns = ', '.join(f'"{column.upper()}"' for column in COLUMNS)
        return normalise(self.e2e.run_query_target_snowflake(
            f'SELECT {columns} FROM {self._table(tap_id)} ORDER BY "ID"'
        ))

    def deleted_marker_count(self, tap_id):
        return self.e2e.run_query_target_snowflake(
            f'SELECT COUNT(*) FROM {self._table(tap_id)} WHERE "_SDC_DELETED_AT" IS NOT NULL'
        )[0][0]

    def column_types(self, tap_id):
        return self.e2e.run_query_target_snowflake(
            'SELECT column_name, data_type, numeric_precision, numeric_scale FROM information_schema.columns '
            f"WHERE table_schema = '{self.schema(tap_id).upper()}' AND table_name = '{TABLE.upper()}' "
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
    keys: KeyAllocator = field(default_factory=KeyAllocator)

    @property
    def source_schema(self):
        return f'ppw_e2e_{self.tap_id}'

    @property
    def source_table(self):
        return f'{self.source_schema}.{TABLE}'

    @property
    def stream_id(self):
        return f'{self.source_schema}-{TABLE}'

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
        db_conn['fastsync_parallelism'] = 1
        if source == 'postgres':
            db_conn['logical_poll_total_seconds'] = 10
        else:
            db_conn.update({'logical_poll_total_seconds': 20, 'break_at_end_lsn': False, 'max_run_seconds': 90})
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
                'tables': [{'table_name': TABLE, 'replication_method': 'LOG_BASED'}],
            }],
        }

    def fastsync_logs(self):
        log_dir = self.tap_dir / 'log'
        return sorted(glob.glob(str(log_dir / '*.fastsync.log*')))

    def bookmark(self):
        return read_state(self.state_path)['bookmarks'][self.stream_id]

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
        for run_query in (self.e2e.run_query_tap_postgres, self.e2e.run_query_tap_yugabyte):
            run_query(f'DROP SCHEMA IF EXISTS {self.source_schema} CASCADE')
            run_query(f'CREATE SCHEMA {self.source_schema}')
            run_query(TABLE_DDL.format(table=self.source_table))
        if self.target.replica_identity:
            # The identity is fixed when the slot is created, so it must be set before the slot exists.
            self.e2e.run_query_tap_yugabyte(
                f'ALTER TABLE {self.source_table} REPLICA IDENTITY {self.target.replica_identity}'
            )
        self.e2e.run_query_tap_postgres(SEED_ROWS.format(table=self.source_table), (INITIAL_ROWS,))

    def drop_source_schemas(self):
        self.e2e.run_query_tap_postgres(f'DROP SCHEMA IF EXISTS {self.source_schema} CASCADE')
        self.e2e.run_query_tap_yugabyte(f'DROP SCHEMA IF EXISTS {self.source_schema} CASCADE')

    def source_rows(self, run_query):
        return normalise(run_query(f'SELECT {", ".join(COLUMNS)} FROM {self.source_table} ORDER BY id'))

    def copy_postgres_to_yugabyte(self):
        """Stand in for the data migration: make YugabyteDB hold exactly PostgreSQL's rows."""
        rows = self.source_rows(self.e2e.run_query_tap_postgres)
        columns = list(zip(*rows))
        self.e2e.run_query_tap_yugabyte(f'DELETE FROM {self.source_table}')
        self.e2e.run_query_tap_yugabyte(
            f'INSERT INTO {self.source_table} ({", ".join(COLUMNS)}) '
            'SELECT * FROM unnest(%s::int[], %s::text[], %s::int[], %s::numeric[], %s::timestamptz[], %s::bool[])',
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

    # ---- Assertions ----------------------------------------------------------------------------------------------

    def assert_target_matches(self, run_query):
        source_rows = self.source_rows(run_query)
        target_rows = self.target.rows(self.tap_id)
        assert source_rows, 'the source has no rows to compare'
        assert target_rows == source_rows, (
            f'{len(source_rows)} source rows, {len(target_rows)} target rows; '
            f'missing {sorted(set(source_rows) - set(target_rows))[:5]}, '
            f'unexpected {sorted(set(target_rows) - set(source_rows))[:5]}'
        )
        assert self.target.deleted_marker_count(self.tap_id) == 0


def without_key_changes(changes):
    """Drop primary key changes from a plan that tap-postgres replicates.

    tap-postgres ignores the old key wal2json sends with an update, so a key change leaves the old row in the target.
    That predates this cutover and tap-yugabyte does not share it, so only YugabyteDB changes include key changes.
    """
    changes.statements = [(sql, params) for sql, params in changes.statements if ' SET id = ' not in sql]
    return changes


def run_cutover(scenario, *, slot_created_early):
    """Replicate from PostgreSQL, cut over to YugabyteDB, and prove nothing was copied again.

    With `slot_created_early`, the slot is created while YugabyteDB is still being kept in sync from PostgreSQL, so
    the first tap-yugabyte run replays changes the target already has.
    """
    e2e = scenario.e2e
    on_postgres, on_yugabyte = e2e.run_query_tap_postgres, e2e.run_query_tap_yugabyte

    # Before the switch: an ordinary tap-postgres tap with an initial load and some CDC.
    scenario.create_source_tables()
    scenario.import_tap('postgres')
    scenario.fast_sync()
    first = without_key_changes(mixed_changes(scenario.source_table, scenario.keys))
    on_postgres(*first.seed)
    commit_each(e2e, first.statements, run_query=on_postgres)
    scenario.run_tap()
    scenario.assert_target_matches(on_postgres)
    column_types = scenario.target.column_types(scenario.tap_id)
    fastsync_logs = scenario.fastsync_logs()
    assert fastsync_logs, 'the initial load did not go through FastSync'

    restart_commit_ht = None
    final = without_key_changes(mixed_changes(scenario.source_table, scenario.keys))
    if slot_created_early:
        # YugabyteDB is kept in sync from PostgreSQL, and the slot exists before the last synced batch.
        scenario.copy_postgres_to_yugabyte()
        restart_commit_ht = scenario.create_yugabyte_slot()
        for run_query in (on_postgres, on_yugabyte):
            run_query(*final.seed)
            commit_each(e2e, final.statements, run_query=run_query)
    else:
        on_postgres(*final.seed)
        commit_each(e2e, final.statements, run_query=on_postgres)

    # Writes are frozen; drain PostgreSQL past the position noted at the freeze.
    frozen_at = scenario.postgres_wal_position()
    scenario.run_tap()
    postgres_bookmark = scenario.bookmark()
    assert postgres_bookmark['lsn'] >= frozen_at, 'the bookmark does not cover the WAL position noted at the freeze'
    scenario.assert_target_matches(on_postgres)

    # YugabyteDB holds PostgreSQL's data, and the slot exists before the service writes to it.
    if not slot_created_early:
        scenario.copy_postgres_to_yugabyte()
        restart_commit_ht = scenario.create_yugabyte_slot()
    assert scenario.source_rows(on_yugabyte) == scenario.source_rows(on_postgres)

    # The service writes to YugabyteDB.
    after = mixed_changes(scenario.source_table, scenario.keys)
    on_yugabyte(*after.seed)
    commit_each(e2e, after.statements, run_query=on_yugabyte)

    # Same tap id, new type and connection. State and the PostgreSQL slot survive the import.
    scenario.import_tap('yugabyte')
    tap_config = json.loads((scenario.tap_dir / 'config.json').read_text(encoding='utf-8'))
    assert tap_config['host'] == e2e.get_conn_env_var('TAP_YUGABYTE', 'HOST')
    assert scenario.bookmark() == postgres_bookmark
    assert scenario.postgres_slot_exists()

    # Point the bookmark at the new slot and run.
    scenario.rewrite_bookmarks(restart_commit_ht)
    assert scenario.bookmark()['version'] == postgres_bookmark['version']
    acknowledge_source_changes(e2e, scenario.source_table, scenario.run_tap)

    scenario.assert_target_matches(on_yugabyte)
    assert scenario.fastsync_logs() == fastsync_logs, 'the cutover copied the table again'
    assert scenario.target.column_types(scenario.tap_id) == column_types
    slot = assert_slot_behind_state(e2e, scenario.yugabyte_slot, [scenario.bookmark()['lsn']])
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

    def make(tap_id):
        root = tmp_path_factory.mktemp(tap_id)
        project_dir = root / 'project'
        shutil.copytree(rendered, project_dir)
        config_dir = root / 'pipelinewise-config'
        config_dir.mkdir()
        command_env = os.environ.copy()
        command_env['PIPELINEWISE_CONFIG_DIRECTORY'] = str(config_dir)
        scenario = MigrationScenario(e2e, target, tap_id, project_dir, config_dir, command_env)
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
