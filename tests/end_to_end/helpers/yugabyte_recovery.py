"""YugabyteDB slot and change-shape helpers shared by the LOG_BASED recovery E2E tests."""

import re

from dataclasses import dataclass

# fastsync.commons.utils imports the CLI, which imports tap_yugabyte; loading it first avoids that import cycle.
from pipelinewise.fastsync.commons import utils as _fastsync_utils  # noqa: F401
from pipelinewise.fastsync.commons.tap_yugabyte import FastSyncTapYugabyte


# Matches tap-yugabyte's SLOT_FEEDBACK_LSN_OFFSET: the slot is acknowledged two below the target's COMMIT LSN.
SLOT_FEEDBACK_LSN_OFFSET = 2
LATEST_COMMITTED_LSN = re.compile(r'Latest committed wal message received was (\d+)')


def slot_config(e2e, tap_id):
    """Return the connection config FastSync uses to administer a tap's replication slot."""
    return {
        'host': e2e.get_conn_env_var('TAP_YUGABYTE', 'HOST'),
        'port': e2e.get_conn_env_var('TAP_YUGABYTE', 'PORT'),
        'user': e2e.get_conn_env_var('TAP_YUGABYTE', 'USER'),
        'password': e2e.get_conn_env_var('TAP_YUGABYTE', 'PASSWORD'),
        'dbname': e2e.get_conn_env_var('TAP_YUGABYTE', 'DB'),
        'tap_id': tap_id,
    }


def slot_name(e2e, tap_id):
    return FastSyncTapYugabyte.generate_replication_slot_name(e2e.get_conn_env_var('TAP_YUGABYTE', 'DB'), tap_id)


def drop_slot(e2e, tap_id):
    """Drop a tap's slot, waiting out YugabyteDB's active-slot window like a tap removal does."""
    FastSyncTapYugabyte.drop_slot(slot_config(e2e, tap_id))


def slot_status(e2e, slot_name):
    """Return the slot's acknowledged LSN and restart HybridTime, if the slot exists."""
    rows = e2e.run_query_tap_yugabyte(
        """
        SELECT active,
               (confirmed_flush_lsn - '0/0'::pg_lsn)::numeric,
               yb_restart_commit_ht
          FROM pg_replication_slots
         WHERE slot_name = %s
        """,
        (slot_name,),
    )
    if not rows:
        return None
    active, confirmed_flush_lsn, restart_commit_ht = rows[0]
    return {
        'active': active,
        'confirmed_flush_lsn': int(confirmed_flush_lsn),
        'restart_commit_ht': int(restart_commit_ht),
    }


def assert_slot_behind_state(e2e, slot_name, state_lsns):
    """The slot may confirm only transactions that every stream's target state already covers."""
    slot = slot_status(e2e, slot_name)
    assert slot is not None
    assert slot['confirmed_flush_lsn'] <= min(state_lsns) - SLOT_FEEDBACK_LSN_OFFSET
    return slot


def latest_committed_lsn(log_path):
    """Return the newest COMMIT LSN tap-yugabyte reports having consumed, or 0 before its first report."""
    lsns = LATEST_COMMITTED_LSN.findall(log_path.read_text(encoding='utf-8'))
    return max(map(int, lsns), default=0)


class KeyAllocator:
    """Hand out primary keys no earlier scenario in the module has used."""

    def __init__(self, start=10 ** 6):
        self._next = start

    def take(self, count):
        start = self._next
        self._next += count
        return range(start, start + count)


@dataclass
class MixedChanges:
    """Change shapes a historical export plus replay must reconcile, each committed as its own transaction."""

    seed: tuple
    old_keys: list
    new_keys: list
    statements: list


def mixed_changes(table, keys):
    """Plan a key change, delete, insert, delete and reinsert, partial update and update-then-key-change.

    `table` is the qualified source table. The seed statement creates the rows the changes act on.
    """
    moved, deleted, reinserted, patched, chained = keys.take(5)
    inserted, moved_to, chained_to = keys.take(3)
    old_keys = [moved, deleted, reinserted, patched, chained]
    return MixedChanges(
        seed=(
            f'INSERT INTO {table} (id, payload, version) SELECT k, md5(k::text), 0 FROM unnest(%s::int[]) AS k',
            (old_keys,),
        ),
        old_keys=old_keys,
        new_keys=[inserted, moved_to, chained_to],
        statements=[
            (f'UPDATE {table} SET id = %s WHERE id = %s', (moved_to, moved)),
            (f'DELETE FROM {table} WHERE id = %s', (deleted,)),
            (f'INSERT INTO {table} (id, payload, version) VALUES (%s, %s, 0)',
             (inserted, 'inserted after the boundary')),
            (f'DELETE FROM {table} WHERE id = %s', (reinserted,)),
            (f'INSERT INTO {table} (id, payload, version) VALUES (%s, %s, 7)',
             (reinserted, 'reinserted under the same key')),
            (f'UPDATE {table} SET version = version + 1 WHERE id = %s', (patched,)),
            (f'UPDATE {table} SET payload = %s WHERE id = %s', ('updated before moving', chained)),
            (f'UPDATE {table} SET id = %s WHERE id = %s', (chained_to, chained)),
        ],
    )


def commit_each(e2e, statements):
    for statement, params in statements:
        e2e.run_query_tap_yugabyte(statement, params)


def acknowledge_source_changes(e2e, table, run_tap):
    """Move the slot past every committed change.

    The tap acknowledges a target state in the next run, and two below its COMMIT LSN, so the last delivered
    transaction always replays; a trailing no-op transaction keeps the real changes inside the acknowledgement.
    """
    e2e.run_query_tap_yugabyte(f'UPDATE {table} SET version = version WHERE id = (SELECT MIN(id) FROM {table})')
    run_tap()
    run_tap()


def assert_export_predates(exported_keys, changes):
    """A slot-pinned export holds every changed row's old key and none of the new keys."""
    assert set(changes.old_keys) <= exported_keys
    assert not exported_keys & set(changes.new_keys)
