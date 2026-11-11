# Source identity and the reset_state boundary

## Intended behaviour

Tap state holds log positions (PostgreSQL LSNs, MariaDB/MySQL binlog file and
position) that only mean something on the server that wrote them. Applying them
to another server can skip changes without any error. This was the cause of a
data loss after a PostgreSQL host change without `reset_state`.

PipelineWise records the source of each PostgreSQL and MariaDB/MySQL tap with
LOG_BASED tables under `source_identity` in `state.json`. Every run that reads
or writes positions checks it first and stops on a mismatch. `reset_state` is
the only way to move state to a new source, and for PostgreSQL it must prove
that the tap's slot on the new primary covers the switchover boundary.

## Decisions and rationale

**PostgreSQL identity is host, system identifier and timeline.** A promoted
physical replica keeps the system identifier, so the identifier alone cannot
tell the old and new primary apart after a switchover. The timeline changes on
promotion. Two replicas promoted from the same parent share both, so the
configured host is added. `read_source_identity` reads the timeline with
`pg_walfile_name(pg_current_wal_lsn())`, which uses the insertion timeline on a
primary (PostgreSQL 15.12 `xlogfuncs.c:406`, 18.4 `xlogfuncs.c:452`).

**tap-postgres checks the identity on its replication connection.** The CLI
reads the identity on a separate connection before the tap starts. If DNS
changes in between, the tap would stream from another server. tap-postgres runs
`IDENTIFY_SYSTEM` on the connection that streams and compares it with the state
before `START_REPLICATION`, so no WAL is read and no feedback is sent. On a
primary `IDENTIFY_SYSTEM` reports the insertion timeline (`walsender.c:421` in
15.12, `:423` in 18.4, through `GetFlushRecPtr`), which is the same value the CLI
records.

**MySQL uses `server_uuid`. MariaDB uses host and `server_id`.** MariaDB has no
`server_uuid`. The host alone passes when a DNS name is repointed to another
server. `server_id` must differ between a primary and its replicas, so it
catches that case. We do not use MariaDB's `server_uid`, because it is a hash of
the port and MAC address (11.4.10 `sql/mysqld.cc:10072-10090`) and changes when
the host is replaced. If RDS changes `server_id` on a replacement, the tap
stops, which is the safe direction.

**State from before this release is trusted once.** Existing state has no
identity. Refusing it would stop every tap on upgrade. The first run records the
connected source and logs a warning when the state already holds log positions.
The rollout precondition in the changelog covers this: every LOG_BASED tap must
have run on its current source before the upgrade, and no switchover may be in
progress.

**The reset_state boundary is bounded on both sides.** The boundary is the new
primary's `pg_current_wal_lsn()`, read before any service user is unlocked. It
is not `restart_lsn_mappings`. The slot must start at or before it, otherwise
commits in between are skipped. The boundary must not be ahead of the new
primary's current WAL position, which rejects typos and values from another
server.

**FastSync and reset_state share one slot check.** `read_slot` and
`finished_slot_start` in `fastsync/commons/tap_postgres.py` decide whether a slot
belongs to the tap (persistent, logical, `wal2json`, tap database) and whether it
is finished (`confirmed_flush_lsn` set, no active backend). `resolve_slot_name`
picks the legacy name first in both paths.

**replica_host must be a standby of host.** FastSync compares the replica's
system identifier and checkpoint timeline (`pg_control_checkpoint()`) with the
primary's before it takes a bookmark there. A different identifier or a higher
timeline fails. A lower timeline is waited for, because a standby's checkpoint
timeline trails a timeline switch until its next restartpoint. We do not use
`pg_stat_wal_receiver.received_tli`, because it is hidden from roles without
`pg_read_all_stats` (18.4 `walreceiver.c:1459`). `pg_last_wal_replay_lsn()`
stays set on a promoted server, so the timeline check is what refuses one.

## Rollback to 0.102.x

0.102.x ignores `source_identity`. Its tap-postgres rebuilds state without
top-level keys, so the first logical run drops the identity. Upgrading again
records it with the warning above. 0.102.x `reset_state` takes no lock and no
boundary.

## Known gaps

- Two PostgreSQL servers with the same system identifier, timeline and host
  string, such as sibling replicas behind one repointed DNS name, are not told
  apart.
- We do not know whether RDS changes the system identifier or timeline on an
  in-place major upgrade or a Multi-AZ failover, or whether it keeps
  `server_id`. A change stops the tap until `reset_state` or a resync.
- `reset_state` does not check that the tap drained the old source up to the
  boundary. The operator must confirm it.
- YugabyteDB FastSync still accepts an existing slot, and `partial_sync_table`
  does not check the identity.

## Affected components

- `cli/source_identity.py`: identity, boundary parsing and the boundary check.
- `cli/pipelinewise.py`: `run_tap`, `fast_sync`, `do_sync_tables` and
  `reset_state`.
- `fastsync/commons/tap_postgres.py`: slot verification and the replica check.
- `singer-connectors/tap-postgres`: keeps top-level state keys and runs
  `IDENTIFY_SYSTEM`.

## Failure-case tests

- Units: `tests/units/cli/test_source_identity.py`, `test_reset_state.py`,
  `tests/units/fastsync/commons/test_fastsync_tap_postgres.py` and tap-postgres
  `tests/unit/test_logical_replication.py`. They cover a promoted replica, a
  sibling on another host, a MariaDB server behind the same host, a boundary one
  byte before the slot start or ahead of the primary, a slot still being
  created, foreign slots, a replica on another cluster or timeline, connection
  errors leaving state unchanged, and a mismatch on the streaming connection.
- Real engine: `tests/end_to_end/test_source_identity.py` in E2E group 01
  covers concurrent slot creation, foreign slots, a primary as `replica_host`,
  the boundary limits and MariaDB/MySQL identities. Replica lag, promotion and a
  standby of the old primary were run against PostgreSQL 15 and 18 containers
  only, because the dev-project stack has no PostgreSQL standby.
