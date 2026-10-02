# pipelinewise-tap-postgres

[![PyPI version](https://badge.fury.io/py/pipelinewise-tap-postgres.svg)](https://badge.fury.io/py/pipelinewise-tap-postgres)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-tap-postgres.svg)](https://pypi.org/project/pipelinewise-tap-postgres/)
[![License: MIT](https://img.shields.io/badge/License-GPLv3-yellow.svg)](https://opensource.org/licenses/GPL-3.0)

[Singer](https://www.singer.io/) tap that extracts data from a [PostgreSQL](https://www.postgresql.com/) database and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible tap connector.

PostgreSQL 14 or later is required for every Singer replication method and for
PipelineWise FullSync/PartialSync. Releases before 14.18, 15.13, 16.9, or 17.5
remain supported but log a warning because wal2json and pgoutput may omit or
misdecode changes. This source minimum does not constrain PostgreSQL targets or
the PipelineWise backend database.

## How to use it

The recommended method of running this tap is to use it from [PipelineWise](https://transferwise.github.io/pipelinewise). When running it from PipelineWise you don't need to configure this tap with JSON files and most of things are automated. Please check the related documentation at [Tap Postgres](https://transferwise.github.io/pipelinewise/connectors/taps/postgres.html)

If you want to run this [Singer Tap](https://singer.io) independently please read further.

### Install and Run

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).


It's recommended to use a virtualenv:

```bash
  python3 -m venv venv
  pip install pipelinewise-tap-postgres
```

or

```bash
  make venv
```

### Create a config.json

```
{
  "host": "localhost",
  "port": 5432,
  "user": "postgres",
  "password": "secret",
  "dbname": "db"
}
```

These are the same basic configuration properties used by the PostgreSQL command-line client (`psql`).

Full list of options in `config.json`:

| Property                   | Type    | Required? | Default | Description                                                                                                                                                                                |
|----------------------------|---------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                       | String  | Yes      | -       | PostgreSQL host                                                                                                                                                                            |
| port                       | Integer | Yes      | -       | PostgreSQL port                                                                                                                                                                            |
| user                       | String  | Yes      | -       | PostgreSQL user                                                                                                                                                                            |
| password                   | String  | Yes      | -       | PostgreSQL password                                                                                                                                                                        |
| dbname                     | String  | Yes      | -       | PostgreSQL database name                                                                                                                                                                   |
| filter_schemas             | String  | No       | None    | Comma separated schema names to scan only the required schemas to improve the performance of data extraction.                                                                              |
| ssl                        | String  | No       | None    | If set to `"true"` then use SSL via postgres sslmode `require` option. If the server does not accept SSL connections or the client certificate is not recognized the connection will fail. |
| logical_poll_total_seconds | Integer | No       | 10800   | Stop running the tap when no data received from wal after certain number of seconds.                                                                                                       |
| break_at_end_lsn           | Boolean | No       | true    | Stop after the decoded transactional logical message that bounds the current logical replication run.                                                                                     |
| max_run_seconds            | Integer | No       | 43200   | Stop running the tap after certain number of seconds.                                                                                                                                      |
| publication_fence_timeout_seconds | Number | No | 300     | Positive finite timeout for publication transaction fences and replica catch-up before an initial LOG_BASED snapshot. |
| debug_lsn                  | String  | No       | None    | If set to `"true"` then add `_sdc_lsn` property to the singer messages to debug postgres LSN position in the WAL stream.                                                                   |
| tap_id                     | String  | LOG_BASED | None  | ID of the pipeline/tap; used for the pgoutput slot and publication names                                                                                                                   |
| itersize                   | Integer | No       | 20000   | Size of PG cursor iterator when doing INCREMENTAL or FULL_TABLE                                                                                                                            |
| default_replication_method | String  | No       | None    | Default replication method to use when no one is provided in the catalog (Values: `LOG_BASED`, `INCREMENTAL` or `FULL_TABLE`)                                                              |
| use_secondary              | Boolean | No       | False   | Use a database replica for `INCREMENTAL`, `FULL_TABLE`, and initial LOG_BASED snapshots. LOG_BASED snapshots wait for the primary bookmark to replay. |
| secondary_host             | String  | No       | -       | PostgreSQL Replica host (required if `use_secondary` is `True`)                                                                                                                            |
| secondary_port             | Integer | No       | -       | PostgreSQL Replica port (required if `use_secondary` is `True`)                                                                                                                            |
| limit                      | Integer | No       | None    | Adds a limit to INCREMENTAL queries to limit the number of records returns per run                                                                                                         |


### Run the tap in Discovery Mode

```
tap-postgres --config config.json --discover                # Should dump a Catalog to stdout
tap-postgres --config config.json --discover > catalog.json # Capture the Catalog
```

### Add Metadata to the Catalog

Each entry under the Catalog's "stream" key will need the following metadata:

```
{
  "streams": [
    {
      "stream_name": "my_topic"
      "metadata": [{
        "breadcrumb": [],
        "metadata": {
          "selected": true,
          "replication-method": "LOG_BASED",
        }
      }]
    }
  ]
}
```

The replication method can be one of `FULL_TABLE`, `INCREMENTAL` or `LOG_BASED`.

**Note**: Log based replication requires a few adjustments in the source postgres database, please read further
for more information.

### Run the tap in Sync Mode

```
tap-postgres --config config.json --catalog catalog.json
```

The tap will write bookmarks to stdout which can be captured and passed as an optional `--state state.json` parameter
to the tap for the next sync.

### Log Based replication requirements

* **A connection to the master instance**. Log-based replication will only work by connecting to the master instance.

* **pgoutput**: Log-based replication uses PostgreSQL's built-in `pgoutput`
  plugin. Fresh sources do not need an external output plugin. Keep wal2json
  installed temporarily when migrating an existing wal2json slot. If the server
  exposes `output_plugin_libraries`, permit both `pgoutput` and `wal2json` there
  until migration completes.


* **postgres config file**: Locate the database configuration file (usually `postgresql.conf`) and define
  the parameters as follows:

    ```
    wal_level=logical
    max_replication_slots=5
    max_wal_senders=5
    ```

    Restart your PostgreSQL service to ensure the changes take effect.

    **Note**: For `max_replication_slots` and `max_wal_senders`, we’re defaulting to a value of 5.
    This should be sufficient unless you have a large number of read replicas connected to the master instance.


* **Replication slot and publication**: Log based replication requires a dedicated logical replication slot.
  In PostgreSQL, a logical replication slot represents a stream of database changes that can then be replayed to a
  client in the order they were made on the original server. Each slot streams a sequence of changes from a single
  database.

  PipelineWise creates new slots as `pgoutput` and uses `ppw_slot_<tap_id>`
  for both the slot and publication. A LOG_BASED `tap_id` can contain only
  lowercase letters, digits, and underscores and can be at most 50 characters.
  The ID must be unique across the PostgreSQL cluster. For a historical ID rename, use PipelineWise's
  [previous_tap_id procedure](../../docs/connectors/taps/postgres.rst#renaming-a-historical-tap-id).

  Before any slot boundary or initial copy, prepare the selected-table
  publication:

  ```
  tap-postgres --config config.json --catalog catalog.json --prepare-publication
  ```

  The tap adds selected tables to `ppw_slot_<tap_id>` without removing existing
  members during normal or filtered runs. PipelineWise `import_config` removes
  persistently deselected members only when its publication comment tracks them
  as managed; it leaves publication creation to the first sync. Untracked
  DBA-added tables remain untouched. Import clears a deselected logical table's
  bookmark before removing it, so re-adding it requires a fresh snapshot.
  After automatic migration finishes, removing the final LOG_BASED selection
  also clears reset markers and drops the canonical and dedicated wal2json
  slots. Selection changes during migration are rejected before state
  invalidation; revert and re-import the selection, finish migration, then
  import the removal again. Shared database-wide slots remain untouched. Retry
  an interrupted cleanup without restoring old bookmarks. Keep a deleted tap
  absent for one successful `import_config` if pending local cleanup blocks its
  re-add. Change a PostgreSQL tap's source, connector type, or target through a
  remove/import/add/import sequence rather than changing it in place. See the
  [PostgreSQL guide](../../docs/connectors/taps/postgres.rst).
  The publication sets `publish_via_partition_root = true` and enables
  insert/update/delete. It uses
  a reserved `pipelinewise-publication-fence-v1:` publication comment to record
  that earlier writing transactions have finished; an existing DBA
  comment is retained inside that metadata. The tap user therefore needs to own
  the publication, including when a DBA pre-creates it, so the tap can alter and
  comment it. Creating the publication also requires database `CREATE` and
  ownership of the published tables.

  Every LOG_BASED publication relation must have a valid non-deferrable primary key and use
  `REPLICA IDENTITY DEFAULT`. For a selected partition root this requirement
  applies to both the root and every current physical leaf. Apply the same
  primary key and default replica identity to each future partition before it
  is attached or receives writes. PipelineWise rejects `REPLICA IDENTITY FULL`
  and alternate replica-identity indexes because Singer targets merge on the
  discovered primary key.

  Attach only an empty new partition, then write through the root. PostgreSQL
  does not publish rows that already existed when a table was attached; resync
  those rows separately if a populated table must become a partition.
  LOG_BASED selection rejects ordinary inheritance parents and partition roots
  with descendants other than local ordinary or partitioned tables. Foreign
  partitions are outside the local WAL stream.

  ```sql
  ALTER TABLE schema_name.table_name REPLICA IDENTITY DEFAULT;
  ```

  A primary-key-changing update emits a delete for the old key before the new
  row. If PostgreSQL omits an unchanged selected TOAST value from that update,
  the tap fails and requires a full resync rather than creating an incomplete
  row under the new key. Changing a table's primary-key definition also
  requires a whole-table full resync; an in-run identity change is rejected
  before the tap refreshes its schema.

  PostgreSQL 14's transactional logical messages provide the run boundary. The
  tap user needs `EXECUTE` on
  `pg_catalog.pg_logical_emit_message(boolean, text, text)` if execution on that
  built-in function has been revoked. The tap starts pgoutput with logical
  messages enabled and fails if it cannot emit and decode its exact
  tap-specific boundary message; it does not fall back to a sampled WAL
  position.

  After the preparation succeeds, a standalone deployment can create a fresh
  slot before its initial sync:

  ```
    SELECT *
    FROM pg_create_logical_replication_slot('ppw_slot_<tap_id>', 'pgoutput');
  ```

  When the canonical slot is missing, PipelineWise creates a fresh pgoutput
  slot after preparing the publication. It then emits a transactional logical
  message and reads the historical `pipelinewise_<database_name>_<tap_id>`
  wal2json slot from the target bookmark through that later commit. Only after
  target acknowledgement does PipelineWise advance pgoutput to the bridge LSN.
  It removes the old slot after the target later acknowledges a pgoutput
  transactional boundary. A durable pending marker makes interrupted or
  duration-limited runs reuse each migration boundary, so later retries
  continue toward the original commit.
  While the two migration slots coexist, publication selection and options are
  frozen. If `import_config` rejects a selection change, finish migration or
  revert the selection and re-import it. An unfiltered whole-tap FastSync can
  explicitly reset the migration.
  Migration temporarily requires one additional free replication slot and
  permission to create, advance, consume, and remove slots. Slot copying and
  permission to copy slots are not required.

  An explicit unfiltered whole-tap FastSync takes new snapshots instead of
  bridging old WAL. It invalidates state, drops both the canonical slot and any
  dedicated wal2json slot, and creates a fresh pgoutput slot before workers run.
  After an interruption, complete that whole-tap resync before ordinary runs.

  PipelineWise never auto-migrates the older database-wide
  `pipelinewise_<database_name>` slot because multiple taps may share it. If
  existing bookmarks depend on it, arrange a whole-tap resync or dedicated
  migration source with the DBA. A new tap without saved LOG_BASED history can
  create its own pgoutput slot while preserving the shared slot.

  Historical tap-specific names are truncated to PostgreSQL's 63-byte identifier
  limit. PipelineWise does not infer ownership of an implicitly truncated name.
  It preserves that slot when the canonical slot already exists, during an
  explicit fresh start, and after final LOG_BASED deselection. To migrate its
  history, verify ownership and use the explicit `previous_tap_id` rename
  procedure linked above.

  Automatic wal2json migration is also rejected when any selected relation is
  a partition root. The wal2json table filter cannot safely include partitions
  attached while its bridge is running. Use a full resync into a fresh pgoutput
  slot for that tap. Fresh pgoutput taps can publish partition roots normally.

### To run tests:

1. Install python test dependencies in a virtual env:
```
 make venv
```

2. You need to have a postgres database to run the tests and export its credentials.

You can make use of the local docker-compose to spin up a test database by running `make start_db`

Test objects will be created in the `postgres` database.

3. To run the unit tests:
```
  make unit_test
```

4. To run the integration tests:
```
  make integration_test
```

### To run Ruff:

Install python dependencies and run python linter
```
  make venv
  make lint
```
