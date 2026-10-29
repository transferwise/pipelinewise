# pipelinewise-tap-yugabyte

[![PyPI version](https://badge.fury.io/py/pipelinewise-tap-yugabyte.svg)](https://badge.fury.io/py/pipelinewise-tap-yugabyte)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-tap-yugabyte.svg)](https://pypi.org/project/pipelinewise-tap-yugabyte/)
[![License: MIT](https://img.shields.io/badge/License-GPLv3-yellow.svg)](https://opensource.org/licenses/GPL-3.0)

[Singer](https://www.singer.io/) tap that extracts data from a [YugabyteDB](https://www.yugabyte.com/) database and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible tap connector.

## How to use it

The recommended method of running this tap is to use it from [PipelineWise](https://transferwise.github.io/pipelinewise). When running it from PipelineWise you don't need to configure this tap with JSON files and most things are automated. Please check the related documentation at [Tap YugabyteDB](https://transferwise.github.io/pipelinewise/connectors/taps/yugabyte.html)

If you want to run this [Singer Tap](https://singer.io) independently please read further.

### Install and Run

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).


It's recommended to use a virtualenv:

```bash
  python3 -m venv venv
  pip install pipelinewise-tap-yugabyte
```

or

```bash
  make venv
```

### Create a config.json

```
{
  "host": "localhost",
  "port": 5433,
  "user": "yugabyte",
  "password": "secret",
  "dbname": "db"
}
```

These are the same basic configuration properties used by the YSQL command-line client (`ysqlsh`). Note the
default port: YSQL speaks the PostgreSQL wire protocol on **5433**, not the PostgreSQL default of 5432.

Full list of options in `config.json`:

| Property                   | Type    | Required? | Default | Description                                                                                                                                                                            |
|----------------------------|---------|-----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                       | String  | Yes       | -       | YugabyteDB host                                                                                                                                                                        |
| port                       | Integer | Yes       | -       | YugabyteDB YSQL port (5433 by default, not the PostgreSQL 5432)                                                                                                                        |
| user                       | String  | Yes       | -       | YugabyteDB user                                                                                                                                                                        |
| password                   | String  | Yes       | -       | YugabyteDB password                                                                                                                                                                    |
| dbname                     | String  | Yes       | -       | YugabyteDB database name                                                                                                                                                               |
| filter_schemas             | String  | No        | None    | Comma separated schema names to scan only the required schemas to improve the performance of data extraction.                                                                          |
| ssl                        | String  | No        | None    | If set to `"true"` then use SSL via YSQL sslmode `require` option. If the server does not accept SSL connections or the client certificate is not recognized the connection will fail. |
| load_balance               | String  | No        | None    | Enables YugabyteDB client-side load balancing across cluster nodes. `"true"` distributes connections uniformly; `"any"` is required together with `topology_keys` for topology-aware placement. |
| topology_keys              | String  | No        | None    | Comma separated `cloud.region.zone[:preference]` list restricting `load_balance: "any"` to the given placements. Ignored unless `load_balance` is set.                                 |
| logical_poll_total_seconds | Integer | No        | 300     | Stop running the tap after this many seconds without a WAL message. `0` disables the idle stop.                                                                                        |
| break_at_end_lsn           | Boolean | No        | true    | Stop running the tap if the newly received lsn is after the max lsn that was detected when the tap started.                                                                            |
| max_run_seconds            | Integer | No        | 43200   | Stop running the tap after certain number of seconds.                                                                                                                                  |
| debug_lsn                  | String  | No        | None    | If set to `"true"` then add `_sdc_lsn` property to the singer messages to debug the YugabyteDB LSN position in the WAL stream.                                                         |
| tap_id                     | String  | No        | None    | ID of the pipeline/tap                                                                                                                                                                 |
| itersize                   | Integer | No        | 20000   | Size of YSQL cursor iterator when doing INCREMENTAL or FULL_TABLE                                                                                                                      |
| keyset_buckets             | Integer | No        | 3       | Number of hash buckets in keyset scans, from 2 to 1024. It must match the bucket count of every table's prerequisite index; changing it means rebuilding them. See [INDEXES.md](INDEXES.md).|
| default_replication_method | String  | No        | None    | Default replication method to use when no one is provided in the catalog (Values: `LOG_BASED`, `INCREMENTAL` or `FULL_TABLE`)                                                          |
| limit                      | Integer | No        | None    | Adds a limit to INCREMENTAL queries to limit the number of records returns per run                                                                                                     |

### Run the tap in Discovery Mode

```
tap-yugabyte --config config.json --discover                # Should dump a Catalog to stdout
tap-yugabyte --config config.json --discover > catalog.json # Capture the Catalog
```

Discovery reads `information_schema`, which is privilege-filtered: a table the configured user cannot read is
silently absent from the catalog rather than reported as an error. For the full list of grants, see
[Prerequisites](../../docs/connectors/taps/yugabyte.rst#prerequisites).

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

**Note**: Log based replication requires a few adjustments in the source YugabyteDB database, please read further
for more information.

### Run the tap in Sync Mode

```
tap-yugabyte --config config.json --catalog catalog.json
```

The tap will write bookmarks to stdout which can be captured and passed as an optional `--state state.json` parameter
to the tap for the next sync.

### Replication method notes

* **FULL_TABLE** resumes an interrupted sync with primary-key keyset pagination, since YugabyteDB's distributed
  MVCC exposes no cluster-wide monotonic row version like PostgreSQL's `xmin`. When the tap scans the table
  itself, resume and parallel buckets need the `<table>_pw_keyset` index described in [INDEXES.md](INDEXES.md).
  Without a usable index, or on a table with no primary key, the tap logs a warning and falls back to one
  non-resumable full scan. When run from PipelineWise with a PostgreSQL or Snowflake target, FastSync copies
  FULL_TABLE tables on every run and the tap does not scan them.
* **INCREMENTAL** requires a `replication-key` and an index on it (see [INDEXES.md](INDEXES.md)). The bookmark is
  compared inclusively (`>=`), so every row sharing the bookmarked value is re-emitted on the next run and the
  target must deduplicate on the primary key. Deletes are not captured.
* **LOG_BASED**: when run from PipelineWise, FastSync loads the initial snapshot. It pins the export to the
  replication slot's restart point with `SET yb_read_time`, so the snapshot and the stream start agree.
  PipelineWise rejects LOG_BASED tables on targets without FastSync, such as S3 CSV, because only FastSync creates
  the replication slot. YugabyteDB does not support managed Iceberg targets yet.
* When the tap runs standalone, its own LOG_BASED initial scan reuses the FULL_TABLE path and is **not** pinned to
  the slot's restart point. A `bootstrap_in_progress` bookmark lets an interrupted scan resume, which also needs
  `<table>_pw_keyset`.
* An update that changes a primary key streams as a delete of the old key and an insert of the new key. The target
  removes the row under the old key.
* Views and materialized views are rejected for LOG_BASED.

### Log Based replication requirements

* **YugabyteDB with YSQL CDC support enabled.** The tap has been developed and tested against YugabyteDB
  `2026.1.x`. YSQL follows PostgreSQL's logical-decoding model but LSNs are YugabyteDB `HYBRID_TIME` values,
  not comparable byte offsets like PostgreSQL's `pg_lsn`; they are only meaningful relative to their own
  replication slot.

* **wal2json plugin**: `wal2json` ships pre-packaged with YugabyteDB, so no separate installation step is
  required (unlike PostgreSQL).
* **Privileges**: the tap user needs the `REPLICATION` role attribute. When run from PipelineWise, FastSync also
  sets `yb_read_time`, which needs superuser or `GRANT SET ON PARAMETER yb_read_time TO <user>`. The grant is the
  least-privilege option. Without either, the initial snapshot fails with
  `permission denied to set parameter "yb_read_time"`. The slot streams changes from every table in the
  database, regardless of table privileges.
* **Replica identity**: the tap refuses LOG_BASED tables with `REPLICA IDENTITY NOTHING`. On PostgreSQL targets it
  also refuses `CHANGE`, YugabyteDB's default, because an update then streams only the changed columns and the
  target writes `NULL` into the rest. Set `REPLICA IDENTITY DEFAULT` or `FULL`. FastSync to PostgreSQL checks the
  same before it creates the slot. The slot fixes each table's identity when it is created, so after a change
  rerun the unfiltered fast_sync to recreate the slot.
* **Port**: Log based replication works only on port 5433. The current YugabyteDB psycopg2 drivers
  (`psycopg2-yugabytedb-binary` 2.9.3.5 and 2.9.3.5.post1) ignore the configured `port` on replication
  connections and always connect to 5433. The tap stops before reading any data if LOG_BASED streams are
  selected on another port. Use FULL_TABLE or INCREMENTAL if the cluster is reachable only on another port.
* **Resync size limit**: When run from PipelineWise, `allowed_resync_max_size` compares YugabyteDB's on-disk size,
  which counts compressed data files plus the write-ahead log and differs from PostgreSQL's. See
  [Resync size limit](../../docs/connectors/taps/yugabyte.rst#resync-size-limit).
* **Existing replication slot**: Log based replication requires a dedicated logical replication slot with LSN
  type `HYBRID_TIME`. When run from PipelineWise, only FastSync creates the slot, on the first sync.
  PipelineWise drops it when the tap is removed, retrying a transient "slot is active" error for up to
  five and a half minutes to wait out YugabyteDB's post-disconnect active-slot window
  (`ysql_cdc_active_replication_slot_window_ms`, five minutes by default). Slot names follow
  `pipelinewise_<dbname>_<tap_id>`. FastSync and the tap fail if the slot's plugin is not `wal2json` or its
  `yb_lsn_type` is not `HYBRID_TIME`. A slot created without the `'HYBRID_TIME'` argument gets YugabyteDB's
  default `SEQUENCE` type, which would stream no changes.

  To create one manually when running the tap standalone, connect as a user with replication privileges and
  run:
  ```
    SELECT *
    FROM pg_create_logical_replication_slot('pipelinewise_<database_name>_<tap_id>', 'wal2json', false, false, 'HYBRID_TIME');
  ```

  **Note**: Replication slots are specific to a given database in a cluster. If you want to connect multiple
  databases - whether in one integration or several - you’ll need to create a replication slot for each database.

* **Acknowledgement and recovery**: consuming WAL does not by itself advance the slot. The tap sends slot feedback
  two below the minimum target-acknowledged LSN stored in `state.json`, and starts each run from that same LSN.
  YugabyteDB can give the next transaction the same LSN as the previous commit record, so this keeps it from being
  skipped. As a result the last acknowledged transaction replays once on the next run, and the target deduplicates
  it. A run with no new changes stops after `logical_poll_total_seconds` (default 300). After an unexpected
  termination, restart the same tap without advancing state. Unacknowledged changes remain replayable while the
  slot exists. Resync only when the slot is unavailable.

### To run tests:

1. Install python test dependencies in a virtual env:
```
 make venv
```

2. You need to have a YugabyteDB database to run the tests and export its credentials.

You can make use of the local docker-compose to spin up a test database by running `make start_db`. The
`yugabytedb/yugabyte` image entrypoint is `yugabyted`, not the PostgreSQL entrypoint, so only the bootstrap
`yugabyte` superuser and database exist on a fresh container.

`make integration_test` sources `tests/integration/env`, which is git-ignored so that local credentials cannot
be committed. Seed it from the checked-in defaults that match the compose stack:
```
  cp tests/integration/env.template tests/integration/env
```

Test objects will be created in the `yugabyte` database.

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

## YSQL divergences in the test fixtures

YugabyteDB's YSQL layer is PostgreSQL 15 wire-compatible but not DDL-compatible. `tests/db/tap_yugabyte_data.sql` therefore deliberately diverges from `tests/db/tap_postgres_data.sql`; do not re-sync it from the Postgres fixture without re-applying these changes.

### Primary keys must be declared inline in `CREATE TABLE`

In YSQL the primary key defines the DocDB row key, so `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY` forces a table rewrite. The rewrite emits `NOTICE: table rewrite may lead to inconsistencies` and, inside a transaction block, aborts the surrounding transaction with `ERROR: current transaction is expired or aborted`.

`city.city_pkey` and `country.country_pkey` are consequently declared inside their `CREATE TABLE` statements and the corresponding `ALTER TABLE ONLY ... ADD CONSTRAINT` statements were deleted. Foreign-key `ALTER TABLE`s (for example `country_capital_fkey`) need no such treatment and are kept.

### The world-data block must not be wrapped in an explicit transaction

`COPY` cannot batch inside an explicit transaction block - YSQL warns `ROWS_PER_TRANSACTION is not supported in a transaction block` - and loading the 4,079-row `city` plus 239-row `country` data set as a single transaction expires it before `COMMIT`. The `BEGIN;`/`COMMIT;` pair around that block was removed so its statements autocommit. The two smaller transactions earlier in the file (the `public` and `public2` schema setup) load little enough data to keep their explicit transactions.

### Role and database creation is done by the seed script

The `yugabytedb/yugabyte` image entrypoint is `yugabyted`, not the `postgres` image entrypoint, so `POSTGRES_USER`/`POSTGRES_PASSWORD`/`POSTGRES_DB` are inert. Only the bootstrap `yugabyte` superuser and database exist on a fresh container. `tests/db/tap_yugabyte_db.sh` connects as that superuser (`TAP_YUGABYTE_SUPERUSER*`) to create the tap role and database before loading the fixtures, guarding both because YSQL has no `CREATE ROLE ... IF NOT EXISTS` or `CREATE DATABASE ... IF NOT EXISTS`.
