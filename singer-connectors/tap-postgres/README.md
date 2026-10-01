# pipelinewise-tap-postgres

[![PyPI version](https://badge.fury.io/py/pipelinewise-tap-postgres.svg)](https://badge.fury.io/py/pipelinewise-tap-postgres)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-tap-postgres.svg)](https://pypi.org/project/pipelinewise-tap-postgres/)
[![License: MIT](https://img.shields.io/badge/License-GPLv3-yellow.svg)](https://opensource.org/licenses/GPL-3.0)

[Singer](https://www.singer.io/) tap that extracts data from a [PostgreSQL](https://www.postgresql.com/) database and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible tap connector.

PostgreSQL 14 or later is required for every Singer replication method and for
PipelineWise FullSync/PartialSync. This source minimum does not constrain
PostgreSQL targets or the PipelineWise backend database.

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

  PipelineWise creates new slots as `pgoutput` and names them
  `pipelinewise_<tap_id>`. A LOG_BASED `tap_id` can contain only lowercase
  letters, digits, and underscores and can be at most 50 characters.

  Before any slot boundary or initial copy, prepare the exact selected-table
  publication:

  ```
  tap-postgres --config config.json --catalog catalog.json --prepare-publication
  ```

  The tap creates or validates `pw_pub_<tap_id>` with only the selected tables,
  `publish_via_partition_root = true`, and insert/update/delete enabled. It uses
  a reserved `pipelinewise-publication-fence-v1:` publication comment to record
  that transactions predating publication setup have finished; an existing DBA
  comment is retained inside that metadata. The tap user therefore needs to own
  the publication, including when a DBA pre-creates it, so the tap can alter and
  comment it. Creating the publication also requires database `CREATE` and
  ownership of the published tables.

  Every LOG_BASED publication relation must have a valid primary key and use
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
    FROM pg_create_logical_replication_slot('pipelinewise_<tap_id>', 'pgoutput');
  ```

  When the canonical slot is missing, PipelineWise can copy the historical
  tap-specific `pipelinewise_<database_name>_<tap_id>` wal2json slot to pgoutput
  at the same LSN. It first bridges wal2json through a post-publication logical
  message and advances pgoutput only after the target durably acknowledges that
  overlap. It removes the old slot only after the target later acknowledges a
  pgoutput transactional boundary. The migration temporarily requires one
  additional free replication slot.

  PipelineWise never auto-migrates the older database-wide
  `pipelinewise_<database_name>` slot because multiple taps may share it. If
  it is the only historical slot and no canonical pgoutput slot exists,
  preflight stops before publication or slot changes and asks a DBA to create
  a dedicated tap-specific migration source. An unrelated database-wide slot
  is ignored when the canonical or tap-specific slot is available.

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
