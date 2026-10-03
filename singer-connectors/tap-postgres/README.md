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
| break_at_end_lsn           | Boolean | No       | true    | Stop running the tap if the newly received lsn is after the max lsn that was detected when the tap started.                                                                                |
| max_run_seconds            | Integer | No       | 43200   | Stop running the tap after certain number of seconds.                                                                                                                                      |
| debug_lsn                  | String  | No       | None    | If set to `"true"` then add `_sdc_lsn` property to the singer messages to debug postgres LSN position in the WAL stream.                                                                   |
| tap_id                     | String  | No       | None    | ID of the pipeline/tap                                                                                                                                                                     |
| itersize                   | Integer | No       | 20000   | Size of PG cursor iterator when doing INCREMENTAL or FULL_TABLE                                                                                                                            |
| default_replication_method | String  | No       | None    | Default replication method to use when no one is provided in the catalog (Values: `LOG_BASED`, `INCREMENTAL` or `FULL_TABLE`)                                                              |
| use_secondary              | Boolean | No       | False   | Use a database replica for `INCREMENTAL` and `FULL_TABLE` replication                                                                                                                      |
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

* **pgoutput plugin**: Built into PostgreSQL. PipelineWise creates and maintains
  `ppw_slot_<tap_id>` as both the slot and publication name. The runtime needs
  replication privileges, database CREATE, and ownership of published tables.
  Keep wal2json installed while existing slots migrate in release 0.94.0.
  Release 0.95.0 will remove wal2json support and its migration code.

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


* **Migration and boundaries**: PipelineWise creates a fresh pgoutput slot,
  bridges the old dedicated wal2json slot to a target-acknowledged commit, then
  retires wal2json and replays the overlap from the original pgoutput position.
  This release retains numeric LSN/commit boundaries without `messages=true`.
  Quiet sources may wait for the idle timeout; overlap completion may need a
  later published transaction. Do not manually advance a migrating slot.
  See the [PostgreSQL source guide](../../docs/connectors/taps/postgres.rst) for
  prerequisites, publication reconciliation, supported tables and rename recovery.

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
