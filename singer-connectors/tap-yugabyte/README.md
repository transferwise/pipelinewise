# pipelinewise-tap-yugabyte

[![PyPI version](https://badge.fury.io/py/pipelinewise-tap-postgres.svg)](https://badge.fury.io/py/pipelinewise-tap-postgres)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-tap-postgres.svg)](https://pypi.org/project/pipelinewise-tap-postgres/)
[![License: MIT](https://img.shields.io/badge/License-GPLv3-yellow.svg)](https://opensource.org/licenses/GPL-3.0)

[Singer](https://www.singer.io/) tap that extracts data from a [YugabyteDB](https://www.yugabyte.com/) database and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible tap connector.

## YSQL divergences in the test fixtures

YugabyteDB's YSQL layer is PostgreSQL 15 wire-compatible but not DDL-compatible. `tests/db/tap_yugabyte_data.sql` therefore deliberately diverges from `tests/db/tap_postgres_data.sql`; do not re-sync it from the Postgres fixture without re-applying these changes.

### Primary keys must be declared inline in `CREATE TABLE`

In YSQL the primary key defines the DocDB row key, so `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY` forces a table rewrite. The rewrite emits `NOTICE: table rewrite may lead to inconsistencies` and, inside a transaction block, aborts the surrounding transaction with `ERROR: current transaction is expired or aborted`.

`city.city_pkey` and `country.country_pkey` are consequently declared inside their `CREATE TABLE` statements and the corresponding `ALTER TABLE ONLY ... ADD CONSTRAINT` statements were deleted. Foreign-key `ALTER TABLE`s (for example `country_capital_fkey`) need no such treatment and are kept.

### The world-data block must not be wrapped in an explicit transaction

`COPY` cannot batch inside an explicit transaction block - YSQL warns `ROWS_PER_TRANSACTION is not supported in a transaction block` - and loading the 4,079-row `city` plus 239-row `country` data set as a single transaction expires it before `COMMIT`. The `BEGIN;`/`COMMIT;` pair around that block was removed so its statements autocommit. The two smaller transactions earlier in the file (the `public` and `public2` schema setup) load little enough data to keep their explicit transactions.

### No logical replication

There is no `wal2json` output plugin for YugabyteDB and its logical replication semantics diverge from PostgreSQL, so `LOG_BASED` is unavailable. `tap-yugabyte` defaults to `FULL_TABLE` in `pipelinewise/cli/tap_properties.py`. `tests/db/tap_yugabyte_data_logical.sql` is still loaded, but only so the `logical1`/`logical2` schemas exist as discovery targets - no replication slot is created.

### Role and database creation is done by the seed script

The `yugabytedb/yugabyte` image entrypoint is `yugabyted`, not the `postgres` image entrypoint, so `POSTGRES_USER`/`POSTGRES_PASSWORD`/`POSTGRES_DB` are inert. Only the bootstrap `yugabyte` superuser and database exist on a fresh container. `tests/db/tap_yugabyte_db.sh` connects as that superuser (`TAP_YUGABYTE_SUPERUSER*`) to create the tap role and database before loading the fixtures, guarding both because YSQL has no `CREATE ROLE ... IF NOT EXISTS` or `CREATE DATABASE ... IF NOT EXISTS`.