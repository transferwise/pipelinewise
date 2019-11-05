0.10.4 (2019-11-05)
-------------------

- Save state message as soon as received from a target connector
- Fixed issue when docker executable not started on non bash enabled systems
- Exit gracefully on SIGINT (CTRL+C) and SIGTERM (kill)
- Add tap run summary table when tap run finished
- Add `--extra_log` optional parameter to `run_tap` command in CLI
- Add `validate` command to CLI
- Optimised string formatting
- More accurate logging of number of exported rows in MySQL FastSync
- Fixed an issue when Snowflake cache table was not refreshed after FastSync comleted from MySQL to Snowflake

**Tap Postgres**
- Bump `pipelinewise-tap-postgres` to 1.2.0
    - Bump to `psycopg2` 2.8.4 with auto keep-alive feature
    - Remove LOG_BASED stream bookmarks from state if it has been de-selected
    - Convert time with timezone columns to UTC
    - Updating stream to lsn position before sending STATE message
    - Removed database name from stream-id
- FastSync: Convert time with timezone columns to UTC

**Target Snowflake**
- Bump `snowflake-connector-python` to 2.0.3
- Bump `pipelinewise-target-snowflake` to 1.1.6
    - Emit state message as soon as new data flushed and loaded into Snowflake
    - Enforce autocommit and secure connection
    - Optional `flush_all_streams` option
    - Configurable `parallelism` option
    - Configurable `parallelism_max` option
    - Fixed issue when updating bookmarks failed when no STATE message received from tap
- FastSync: Enforce autocommit and secure connection

**Target Redshift**
- Bump pipelinewise-target-redshift to 1.0.7
    - Configurable COPY option
    - Configurable parallelism option
    - Grant permissions to users and groups individually
- FastSync: Grant permissions to users and groups individually

**Target Postgres**
- Bump pipelinewise-target-postgres to 1.0.4
    - Fixed issue when permission not granted correctly on newly created tables

Doc-only changes
----------------
- Updated Tap Postgres, Tap Redshift pages with new features
- Removed `sync_period` references

0.10.3 (2019-09-23)
-------------------

**Transform Field**
- Bump pipelinewise-transform-field to 1.1.1
    - Add MASK-HIDDEN transformation type

0.10.2 (2019-09-20)
-------------------

**Tap S3-CSV**
- Bump pipelinewise-tap-s3-csv to 1.0.5
    - Add non-AWS S3 support

**Tap Postgres**
- Bump pipelinewise-tap-postgres to 1.1.6
- FastSync: Fixed issue when 24:00:00 formatted timestamps not loaded from Postgres to Snowflake

**Target Redshift**
- Bump pipelinewise-target-redshift to 1.0.6
    - Fixed issue when AWS credentials sometimes were visible in logs

Doc-only changes
----------------
- Updated Tap S3 CSV pages
- Add contribution page

0.10.1 (2019-09-09)
-------------------

**Tap Postgres**
- Bump tap-postgres to 1.1.5
    - Lowercase pg_replication slot name
- FastSync: Lowercase pg_replication slot name

**Target Redshift**
- Bump pipelinewise-target-redshift to 1.0.5
    - Set varchar column length dynamically
- FastSync: Set varchar column length dynamically

**Tap Oracle**
- Add Tap Oracle singer connector
- Add Oracle Instant Client to docker image

Doc-only changes
----------------
- Fixed sample YAML files for multiple connectors
- Fixed typos in multiple pages
- Fixed hard_delete option
- Updated contributors
- Add Tap Oracle

0.9.2 (2019-09-04)
-------------------

- Build docker image with no pipelinewise user
- Fixed issue when arguments were not passed correctly to docker container

0.9.1 (2019-09-01)
-------------------

- [REF-x] xyz
- [REF-x] xyz
