0.13.2 (2020-03-05)
-------------------

**FastSync Postgres**
    - Handle reserved words in column names in FastSync from PostgreSQL

0.13.1 (2020-03-02)
-------------------
- Bump `ansible` to 2.7.16

**FastSync MySQL**
    - Handle reserved words in column names in FastSync from MySQL
    - Fixed issue when `parallelism` and `parallelism_max` parameters were not used in tap YAML files

**Tap Postgres**
- Bump `tap-postgres` to 1.4.1
    - Remove unused timestamps in log

0.13.0 (2020-02-26)
-------------------
**Logging refactoring**:
- Structured logs in Pipelinewise, FastSync and majority of plugins.
- Include a logging config file in Pipelinewise repository and package [here](./pipelinewise/logging.conf). 
- Ability to provide a custom logging config by setting the env variable `LOGGING_CONF_FILE` to be the 
        path to the `.conf` file 

0.12.4 (2020-02-19)
-------------------

**Tap Jira**
- Bump `tap-jira` to 2.0.0
    - Update key property for stream users

0.12.3 (2020-02-19)
-------------------

**FastSync MySQL**
    - Fix bug: map BINARY MySQL column to BINARY type IN SF
    
0.12.2 (2020-02-03)
-------------------

**Transform field**
- Bump `pipelinewise-transform-field` to 1.1.2
    - Make validation turned off by default.


0.12.1 (2020-01-31)
-------------------

- FastSync: Changed the default /tmp folder for snowflake encryption

**Target Snowflake**
- Bump `pipelinewise-target-snowflake` to 1.4.1
    - Changed the default /tmp folder for encryption

0.12.0 (2020-01-21)
-------------------

- FastSync: Support BINARY and VARBINARY column types from MySQL sources
- FastSync: Fixed an issue when `MASK-HIDDEN` type of transformations were not applied in Snowflake targets
- Write temporary files to `~/.pipelinewise/tmp` directory
- Add `stop_tap` command
- Fixed an issue when post import Primary Keys check was not working correctly
- Fixed an issue when `discover_tap` command sometimes was failing

**Tap MySQL**
- Bump `pipelinewise-tap-mysql` to 1.1.3
    - Support to extract BINARY and VARBINARY column types
    - Improved performance of reading data from MySQL binary log
    - Increase default session `wait_timeout` to 28800
    - Increase default session `innodb_lock_wait_timeout` to 3600

**Tap S3 CSV**
- Bump `pipelinewise-tap-s3-csv` to 1.0.7
    - Improved column type guesser

**Tap Kafka**
- Bump `pipelinewise-tap-kafka` to 2.0.0
    - Rewamp output schema, export the consumed JSON messages from Kafka topics to fixed columns
    - Disable data flattening

**Target Snowflake**
- Bump `pipelinewise-target-snowflake` to 1.3.0
    - Load binary data into Snowflake `BINARY` column types
    - Adjust timestamps from taps automatically to the max allowed `9999-12-31 23:59:59` when it's required
    - Add `validate_record` optional parameter and default to False
    - Add `temp_dir` optional parameter to overwrite system defaults

0.11.1 (2019-11-28)
-------------------

- FastSync: Add fastsync support from S3-CSV to Snowflake
- Add post import checks to detect tables with no primary key early
- Add optional `--connectors` to the install script to install taps and targets selectively

**Tap Zendesk**
- Forked singer connector to `pipelinewise-tap-zendesk==1.0.0`
    - Improved performance by getting data from Zendesk API in parallel

**Tap Postgres**
- Bump `pipelinewise-tap-postgres` to 1.3.0
    - Add `max_run_seconds` configurable option
    - Add `break_at_end_lsn` configurable option
    - Only send feedback when lsn_comitted has increased

**Tap Snowflake**
- Bump `pipelinewise-tap-snowflake` to 1.0.5
    - Bump `snowflake-connector-python` to 2.0.4

**Tap Kafka**
- Bump `pipelinewise-tap-kafka` to 1.0.2
    - Add `encoding` configurable option

**Target Redshift**
- Bump `pipelinewise-target-redshift` to 1.1.0
    - Emit new state message as soon as data flushed to Redshift
    - Add `flush_all_streams` option
    - Add `max_parallelism` option

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

- Initial release

