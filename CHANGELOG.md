0.20.1 (2020-07-24)
-------------------

**Tap MySQL**
- revert back to `pipelinewise-tap-mysql` to 1.3.2
    - 1.3.3 is breaking the replication


0.20.0 (2020-07-24)
-------------------

- Fixed an issue when `stop_tap` command doesn't kill child processes only the parent PPW executable

**Tap MongoDB**
- Bump `pipelinewise-tap-mongodb` to 1.1.0
    - Add `await_time_ms` parameter to control how long the log_based method would wait for new change streams before stopping, default is 1000ms=1s which is the default anyway in the server.
    - Add `update_buffer_size` parameter to control how many update operation we should keep in the memory before having to make a call to `find` operation to get the documents from the server. The default value is 1, i.e every detected update will be sent to stdout right away.

**Tap MySQL**
- Bump `pipelinewise-tap-mysql` to 1.3.3
    - During `LOG_BASED` runtime, detect new columns, incl renamed ones, by comparing the columns in the binlog event to the stream schema, and if there are any additional columns, run discovery and send a new `SCHEMA` message to target. This helps avoid data loss.

**Tap Zendesk**
- Bump `pipelinewise-tap-zendesk` to 1.2.1
    - Use `start_time` query parameter to load satisfaction_ratings stream incrementally

**Target Snowflake**
- Bump `pipelinewise-target-snowflake` to 1.7.0
    - Add `s3_acl` option to support ACL for S3 upload

**Target Redshift**
- Bump `pipelinewise-target-redshift` to 1.5.0
    - Add `s3_acl` option to support ACL for S3 upload

0.19.0 (2020-07-21)
-------------------
- Add tap-github
- Extract and send known error patterns from logs to alerts

**Tap MongoDB**
- Bump `pipelinewise-tap-mongodb` to 1.0.1
    - Fix case where resume tokens are not json serializable by extracting and saving `_data` only

**Tap Zendesk**
- Bump `pipelinewise-tap-zendesk` to 1.2.0
    - Configurable `rate_limit`, `max_workers` and `batch_size` parameters

0.18.1 (2020-07-15)
-------------------
- Fixed an issue when vault encrypted values were not in loaded from `config.yml`

0.18.0 (2020-07-14)
-------------------
- Add generic alert sender with Slack and VictorOps integration

**Tap Postgres**
- Bump `pipelinewise-tap-postgres` to 1.6.3
    - Fixed a data loss issue when running `LOG_BASED` the tap not sending new `SCHEMA`

0.17.1 (2020-07-09)
-------------------
- Fixed an issue when using FastSync on big MongoDB collections caused memory errors
- Fixed an issue when `sync_tables` command was not working and failed with exception
- Fixed an issue when custom `stream_buffer_size` option produced unreadable log files

0.17.0 (2020-06-29)
-------------------
- Add tap-mongodb with FastSync components to Snowflake and Postgres
- Add tap-google-analytics (as an optional extra connector, with no FastSync)
- Add configurable `stream_buffer_size` option to use large buffers between taps and targets to avoid taps being blocked by long running targets.

**FastSync**
- Fixed an issue when some bad but valid MySQL dates are not loaded correctly into Snowflake

**Tap MySQL**
- Bump `pipelinewise-tap-mysql` to 1.3.2
    - Fixed some dependency issues and bump `pymysql` to 0.9.3
    - Full changelog at https://github.com/transferwise/pipelinewise-tap-mysql/blob/master/CHANGELOG.md#132-2020-06-15

**Target Snowflake**
- Bump `pipelinewise-target-snowflake` to 1.6.6
    - Fixed an issue when new columns sometimes not added to target table
    - Fixed an issue when the query runner returned incorrect value when multiple queries running in one transaction
    - FUll changelog at https://github.com/transferwise/pipelinewise-target-snowflake/blob/master/CHANGELOG.md#166-2020-06-26


0.16.0 (2020-05-19)
-------------------
- Support reserved words as table and column names across every component, including fastsync and singer executables
- Support loading tables with space in the name
- Add tap-zuora
- Switch to `psycopg-binary` 2.8.5 in every component, including fastsync and singer executables

**FastSync**
- Fixed an issue when composite primary keys not created correctly by fastsync
- Create database specific unique replication slot names from tap-postgres
- Fixed an issue when parallel running `CREATE SCHEMA IF NOT EXISTS` commands caused deadlock in PG
- Support fastsync between tap-mysql, tap-postgres, tap-s3-csv to target-snowflake, target-postgres and target-redshift

**Tap Postgres**
- Bump `pipelinewise-tap-postgres` to 1.6.2
    - Fixed issue when `JSON` type not converted to dictionary
    - Fixed an issue when existing replication slot not found

**Tap MySQL**
- Bump `pipelinewise-tap-mysql` to 1.3.0
    - Add optional `session_sqls` connection parameter
    - Support MySQL `JSON` column type

**Tap Oracle**
- Bump `pipelinewise-tap-oracle` to 1.0.1
    - Fixed an issue when output messages were not compatible with `pipelinewise-transform-field` component

**Target Snowflake**
- Bump `pipelinewise-target-snowflake` to 1.6.4
    - Fix loading tables with space in the name

**Target Postgres**
- Bump `pipelinewise-target-postgres` to 2.0.0
    - Implement missing and equivalent features of `pipelinewise-target-snowflake`
    - Full changelog at https://github.com/transferwise/pipelinewise-target-postgres/blob/master/CHANGELOG.md#200-2020-05-02

**Target Redshift**
- Bump `pipelinewise-target-redshift` to 2.0.0
    - Implement missing and equivalent features of `pipelinewise-target-snowflake`
    - Full changelog at https://github.com/transferwise/pipelinewise-target-redshift/blob/master/CHANGELOG.md#140-2019-05-11

0.15.0 (2020-04-09)
-------------------
**FastSync**
- To Snowflake: Support for IAM roles, AWS Session Tokens and to pass credentials as environment variables

**Tap Kafka**
- Bump `pipelinewise-tap-kafka` to 3.0.0
    - Add local storage of consumed messages and instant commit kafka offsets
    - Add more configurable options: `consumer_timeout_ms`, `session_timeout_ms`, `heartbeat_interval_ms`, `max_poll_interval_ms`
    - Add two new fixed output columns: `MESSAGE_PARTITION` and `MESSAGE_OFFSET`

**Tap Snowflake**
- Bump `pipelinewise-tap-snowflake` to 2.0.0
    - Discover only the required tables to avoid issues when too many tables in the database causing `SHOW COLUMNS` column to return more than the maximum 10000 rows

**Target Snowflake**
- Bump `pipelinewise-target-snowflake` to 1.6.3
    - Generate compressed CSV files by default. Optionally can be disabled by the `no_compression` config option


0.14.3 (2020-03-25)
-------------------
- Support tap/target config files with `.yaml` extension when importing config
- Fixed dependency conflict in install script
- Fixed an issue when `add_metadata_columns` was not defined in `inheritable_config.json`

**FastSync**
- From MySQL: Increased default batch size to 50.000 rows when fastsync exporting data from MySQL tables
- To Snowflake: Log inserts, updates and csv file sizes in the same format to target-snowflake connector

**Tap Kafka**
- Bump `pipelinewise-tap-kafka` to 2.1.1
    - Commit offset from the state file and not from the consumed messages

**Tap Snowflake**
- Bump `pipelinewise-tap-snowflake` to 1.1.2
    - Fixed some dependency conflicts

**Target Snowflake**
- Bump `pipelinewise-target-snowflake` to 1.6.2
    - Log inserts, updates and csv file sizes in a more consumable format


0.14.2 (2020-03-19)
-------------------

**Singer transformation**
- Make tranformation consistent between FastSync and Singer by updating transform-field to transform without trimming.

**tap-snowflake**
- Remove PIPELINEWISE.COLUMNS cache table.


0.14.1 (2020-03-13)
-------------------

**FastSync S3-csv to Snowflake**
- Fix bug when `date_overrides` is present.

**FastSync and singer target-snowflake**
- Remove PIPELINEWISE.COLUMNS cache table.


0.14.0 (2020-03-10)
-------------------

**FastSync Postgres**
- Support reserved words as table names.

**Install script**
- update script to search full name plugins.

**Tap Postgres**
- Bump `pipelinewise-tap-postgres` to 1.5.1
    - Support per session wal_sender_timeout


0.13.3 (2020-03-09)
-------------------

**FastSync Postgres & Mysql**
- fix "'NoneType' object has no attribute 'upper'" that happens when table has no PK.
- fix "Information schema query returned too much data".


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

