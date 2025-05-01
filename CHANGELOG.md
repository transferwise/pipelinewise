0.69.0 (2025-04-29)
-------------------
- Add `reset_state` command for MySQL taps

0.68.0 (2025-01-10)
-------------------
- Bump `requests` from `2.20.0` to `2.32.2` in `/singer-connectors/tap-github`
- Add `reset_state` command for PG taps

0.67.0 (2024-11-19)
-------------------
- Fix map date column to correct Postgres type
- New argument for sync tables to select replication method

0.66.1 (2024-10-31)
-------------------
- Bug fix for partial sync multiprocessing


0.66.0 (2024-10-11)
-------------------
- Bump programming language to Python 3.10
- Bump `ansible-core` from `2.11.8` to `2.17.5`
- Bump `tzlocal` from `2.1.*` to `4.0.2` in `tap-mongodb`
- Bump `tzlocal` from `2.1` to `4.0.2` in `tap-mysql`
- Bump `pipelinewise-singer-python` from `1.*` to `2.*` in `target-postgres`
- Bump `pipelinewise-singer-python` from `1.*` to `2.*` in `target-snowflake`


0.65.3 (2024-09-13)
-------------------
- Bump `zenpy` in `tap-zendesk` from `2.0.0` to `2.0.52`

0.65.2 (2024-09-05)
-------------------
- Pin tap-zendesk to v1.2.1

0.65.0 (2024-08-27)
-------------------
- Remove FastSync for target Redshift
- Remove FastSync for target Bigquery
- Incorporate used singer connectors into main codebase
- Reduce testing codebase
- Simplify Makefile

0.64.1 (2024-07-25)
-------------------
- Remove row count check in `sync_tables` for `tap_mysql` and `tap_postgres`

0.64.0 (2024-07-19)
-------------------
- Update `sync_tables` and `import_config` commands
  - new optional config setting for source tables size checking
  - checking the size of source tables in `tap_mysql` and `tap_postgres` to `target_snowflake` and failing fast sync
    if the size is not allowed
  - added `--force` argument for `sync_tables` command to ignore size checking

0.63.0 (2024-07-08)
---------------------
- Bump `pipelinewise-tap-jira`from `2.0.1` to `2.2.0`
- Bump `jinja2`from `3.0.2` to `3.1.2`
- Update Github Actions

0.60.2b1 (2024-06-12)
---------------------
- Bump `pymongo`from `4.7.3` to `3.13.0`

0.60.1 (2024-06-12)
-------------------
- Bump `pymongo`from `3.12.3` to `4.7.3`

0.60.0 (2024-05-10)
-------------------
- Bump `pipelinewise-tap-mysql`from `1.5.6` to `1.6.0`
- Attempt SSL for MariaDB and PG sources as Preferred connection method

0.58.3 (2023-11-28)
-------------------
- Bump `pipelinewise-tap-kafka`from `8.2.0` to `8.2.1`

0.58.2 (2023-11-20)
-------------------
- Bump `pipelinewise-tap-kafka`from `8.1.0` to `8.2.0`

0.58.1 (2023-10-09)
-------------------
- Bump `pipelinewise-tap-github`from `1.1.0` to `1.1.1`

0.58.0 (2023-10-04)
-------------------
- Bump `pipelinewise-tap-github`from `1.0.3` to `1.1.0`
- Bump `joblib`from `1.2.0` to `1.3.2`

0.57.0 (2023-08-09)
-------------------

- Bump `pipelinewise-tap-kafka`from `8.0.0` to `8.1.0`
- Update dependencies

0.56.0 (2023-08-09)
-------------------

- Bump `pipelinewise-target-snowflake`from `2.2.0` to `2.3.0`
- Update dependencies

0.55.0 (2023-07-25)
-------------------

- Bump `pipelinewise-tap-mysql`from `1.5.4` to `1.5.5`
- Support for dynamic values in defined partial sync

0.54.0 (2023-07-04)
-------------------

- Add support for the new limit config in `tap-postgres`
- Bump pipelinewise_tap_s3_csv from `3.0.0` to `3.0.1`
- Bump pipelinewise-tap-postgres from `1.8.4` to `2.1.0`
- Bugfix replication slot creation in partial sync

0.53.4 (2023-06-22)
-------------------

- Bugfix for trigger unwanted tables syncing trigger
- Bugfix for building `wal2json` in PostgreSQL Docker image
- Bump `pipelinewise-tap-mysql` from `1.5.3` to `1.5.4`

0.53.3 (2023-05-16)
-------------------

- Bugfix for removing pid file in stop_tap command

0.53.2 (2023-04-25)
-------------------

- Bump `pipelinewise-tap-mysql` from `1.5.2` to `1.5.3` to mitigate bug in MariaDB 10.6.12

0.53.1 (2023-04-21)
-------------------

- Rollback `snowflake-connector-python` from `2.8.2` to `2.7.6`

0.53.0 (2023-04-13)
-------------------

- Bugfix for renaming log files when stopping the tap
- Bump `snowflake-connector-python` from `2.7.6` to `2.8.2`
- Remove `tap-adwords`

0.52.2 (2023-03-20)
-------------------

- Bump `pipelinewise-tap-slack` from `1.1.0` to `1.1.1`

0.52.1 (2023-02-22)
-------------------

- Extend silentremove to support deleting folders
- Bugfix for selected fastsync tables
- Bugfix for graceful exit

0.52.0 (2023-02-02)
-------------------

- Bump `tap-s3-csv` from `2.0.0` to `3.0.0`
- Implement Defined Partial Sync for `MariaDB` and `Postgres` to `SnowFlake`

0.51.0 (2022-12-10)
-------------------

- Drop `pipelinewise-tap-postgres` from `2.0.0` to `1.8.4`:
  - wal2json format version 2 causing issues on older Postgres servers

- Bump `pipelinewise-tap-kafka` from `7.1.2` to `8.0.0`
  - Switch from `subscribe` to `assign` for better initial offset control
  - Implement specifying partitions in configuration

0.50.0 (2022-12-05)
-------------------

- Bump `pipelinewise-tap-postgres` from `1.8.4` to `2.0.0`:
  - Use wal2json format version 2

- Bump `psycopg2-binary` from `2.8.6` to `2.9.5`

0.49.0 (2022-10-27)
-------------------
- Added `taps` option for `import` command to make it possible for importing specific taps.

0.48.7 (2022-10-19)
-------------------

- Bump `pipelinewise-tap-kafka` from `7.1.0` to `7.1.2`
  - Introducing the use of the seek method to reset the source partition offsets at the start of a run

0.48.6 (2022-10-06)
-------------------

- Bump `joblib` from `1.1.0` to `1.2.0`
- Bugfix for closing `MySQL`/`MariaDB` conenctions in `FastSync`
- Removing `FastSync` from `s3-csv` and using only `singer`

0.48.5 (2022-09-22)
-------------------

- Partial sync will now create table in target if it doesn't exist. [#1014](https://github.com/transferwise/pipelinewise/pull/1014)

0.48.4 (2022-09-09)
-------------------

- Bump `pipelinewise-tap-postgres` from `1.8.3` to `1.8.4`.

0.48.3 (2022-09-08)
-------------------

- Refactor partialsync to use merge (#1010)

0.48.2 (2022-09-01)
-------------------

- Bump `pipelinewise-tap-mysql` from `1.5.1` to `1.5.2`.

0.48.1 (2022-07-21)
-------------------

- Partial sync bug fixes for selected tables and space in the name of table and values

0.48.0 (2022-07-14)
-------------------

- Bump `pipelinewise-tap-kakfa` from `7.0.0` to `7.1.0`.

0.47.1 (2022-07-08)
-------------------

- Partial sync bug fix for `start` and `end` values

0.47.0 (2022-07-07)
-------------------

- Bump `ujson` from  `5.3.0` to `5.4.0`
- Partial sync for `MariaDB` and `Postgres`

0.46.0 (2022-06-14)
-------------------

- Bump `target-s3-csv` to `2.0.0`
- Allow non-x86 architectures for dev-project
- Rename .env to .env.template
- [CI][Fix] Linting and unit testing not running for external PRs
- Bump `ujson` from `5.1.0` to `5.3.0`

0.45.0 (2022-05-12)
-------------------

- Bump `target-snowflake` to `2.2.0`

0.44.0 (2022-05-05)
-------------------

- Bump `target-snowflake` to `2.1.0`

0.43.1 (2022-04-11)
-------------------

*Fixes*
- Patch target-snowflake.
- Bump dependencies


0.43.0 (2022-04-07)
-------------------

*Breaking changes*
- Bump `tap-kafka` to `7.0.0`
- Drop not null constraints on Snowflake tables PK columns.

*Added*
- Send failure alerts to slack channel defined in tap, `slack_alert_channel`
- Backup state file before tap starting

*Fixes*
- Patch tap-mysql to `1.5.1`
- Don't use log files to check tap status before starting it.
- Change to e2e tests structure

0.42.1 (2022-03-17)
-------------------
- Bump `tap-kafka` to `6.0.0`

0.42.0 (2022-03-17)
-------------------
- Allow non-default configuration directory
- Improved OS signal handling for graceful termination
- Relax limit on split_file_max_chunks
- Make tap-mysql-fastsync compatible with MySQL 8
- Fixed failing pg-to-pg fastsync on empty tables
- Replace `ansible` with `ansible-core`
- Bump `tap-postgres` to `1.8.3`
- Bump `tap-snowflake` to `3.0.0`
- Check singer connectors installable for Python 3.7 3.8 3.9
- Support log_based using GTID for MySQL and Mariadb

0.41.0 (2022-02-10)
-------------------

- Dropped support for python 3.6
- Bump `ujson` from `4.3.0` to `5.1.0`
- Bump `pipelinewise-tap-s3-csv` to `2.0.0`
- Fix for config json files
- Fix: e2e tests fail when SF credentials are not present

0.40.0 (2022-01-27)
-------------------
- Bump `pipelinewise-tap-kafka` from `5.0.1` to `5.1.0`

0.39.1 (2022-01-26)
-------------------
- Bump `pipelinewise-tap-kafka` from `5.0.0` to `5.0.1`

0.39.0 (2022-01-25)
-------------------
- Bump `pipelinewise-tap-kafka` from `4.0.1` to `5.0.0`
- Bump `pipelinewise-target-bigquery` from `1.1.1` to `1.2.0`
- Bump `pipelinewise-transform-field` from `2.2.0` to `2.3.0`
- Prevent usage of extended transformation feature when FastSync exists
- Fixed fastsync from postgres to bigquery
- Fixed an issue when `SplitGzipFile` doesn't work with binary mode

0.38.0 (2022-01-14)
-------------------
- MySQL tap now connects to replica instance during fastsync if credentials are provided
- Added fastsync support for MongoDB Atlas
- Docker base image to Python 3.8
- Bump `pyyaml` from `5.4.1` to `6.0`
- Bump `pipelinewise-target-snowflake` from `1.14.1` to `1.15.0`
- Bump `pipelinewise-tap-s3-csv` from `1.2.2` to `1.2.3`
- Bump `pipelinewise-tap-postgres` from `1.8.1` to `1.8.2`

0.37.2 (2021-12-10)
-------------------
- Bump `pipelinewise-tap-github` from `1.0.2` to `1.0.3`


0.37.1 (2021-12-10)
-------------------
- Make a postfix for Snowflake schemas in end-to-end tests.
- Bump `google-cloud-bigquery` from `1.24.0` to `2.31.0` ([Changelog](https://github.com/googleapis/python-bigquery/blob/main/CHANGELOG.md#2310-2021-11-24))


0.37.0 (2021-11-19)
-------------------

*New*
- Added cleanup method for state file.
- Bump `pytest-cov` from `2.12.1` to `3.0.0` ([Changelog](https://github.com/pytest-dev/pytest-cov/blob/master/CHANGELOG.rst#300-2021-10-04))
- Bump `joblib` from `1.0.0` to `1.1.0`
- Bump `flake8` from `3.9.2` to `4.0.1`
- Bump `jinja2` from `3.0.1` to `3.0.2`
- Bump `python-dotenv` from `0.19.0` to `0.19.1`
- Bump `target-snowflake` from `1.14.0` to `1.14.1`
- Bump `ansible` from `4.4.0` to `4.7.0`
- Bump `pytest` from `6.2.4` to `6.2.5`

*Changes*
- Fully migrate CI to Github Actions.
- Update `ujson` requirement from `==4.1.*` to `>=4.1,<4.3`
- Update `tzlocal` requirement from `<2.2,>=2.0` to `>=2.0,<4.1`

*Fixes*
- Make process in docker-compose file.
- proc.info parsing in a case cmdline is None!


0.36.0 (2021-09-30)
-------------------

*New*
- Add new transformation type: **MASK-STRING-SKIP-ENDS**
- Bump `pipelinewise-target-snowflake` from `1.13.1` to `1.14.0` ([Changelog](https://github.com/transferwise/pipelinewise-target-snowflake/blob/master/CHANGELOG.md#1140-2021-09-30))
    - Support `date` property format
    - Don't log record on failure to avoind exposing data

*Changes*
- Use Makefile for installation
- Enforce PEP8

*Fixes*
- Dates out of range (with year > 9999) in FastSync from PG.
- Bump `pipelinewise-tap-postgres` from `1.8.0` to `1.8.1` ([Changelog](https://github.com/transferwise/pipelinewise-tap-postgres/blob/master/CHANGELOG.md#181-2021-09-23))
    -  LOG_BASED: Handle dates with year > 9999.
    -  INCREMENTAL & FULL_TABLE: Avoid processing timestamps arrays as timestamp

- `Decimal` not JSON serializable in FastSync MongoDB
- Don't use non-existent FastSync for MongoDB-Redshift pipelines.


0.35.2 (2021-08-17)
-------------------
- Bump `pipelinewise-tap-github` from `1.0.1` to `1.0.2`
- Update a few vulnerable or outdated dependencies to latest

0.35.1 (2021-08-13)
-------------------
- Bump `pipelinewise-tap-github` from `1.0.0` to `1.0.1`
- Bump `pipelinewise-tap-kafka` from `4.0.0` to `4.0.1`
- Bump `tap-jira` from `2.0.0` to `2.0.1`
- Bump `pipelinewise-target-s3-csv` from `1.4.0` to `1.5.0`

0.35.0 (2021-08-04)
-------------------
- Support `"none"` as a value for `--connectors` in `install.sh` script to install a stripped down Pipelinewise without any connectors.
- Optimize Dockerfile
- Do not log invalid json objects if they fail validation against json schema.
- Replace `github-tap` with fork `pipelinewise-tap-github` version `1.0.0`
- Add schema validation for github tap
- Increase batch_size_rows from 1M to 5M
- Increase split_file_chunk_size_mb from 2500 to 5000
- Add latest tag to docker image
- Bump `pipelinewise-tap-s3-csv` from `1.2.1` to `1.2.2`
- Update pymongo requirement from `<3.12,>=3.10` to `>=3.10,<3.13`

0.34.1 (2021-07-15)
-------------------
- Bump `pipelinewise-target-snowflake` from `1.13.0` to `1.13.1`
    - Fixed an issue with S3 metadata required for decryption not being included in archived load files
- Fixed an issue in fastsync to BigQuery data type mapping
- Add `location` config parameter to fastsync to BigQuery

0.34.0 (2021-06-24)
-------------------
- Add `split_large_files` option to FastSync target-snowflake to load large files in parallel into Snowflake
- Add `archive_load_files` option to FastSync target-snwoflake to archive load files on S3
- Bump `pipelinewise-tap-postgres` from `1.7.1` to `1.8.0`
    - Add discovering of partitioned table
- Bump `pipelinewise-target-snowflake` from `1.12.0` to `1.13.0`
    - Add `archive_load_files` parameter to optionally archive load files on S3

0.33.0 (2021-04-12)
-------------------

- Add `batch_wait_limit_seconds` option to every tap/target combination
- Bump `pipelinewise-target-snowflake` from `1.11.1` to `1.12.0`
    - Add `batch_wait_limit_seconds` option
- Bump `pipelinewise-tap-mysql` from `1.4.2` to `1.4.3`
- Bump a few vulnerable and security outdated packages

0.32.1 (2021-03-26)
-------------------

- Bump `pipelinewise-target-snowflake` from `1.11.0` to `1.11.1`

0.32.0 (2021-03-22)
-------------------

- Add transformation validation post import check to detect and deny load time transformations that's changing data types
- Fixed an issue when fastsync to Postgres and Snowflake were failing if multiple load time transformations defined on the same column
- Fixed an issue when fastsync not using unique file names and causing table name collision in the target database
- Bump `pipelinewise-tap-mysql` from `1.4.0` to `1.4.2`
    - Fixed an issue when data sometimes lost during `LOG_BASED` replication
- Bump `pipelinewise-tap-twilio` from `1.1.1` to `1.1.2`
    - Fix missing elements for streams without ordered response
- Bump `pipelinewsie-target-snowflake` from `1.10.1` to `1.11.0`

0.31.1 (2021-02-26)
-------------------

- Add support for AWS profile based authentication to FastSync tap-s3-csv.

0.31.0 (2021-02-23)
-------------------

- Update TransferWise references to Wise
- Bump `pipelinewise-tap-twilio` to `1.1.1`
- Bump `psycopg-binary` from `2.8.5` to `2.8.6`

0.30.0 (2021-01-22)
-------------------

- Drop postgres replication slot in case of full re-sync of a tap
- Add `fastsync_parallelism` optional parameter to customize the number of cores to use for parallelisation in FastSync
- Bump `pipelinewise-tap-twilio` to `1.0.2`

0.29.0 (2021-01-13)
-------------------

- Add tap-twilio

0.28.1 (2021-01-12)
-------------------

- patch `pipelinewise-tap-snowflake`
- Bumping dependencies of Pipelinewise

0.28.0 (2021-01-08)
-------------------

**New**
- Support environement variables in tap yaml files and rendering them with jinja2 template.

**Fixes**
- bump pipelinewise-target-snowflake to 1.10.1
- Map Mysql's `tinyint(1) unsigned` column type to targets' number column type
- Bumping dependencies of Pipelinewise
- Detect the copyright year dynamically

0.27.0 (2020-12-04)
-------------------

- Bumping `snowflake-connector-python` across all componenets that uses to `2.3.6`
- Tagging all queries issues to Snowflake by FastSync Snowflake and singer target-snowflake.
- Add ssl support to mongodump in FastSync mongodb.
- Add support for MySQL spatial types.

- Fix issues build PPW docker images
- Update documentation.


0.26.0 (2020-10-30)
-------------------

- Add tap-mixpanel
- Bump `joblib` to 0.16.0 to fix some issues when running on python 3.8

0.25.0 (2020-10-23)
-------------------

- Add `--profiler` optional parameter to pipelinewise commands
- Use `--debug` logging in every subprocess
- Fixed an issue when fastsync not extracting NULL characters correctly from MySQL

**Tap Postgres**
- Bump `pipelinewise-tap-postgres` to 1.7.1
    - Parse data from json(b) when converting a row to a record message in log based replication method.

**Tap MySQL**
- Bump `pipelinewise-tap-mysql` to 1.3.8
    - Fix mapping bit to boolean values

**Tap Slack**
- Bump `pipelinewise-tap-slack` to 1.1.0
    - Extract user profiles from `users.list` API endpoint
    - Extract message attachments from `conversations.history` API endpoint
    - Fixed an issue when incremental bookmarks were not sent correctly in the `STATE` messages

0.24.1 (2020-10-02)
-------------------

- Exit as failure when another instance of the tap is running or the tap is not enabled

**Tap Slack**
- Bump `pipelinewise-tap-slack` to 1.0.1
    - Fixed an issue when `thread_ts` values were not populated correctly in `messages` and `threads` streams

0.24.0 (2020-10-01)
-------------------

- Add tap-slack
- Add tap-shopify

**Tap MongoDB**
- Bump `pipelinewise-tap-mongodb` to 1.2.0
    - Add support for SRV urls

0.23.0 (2020-09-25)
-------------------

- Fixed an issue when missing empty breadcrumb in tap properties file didn't raise an exception
- Add option to build docker images only with selected tap and target connectors

**Tap Postgres**
- Bump `pipelinewise-tap-postgres` to 1.7.0
    - Option to enable SSL mode
    - Fixed an issue when timestamps out of the ISO-8601 range caused some failures
    - Fixed an issue when when postgres replication slot name not generated correctly and contained invalid characters

**Target Postgres**
- Bump `pipelinewise-target-postgres` to 2.1.0
    - Option to enable SSL mode

0.22.1 (2020-09-10)
-------------------

**Tap MySQL**
- Bump `pipelinewise-tap-mysql` to 1.3.7
    - Fixed an issue when `tap-mysql` was logging every extracted record on INFO level
    - Fixed an issue when `TIME` column types replaced the whole record

**Target S3 CSV**
- Bump `pipelinewise-target-s3-csv` to 1.4.0
    - Fixed an issue when `target-s3-csv` created temp files in system `/tmp` instead of PPW specific `~/.pipelinewise/tmp`

0.22.0 (2020-08-28)
-------------------

**FastSync**
- Fixed an issue when MySQL `TIME` column type mapping was not in sync with target-postgres and target-snowflake `TIME` type mappings
- Fixed an issue when Postgres `TIMESTAMP WITH TIME ZONE` columns were not mapped correctly to the UTC equivalent data types in the target

**Tap Kafka**
- Performance improvements
- Change the syntax of `primary_keys` option from JSONPath to `/slashed/paths` ala XPath

0.21.3 (2020-08-19)
-------------------

- Fixed an issue when tap was not started if stream buffer size is greater than 1G

0.21.2 (2020-08-18)
-------------------

- Increase max batch_size_rows to 1000k from 500k
- Increesa max stream_buffer_size to 2500

0.21.1 (2020-08-05)
-------------------

**Tap MySQL**
- Fix two issues when a new discovery is done after detecting new changes in binlogs.


0.21.0 (2020-08-04)
-------------------

- Improve alert messages to include botocore and generic python exception and error patterns in the alerts

**Tap S3 CSV**, **Target Snowflake**, **Target S3 CSV**, **Target Redshift**
- Add `aws_profile` option to support Profile based authentication to S3
- Add option to authenticate to S3 using `AWS_PROFILE`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` environment variables

**Target Snowflake**
- Fixed an issue when target-snowflake was failing when `QUOTED_IDENTIFIERS_IGNORE_CASE` snowflake parameter set to True
- Fixed an issue when new `SCHEMA` singer message triggered a flush event even if the newly received `SCHEMA` message is the same as the previous one
- Add `s3_endpoint_url` option to support non-native S3 accounts

**Target S3 CSV**
- Add `naming_convention` option to create custom and dynamically named files on S3

**Tap Snowflake**
- Eliminate some warning messages of `optional pandas and/or pyarrow not installed`.

**Tap Zendesk**
- Fixed and issue when `rate_limit`, `max_workers` and `batch_size` were not configurable via the tap-zendesk YAML file

0.20.2 (2020-07-27)
-------------------

- Fixed an issue when `stop_tap` command didn't kill running tap and child processes

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

