2.3.0 (2023-08-08)
-------------------

*Changes*
- Update dependencies:
    - snowflake-connector-python[pandas]
    - boto3
    - pytest
    - python-dotenv


2.2.0 (2022-05-12)
-------------------

*Changes*
- Revert use of `ujson` 


2.1.0 (2022-05-05)
-------------------

*Changes*
- Use `usjon` for JSON encoding/decoding

2.0.1 (2022-04-08)
-------------------

*Fixes*
- Only drop pk constraint if table has one
- Don't raise `PrimaryKeyNotFoundException` when a record has a flasy pk value


2.0.0 (2022-03-29)
-------------------

*Fixes*
- Respecting `flush_all_streams` when SCHEMA messages arrive.
- Improve logging for failed merge & copy queries.
- Drop NOT NULL constraint from primary key columns.
- Update PK constraints according to changes to SCHEMA's key properties.

*Changes*
- Dropping support for Python 3.6
- Adding support for Python 3.9
- Bump pytest to `7.1.1`
- Bump boto3 to `1.21`


1.15.0 (2022-01-14)
-------------------

*Added*
- Support parallelism for table stages

*Fixes*
- Emit last encountered state message if there are no records.

*Changes*
- Migrate CI to github actions
- Bump dependencies


1.14.1 (2021-10-14)
-------------------
- Increase `max_records` when selecting columns by an order of magnitude
- Bumping dependencies

1.14.0 (2021-09-30)
-------------------
- Add support for `date` property format
- Stop logging record when error happens

1.13.1 (2021-07-15)
-------------------
- Fixed an issue with S3 metadata required for decryption not being included in archived load files.

1.13.0 (2021-06-23)
-------------------
- Add `archive_load_files` parameter to optionally archive load files on S3
- Bumping dependencies

1.12.0 (2021-04-12)
-------------------
- Add optional `batch_wait_limit_seconds` parameter
- Bumping dependencies

1.11.1 (2021-03-23)
-------------------
- Fixed an issue when `SHOW FILE FORMATS` ran too many times slowing down the startup time of the target
- Bump `snowflake-connectory-python` from `2.3.10` to `2.4.1`
- Bump `numpy` from `<1.20.0` to `<1.21.0`

1.11.0 (2021-03-17)
-------------------
- Add parquet support
- Add check and few logs in the date parsing routine
- Bumping dependencies
  
1.10.1 (2021-01-08)
-------------------
- Update caching mechanism to fix issue with badly ordered queryies in a transaction
- Introduced a reserved named parameter for prepared statements.
- Do not use parallel file upload with PUT command and table stages.
- Bumping dependencies

1.10.0 (2020-12-03)
-------------------

- Add `{{database}}` token to `query_tag` parameter
- Use Jinja style `query_tag` template variables

1.9.1 (2020-12-02)
-------------------

- Fixed a dependency issue
- Add everything from the unreleased `1.9.0`

1.9.0 (2020-11-18) - NOT RELEASED TO PyPI
-----------------------------------------

- Use snowflake table stages by default to load data into tables
- Add optional `query_tag` parameter
- Add optional `role` parameter to use custom roles
- Fixed an issue when generated file names were not compatible with windows
- Bump `joblib` to `0.16.0` to be python 3.8 compatible
- Bump `snowflake-connectory-python` to `2.3.6`
- Bump `boto3` to `1.16.20`

1.8.0 (2020-08-03)
-------------------

- Fixed an issue when `pipelinewise-target-snowflake` failed when `QUOTED_IDENTIFIERS_IGNORE_CASE` snowflake parameter set to true
- Add `aws_profile` option to support Profile based authentication to S3
- Add option to authenticate to S3 using `AWS_PROFILE`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` environment variables
- Add `s3_endpoint_url` and `s3_region_name` options to support non-native S3 accounts
- Flush stream only if the new schema is not the same as the previous one

1.7.0 (2020-07-23)
-------------------

- Add `s3_acl` option to support ACL for S3 upload
- Fixed an issue when no primary key error logged as `INFO` and not as `ERROR`

1.6.6 (2020-06-26)
-------------------

- Fixed an issue when new columns sometimes not added to target table
- Fixed an issue when the query runner returned incorrect value when multiple queries running in one transaction

1.6.5 (2020-06-17)
-------------------

- Switch jsonschema to use Draft7Validator

1.6.4 (2020-04-20)
-------------------

- Fix loading tables with space in the name

1.6.3 (2020-04-03)
-------------------

- Generate compressed CSV files by default. Optionally can be disabled by the `no_compression` config option

1.6.2 (2020-03-23)
-------------------

- Log inserts, updates and csv size_bytes in a more consumable format

1.6.1 (2020-03-12)
-------------------

- Use SHOW SCHEMAS|TABLES|COLUMNS instead of INFORMATION_SCHEMA

1.6.0 (2020-03-06)
-------------------

- Support usage of reserved words as table names.

1.5.0 (2020-02-18)
-------------------

- Support custom logging configuration by setting `LOGGING_CONF_FILE` env variable to the absolute path of a .conf file

1.4.1 (2020-01-31)
-------------------

- Change default /tmp folder for encrypting files

1.4.0 (2020-01-28)
-------------------

- Make AWS key optional and obtain it secondarily from env vars

1.3.0 (2020-01-15)
-------------------

- Add temp_dir optional parameter to config

1.2.1 (2020-01-13)
-------------------

- Fixed issue when JSON value not sent correctly

1.2.0 (2020-01-07)
-------------------

- Load binary data into Snowflake BINARY data type column

1.1.8 (2019-12-09)
-------------------

- Add missing module `python-dateutil`

1.1.7 (2019-12-09)
-------------------

- Review dates & timestamps and fix them before insert/update

1.1.6 (2019-11-05)
-------------------

- Pinned stable version of `urllib3`

1.1.5 (2019-11-04)
-------------------

- Pinned stable version of `botocore` and `boto3`

1.1.4 (2019-11-04)
-------------------

- Fixed issue when extracting bookmarks from the state messages sometimes failed
 
1.1.3 (2019-11-04)
-------------------

- Bump `snowflake-connector-python` to 2.0.3

1.1.2 (2019-10-25)
-------------------

- Fixed an issue when number of rows in buckets were not calculated correctly and caused flushing of data at the wrong time with degraded performance
 
1.1.1 (2019-10-18)
-------------------

- Fixed an issue when sometimes the last bucket of data was not flushed correctly 

1.1.0 (2019-10-14)
-------------------

- Bump `snowflake-connector-python` to 2.0.1
- Always use secure connection to Snowflake and force auto commit
- Add `flush_all_streams` option
- Add `parallelism` option
- Add `max_parallelism` option

1.0.9 (2019-10-01)
-------------------

- Emit new state message as soon as data flushed to Snowflake

1.0.8 (2019-08-16)
-------------------

- Log SQLs only in debug mode

1.0.7 (2019-08-06)
-------------------

- Further improvements in `information_schema.tables` caching

1.0.6 (2019-07-26)
-------------------

- Improved and optimised `information_schema.tables` caching

1.0.5 (2019-07-17)
-------------------

- Caching `information_schema.tables` to avoid long running SQLs in snowflake
- Instead of DROPPING exiting column RENAME it

1.0.4 (2019-07-01)
-------------------

- Add `data_flattening_max_level` option

1.0.3 (2019-06-29)
-------------------

- Optimised queries to `information_schema.tables`

1.0.2 (2019-06-11)
-------------------

- Create `_sdc_deleted_at` as `VARCHAR` to avoid issues caused by invalid formatted date-times received from taps

1.0.1 (2019-06-07)
-------------------

- Manage only three metadata columns: `_sdc_extracted_at`, `_sdc_batched_at` and `_sdc_deleted_at`

1.0.0 (2019-06-03)
-------------------

- Initial release
