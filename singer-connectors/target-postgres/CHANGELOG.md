2.1.2 (2023-01-17)
-------------------
- Add python 3.10 compatibility
- Bump `joblib` from 0.16.0 to 1.2.0
- Bump `psycopg2-binary` from 2.8.5 to 2.9.5 to get macOS arm64 (Apple M1) wheels

2.1.1 (2021-05-11)
-------------------
- Bump `joblib` from 0.13.2 to 0.16.0 to be Python 3.8 compatible


2.1.0 (2020-09-22)
-------------------

- Enable SSL mode if `ssl` option is `"true"` in config

2.0.1 (2020-06-17)
-------------------

- Switch jsonschema to use Draft7Validator

2.0.0 (2020-05-02)
-------------------

**WARNING**: This release includes non backward compatible changes.
Starting from `pipelinewise-target-postgres-2.0.0` the `integer` JSON Schema column types with minimum and maximum
boundaries are loaded into Postgres `SMALLINT`, `INTEGER` and `BIGINT` values. If you're upgrading from an
earlier version of pipelinewise-target-postgres then it's recommended to re-sync every table otherwise all the existing
`NUMERIC` columns in Postgres will be versioned to the corresponding integer type.

Further info about versioning columns at https://transferwise.github.io/pipelinewise/user_guide/schema_changes.html?highlight=versioning#versioning-columns

### Changes
- Add `flush_all_streams` option
- Add `parallelism` option
- Add `max_parallelism` option
- Add `validate_records` option
- Log inserts, updates and csv size_bytes in a more consumable format
- Fixed an issue when JSON values sometimes not sent correctly
- Support usage of reserved words as table and column names
- Add `temp_dir` optional parameter to config
- Load `integer` JSON Schema types with min and max boundaries to Postgres `SMALLINT`, `INTEGER`, `BIGINT` column types
- Switch to `psychopg-binary` 2.8.5

1.1.0 (2019-02-18)
-------------------

- Support custom logging configuration by setting `LOGGING_CONF_FILE` env variable to the absolute path of a .conf file

1.0.4 (2019-10-01)
-------------------

- Grant privileges correctly when table created

1.0.3 (2019-09-01)
-------------------

- Fixed type mapping

1.0.2 (2019-08-16)
-------------------

- Add license details

1.0.1 (2019-08-12)
-------------------

- Sync column versioning feature to other supported targets
- Sync data flattening option to other supported targets
- Sync metadata columns behaviour to other supported targets
- Bump `psycopg2` to 2.8.2

1.0.0 (2019-06-03)
-------------------

- Initial release
