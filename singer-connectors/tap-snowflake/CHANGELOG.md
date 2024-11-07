3.0.0 (2022-03-04)
------------------

- Added private key authentication
- Bumped `snowflake-connector-python[pandas]` from `2.4` to `2.7`
- Dropped python3.6 support

2.0.3 (2021-01-11)
-------------------

- Stop using `LAST_QUERY_ID()` Snowflake function
- Bumping dependencies

2.0.2 (2020-12-04)
-------------------

- Bump `snowflake-connector-python` to 2.3.6

2.0.1 (2020-07-27)
-------------------

- Add optional `pandas` and `pyarrow` packages to avoid runtime warning messages

2.0.0 (2020-04-08)
-------------------

- Discover only the required tables

1.1.2 (2020-03-19)
-------------------

- Delete redundant library `pytz` package

1.1.1 (2020-03-18)
-------------------

- Use `SHOW SCHEMAS|TABLES|COLUMNS` instead of `INFORMATION_SCHEMA`
- Bump `snowflake-connector-python` to 2.2.2

1.1.0 (2020-02-20)
-------------------

- Make logging customizable

1.0.0 (2019-06-02)
-------------------

- Initial release
