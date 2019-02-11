# target-snowflake

[Singer](https://www.singer.io/) target that loads data into Snowflake following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

## Flow diagram

![Flow Diagram](flow-diagram.jpg)

## Usage

### To run tests:

1. Define environment variables that requires running the tests
```
  export TARGET_SNOWFLAKE_ACCOUNT=<snowflake-account-name>
  export TARGET_SNOWFLAKE_DBNAME=<snowflake-database-name>
  export TARGET_SNOWFLAKE_SCHEMA=<snowflake-schema>
  export TARGET_SNOWFLAKE_USER=<snowflake-user>
  export TARGET_SNOWFLAKE_PASSWORD=<snowfale-password>
  export TARGET_SNOWFLAKE_WAREHOUSE=<snowflake-warehouse>
  export TARGET_SNOWFLAKE_S3_BUCKET=<s3-external-bucket>
  export TARGET_SNOWFLAKE_S3_KEY_PREFIX=<bucket-directory>
  export TARGET_SNOWFLAKE_AWS_ACCESS_KEY=<aws-access-key-id>
  export TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY=<aws-access-secret-access-key>
```

2. Install python dependencies in a virtual env and run nose unit and integration tests
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
  pip install nose
```

3. To run unit tests:
```
  nosetests --where=tests/unit
```

4. To run integration tests:
```
  nosetests --where=tests/integration
```

### To run pylint:

1. Install python dependencies and run python linter
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
  pip install pylint
  pylint target_snowflake -d C,W,unexpected-keyword-arg,duplicate-code
```
