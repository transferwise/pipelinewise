# target-snowflake

[Singer](https://www.singer.io/) target that loads data into Snowflake following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

## Flow diagram

![Flow Diagram](flow-diagram.jpg)

### To run

Like any other target that's following the singer specificiation:

`some-singer-tap | target-snowflake --config [config.json]`

It's reading incoming messages from STDIN and using the properites in `config.json` to upload data into Snowflake

### Configuration settings

Settings are stored in a `config.json` JSON and in a map 

| Property                            | Type    | Required?  | Description                                                   |
|-------------------------------------|---------|------------|---------------------------------------------------------------|
| account                             | String  | Yes        | Snowflake account name (i.e. rtXXXXX.eu-central-1)            |
| dbname                              | String  | Yes        | Snowflake Database name                                       |
| user                                | String  | Yes        | Snowflake User                                                |
| password                            | String  | Yes        | Snowflake Password                                            |
| warehouse                           | String  | Yes        | Snowflake virtual warehouse name                              |
| aws_access_key_id                   | String  | Yes        | S3 Access Key Id                                              |
| aws_secret_access_key               | String  | Yes        | S3 Secret Access Key                                          |
| s3_bucket                           | String  | Yes        | S3 Bucket name                                                |
| s3_key_prefix                       | String  |            | A static prefix before the generated S3 key names. Using prefixes you can upload files into specific directories in the S3 bucket. |
| batch_size                          | Integer |            | Maximum number of rows in each batch. At the end of each batch, the rows in the batch are loaded into Snowflake. Defaults to 100000 rows |
| schema                              | String  |            | Name of the schema where the tables will be created           |
| dynamic_schema_name                 | Boolean |            | When it's true, the schema names will be created automatically derived from the incoming stream name |
| dynamic_schema_name_postfix         | String  |            | Can be use together with the `dynamic_schema_name` option to add an optional postfix to the end of the generated schema name |
| grant_select_to                     | String or List of String |            | Grant USAGE privilege on newly created schemas and grant SELECT privilege on new created tables to a specific role or a list of roles. |
| disable_table_cache                 | Boolean |            | By default the connector caches the available table structures in Snowflake at startup. In this way it doesn't need to run additional queries when ingesting data to check if altering the target tables is required. With `disable_table_cache` option you can turn off this caching. You will always see the most recent table structures but will cause an extra query runtime. |
| client_side_encryption_master_key   | String  |            | When this is defined, Client-Side Encryption is enabled. The data in S3 will be encrypted, No third parties, including Amazon AWS and any ISPs, can see data in the clear. Snowflake COPY command will decrypt the data once it's in Snowflake. The master key must be 256-bit length and must be encoded as base64 string. |
| client_side_encryption_stage_object | String  |            | Required when `client_side_encryption_master_key` is defined. The name of the encrypted stage object in Snowflake that created separately and using the same encryption master key. |
| add_metadata_columns                | Boolean |            | Metadata columns add extra row level information about data ingestions, (i.e. when was the row read in source, when was inserted or deleted in snowflake etc.) Metadata columns are creating automatically by adding extra columns to the tables with a column prefix `_SDC_`. The column names are following the stitch naming conventions documented at https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns. Enabling metadata columns will flag the deleted rows by setting the `_SDC_DELETED_AT` metadata column. Without the `add_metadata_columns` option the deleted rows from singer taps will not be recongisable in Snowflake. |
| hard_delete                         | Boolean |            | When `hard_delete` option is true then DELETE SQL commands will be performed in Snowflake to delete rows in tables. It's achieved by continuously checking the  `_SDC_DELETED_AT` metadata column sent by the singer tap. Due to deleting rows requires metadata columns, `hard_delete` option automatically enables the `add_metadata_columns` option as well. |



### To run tests:

1. Define environment variables that requires running the tests
```
  export TARGET_SNOWFLAKE_ACCOUNT=<snowflake-account-name>
  export TARGET_SNOWFLAKE_DBNAME=<snowflake-database-name>
  export TARGET_SNOWFLAKE_USER=<snowflake-user>
  export TARGET_SNOWFLAKE_PASSWORD=<snowfale-password>
  export TARGET_SNOWFLAKE_WAREHOUSE=<snowflake-warehouse>
  export TARGET_SNOWFLAKE_SCHEMA=<snowflake-schema>
  export TARGET_SNOWFLAKE_AWS_ACCESS_KEY=<aws-access-key-id>
  export TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY=<aws-access-secret-access-key>
  export TARGET_SNOWFLAKE_S3_BUCKET=<s3-external-bucket>
  export TARGET_SNOWFLAKE_S3_KEY_PREFIX=<bucket-directory>
  export CLIENT_SIDE_ENCRYPTION_MASTER_KEY=<client_side_encryption_master_key>
  export CLIENT_SIDE_ENCRYPTION_STAGE_OBJECT=<client_side_encryption_stage_object>
```

2. Install python dependencies in a virtual env and run nose unit and integration tests
```
  cd singer-connectors/target-snowflake
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
  cd singer-connectors/target-snowflake
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
  pip install pylint
  pylint target_snowflake -d C,W,unexpected-keyword-arg,duplicate-code
```
