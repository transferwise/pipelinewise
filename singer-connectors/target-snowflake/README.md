# target-snowflake

[Singer](https://www.singer.io/) target that loads data into Snowflake following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

## Flow diagram

![Flow Diagram](flow-diagram.jpg)

### To run

Like any other target that's following the singer specificiation:

`some-singer-tap | target-snowflake --config [config.json]`

It's reading incoming messages from STDIN and using the properites in `config.json` to upload data into Snowflake

### Pre-requirements

You need to create two objects in snowflake in one schema before start using this target.

1. A named external stage object on S3. This will be used to upload the CSV files to S3 and to MERGE data into snowflake tables.

```
CREATE STAGE {schema}.{stage_name}
url='s3://{s3_bucket}'
credentials=(AWS_KEY_ID='{aws_key_id}' AWS_SECRET_KEY='{aws_secret_key}')
encryption=(MASTER_KEY='{client_side_encryption_master_key}');
```

The `encryption` option is optional and used for client side encryption. If you want client side encryption enabled you'll need
to define the same master key in the target `config.json`. Furhter details below in the Configuration settings section.

2. A named file format. This will be used by the MERGE/COPY commands to parse the CSV files correctly from S3:

`CREATE file format IF NOT EXISTS {schema}.{file_format_name} type = 'CSV' escape='\\' field_optionally_enclosed_by='"';`


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
| s3_key_prefix                       | String  |            | (Default: None) A static prefix before the generated S3 key names. Using prefixes you can upload files into specific directories in the S3 bucket. |
| stage                               | String  | Yes        | Named external stage name created at pre-requirements section. Has to be a fully qualified name including the schema name |
| file_format                         | String  | Yes        | Named file format name created at pre-requirements section. Has to be a fully qualified name including the schema name. |
| batch_size                          | Integer |            | (Default: 100000) Maximum number of rows in each batch. At the end of each batch, the rows in the batch are loaded into Snowflake. |
| default_target_schema               | String  |            | Name of the schema where the tables will be created. If `schema_mapping` is not defined then every stream sent by the tap is loaded into this schema.    |
| default_target_schema_select_permission | String  |            | Grant USAGE privilege on newly created schemas and grant SELECT privilege on newly created tables to a specific role or a list of roles. If `schema_mapping` is not defined then every stream sent by the tap is granted accordingly.   |
| schema_mapping                      | Object  |            |    |
| disable_table_cache                 | Boolean |            | (Default: False) By default the connector caches the available table structures in Snowflake at startup. In this way it doesn't need to run additional queries when ingesting data to check if altering the target tables is required. With `disable_table_cache` option you can turn off this caching. You will always see the most recent table structures but will cause an extra query runtime. |
| client_side_encryption_master_key   | String  |            | (Default: None) When this is defined, Client-Side Encryption is enabled. The data in S3 will be encrypted, No third parties, including Amazon AWS and any ISPs, can see data in the clear. Snowflake COPY command will decrypt the data once it's in Snowflake. The master key must be 256-bit length and must be encoded as base64 string. |
| client_side_encryption_stage_object | String  |            | (Default: None) Required when `client_side_encryption_master_key` is defined. The name of the encrypted stage object in Snowflake that created separately and using the same encryption master key. |
| add_metadata_columns                | Boolean |            | (Default: False) Metadata columns add extra row level information about data ingestions, (i.e. when was the row read in source, when was inserted or deleted in snowflake etc.) Metadata columns are creating automatically by adding extra columns to the tables with a column prefix `_SDC_`. The column names are following the stitch naming conventions documented at https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns. Enabling metadata columns will flag the deleted rows by setting the `_SDC_DELETED_AT` metadata column. Without the `add_metadata_columns` option the deleted rows from singer taps will not be recongisable in Snowflake. |
| hard_delete                         | Boolean |            | (Default: False) When `hard_delete` option is true then DELETE SQL commands will be performed in Snowflake to delete rows in tables. It's achieved by continuously checking the  `_SDC_DELETED_AT` metadata column sent by the singer tap. Due to deleting rows requires metadata columns, `hard_delete` option automatically enables the `add_metadata_columns` option as well. |



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
  export TARGET_SNOWFLAKE_STAGE=<stage-object-with-schema-name>
  export TARGET_SNOWFLAKE_FILE_FORMAT=<file-format-object-with-schema-name>
  export CLIENT_SIDE_ENCRYPTION_MASTER_KEY=<client_side_encryption_master_key>
  export CLIENT_SIDE_ENCRYPTION_STAGE_OBJECT=<client_side_encryption_stage_object>
```

2. Install python dependencies in a virtual env and run nose unit and integration tests
```
  python3 -m venv venv
  . venv/bin/activate
  cd singer-connectors/target-snowflake
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
  cd singer-connectors/target-snowflake
  pip install --upgrade pip
  pip install .
  pip install pylint
  pylint target_snowflake -d C,W,unexpected-keyword-arg,duplicate-code
```
