FastSync - MySQL to Snowflake
-----------------------------

Sync mysql tables to snowflake with the most optimal steps:

* Get binlog position
* Generate Snowflake compatible DDL to create destination table
* Export to CSV
* Zip (TODO: split to multiple files up to 100MB/file)
* Upload to S3
* CREATE OR REPLACE target and temp table in Snowflake
* Load into Snowflake temp table with COPY
* Obfuscate columns in Snowflake temp table with UPDATE
* Swap temp to to final destination table in snowflake
* Write binlog position to singer state file

`mysql-to-snowflake --tap [MYSQL_TAP_CONFIG] --target [SNOWFLAKE_TARGET_CONFIG] --state [TAP_STATE] [--transform [TRANSFORM]] --tables [LIST_OF_TABLES]`

Sample config files

## mysql-config.json

```
  {
    "host": "<mysql-host>",
    "port": <mysql-port>,
    "dbname": "<mysql-db",
    "user": "<mysql-user>",
    "password": "<mysql-password>"
  }
```

## snowflake-config.json

```
  {
      "account": "<snowflake-account-name>",
      "dbname": "<snowflake-database-name>",
      "user": "<snowflake-user>",
      "password": "<snowfale-password>",
      "warehouse": "<snowflake-warehouse>",
      "aws_access_key_id": "<aws-access-key-id>",
      "aws_secret_access_key": "<aws-access-secret-access-key>",
      "s3_bucket": "<s3-external-bucket>",
      "s3_key_prefix": "<bucket-directory>",
      "schema": "<snowflake-schema>",
      "client_side_encryption_master_key": "<client_side_encryption_master_key>",
      "client_side_encryption_stage_object": "<client_side_encryption_stage_object>"
  }
```

## transformation.json

```
  {
    "transformations": [
      {
        "stream": "user_profile",
        "fieldId": "last_name",
        "type": "HASH"
      },
      {
        "stream": "user_profile",
        "fieldId": "registration_number",
        "type": "SET_NULL"
      }
    }
  }
```
