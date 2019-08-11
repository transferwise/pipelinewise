FastSync - MySQL to Redshift
----------------------------

Sync MySQL/MariaDB tables to Amazon Redshift with the most optimal steps:

* Get binlog and inremental key positions if required 
* Generate Redshift compatible DDL to create destination table
* Export to CSV
* Zip (TODO: split to multiple files up to 100MB/file)
* Upload to S3
* CREATE OR REPLACE target and temp stage in Redshift
* Load into Redshift stage stage with COPY
* Obfuscate columns in Redshift temp table with UPDATE
* Swap stage to final destination table in Redshift
* Write binlog and incremental key position to singer state file

`mysql-to-redshift --tap [MYSQL_TAP_CONFIG] --target [REDSHIFT_TARGET_CONFIG] --state [TAP_STATE] [--transform [TRANSFORM]] --tables [LIST_OF_TABLES]`

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

## redshift-config.json

```
  {
    "host": "xxxxxx.redshift.amazonaws.com",
    "port": 5439,
    "user": "my_user",
    "password": "password",
    "dbname": "database_name",
    "aws_access_key_id": "secret",
    "aws_secret_access_key": "secret",
    "s3_bucket": "bucket_name",
    "default_target_schema": "my_target_schema"
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
