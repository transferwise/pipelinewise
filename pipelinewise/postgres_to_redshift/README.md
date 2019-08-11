FastSync - Postgres to Redshift
-------------------------------

Sync PostgreSQL tables to Amazon Redshift with the most optimal steps:

* Get wal and inremental key positions if required 
* Generate Redshift compatible DDL to create destination table. TODO: Add Primary Key
* Export to CSV with Postgres COPY
* Zip
* Upload to S3
* CREATE OR REPLACE target and temp stage in Redshift
* Load into Redshift stage stage with COPY
* Obfuscate columns in Redshift temp table with UPDATE
* Swap stage to final destination table in Redshift
* Write wal and incremental key position to singer state file

`postgres-to-redshift --tap [POSTGRES_TAP_CONFIG] --target [REDSHIFT_TARGET_CONFIG] --state [TAP_STATE] [--transform [TRANSFORM]] --tables [LIST_OF_TABLES]`


Sample tap config files

## postgres-config.json

```
  {
    "host": "<postgres-host>",
    "port": <postgres-port>,
    "dbname": "<postgres-db",
    "user": "<postgres-user>",
    "password": "<postgres-password>"
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