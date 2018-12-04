FastSync - Mysql to Snowflake
-----------------------------

**This is very quick and very dirty**

This takes snapshots of tables from a MySQL tap and loads into Snowflake tables efficiently:
* Run a generated SELECT in MySQL with embedded obfuscations to extract full table
* Export to CSV, split, zip and upload the results to S3
* Create the destination tables in Snowflake
* Load into snowflake temp tables from S3
* Update the tap state file with updated incremental key or binlog positions 

To run:

`mysql-to-snowflake --mysql-config [MYSQL_TAP_CONFIG] --snowflake-config [SNOWFLAKE_TARGET_CONFIG] --properties [MYSQL_CATALOG] --state [MYSQL_STATE] --transform-config [TRANSFORMATIONS_CONFIG] --export-dir [TEMP_PATH_DIR] --limit-tables [OPTIONAL_LIST_OF_TABLES]`

The output is the updated state file (bookmark) that is valid for the next binlog or key-based incremental load.