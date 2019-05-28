# tap-snowflake

Singer tap for Snowflake supporting Full Table & Incremental Replication


[Singer](https://www.singer.io/) tap that extracts data from a [Snowflake](https://www.snowflake.com/) database and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible tap connector.

### To run

1. Create a `config.json` file with connection details to postgres.

   ```json
   {
     "account": "rtxxxxx.eu-central-1",
     "dbname": "database_name",
     "user": "my_user",
     "password": "password",
     "warehouse": "my_virtual_warehouse",
     "filter_dbs": "database_name",
     "filter_schemas": "schema1,schema2"
   }
   ```

`filter_dbs` and `filter_schemas` are optional.

2. Run it in discovery mode to generate a `properties.json`

3. Edit the `properties.json` and select the streams to replicate

4. Run the tap like any other singer compatible tap:

```
  tap-snowflake --config config.json --properties properties.json --state state.json
```

### Discovery mode

The tap can be invoked in discovery mode to find the available tables and
columns in the database:

```bash
$ tap-snowflake --config config.json --discover

```

A discovered catalog is output, with a JSON-schema description of each table. A
source table directly corresponds to a Singer stream.

## Replication methods

The two ways to replicate a given table are `FULL_TABLE` and `INCREMENTAL`.

### Full Table

Full-table replication extracts all data from the source table each time the tap
is invoked.

### Incremental

Incremental replication works in conjunction with a state file to only extract
new records each time the tap is invoked. This requires a replication key to be
specified in the table's metadata as well.
