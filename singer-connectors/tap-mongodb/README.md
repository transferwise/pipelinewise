# pipelinewise-tap-mongodb

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md) from a MongoDB source.

## Set up local dev environment:

```shell script
make setup
```

## Activate virtual environment

```shell script
. venv/bin/activate
```

## Set up Config file

Create json file called `config.json`, with the following contents: 
```json
{
  "password": "<password>",
  "user": "<username>",
  "host": "<host ip address>",
  "auth_database": "<database name to authenticate on>",
  "database": "<database name to sync from>"
}
```
The following parameters are optional for your config file:

| Name | Type | Default value| Description |
| -----|------|--------|------------ |
| `srv` | Boolean | false | uses a `mongodb+srv` protocol to connect. Disables the usage of `port` argument if set to `True` |
| `port` | Integer | false | Connection port. Required if a non-srv connection is being used.  |
| `replica_set` | string | null | name of replica set |
| `ssl` | Boolean | false | can be set to true to connect using ssl |
| `verify_mode` | Boolean | true | Default SSL verify mode |
| `include_schemas_in_destination_stream_name` | Boolean |false  | forces the stream names to take the form `<database_name>-<collection_name>` instead of `<collection_name>`|
| `update_buffer_size` | int | 1 | [LOG_BASED] The size of the buffer that holds detected update operations in memory, the buffer is flushed once the size is reached |
| `await_time_ms` | int | 1000 | [LOG_BASED] The maximum amount of time in milliseconds the loge_base method waits for new data changes before exiting. |

All of the above attributes are required by the tap to connect to your mongo instance. 
here is a [sample configuration file](./sample_config.json).

## Run in discovery mode
Run the following command and redirect the output into the catalog file
```shell script
tap-mongodb --config ~/config.json --discover > ~/catalog.json
```

Your catalog file should now look like this:
```json
{
  "streams": [
    {
      "table_name": "<table name>",
      "tap_stream_id": "<tap_stream_id>",
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "row-count":<int>,
            "is-view": <bool>,
            "database-name": "<database name>",
            "table-key-properties": [
              "_id"
            ],
            "valid-replication-keys": [
              "_id"
            ]
          }
        }
      ],
      "stream": "<stream name>",
      "schema": {
        "type": "object"
      }
    }
  ]
}
```

## Edit Catalog file
### Using valid json, edit the config.json file
To select a stream, enter the following to the stream's metadata:
```json
"selected": true,
"replication-method": "<replication method>",
```

`<replication-method>` must be either `FULL_TABLE`, `INCREMENTAL` or `LOG_BASED`, if it's `INCREMENTAL`, make sure to add a `"replication-key"`.


For example, if you were to edit the example stream to select the stream as well as add a projection, config.json should look this:
```json
{
  "streams": [
    {
      "table_name": "<table name>",
      "tap_stream_id": "<tap_stream_id>",
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "row-count": <int>,
            "is-view": <bool>,
            "database-name": "<database name>",
            "table-key-properties": [
              "_id"
            ],
            "valid-replication-keys": [
              "_id"
            ],
            "selected": true,
            "replication-method": "<replication method>"
          }
        }
      ],
      "stream": "<stream name>",
      "schema": {
        "type": "object"
      }
    }
  ]
}

```
## Run in sync mode:
```shell script
tap-mongodb --config ~/config.json --catalog ~/catalog.json
```

The tap will write bookmarks to stdout which can be captured and passed as an optional `--state state.json` parameter to the tap for the next sync.

## Logging configuration
The tap uses a predefined logging config if none is provided, however, you can set your own config by setting the environment variable `LOGGING_CONFIG_FILE` as the path to the logging config.
A sample config is available [here](./sample_logging.conf).

---

Copyright &copy; 2020 TransferWise
