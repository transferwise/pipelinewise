# Notice
To better serve Wise business and customer needs, the PipelineWise codebase needs to shrink.
We have made the difficult decision that, going forward many components of PipelineWise will be removed or incorporated in the main repo.
The last version before this decision is [v0.64.1](https://github.com/transferwise/pipelinewise/tree/v0.64.1)

We thank all in the open-source community, that over the past 6 years, have helped to make PipelineWise a robust product for heterogeneous replication of many many Terabytes, daily

# pipelinewise-target-postgres

[![PyPI version](https://badge.fury.io/py/pipelinewise-target-postgres.svg)](https://badge.fury.io/py/pipelinewise-target-postgres)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-target-postgres.svg)](https://pypi.org/project/pipelinewise-target-postgres/)
[![License: Apache2](https://img.shields.io/badge/License-Apache2-yellow.svg)](https://opensource.org/licenses/Apache-2.0)

[Singer](https://www.singer.io/) target that loads data into PostgreSQL following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible target connector.

## How to use it

The recommended method of running this target is to use it from [PipelineWise](https://transferwise.github.io/pipelinewise). When running it from PipelineWise you don't need to configure this tap with JSON files and most of things are automated. Please check the related documentation at [Target Postgres](https://transferwise.github.io/pipelinewise/connectors/targets/postgres.html)

If you want to run this [Singer Target](https://singer.io) independently please read further.

### Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
make venv
```

### To run

Like any other target that's following the singer specification:

`some-singer-tap | target-postgres --config [config.json]`

It's reading incoming messages from STDIN and using the properties in `config.json` to upload data into Postgres.

**Note**: To avoid version conflicts run `tap` and `targets` in separate virtual environments.


#### Spin up a PG DB

Make use of the available docker-compose file to spin up a PG DB.

```bash
docker-compose up -d --build db
```


### Configuration settings

Running the target connector requires a `config.json` file. An example with the minimal settings:

```json
{
    "host": "localhost",
    "port": 5432,
    "user": "my_user",
    "password": "secret",
    "dbname": "target_db",
    "default_target_schema": "public"
}
```

Full list of options in `config.json`:

| Property                            | Type    | Required?  | Description                                                   |
|-------------------------------------|---------|------------|---------------------------------------------------------------|
| host                                | String  | Yes        | PostgreSQL host                                               |
| port                                | Integer | Yes        | PostgreSQL port                                               |
| user                                | String  | Yes        | PostgreSQL user                                               |
| password                            | String  | Yes        | PostgreSQL password                                           |
| dbname                              | String  | Yes        | PostgreSQL database name                                      |
| batch_size_rows                     | Integer |            | (Default: 100000) Maximum number of rows in each batch. At the end of each batch, the rows in the batch are loaded into Postgres. |
| flush_all_streams                   | Boolean |            | (Default: False) Flush and load every stream into Postgres when one batch is full. Warning: This may trigger the COPY command to use files with low number of records. |
| parallelism                         | Integer |            | (Default: 0) The number of threads used to flush tables. 0 will create a thread for each stream, up to parallelism_max. -1 will create a thread for each CPU core. Any other positive number will create that number of threads, up to parallelism_max. |
| max_parallelism                     | Integer |            | (Default: 16) Max number of parallel threads to use when flushing tables. |
| default_target_schema               | String  |            | Name of the schema where the tables will be created. If `schema_mapping` is not defined then every stream sent by the tap is loaded into this schema.    |
| default_target_schema_select_permission | String  |            | Grant USAGE privilege on newly created schemas and grant SELECT privilege on newly created
| schema_mapping                      | Object  |            | Useful if you want to load multiple streams from one tap to multiple Postgres schemas.<br><br>If the tap sends the `stream_id` in `<schema_name>-<table_name>` format then this option overwrites the `default_target_schema` value. Note, that using `schema_mapping` you can overwrite the `default_target_schema_select_permission` value to grant SELECT permissions to different groups per schemas or optionally you can create indices automatically for the replicated tables.<br><br> **Note**: This is an experimental feature and recommended to use via PipelineWise YAML files that will generate the object mapping in the right JSON format. For further info check a [PipelineWise YAML Example](https://transferwise.github.io/pipelinewise/connectors/taps/mysql.html#configuring-what-to-replicate). |
| add_metadata_columns                | Boolean |            | (Default: False) Metadata columns add extra row level information about data ingestions, (i.e. when was the row read in source, when was inserted or deleted in postgres etc.) Metadata columns are creating automatically by adding extra columns to the tables with a column prefix `_SDC_`. The column names are following the stitch naming conventions documented at https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns. Enabling metadata columns will flag the deleted rows by setting the `_SDC_DELETED_AT` metadata column. Without the `add_metadata_columns` option the deleted rows from singer taps will not be recognisable in Postgres. |
| hard_delete                         | Boolean |            | (Default: False) When `hard_delete` option is true then DELETE SQL commands will be performed in Postgres to delete rows in tables. It's achieved by continuously checking the  `_SDC_DELETED_AT` metadata column sent by the singer tap. Due to deleting rows requires metadata columns, `hard_delete` option automatically enables the `add_metadata_columns` option as well. |
| data_flattening_max_level           | Integer |            | (Default: 0) Object type RECORD items from taps can be transformed to flattened columns by creating columns automatically.<br><br>When value is 0 (default) then flattening functionality is turned off. |
| primary_key_required                | Boolean |            | (Default: True) Log based and Incremental replications on tables with no Primary Key cause duplicates when merging UPDATE events. When set to true, stop loading data if no Primary Key is defined. |
| validate_records                    | Boolean |            | (Default: False) Validate every single record message to the corresponding JSON schema. This option is disabled by default and invalid RECORD messages will fail only at load time by Postgres. Enabling this option will detect invalid records earlier but could cause performance degradation. |
| temp_dir                            | String  |            | (Default: platform-dependent) Directory of temporary CSV files with RECORD messages. |

### To run tests:

1. Define environment variables that requires running the tests
```
  export TARGET_POSTGRES_HOST=<postgres-host>
  export TARGET_POSTGRES_PORT=<postgres-port>
  export TARGET_POSTGRES_USER=<postgres-password>
  export TARGET_POSTGRES_PASSWORD=<postgres-password>
  export TARGET_POSTGRES_DBNAME=<postgres-dbname>
  export TARGET_POSTGRES_SCHEMA=<postgres-schema>
```

**PS**: You can run `make env` to export pre-defined environment variables


2. Install python dependencies in a virtual env and run unit and integration tests
```
  make venv
```

3. To run unit tests:
```
  make unit_test
```

4. To run integration tests:
```
  make integration_test
```

### To run pylint:

1. Install python dependencies and run python linter
```
 make venv pylint
```

## License

Apache License Version 2.0

See [LICENSE](LICENSE) to see the full text.
