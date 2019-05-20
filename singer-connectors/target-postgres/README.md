# target-postgres

[Singer](https://www.singer.io/) target that loads data into Snowflake following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

### To run

1. Create a `config.json` file with connection details to postgres.

   ```json
   {
     "host": "localhost",
     "port": 5432,
     "user": "my_analytics",
     "password": "myuser",
     "default_target_schema": "myschema"
   }
   ```

There are several extra optional settings that you can add into the `config.json`. For the (full list check 

2. Run `target-postgres` like any other target that's following the singer specificiation:

`some-singer-tap | target-postgres --config config.json`

**Note**: To avoid version conflicts run `tap` and `targets` in separate virtual environments.


### Configuration settings

Available options in `config.json`: 

| Property                            | Type    | Required?  | Description                                                   |
|-------------------------------------|---------|------------|---------------------------------------------------------------|
| host                                | String  | Yes        | PostgreSQL host                                               |
| port                                | Integer | Yes        | PostgreSQL port                                               |
| user                                | String  | Yes        | PostgreSQL user                                               |
| password                            | String  | Yes        | PostgreSQL password                                           |
| dbname                              | String  | Yes        | PostgreSQL database name                                      |
| batch_size                          | Integer |            | (Default: 100000) Maximum number of rows in each batch. At the end of each batch, the rows in the batch are loaded into Snowflake. |
| default_target_schema               | String  |            | Name of the schema where the tables will be created. If `schema_mapping` is not defined then every stream sent by the tap is loaded into this schema.    |
| default_target_schema_select_permission | String  |            | Grant USAGE privilege on newly created schemas and grant SELECT privilege on newly created 
| schema_mapping                      | Object  |            |    |
| add_metadata_columns                | Boolean |            | (Default: False) Metadata columns add extra row level information about data ingestions, (i.e. when was the row read in source, when was inserted or deleted in snowflake etc.) Metadata columns are creating automatically by adding extra columns to the tables with a column prefix `_SDC_`. The column names are following the stitch naming conventions documented at https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns. Enabling metadata columns will flag the deleted rows by setting the `_SDC_DELETED_AT` metadata column. Without the `add_metadata_columns` option the deleted rows from singer taps will not be recongisable in Snowflake. |
| hard_delete                         | Boolean |            | (Default: False) When `hard_delete` option is true then DELETE SQL commands will be performed in Snowflake to delete rows in tables. It's achieved by continuously checking the  `_SDC_DELETED_AT` metadata column sent by the singer tap. Due to deleting rows requires metadata columns, `hard_delete` option automatically enables the `add_metadata_columns` option as well. |


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
  pylint target_postgres -d C,W,unexpected-keyword-arg,duplicate-code
```
