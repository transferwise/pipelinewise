# target-s3-csv

[Singer](https://www.singer.io/) target that uploads loads data to S3 in CSV format
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible target connector.

### To run

1. Create a `config.json` file with connection details to postgres.

   ```json
   {
     "aws_access_key_id": "localhost",
     "aws_secret_access_key": 5432,
     "bucket": "my_analytics"
   }
   ```

There are several extra optional settings that you can add into the `config.json`. Check the full list below.

2. Run `target-s3-csv` like any other target that's following the singer specificiation:

`some-singer-tap | target-s3-csv --config config.json`

**Note**: To avoid version conflicts run `tap` and `targets` in separate virtual environments.


### Configuration settings

Available options in `config.json`: 

| Property                            | Type    | Required?  | Description                                                   |
|-------------------------------------|---------|------------|---------------------------------------------------------------|
| aws_access_key_id                   | String  | Yes        | S3 Access Key Id                                              |
| aws_secret_access_key               | String  | Yes        | S3 Secret Access Key                                          |
| s3_bucket                           | String  | Yes        | S3 Bucket name                                                |
| s3_key_prefix                       | String  |            | (Default: None) A static prefix before the generated S3 key names. Using prefixes you can 
| delimiter                           | String  |            | (Default: ',') A one-character string used to separate fields. |
| quotechar                           | String  |            | (Default: '"') A one-character string used to quote fields containing special characters, such as the delimiter or quotechar, or which contain new-line characters. |
| add_metadata_columns                | Boolean |            | (Default: False) Metadata columns add extra row level information about data ingestions, (i.e. when was the row read in source, when was inserted or deleted in snowflake etc.) Metadata columns are creating automatically by adding extra columns to the tables with a column prefix `_SDC_`. The column names are following the stitch naming conventions documented at https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns. Enabling metadata columns will flag the deleted rows by setting the `_SDC_DELETED_AT` metadata column. Without the `add_metadata_columns` option the deleted rows from singer taps will not be recongisable in Snowflake. |


### To run tests:

1. Install python dependencies in a virtual env and run nose unit and integration tests
```
  python3 -m venv venv
  . venv/bin/activate
  cd singer-connectors/target-s3-csv
  pip install --upgrade pip
  pip install .
  pip install nose
```

3. To run unit tests:
```
  nosetests --where=tests/unit
```

### To run pylint:

1. Install python dependencies and run python linter
```
  python3 -m venv venv
  . venv/bin/activate
  cd singer-connectors/target-s3-csv
  pip install --upgrade pip
  pip install .
  pip install pylint
  pylint target_s3_csv -d C,W,unexpected-keyword-arg,duplicate-code
```
