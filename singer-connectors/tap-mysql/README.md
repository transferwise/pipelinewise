# pipelinewise-tap-mysql

[![PyPI version](https://badge.fury.io/py/pipelinewise-tap-mysql.svg)](https://badge.fury.io/py/pipelinewise-tap-mysql)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-tap-mysql.svg)](https://pypi.org/project/pipelinewise-tap-mysql/)
[![License: MIT](https://img.shields.io/badge/License-GPLv3-yellow.svg)](https://opensource.org/licenses/GPL-3.0)

[Singer](https://www.singer.io/) tap that extracts data from a [MySQL](https://www.mysql.com/) database and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible tap connector.

## How to use it

The recommended method of running this tap is to use it from [PipelineWise](https://transferwise.github.io/pipelinewise). When running it from PipelineWise you don't need to configure this tap with JSON files and most of things are automated. Please check the related documentation at [Tap MySQL](https://transferwise.github.io/pipelinewise/connectors/taps/mysql.html)

If you want to run this [Singer Tap](https://singer.io) independently please read further.

## Usage

This section dives into basic usage of `tap-mysql` by walking through extracting
data from a table. It assumes that you can connect to and read from a MySQL
database.

### Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
  python3 -m venv venv
  pip install pipelinewise-tap-mysql
```

or

```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
```

### Have a source database

There's some important business data siloed in this MySQL database -- we need to
extract it. Here's the table we'd like to sync:

```
mysql> select * from example_db.animals;
+----|----------|----------------------+
| id | name     | likes_getting_petted |
+----|----------|----------------------+
|  1 | aardvark |                    0 |
|  2 | bear     |                    0 |
|  3 | cow      |                    1 |
+----|----------|----------------------+
3 rows in set (0.00 sec)
```

### Create the configuration file

Create a config file containing the database connection credentials, see [sample](config.json.sample).

List of config parameters:

| Parameter         | type                          | required | default                                                                                                                                                           | description                                                                                                               |
|-------------------|-------------------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| host              | string                        | yes      | -                                                                                                                                                                 | mysql/mariadb host                                                                                                        |
| port              | int                           | yes      | -                                                                                                                                                                 | mysql/mariadb port                                                                                                        |
| user              | string                        | yes      | -                                                                                                                                                                 | db username                                                                                                               |
| password          | string                        | yes      | -                                                                                                                                                                 | db password                                                                                                               |
| cursorclass       | string                        | No       | `pymysql.cursors.SSCursor`                                                                                                                                        | set cursorclass used by PyMYSQL                                                                                           |
| database          | string                        | No       | -                                                                                                                                                                 | Database to use, None to not use a particular one. Used by PyMYSQL                                                        |
| server_id         | int                           | False    | Randomly generated int                                                                                                                                            | Used as the slave id when this tap is connecting to the server                                                            |
| filter_dbs        | string                        | False    | -                                                                                                                                                                 | Comma separated list of schemas to extract tables only from particular schemas and to improve data extraction performance |
| use_gtid          | bool                          | False    | False                                                    <br/>                                                                                                         | Flag to enable log based replication using GTID               |
| engine            | string ('mysql' or 'mariadb') | False    | 'mysql'                                                                                                                                                           | Indicate which flavor the server is, used for LOG_BASED with GTID                                                         |
| ssl               | string ("true")               | No       | False                                                                                                                                                             | Enable SSL connection                                                                                                     |
| ssl_ca            | string                        | No       | -                                                                                                                                                                 | for self-signed SSL                                                                                                       |
| ssl_cert          | string                        | No       | -                                                                                                                                                                 | for self-signed SSL                                                                                                       |
| ssl_key           | string                        | No       | -                                                                                                                                                                 | for self-signed SSL                                                                                                       |
| internal_hostname | string | No       | -                                                                                                                                                                 | Override match hostname for google cloud                                                                                  |
| session_sqls      | List of strings               | No       | ```['SET @@session.time_zone="+0:00"', 'SET @@session.wait_timeout=28800', 'SET @@session.net_read_timeout=3600', 'SET @@session.innodb_lock_wait_timeout=3600']``` | Set session variables dynamically.                                                                                        |


### Discovery mode

The tap can be invoked in discovery mode to find the available tables and
columns in the database:

```bash
$ tap-mysql --config config.json --discover

```

A discovered catalog is output, with a JSON-schema description of each table. A
source table directly corresponds to a Singer stream.

```json
{
  "streams": [
    {
      "tap_stream_id": "example_db-animals",
      "table_name": "animals",
      "schema": {
        "type": "object",
        "properties": {
          "name": {
            "inclusion": "available",
            "type": [
              "null",
              "string"
            ],
            "maxLength": 255
          },
          "id": {
            "inclusion": "automatic",
            "minimum": -2147483648,
            "maximum": 2147483647,
            "type": [
              "null",
              "integer"
            ]
          },
          "likes_getting_petted": {
            "inclusion": "available",
            "type": [
              "null",
              "boolean"
            ]
          }
        }
      },
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "row-count": 3,
            "table-key-properties": [
              "id"
            ],
            "database-name": "example_db",
            "selected-by-default": false,
            "is-view": false,
          }
        },
        {
          "breadcrumb": [
            "properties",
            "id"
          ],
          "metadata": {
            "sql-datatype": "int(11)",
            "selected-by-default": true
          }
        },
        {
          "breadcrumb": [
            "properties",
            "name"
          ],
          "metadata": {
            "sql-datatype": "varchar(255)",
            "selected-by-default": true
          }
        },
        {
          "breadcrumb": [
            "properties",
            "likes_getting_petted"
          ],
          "metadata": {
            "sql-datatype": "tinyint(1)",
            "selected-by-default": true
          }
        }
      ],
      "stream": "animals"
    }
  ]
}

```

### Field selection

In sync mode, `tap-mysql` consumes the catalog and looks for tables and fields
have been marked as _selected_ in their associated metadata entries.

Redirect output from the tap's discovery mode to a file so that it can be
modified:

```bash
$ tap-mysql -c config.json --discover > properties.json
```

Then edit `properties.json` to make selections. In this example we want the
`animals` table. The stream's metadata entry (associated with `"breadcrumb": []`) 
gets a top-level `selected` flag, as does its columns' metadata entries. Additionally,
we will mark the `animals` table to replicate using a `FULL_TABLE` strategy. For more,
information, see [Replication methods and state file](#replication-methods-and-state-file).

```json
[
  {
    "breadcrumb": [],
    "metadata": {
      "row-count": 3,
      "table-key-properties": [
        "id"
      ],
      "database-name": "example_db",
      "selected-by-default": false,
      "is-view": false,
      "selected": true,
      "replication-method": "FULL_TABLE"
    }
  },
  {
    "breadcrumb": [
      "properties",
      "id"
    ],
    "metadata": {
      "sql-datatype": "int(11)",
      "selected-by-default": true,
      "selected": true
    }
  },
  {
    "breadcrumb": [
      "properties",
      "name"
    ],
    "metadata": {
      "sql-datatype": "varchar(255)",
      "selected-by-default": true,
      "selected": true
    }
  },
  {
    "breadcrumb": [
      "properties",
      "likes_getting_petted"
    ],
    "metadata": {
      "sql-datatype": "tinyint(1)",
      "selected-by-default": true,
      "selected": true
    }
  }
]
```

### Sync mode

With a properties catalog that describes field and table selections, the tap can be invoked in sync mode:

```bash
$ tap-mysql -c config.json --properties properties.json
```

Messages are written to standard output following the Singer specification. The
resultant stream of JSON data can be consumed by a Singer target.

```json
{"value": {"currently_syncing": "example_db-animals"}, "type": "STATE"}

{"key_properties": ["id"], "stream": "animals", "schema": {"properties": {"name": {"inclusion": "available", "maxLength": 255, "type": ["null", "string"]}, "likes_getting_petted": {"inclusion": "available", "type": ["null", "boolean"]}, "id": {"inclusion": "automatic", "minimum": -2147483648, "type": ["null", "integer"], "maximum": 2147483647}}, "type": "object"}, "type": "SCHEMA"}

{"stream": "animals", "version": 1509133344771, "type": "ACTIVATE_VERSION"}

{"record": {"name": "aardvark", "likes_getting_petted": false, "id": 1}, "stream": "animals", "version": 1509133344771, "type": "RECORD"}

{"record": {"name": "bear", "likes_getting_petted": false, "id": 2}, "stream": "animals", "version": 1509133344771, "type": "RECORD"}

{"record": {"name": "cow", "likes_getting_petted": true, "id": 3}, "stream": "animals", "version": 1509133344771, "type": "RECORD"}

{"stream": "animals", "version": 1509133344771, "type": "ACTIVATE_VERSION"}

{"value": {"currently_syncing": "example_db-animals", "bookmarks": {"example_db-animals": {"initial_full_table_complete": true}}}, "type": "STATE"}

{"value": {"currently_syncing": null, "bookmarks": {"example_db-animals": {"initial_full_table_complete": true}}}, "type": "STATE"}
```

## Replication methods and state file

In the above example, we invoked `tap-mysql` without providing a _state_ file and without specifying a replication 
method. The ways to replicate a given table are `FULL_TABLE`, `LOG_BASED` and `INCREMENTAL`.

### LOG_BASED

LOG_BASED replication makes use of the server's binary logs (binlogs), this method can work with primary 
servers, the tap acts as a replica and requests the primary to stream log events,the tap then consumes events 
pertaining to row changes (inserts, updates, deletes), binlog file rotate and gtid events.

Log_based method always requires an initial sync to get a snapshot of the table and current binlog coordinates/gtid 
position.

The tap support two ways of consuming log events: using binlog coordinates or GTID, the default behavior is using 
binlog coordinates, when turning the `use_gtid` flag, you have to specify the engine flavor (mariadb/mysql) due to 
how different are the GTID implementations in these two engines.

When enabling the `use_gtid` flag and the engine is MariaDB, the tap will dynamically infer the GTID pos from 
existing binlog coordinate in the state, if the engine is mysql, it will fail.

#### State when using binlog coordinates
```json
{
  "bookmarks": {
    "example_db-table1": {"log_file": "mysql-binlog.0003", "log_pos": 3244},
    "example_db-table2": {"log_file": "mysql-binlog.0001", "log_pos": 42},
    "example_db-table3": {"log_file": "mysql-binlog.0003", "log_pos": 100}
  }
}
```

#### State when using GTID
```json
{
  "bookmarks": {
    "example_db-table1": {"log_file": "mysql-binlog.0003", "log_pos": 3244, "gtid": "0:364864374:599"},
    "example_db-table2": {"log_file": "mysql-binlog.0001", "log_pos": 42, "gtid": "0:364864374:375"},
    "example_db-table3": {"log_file": "mysql-binlog.0003", "log_pos": 100, "gtid": "0:364864374:399"}
  }
}
```

### Full Table

Full-table replication extracts all data from the source table each time the tap is invoked.

### Incremental

Incremental replication works in conjunction with a state file to only extract
new records each time the tap is invoked. This requires a replication key to be
specified in the table's metadata as well.

#### Example

Let's sync the `animals` table again, but this time using incremental
replication. The replication method and replication key are set in the
table's metadata entry in properties file:

```json
{
  "streams": [
    {
      "tap_stream_id": "example_db-animals",
      "table_name": "animals",
      "schema": { ... },
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "row-count": 3,
            "table-key-properties": [
              "id"
            ],
            "database-name": "example_db",
            "selected-by-default": false,
            "is-view": false,
            "replication-method": "INCREMENTAL",
            "replication-key": "id"
          }
        },
        ...
      ],
      "stream": "animals"
    }
  ]
}
```

We have no meaningful state so far, so just invoke the tap in sync mode again
without a state file:

```bash
$ tap-mysql -c config.json --properties properties.json
```

The output messages look very similar to when the table was replicated using the
default `FULL_TABLE` replication method. One important difference is that the
`STATE` messages now contain a `replication_key_value` -- a bookmark or
high-water mark -- for data that was extracted:

```json
{"type": "STATE", "value": {"currently_syncing": "example_db-animals"}}

{"stream": "animals", "type": "SCHEMA", "schema": {"type": "object", "properties": {"id": {"type": ["null", "integer"], "minimum": -2147483648, "maximum": 2147483647, "inclusion": "automatic"}, "name": {"type": ["null", "string"], "inclusion": "available", "maxLength": 255}, "likes_getting_petted": {"type": ["null", "boolean"], "inclusion": "available"}}}, "key_properties": ["id"]}

{"stream": "animals", "type": "ACTIVATE_VERSION", "version": 1509135204169}

{"stream": "animals", "type": "RECORD", "version": 1509135204169, "record": {"id": 1, "name": "aardvark", "likes_getting_petted": false}}

{"stream": "animals", "type": "RECORD", "version": 1509135204169, "record": {"id": 2, "name": "bear", "likes_getting_petted": false}}

{"stream": "animals", "type": "RECORD", "version": 1509135204169, "record": {"id": 3, "name": "cow", "likes_getting_petted": true}}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"version": 1509135204169, "replication_key_value": 3, "replication_key": "id"}}, "currently_syncing": "example_db-animals"}}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"version": 1509135204169, "replication_key_value": 3, "replication_key": "id"}}, "currently_syncing": null}}
```

Note that the final `STATE` message has a `replication_key_value` of `3`,
reflecting that the extraction ended on a record that had an `id` of `3`.
Subsequent invocations of the tap will pick up from this bookmark.

Normally, the target will echo the last `STATE` after it's finished processing
data. For this example, let's manually write a `state.json` file using the
`STATE` message:

```json
{
  "bookmarks": {
    "example_db-animals": {
      "version": 1509135204169,
      "replication_key_value": 3,
      "replication_key": "id"
    }
  },
  "currently_syncing": null
}
```

Let's add some more animals to our farm:

```
mysql> insert into animals (name, likes_getting_petted) values ('dog', true), ('elephant', true), ('frog', false);
```

```bash
$ tap-mysql -c config.json --properties properties.json --state state.json
```

This invocation extracts any data since (and including) the
`replication_key_value`:

```json
{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"replication_key": "id", "version": 1509135204169, "replication_key_value": 3}}, "currently_syncing": "example_db-animals"}}

{"key_properties": ["id"], "schema": {"properties": {"name": {"maxLength": 255, "inclusion": "available", "type": ["null", "string"]}, "id": {"maximum": 2147483647, "minimum": -2147483648, "inclusion": "automatic", "type": ["null", "integer"]}, "likes_getting_petted": {"inclusion": "available", "type": ["null", "boolean"]}}, "type": "object"}, "type": "SCHEMA", "stream": "animals"}

{"type": "ACTIVATE_VERSION", "version": 1509135204169, "stream": "animals"}

{"record": {"name": "cow", "id": 3, "likes_getting_petted": true}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}
{"record": {"name": "dog", "id": 4, "likes_getting_petted": true}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}
{"record": {"name": "elephant", "id": 5, "likes_getting_petted": true}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}
{"record": {"name": "frog", "id": 6, "likes_getting_petted": false}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"replication_key": "id", "version": 1509135204169, "replication_key_value": 6}}, "currently_syncing": "example_db-animals"}}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"replication_key": "id", "version": 1509135204169, "replication_key_value": 6}}, "currently_syncing": null}}
```

## To run tests:

1. You'll need to have a running MySQL or MariaDB server to run the tests. Run the following SQL commands as a privileged user to create the required objects:
```
CREATE USER <mysql-user> IDENTIFIED BY '<mysql-password>';
CREATE DATABASE tap_mysql_test;
GRANT ALL PRIVILEGES ON tap_mysql_test.* TO <mysql-user>;
```

**Note**: The user and password can be anything but the database name needs to be `tap_mysql_test`.

2. Define the environment variables that are required to run the tests:
```
  export TAP_MYSQL_HOST=<mysql-host>
  export TAP_MYSQL_PORT=<mysql-port>
  export TAP_MYSQL_USER=<mysql-user>
  export TAP_MYSQL_PASSWORD=<mysql-password>
  export TAP_MYSQL_ENGINE=<engine>
```

3. Install python test dependencies in a virtual env

```bash
python3 -m venv venv
. venv/bin/activate
pip install --upgrade pip
pip install .[test]
```

4. To run tests:
```bash
nosetests -c .noserc tests
```

### To run pylint:

1. Install python dependencies and run python linter
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .[test]
  pylint --rcfile .pylintrc tap_mysql
```

---

Based on Stitch documentation
