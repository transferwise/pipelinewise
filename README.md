# PipelineWise
PipelineWise is a Data Pipeline Framework using the singer.io specification to ingest and replicate data from various sources to various destinations.

Documentation is available at https://transferwise.github.io/pipelinewise/

## Links

* [Singer ETL specification](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md)
* [Singer.io community slack channel](https://singer-slackin.herokuapp.com/)

## Components

* **Command Line Interface**: start/stop/query components from the command line
* **Singer Connectors**: Simple, Composable, Open Source ETL framework 
 
### Singer Connector Definitions
* **Tap**: Extracts data from any source and write it to a standard stream in a JSON-based format.
* **Target**: Consumes data from taps and do something with it, like load it into a file, API or database

### Supported Connectors

| Type      | Name       | Latest Version | Description                                          |
|-----------|------------|----------------|------------------------------------------------------|
| Tap       | **[Postgres](https://github.com/transferwise/pipelinewise-tap-postgres)** | [![PyPI version](https://badge.fury.io/py/pipelinewise-tap-postgres.svg)](https://badge.fury.io/py/pipelinewise-tap-postgres) | Extracts data from PostgreSQL databases. Supporting Log-Based Inremental, Key-Based Incremental and Full Table replications |
| Tap       | **[MySQL](https://github.com/transferwise/pipelinewise-tap-mysql)** | [![PyPI version](https://badge.fury.io/py/pipelinewise-tap-mysql.svg)](https://badge.fury.io/py/pipelinewise-tap-mysql) | Extracts data from MySQL databases. Supporting Log-Based Inremental, Key-Based Incremental and Full Table replications |
| Tap       |**[Kafka](https://github.com/transferwise/pipelinewise-tap-kafka)** | [![PyPI version](https://badge.fury.io/py/pipelinewise-tap-kafka.svg)](https://badge.fury.io/py/pipelinewise-tap-kafka) | Extracts data from Kafka topics |
| Tap       |**[AdWords](https://github.com/singer-io/tap-adwords)** | [![PyPI version](https://badge.fury.io/py/tap-adwords.svg)](https://badge.fury.io/py/tap-adwords) | Extracts data Google Ads API (former Google Adwords) using OAuth and support incremental loading based on input state |
| Tap       | **[S3 CSV](https://github.com/transferwise/pipelinewise-tap-s3-csv)** | [![PyPI version](https://badge.fury.io/py/pipelinewise-tap-s3-csv.svg)](https://badge.fury.io/py/pipelinewise-tap-s3-csv) | Extracts data from S3 csv files (currently a fork of tap-s3-csv because we wanted to use our own auth method) |
| Tap       | **[Zendesk](https://github.com/singer-io/tap-zendesk)** | [![PyPI version](https://badge.fury.io/py/tap-zendesk.svg)](https://badge.fury.io/py/tap-zendesk) | Extracts data from Zendesk using OAuth and Key-Based incremental replications |
| Tap       | **[Snowflake](https://github.com/transferwise/pipelinewise-tap-snowflake)** | [![PyPI version](https://badge.fury.io/py/pipelinewise-tap-snowflake.svg)](https://badge.fury.io/py/pipelinewise-tap-snowflake) | Extracts data from Snowflake databases. Supporting Key-Based Incremental and Full Table replications |
| Tap       | **[Salesforce](https://github.com/singer-io/tap-salesforce)** | [![PyPI version](https://badge.fury.io/py/tap-salesforce.svg)](https://badge.fury.io/py/tap-zendesk) | Extracts data from Salesforce database using BULK and REST extraction API with Key-Based incremental replications |
| Tap       | **[Jira](https://github.com/singer-io/tap-jira)** | [![PyPI version](https://badge.fury.io/py/tap-jira.svg)](https://badge.fury.io/py/tap-jira) | Extracts data from Atlassian Jira using Base auth or OAuth credentials |
| Target    | **[Postgres](https://github.com/transferwise/pipelinewise-target-postgres)** | [![PyPI version](https://badge.fury.io/py/pipelinewise-target-postgres.svg)](https://badge.fury.io/py/pipelinewise-target-postgres) | Loads data from any tap into PostgreSQL database |
| Target    | **[Snowflake](https://github.com/transferwise/pipelinewise-target-snowflake)** | [![PyPI version](https://badge.fury.io/py/pipelinewise-target-snowflake.svg)](https://badge.fury.io/py/pipelinewise-target-snowflake) | Loads data from any tap into Snowflake Data Warehouse |
| Target    | **[S3 CSV](https://github.com/transferwise/pipelinewise-target-s3-csv)** | [![PyPI version](https://badge.fury.io/py/pipelinewise-target-s3-csv.svg)](https://badge.fury.io/py/pipelinewise-target-s3-csv) | Uploads data from any tap to S3 in CSV format |
| Transform | **[Field](https://github.com/transferwise/pipelinewise-transform-field)** | [![PyPI version](https://badge.fury.io/py/pipelinewise-transform-field.svg)](https://badge.fury.io/py/pipelinewise-transform-field) | Transforms fields from any tap and sends the results to any target. Recommended for data masking/ obfuscation |

## Installation

### Building from source

1. Make sure that every dependencies installed on your system:
* Python 3.x
* python3-dev
* python3-venv

2. Run the install script that installs the PipelineWise CLI and every supported singer connectors into separated virtual environments:
   
```sh
$ ./install.sh
```
(Press Y to accept the license agreement of the required singer components)

3. To start CLI you need to activate the CLI virtual environment and has to set `PIPELINEWISE_HOME` environment variable:
   
```sh
$ source {ACTUAL_ABSOLUTE_PATH}/.virtualenvs/cli/bin/activate
$ export PIPELINEWISE_HOME={ACTUAL_ABSOLUTE_PATH}
```
(The `ACTUAL_ABSOLUTE_PATH` differs on every system, the install script prints you the correct command that fits
to your system once the installation completed)

4. Run any pipelinewise commands to test the installation:
   
```sh
$ pipelinewise status # Can be any other valid PipelineWise command
```

## Developing with Docker

If you have [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) installed, you can
easily create a local development environment setup quickly that includes not only the PipelineWise executables but
some open source databases for a more convenient development experience:
* PipelineWise CLI with every supported tap and target connectors
* MySQL test source database    (for tap-mysql)
* Postgres test source database (for tap-postgres)
* Postgres test target database (for target-snowflake)

To create local development environment:
```
$ docker-compose up --build
```

As soon as you see `PipelineWise Dev environment is ready in Docker container(s).` you can shell into the container and
start running PipelineWise commands. At this point every virtual environment is created and every required environment
variables is set.

To shell into the ready to use PipelineWise container:

```sh
$ docker exec -it pipelinewise_dev bash
```

To run PipelineWise command:

```sh
$ pipelinewise status # Can be any other valid PipelineWise command
```

To run unit tests:

```sh
$ pytest --disable-pytest-warnings
```

To run unit tests and report code coverage:

```
$ cd cli
$ coverage run -m pytest --disable-pytest-warnings && coverage report
```

To generate HTML coverage report.

```
$ cd cli
$ coverage run -m pytest --disable-pytest-warnings && coverage html -d coverage_html
```

**Note**: The HTML report will be generated in `cli/coverage_html/index.html`
and can be opened **only** from the docker host and not inside from the container.


To refresh the containers with new local code changes stop the running instances with ctrl+c
and restart as usual.

```sh
$ docker exec -it pipelinewise_dev bash
```

### Test databases in the docker development environment

The docker environment 

| Database      | Port (from docker host) | Port (inside from CLI container) | Database Name      |
|---------------|-------------------------|----------------------------------|--------------------|
| Postgres (1)  | localhost:15432         | db_postgres_source:5432          | postgres_source_db |
| MySQL         | localhost:13306         | db_mysql_source:3306             | mysql_source_db    |
| Postgres (2)  | localhost:15433         | db_postgres_dwh:5432             | postgres_dwh       |

For user and passwords check the `.env` file.


## Loading configurations and running taps

1. Clone [analytics-platform-config](https://github.com/transferwise/analytics-platform-config) repo.

    For convenience, create a new branch in your local environment, and remove all taps and targets but `target_snowflake_test.yml` (Otherwise when you load config, Singer will try to connect to each production database that has been configured in the taps).
snowflake_test tap has been configured to use Snowflake test database, AWS staging buckets, etc.

2. Activate CLI virtualenv: `. .virtualenvs/cli/bin/activate`

    You can activate singer-connectors virtual envs (all taps have their own virtualenv) with `. .virtualenvs/tap-mysql/bin/activate` (just substitute your own tap)

3. Set `PIPELINEWISE_HOME` environment variable: `export PIPELINEWISE_HOME=<LOCAL_ABSOLUTE_PATH_TO_THIS_REPOSITORY>`

4. Import configurations:  `pipelinewise import_config --dir ~/analytics-platform-config/pipelinewise/ --secret ~/ap-secret.txt`
    - --dir argument points to analytics-platform-config repo
    - --secret points to vault encryption key
    
    If it says ~/.pipelinewise directory doesn't exist, simply create that dir. TODO! create through install. 

5. Run your tap: `pipelinewise run_tap --target snowflake_test --tap adwords`

    Logs for tap outputs are stored in `~/.pipelinewise/snowflake_test/`
