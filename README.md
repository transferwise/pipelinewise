# PipelineWise

[![CircleCI](https://circle.tw.ee/gh/transferwise/pipelinewise.svg?style=svg&circle-token=ae106b83e9ebfdee3aa410bf095b5ccb4f222b95)](https://circle.tw.ee/gh/transferwise/pipelinewise)

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

### Requirements
* Python 3.x
* python3-dev
* python3-venv

### Build from source:

1. `./intall.sh` : Installs the PipelineWise CLI and supported singer connectors in separated virtual environments


#### Loading configurations and running taps

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


### To run tests:

1. Install python dependencies in a virtual env:
```
  python3 -m venv .virtualenvs/cli
  . .virtualenvs/cli/bin/activate
  pip install --upgrade pip
  pip install -e cli
  pip install pytest coverage
```

2. To run unit tests and report code coverage:
```
  cd cli
  coverage run -m pytest --disable-pytest-warnings
  coverage report
```

