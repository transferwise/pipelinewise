# PipelineWise
PipelineWise is a Data Pipeline Framework using the singer.io specification to ingest and replicate data from various sources to various destinations.

Documentation is available at https://transferwise.github.io/pipelinewise/index.html

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
* **tap-postgres**: Extracts data from PostgreSQL databases. Supporting Log-Based Inremental, Key-Based Incremental and Full Table replications
* **tap-mysql**: Extracts data from MySQL databases. Supporting Log-Based Inremental, Key-Based Incremental and Full Table replications
* **tap-kafka**: Extracts data from Kafka streams
* **tap-adwords**: Extracts data Google Ads API (former Google Adwords) using OAuth and support incremental loading based on input state
* **tap-s3-csv**: Extracts data from S3 csv files (currently a fork of tap-s3-csv because we wanted to use our own auth method)
* **tap-zendesk**: Extracts data from Zendesk using OAuth and Key-Based incremental replications
* **tap-snowflake**: Extracts data from Snowflake databases. Supporting Key-Based Incremental and Full Table replications
* **target-postgres**: Loads data from any tap into PostgreSQL database
* **target-snowflake**: Loads data from any tap into Snowflake Data Warehouse
* **target-s3-csv**: Uploads data from any tap to S3 in CSV format
* **transform-field**: Transforms fields from any tap and sends the results to any target. Recommended for data masking/ obfuscation

## Installation

### Requirements
* Python 3.x
* libsnappy-dev
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

5. Run your tap: `pipelinewise run_tap --target snowflake_test --tap adwords`

    Logs for tap outputs are stored in `~/.pipelinewise/snowflake_test/`
