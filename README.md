# PipelineWise
PipelineWise is an ETL and Data Pipeline Framework using the singer.io specification to load data from various sources to various destinations

[![CircleCI](https://circle.tw.ee/gh/transferwise/pipelinewise.svg?style=svg)](https://circle.tw.ee/gh/transferwise/pipelinewise)

## Links

* [Singer ETL specification](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md)
* [Singer.io community slack channel](https://singer-slackin.herokuapp.com/)

## Components

* **Admin Console**: Web interface to add new data flows and monitor existing ones
* **Rest API**: API to start/stop/query components remotely
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
* **tap-zendesk**: Extracts data from Zendesk using OAuth and Key-Based incremental replications
* **target-postgres**: Loads data from any tap into PostgreSQL database
* **target-snowflake**: Loads data from any tap into Snowflake Data Warehouse
* **transform-field**: Transforms fields from any tap and sends the results to any target. Recommended for data masking/ obfuscation

## Installation

### Requirements
* Python 3.x
* Node 8.x
* libsnappy-dev
* python3-dev

### Build from source:
1. `./intall.sh` : Install singer connectors in separated virtual environments, CLI, REST API and Web Frontend

### To run in development mode:

Development mode detects code changes both in the python REST API and the Node.JS User interface and reloads the application automatically for a conveninent development experience

#### REST-API (Dev Mode)

1. Set some environment variables that requires to run it in development mode:
```
  export PIPELINEWISE_SETTINGS=development
  export PIPELINEWISE_HOME=<LOCAL_PATH_TO_THIS_GIT_REPO>
  export FLASK_DEBUG=1
  export FLASK_APP=rest_api
```

2. Activate the python virtual environment and start the REST API listening on `http://localhost:5000`: 
```
  . .virtualenvs/rest-api/bin/activate
  cd rest-api
  flask run
```

#### User Interface (Dev Mode)

1. Start the web user interface listening on `http://localhost:3000`
```
  cd admin-console
  npm run start
```

**Note**: The web UI is using the REST-API to communicate to singer components and that needs to run separately.
