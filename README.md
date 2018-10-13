# PipelineWise
PipelineWise is an ETL and Data Pipeline Framework using the singer.io specification to load data from various sources to various destinations

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
* **tap-postgres**: Extract data from PostgreSQL databases. Supporting Log-Based Inremental, Key-Based Incremental and Full Table replications
* **tap-mysql**: Extract data from MySQL databases. Supporting Log-Based Inremental, Key-Based Incremental and Full Table replications
* **tap-zendesk**: Extract data from Zendesk using OAuth and Key-Based incremental replications
* **target-postgres**: Loads data from any tap into PostgreSQL database

## Installation

### Requirements
* Python 3.x
* Node 8.x

### Build from source:
1. `./intall.sh` : Install singer connectors in separated virtual environments, CLI, REST API and Web Frontend

### To run in development mode:

Development mode detects code changes both in the python REST API and the Node.JS User interface and reloads the application automatically for a conveninent development experience

#### REST-API (Dev Mode)

1. `. .virtualenvs/rest-api/bin/activate && cd rest-api && export FLASK_APP=rest_api && export FLASK_DEBUG=1 && export PIPELINEWISE_SETTINGS=development && flask run` : Start REST API at `http://localhost:5000`

#### User Interface (Dev Mode)

2. `cd admin-console && npm run start` : Start web interface at `http://localhost:3000`
