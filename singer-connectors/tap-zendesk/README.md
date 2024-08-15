# Pipelinewise-tap-zendesk

[![PyPI version](https://badge.fury.io/py/pipelinewise-tap-zendesk.svg)](https://badge.fury.io/py/pipelinewise-tap-zendesk)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-tap-zendesk.svg)](https://pypi.org/project/pipelinewise-tap-zendesk/)
[![License: MIT](https://img.shields.io/badge/License-GPLv3-yellow.svg)](https://opensource.org/licenses/GPL-3.0)

[Singer](https://www.singer.io/) tap that extracts data from a Zendesk API and produces JSON-formatted data following 
the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible tap connector.

## How to use it

The recommended method of running this tap is to use it from [PipelineWise](https://transferwise.github.io/pipelinewise). When running it from PipelineWise you don't need to configure this tap with JSON files and most of things are automated. Please check the related documentation at [Kafka](https://transferwise.github.io/pipelinewise/connectors/taps/zendesk.html)

If you want to run this [Singer Tap](https://singer.io) independently please read further.

## Install and Run

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
  python3 -m venv venv
  pip install pipelinewise-tap-zendesk
```

or

```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install -e .[test]
```

### Configuration

### Authentication

### Using OAuth

OAuth is the default authentication method for `tap-zendesk`. To use OAuth, you will need to fetch an `access_token` from a configured Zendesk integration. See https://support.zendesk.com/hc/en-us/articles/203663836 for more details on how to integrate your application with Zendesk.

**config.json**
```json
{
  "access_token": "AVERYLONGOAUTHTOKEN",
  "subdomain": "acme",
  "start_date": "2000-01-01T00:00:00Z"
}
```

### Using API Tokens

For a simplified, but less granular setup, you can use the API Token authentication which can be generated from the Zendesk Admin page. See https://support.zendesk.com/hc/en-us/articles/226022787-Generating-a-new-API-token- for more details about generating an API Token. You'll then be able to use the admins's `email` and the generated `api_token` to authenticate.

**config.json**
```json
{
  "email": "user@domain.com",
  "api_token": "THISISAVERYLONGTOKEN",
  "subdomain": "acme",
  "start_date": "2000-01-01T00:00:00Z"
}
```

### Run the tap in Discovery Mode

```
tap-zendesk --config config.json --discover                # Should dump a Catalog to stdout
tap-zendesk --config config.json --discover > catalog.json # Capture the Catalog
```

### Run the tap in Sync Mode

```
tap-zendesk --config config.json --catalog catalog.json
```

The tap will write bookmarks to stdout which can be captured and passed as an optional `--state state.json` parameter to the tap for the next sync.


### To run tests:

1. Install python test dependencies in a virtual env and run tests
```
make venv
```

2. To run tests:
```
make unit_test
```

### To run pylint:

1. Install python dependencies and run python linter
```
make venv pylint
```