# pipelinewise-tap-github

[![PyPI version](https://badge.fury.io/py/pipelinewise-tap-github.svg)](https://badge.fury.io/py/pipelinewise-tap-github)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-tap-github.svg)](https://pypi.org/project/pipelinewise-tap-github/)
[![License: MIT](https://img.shields.io/badge/License-AGPLv3-yellow.svg)](https://opensource.org/licenses/AGPL-3.0)

[Singer](https://singer.io) tap that produces JSON-formatted data from the GitHub API following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible tap connector.

This tap:
- Pulls raw data from the [GitHub REST API](https://developer.github.com/v3/)
- Extracts the following resources from GitHub for a single repository:
  - [Assignees](https://developer.github.com/v3/issues/assignees/#list-assignees)
  - [Collaborators](https://developer.github.com/v3/repos/collaborators/#list-collaborators)
  - [Commits](https://developer.github.com/v3/repos/commits/#list-commits-on-a-repository)
  - [Issues](https://developer.github.com/v3/issues/#list-issues-for-a-repository)
  - [Pull Requests](https://developer.github.com/v3/pulls/#list-pull-requests)
  - [Comments](https://developer.github.com/v3/issues/comments/#list-comments-in-a-repository)
  - [Reviews](https://developer.github.com/v3/pulls/reviews/#list-reviews-on-a-pull-request)
  - [Review Comments](https://developer.github.com/v3/pulls/comments/)
  - [Stargazers](https://developer.github.com/v3/activity/starring/#list-stargazers)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick start

1. Install

   We recommend using a virtualenv:

    ```bash
    python3 -m venv venv
    . venv/bin/activate
    pip install --upgrade pip
    pip install .
    ```

2. Create a GitHub access token

    Login to your GitHub account, go to the
    [Personal Access Tokens](https://github.com/settings/tokens) settings
    page, and generate a new token with at least the `repo` scope. Save this
    access token, you'll need it for the next step.

3. Create the config file

    Create a JSON file containing the required fields and/or the optional ones.
    You can decide between allow-list or deny-list strategy combining organization with repos_include and repos_exclude using wildcards.

Config                      |Required?  |Description
:---------------------------|:---------:|:---------------
access_token                |yes        |The access token to access github api
start_date                  |yes        |The date inclusive to start extracting the data
organization                |no         |The organization you want to extract the data from
repos_include               |no         |Allow list strategy to extract selected repos data from organization. Supports wildcard matching   
repos_exclude               |no         |Deny list to extract all repos from organization except the ones listed. Supports wildcard matching 
include_archived            |no         |true/false to include archived repos. Default false  
include_disabled            |no         |true/false to include disabled repos. Default false 
repository                  |no         |(DEPRECATED) Allow list strategy to extract selected repos data from organization(has priority over repos_exclude) 
max_rate_limit_wait_seconds |no         |Max time to wait if you hit the github api limit. DEFAULT to 600s

Example:
```json
{
  "access_token": "ghp_16C7e42F292c6912E7710c838347Ae178B4a",
  "organization": "singer-io", 
  "repos_exclude": "*tests* api-docs",
  "repos_include": "tap* getting-started pipelinewise-github",
  "start_date": "2021-01-01T00:00:00Z",
  "include_archived": false,
  "include_disabled": false,
  "max_rate_limit_wait_seconds": 800
}
```

> You can also pass `singer-io/tap-github another-org/tap-octopus` on `repos_include`.

> For retro compatibility you can pass `repository: "singer-io/tap-github singer-io/getting-started"`

> :warning: **If you have very small repos with total size less than 0.5KB**: These will currently be excluded, as the Github repositories API returns `size: 0` for these, and `tap_github/__init__.py` currently uses `size <= 0` as a way to filter out repos with no commits.

4. Run the tap in discovery mode to get properties.json file

    ```bash
    tap-github --config config.json --discover > properties.json
    ```
5. In the properties.json file, select the streams to sync

    Each stream in the properties.json file has a "schema" entry.  To select a stream to sync, add `"selected": true` to that stream's "schema" entry.  For example, to sync the pull_requests stream:
    ```
    ...
    "tap_stream_id": "pull_requests",
    "schema": {
      "selected": true,
      "properties": {
        "updated_at": {
          "format": "date-time",
          "type": [
            "null",
            "string"
          ]
        }
    ...
    ```

6. Run the application

    `tap-github` can be run with:

    ```bash
    tap-github --config config.json --properties properties.json
    ```


## To run tests

1. Install python test dependencies in a virtual env and run nose unit and integration tests
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install -e .[test]
```

2. To run unit tests:
```
  pytest tests/unittests
```
