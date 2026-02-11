# pipelinewise-tap-mixpanel

[![PyPI version](https://badge.fury.io/py/pipelinewise-tap-mixpanel.svg)](https://badge.fury.io/py/pipelinewise-tap-mixpanel)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-tap-mixpanel.svg)](https://pypi.org/project/pipelinewise-tap-mixpanel/)
[![License: AGPL](https://img.shields.io/badge/License-AGPLv3-yellow.svg)](https://opensource.org/licenses/AGPL-3.0)

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).
This fork is maintained to be PipelineWise compatible.

This tap:

- Pulls raw data from the [Mixpanel Event Export API](https://developer.mixpanel.com/docs/exporting-raw-data) and the [Mixpanel Query API](https://developer.mixpanel.com/docs/data-export-api).
- Supports the following two servers
  - Standard Server
  - EU Residency Server
- Extracts the following resources:
  - Export (Events)
  - Engage (People/Users)
  - Funnels
  - Annotations
  - Cohorts
  - Cohort Members
  - Revenue
- Outputs the schema for each resource
- Incrementally pulls data based on the input state
- Uses date-windowing to chunk/loop through `export`, `revenue`, `funnels`.
- Incorporates attribution window for latency look-back to accommodate delays in data reconciliation.


## Streams

**[export](https://developer.mixpanel.com/docs/exporting-raw-data#section-export-api-reference)**
- Standard Server endpoint: https://data.mixpanel.com/api/2.0/export
- EU Residency Server endpoint: https://data-eu.mixpanel.com/api/2.0/export
- Primary key fields: `event`, `time`, `distinct_id`
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `time`
  - Bookmark query field: `from_date`, `to_date`
- Transformations: De-nest `properties` to root-level, re-name properties with leading `$...` to `mp_reserved_...`, convert datetimes from project timezone to UTC.
- Optional parameters
  - `export_events` to export only certain events

**[engage](https://developer.mixpanel.com/docs/data-export-api#section-engage)**
  - Standard Server endpoint: https://mixpanel.com/api/2.0/engage
  - EU Residency Server endpoint: https://eu.mixpanel.com/api/2.0/engage
- Primary key fields:  `distinct_id`
- Replication strategy: FULL_TABLE (all records, every load)
- Transformations: De-nest `$properties` to root-level, re-name properties with leading `$...` to `mp_reserved_...`.

**[funnels](https://developer.mixpanel.com/docs/data-export-api#section-funnels)**
- Standard Server endpoint 1 (name, id): https://mixpanel.com/api/2.0/funnels/list
- Standard Server endpoint 2 (date, measures): https://mixpanel.com/api/2.0/funnels
- EU Residency Server endpoint 1 (name, id): https://eu.mixpanel.com/api/2.0/funnels/list
- EU Residency Server endpoint 2 (date, measures): https://eu.mixpanel.com/api/2.0/funnels
- Primary key fields: `funnel_id`, `date`
- Parameters:
  - `funnel_id`: {funnel_id} (from Endpoint 1)
  - `unit`: day
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `date`
  - Bookmark query field: `from_date`, `to_date`
- Transformations: Combine Endpoint 1 & 2 results, convert `date` keys to list to `results` list-array.

**[revenue](https://developer.mixpanel.com/docs/data-export-api#section-hr-span-style-font-family-courier-revenue-span)**
- Standard Server endpoint: https://mixpanel.com/api/2.0/engage/revenue
- EU Residency Server endpoint: https://eu.mixpanel.com/api/2.0/engage/revenue
- Primary key fields: `date`
- Parameters:
  - `unit`: day
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `date`
  - Bookmark query field: `from_date`, `to_date`
- Transformations: Convert `date` keys to list to `results` list-array.

**[annotations](https://developer.mixpanel.com/docs/data-export-api#section-annotations)**
- Standard Server endpoint: https://mixpanel.com/api/2.0/annotations
- EU Residency Server endpoint: https://eu.mixpanel.com/api/2.0/annotations
- Primary key fields: `date`
- Replication strategy: FULL_TABLE
- Transformations: None.

**[cohorts](https://developer.mixpanel.com/docs/cohorts#section-list-cohorts)**
- Standard Server endpoint: https://mixpanel.com/api/2.0/cohorts/list
- EU Residency Server endpoint: https://eu.mixpanel.com/api/2.0/cohorts/list
- Primary key fields: `id`
- Replication strategy: FULL_TABLE
- Transformations: None.

**[cohort_members (engage)](https://developer.mixpanel.com/docs/data-export-api#section-engage)**
- Standard Server endpoint: https://mixpanel.com/api/2.0/cohorts/list
- EU Residency Server endpoint: https://eu.mixpanel.com/api/2.0/cohorts/list
- Primary key fields: `distinct_id`, `cohort_id`
- Parameters:
  - `filter_by_cohort`: {cohort_id} (from `cohorts` endpoint)
- Replication strategy: FULL_TABLE
- Transformations: For each `cohort_id` in `cohorts` endpoint, query `engage` endpoint with `filter_by_cohort` parameter to create list of `distinct_id` for each `cohort_id`.


## Authentication
The Mixpanel API uses Basic Authorization with the `api_secret` from the tap config in base-64 encoded format. It is slightly different than normal Basic Authorization with username/password. All requests should include this header with the `api_secret` as the username, with no password:

- Authorization: `Basic <base-64 encoded api_secret>`

### Note 
- If you selected eu_residency_server then please make sure you enter api_secret of that project only.

More details may be found in the [Mixpanel API Authentication](https://developer.mixpanel.com/docs/data-export-api#section-authentication) instructions. 


## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-mixpanel
    > pip install .
    ```
2. Dependent libraries. The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install jsonlines
    > pip install singer-tools
    > pip install target-stitch
    > pip install target-json
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)
    - [jsonlines](https://jsonlines.readthedocs.io/en/latest/) needed for `export` endpoint json-lines formatted data

3. Create your tap's `config.json` file.  The tap config file for this tap should include these entries:
   - `start_date` - the default value to use if no bookmark exists for an endpoint (rfc3339 date string)
   - `user_agent` (string, optional): Process and email for API logging purposes. Example: `tap-mixpanel <api_user_email@your_company.com>`
   - `api_secret` (string, `ABCdef123`): an API secret for each project in Mixpanel. This can be found in the Mixpanel Console, upper-right Settings (gear icon), Organization Settings > Projects and in the Access Keys section. For this tap, only the api_secret is needed (the api_key is legacy and the token is used only for uploading data). Each Mixpanel project has a different api_secret; therefore each Singer tap pipeline instance is for a single project.
   - `date_window_size` (integer, `30`): Number of days for date window looping through transactional endpoints with from_date and to_date. Default date_window_size is 30 days. Clients with large volumes of events may want to decrease this to 14, 7, or even down to 1-2 days.
   - `attribution_window` (integer, `5`): Latency minimum number of days to look-back to account for delays in attributing accurate results. [Default attribution window is 5 days](https://help.mixpanel.com/hc/en-us/articles/115004616486-Tracking-If-Users-Are-Offline).
   - `project_timezone` (string like `US/Pacific`): Time zone in which integer date times are stored. The project timezone may be found in the project settings in the Mixpanel console. [More info about timezones](https://help.mixpanel.com/hc/en-us/articles/115004547203-Manage-Timezones-for-Projects-in-Mixpanel). 
   - `select_properties_by_default` (`true` or `false`): Mixpanel properties are not fixed and depend on the date being uploaded. During Discovery mode and catalog.json setup, all current/existing properties will be captured. Setting this config parameter to true ensures that new properties on events and engage records are captured. Otherwise new properties will be ignored.
   - `eu_residency_server` (`true` or `false`): Data Residency refers to the physical/geographical storage location of an organization's data or information. Setting this config parameter to true ensures that it uses eu_residency_server endpoint to capture the records. As a Mixpanel customer in the EU, you have the option to send your data to Mixpanel's EU data center, and have your data stored exclusively in the EU when creating a new project. [More info about eu_residency_server](https://help.mixpanel.com/hc/en-us/articles/360039135652-Data-Residency-in-EU).
   - `request_timeout` (integer, `300`): Max time for which request should wait to get a response. Default request_timeout is 300 seconds.
   
    ```json
    {
        "api_secret": "YOUR_API_SECRET",
        "date_window_size": "30",
        "attribution_window": "5",
        "project_timezone": "US/Pacific",
        "select_properties_by_default": "true",
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-mixpanel <api_user_email@your_company.com>",
        "eu_residency_server": "true",
        "request_timeout": 300
    }
    ```
    
      If you want to export only certain events from the [Raw export API](https://developer.mixpanel.com/reference/export)then provide the value of `export_events`

    ```bash
   "export_events": "event_one,event_two"
   ```

    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "engage",
        "bookmarks": {
            "export": "2019-09-27T22:34:39.000000Z",
            "funnels": "2019-09-28T15:30:26.000000Z",
            "revenue": "2019-09-28T18:23:53Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-mixpanel --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-mixpanel --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-mixpanel --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-mixpanel --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While developing the mixpanel tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_mixpanel -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.67/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-mixpanel --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
    The output is valid.
    It contained 15697 messages for 7 streams.

          7 schema messages
      15661 record messages
        29 state messages

    Details by stream:
    +----------------+---------+---------+
    | stream         | records | schemas |
    +----------------+---------+---------+
    | revenue        | 134     | 1       |
    | export         | 2811    | 1       |
    | funnels        | 132     | 1       |
    | cohort_members | 454     | 1       |
    | engage         | 12119   | 1       |
    | cohorts        | 5       | 1       |
    | annotations    | 6       | 1       |
    +----------------+---------+---------+

    ```

    #### Unit Tests

    Unit tests may be run with the following.

    ```
    python -m pytest --verbose
    ```

    Note, you may need to install test dependencies.

    ```
    pip install -e .'[dev]'
    ```
---

Copyright &copy; 2019 Stitch. PipelineWise compatibility updates maintained by Wise.
