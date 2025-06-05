# pipelinewise-tap-twilio

[![PyPI version](https://badge.fury.io/py/pipelinewise-tap-twilio.svg)](https://badge.fury.io/py/pipelinewise-tap-twilio)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-tap-twilio.svg)](https://pypi.org/project/pipelinewise-tap-twilio/)
[![License: MIT](https://img.shields.io/badge/License-AGPLv3-yellow.svg)](https://opensource.org/licenses/AGPL-3.0)

[Singer](https://www.singer.io/) tap that extracts data from a [Twilio API](https://www.twilio.com/) and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible tap connector.

This tap:

- Extracts the following resources:
  - [accounts](https://www.twilio.com/docs/usage/api/account#read-multiple-account-resources)
  - [addresses](https://www.twilio.com/docs/usage/api/address#read-multiple-address-resources)
  - [dependent_phone_numbers](https://www.twilio.com/docs/usage/api/address?code-sample=code-list-dependent-pns-subresources&code-language=curl&code-sdk-version=json#instance-subresources)
  - [applications](https://www.twilio.com/docs/usage/api/applications#read-multiple-application-resources)
  - [available_phone_number_countries](https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-resource#read-a-list-of-countries)
  - [available_phone_numbers_local](https://www.twilio.com/docs/phone-numbers/api/availablephonenumberlocal-resource#read-multiple-availablephonenumberlocal-resources)
  - [available_phone_numbers_mobile](https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-mobile-resource#read-multiple-availablephonenumbermobile-resources)
  - [available_phone_numbers_toll_free](https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-tollfree-resource#read-multiple-availablephonenumbertollfree-resources)
  - [incoming_phone_numbers](https://www.twilio.com/docs/phone-numbers/api/incomingphonenumber-resource#read-multiple-incomingphonenumber-resources)
  - [keys](https://www.twilio.com/docs/usage/api/keys#read-a-key-resource)
  - [calls](https://www.twilio.com/docs/sms/api/message-resource#read-multiple-message-resources)
  - [conferences](https://www.twilio.com/docs/voice/api/conference-resource#read-multiple-conference-resources)
  - [conference_participants](https://www.twilio.com/docs/voice/api/conference-participant-resource#read-multiple-participant-resources)
  - [outgoing_caller_ids](https://www.twilio.com/docs/voice/api/outgoing-caller-ids#outgoingcallerids-list-resource)
  - [recordings](https://www.twilio.com/docs/voice/api/recording#read-multiple-recording-resources)
  - [usage_records](https://www.twilio.com/docs/usage/api/usage-record#read-multiple-usagerecord-resources)
  - [usage_triggers](https://www.twilio.com/docs/usage/api/usage-trigger#read-multiple-usagetrigger-resources)
  - [transcriptions](https://www.twilio.com/docs/voice/api/recording-transcription?code-sample=code-read-list-all-transcriptions&code-language=curl&code-sdk-version=json#read-multiple-transcription-resources)
  - [queues](https://www.twilio.com/docs/voice/api/queue-resource#read-multiple-queue-resources)
  - [message_media](https://www.twilio.com/docs/sms/api/media-resource#read-multiple-media-resources)
  - [alerts](https://www.twilio.com/docs/usage/monitor-alert#read-multiple-alert-resources) 
- Extracts TaskRouter resources:
  - [workspaces](https://www.twilio.com/docs/taskrouter/api/workspace#list-all-workspaces)
  - [activities](https://www.twilio.com/docs/taskrouter/api/activity#read-multiple-activity-resources)
  - [events](https://www.twilio.com/docs/taskrouter/api/event#list-all-events)
  - [tasks](https://www.twilio.com/docs/taskrouter/api/task#read-multiple-task-resources)
  - [task_channels](https://www.twilio.com/docs/taskrouter/api/task-channel#read-multiple-taskchannel-resources)
  - [task_queues](https://www.twilio.com/docs/taskrouter/api/task-queue#action-list)
  - [cumulative_statistics](https://www.twilio.com/docs/taskrouter/api/taskqueue-statistics#taskqueue-cumulative-statistics)
  - [workers](https://www.twilio.com/docs/taskrouter/api/worker#read-multiple-worker-resources)
  - [worker_channels](https://www.twilio.com/docs/taskrouter/api/worker-channel#read-multiple-workerchannel-resources)
  - [workflows](https://www.twilio.com/docs/taskrouter/api/workflow#read-multiple-workflow-resources)
- Extracts Programmable Chat resources (`members` and `chat_messages` are `FULL_TABLE` synced, so take care syncing them, they result in a lot of request/data):
  - [services](https://www.twilio.com/docs/chat/rest/service-resource#read-multiple-service-resources)
  - [roles](https://www.twilio.com/docs/chat/rest/role-resource#read-multiple-role-resources)
  - [chat_channels](https://www.twilio.com/docs/chat/rest/channel-resource#read-multiple-channel-resources)
  - [members](https://www.twilio.com/docs/chat/rest/member-resource?code-sample=code-read-multiple-member-resources)
  - [chat_messages](https://www.twilio.com/docs/chat/rest/message-resource#read-multiple-message-resources)
  - [users](https://www.twilio.com/docs/chat/rest/user-resource#read-multiple-user-resources)
 
- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams

### Standard Endpoints:

[accounts](https://www.twilio.com/docs/usage/api/account#read-multiple-account-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts.json
- Primary key fields: sid
- Replication strategy: FULL_TABLE
- Transformations: subresources_to_array


[addresses](https://www.twilio.com/docs/usage/api/address#read-multiple-address-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Addresses.json
- Parent: account
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[dependent_phone_numbers](https://www.twilio.com/docs/usage/api/address?code-sample=code-list-dependent-pns-subresources&code-language=curl&code-sdk-version=json#instance-subresources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Addresses/{ParentId}/DependentPhoneNumbers.json
- Parent: addresses
- Primary key fields: sid
- Replication strategy: FULL_TABLE
- Transformations: subresources_to_array


[applications](https://www.twilio.com/docs/usage/api/applications#read-multiple-application-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Applications.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[available_phone_number_countries](https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-resource#read-a-list-of-countries)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/AvailablePhoneNumbers.json
- Parent: accounts
- Primary key fields: country_code
- Replication strategy: FULL_TABLE
- Transformations: subresources_to_array


[available_phone_numbers_local](https://www.twilio.com/docs/phone-numbers/api/availablephonenumberlocal-resource#read-multiple-availablephonenumberlocal-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/AvailablePhoneNumbers/{ParentId}/Local.json
- Parent: available_phone_number_countries
- Primary key fields: iso_country, phone_number
- Replication strategy: FULL_TABLE
- Transformations: subresources_to_array


[available_phone_numbers_mobile](https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-mobile-resource#read-multiple-availablephonenumbermobile-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/AvailablePhoneNumbers/{ParentId}/Mobile.json
- Parent: available_phone_number_countries
- Primary key fields: iso_country, phone_number
- Replication strategy: FULL_TABLE
- Transformations: subresources_to_array


[available_phone_numbers_toll_free](https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-tollfree-resource#read-multiple-availablephonenumbertollfree-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/AvailablePhoneNumbers/{ParentId}/TollFree.json
- Parent: available_phone_number_countries
- Primary key fields: iso_country, phone_number
- Replication strategy: FULL_TABLE
- Transformations: none


[incoming_phone_numbers](https://www.twilio.com/docs/phone-numbers/api/incomingphonenumber-resource#read-multiple-incomingphonenumber-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/IncomingPhoneNumbers.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[keys](https://www.twilio.com/docs/usage/api/keys#read-a-key-resource)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Keys.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array

[calls](https://www.twilio.com/docs/sms/api/message-resource#read-multiple-message-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Calls.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[conferences](https://www.twilio.com/docs/voice/api/conference-resource#read-multiple-conference-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Conferences.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[conference_participants](https://www.twilio.com/docs/voice/api/conference-participant-resource#read-multiple-participant-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Conferences/{ParentId}/Participants.json
- Parent: conferences
- Primary key fields: uri
- Replication strategy: FULL_TABLE
- Transformations: subresources_to_array


[outgoing_caller_ids](https://www.twilio.com/docs/voice/api/outgoing-caller-ids#outgoingcallerids-list-resource)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/OutgoingCallerIds.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[recordings](https://www.twilio.com/docs/voice/api/recording#read-multiple-recording-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Recordings.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[transcriptions](https://www.twilio.com/docs/voice/api/recording-transcription?code-sample=code-read-list-all-transcriptions&code-language=curl&code-sdk-version=json#read-multiple-transcription-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Transcriptions.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[queues](https://www.twilio.com/docs/voice/api/queue-resource#read-multiple-queue-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Queues.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[message_media](https://www.twilio.com/docs/sms/api/media-resource#read-multiple-media-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Messages/{ParentId}/Media.json
- Parent: messages
- Primary key fields: sid
- Replication strategy: FULL_TABLE
- Transformations: subresources_to_array


[usage_records](https://www.twilio.com/docs/usage/api/usage-record#read-multiple-usagerecord-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Usage/Records.json
- Parent: accounts
- Primary key fields: account_sid, category, start_date
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[usage_triggers](https://www.twilio.com/docs/usage/api/usage-trigger#read-multiple-usagetrigger-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Usage/Triggers.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[alerts](https://www.twilio.com/docs/usage/monitor-alert#read-multiple-alert-resources)
- Endpoint: https://monitor.twilio.com/v1/Alerts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: none

### TaskRouter Endpoints:

[workspaces](https://www.twilio.com/docs/taskrouter/api/workspace#list-all-workspaces)
- Endpoint: https://taskrouter.twilio.com/v1/Workspaces
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: none


[activities](https://www.twilio.com/docs/taskrouter/api/activity#read-multiple-activity-resources)
- Endpoint: https://taskrouter.twilio.com/v1/Workspaces/{ParentId}/Activities
- Parent: workspaces
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: none


[events](https://www.twilio.com/docs/taskrouter/api/event#list-all-events)
- Endpoint: https://taskrouter.twilio.com/v1/Workspaces/{ParentId}/Events
- Parent: workspaces
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: none


[tasks](https://www.twilio.com/docs/taskrouter/api/task#read-multiple-task-resources)
- Endpoint: https://taskrouter.twilio.com/v1/Workspaces/{ParentId}/Tasks
- Parent: workspaces
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: none


[task_channels](https://www.twilio.com/docs/taskrouter/api/task-channel#read-multiple-taskchannel-resources)
- Endpoint: https://taskrouter.twilio.com/v1/Workspaces/{ParentId}/TaskChannels
- Parent: workspaces
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: none


[task_queues](https://www.twilio.com/docs/taskrouter/api/task-queue#action-list)
- Endpoint: https://taskrouter.twilio.com/v1/Workspaces/{ParentId}/TaskQueues
- Parent: workspaces
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: none


[cumulative_statistics](https://www.twilio.com/docs/taskrouter/api/taskqueue-statistics#taskqueue-cumulative-statistics)
- Endpoint: https://taskrouter.twilio.com/v1/Workspaces/{ParentId}/TaskQueues/{ParentId}/CumulativeStatistics
- Parent: task_queues
- Replication strategy: FULL_TABLE
- Transformations: none


[workers](https://www.twilio.com/docs/taskrouter/api/worker#read-multiple-worker-resources)
- Endpoint: https://taskrouter.twilio.com/v1/Workspaces/{ParentId}/Workers
- Parent: workspaces
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: none


[channels](https://www.twilio.com/docs/taskrouter/api/worker-channel#read-multiple-workerchannel-resources)
- Endpoint: https://taskrouter.twilio.com/v1/Workspaces/{ParentId}/Workers/{ParentId}/Channels
- Parent: workers
- Primary key fields: sid
- Replication strategy: FULL_TABLE
- Transformations: none


[workflows](https://www.twilio.com/docs/taskrouter/api/workflow#read-multiple-workflow-resources)
- Endpoint: https://taskrouter.twilio.com/v1/Workspaces/{ParentId}/Workflows
- Parent: workspaces
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: none

### Programmable Chat Endpoints:

[services](https://www.twilio.com/docs/chat/rest/service-resource#read-multiple-service-resources)
- Endpoint: https://chat.twilio.com/v2/Services
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: none


[roles](https://www.twilio.com/docs/chat/rest/role-resource#read-multiple-role-resources)
- Endpoint: https://chat.twilio.com/v2/Services/{ParentId}/Roles
- Parent: services
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: none


[chat_channels](https://www.twilio.com/docs/chat/rest/channel-resource#read-multiple-channel-resources)
- Endpoint: https://chat.twilio.com/v2/Services/{ParentId}/Channels
- Parent: services
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: none


[members](https://www.twilio.com/docs/chat/rest/member-resource?code-sample=code-read-multiple-member-resources)
- Endpoint: https://chat.twilio.com/v2/Services/{ParentId}/Channels/{ParentId}/Members
- Parent: workspaces
- Primary key fields: sid
- Replication strategy: FULL_TABLE
- Transformations: none


[chat_messages](https://www.twilio.com/docs/chat/rest/message-resource#read-multiple-message-resources)
- Endpoint: https://chat.twilio.com/v2/Services/{ParentId}/Channels/{ParentId}/Messages
- Parent: workspaces
- Primary key fields: sid
- Replication strategy: FULL_TABLE
- Transformations: none


[users](https://www.twilio.com/docs/chat/rest/user-resource#read-multiple-user-resources)
- Endpoint: https://chat.twilio.com/v2/Services/{ParentId}/Users
- Parent: services
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: none

[call_metrics](https://www.twilio.com/docs/voice/voice-insights/api/call/call-metrics-resource#read-multiple-call-metrics-resources)
- Endpoint: https://insights.twilio.com/v1/Voice/{CallSid}/Metrics
- Primary key fields: call_sid
- Replication strategy: INCREMENTAL
- Transformations: none

[call_summary] (https://www.twilio.com/docs/voice/voice-insights/api/call/call-summary-resource)
- Endpoint: https://insights.twilio.com/v1/Voice/{CallSid}/Summary
- Primary key fields: call_sid
- Replication strategy: INCREMENTAL
- Transformations: none

## Authentication
This tap authenticates to the Twilio API using Basic Auth.

To set up authentication simply include your Twilio `account_sid` and `auth_token` in the tap config.


## Quick Start

1. Install

    ```bash
      make venv
    ```

2. Create your tap's `config.json` file. The `api_key` is available in the twilio Console UI (see **Authentication** above). The `date_window_days` is the integer number of days (between the from and to dates) for date-windowing through the date-filtered endpoints (default = 30). The `start_date` is the absolute beginning date from which incremental loading on the initial load will start.

    ```json
        {
            "account_sid": "YOUR_ACCOUNT_SID",
            "auth_token": "YOUR_AUTH_TOKEN",
            "start_date": "2019-01-01T00:00:00Z",
            "user_agent": "tap-twilio <api_user_email@your_company.com>",
        }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "registers",
        "bookmarks": {
            "acounts": "2020-03-23T10:31:14.000000Z",
            "...": "2020-03-23T00:00:00.000000Z"
        }
    }
    ```

3. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-twilio --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

4. Run the Tap in Sync Mode (with catalog)
 
    For Sync mode:
    ```bash
    > tap-twilio --config tap_config.json --catalog catalog.json
    ```

   Messages are written to standard output following the Singer specification.
   The resultant stream of JSON data can be consumed by a Singer target.

## To run tests

Install python test dependencies in a virtual env and run tests
```
make venv test
```

## To lint the code

Install python test dependencies in a virtual env and run linter
```
make venv pylint
```

## Licence

GNU AFFERO GENERAL PUBLIC [LICENSE](./LICENSE)

