# pipelinewise-tap-slack

[![PyPI version](https://badge.fury.io/py/pipelinewise-tap-slack.svg)](https://badge.fury.io/py/pipelinewise-tap-slack)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-tap-slack.svg)](https://pypi.org/project/pipelinewise-tap-slack/)
[![License: MIT](https://img.shields.io/badge/License-GPLv3-yellow.svg)](https://opensource.org/licenses/GPL-3.0)

[Singer](https://www.singer.io/) tap that extracts data from a [Slack](https://www.slack.com/) workspace and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible tap connector.

## How to use it

The recommended method of running this tap is to use it from [PipelineWise](https://transferwise.github.io/pipelinewise). When running it from PipelineWise you don't need to configure this tap with JSON files and most of things are automated. Please check the related documentation at [Tap Slack](https://transferwise.github.io/pipelinewise/connectors/taps/slack.html)

If you want to run this [Singer Tap](https://singer.io) independently please read further.

## Installation

It is highly recommended installing `tap-slack` in it's own isolated virtual environment. For example:

```bash
python3 -m venv ~/.venvs/tap-slack
source ~/.venvs/tap-slack/bin/activate
pip3 install pipelinewise-tap-slack
deactivate
```

## Setup

The tap requires a [Slack API token](https://github.com/slackapi/python-slackclient/blob/master/documentation_v2/auth.md#tokens--authentication) to interact with your Slack workspace. You can obtain a token for a single workspace by creating a new [Slack App](https://api.slack.com/apps?new_app=1) in your workspace and assigning it the relevant [scopes](https://api.slack.com/docs/oauth-scopes). As of right now, the minimum required scopes for this App are:
 - `channels:history`
 - `channels:join`
 - `channels:read`
 - `files:read`
 - `groups:read`
 - `links:read`
 - `reactions:read`
 - `remote_files:read`
 - `remote_files:write`
 - `team:read`
 - `usergroups:read`
 - `users.profile:read`
 - `users:read`
 - `users:read.email` This scope is only required if you want to extract the user emails as well.

Create a config file containing the API token and a start date, e.g.:

```json
{
  "token":"xxxx",
  "start_date":"2020-05-01T00:00:00"
}
```

### Private channels

Optionally, you can also specify whether you want to sync private channels or not by adding the following to the config:

```json
    "private_channels":"false"
```

By default, private channels will be synced.

### Joining Public Channels

By adding the following to your config file you can have the tap auto-join all public channels in your ogranziation.
```json
"join_public_channels":"true"
```
If you do not elect to have the tap join all public channels you must invite the bot to all channels you wish to sync.

### Specify channels to sync

By default, the tap will sync all channels it has been invited to. However, you can limit the tap to sync only the channels you specify by adding their IDs to the config file, e.g.:

```json
"channels":[
    "abc123",
    "def345"
  ]
```

Note this needs to be channel ID, not the name, as [recommended by the Slack API](https://api.slack.com/types/conversation#other_attributes). To get the ID for a channel, either use the Slack API or [find it in the URL](https://www.wikihow.com/Find-a-Channel-ID-on-Slack-on-PC-or-Mac).

### Archived Channels

You can control whether or not the tap will sync archived channels by including the following in the tap config:
```json
  "exclude_archived": "false"
```
It's important to note that a bot *CANNOT* join an archived channel, so unless the bot was added to the channel prior to it being archived it will not be able to sync the data from that channel.

### Date Windowing

Due to the potentially high volume of data when syncing certain streams (messages, files, threads)
this tap implements date windowing based on a configuration parameter.

including 
```json
"date_window_size": "5"
```

Will cause the tap to sync 5 days of data per request, for applicable streams. The default value if 
one is not defined is to window requests for 7 days at a time.

## Usage

It is recommended to follow Singer [best practices](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-and-developing-singer-taps-and-targets) when running taps either [on their own](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap) or [with a Singer target](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target).

In practice, it will look something like the following:

```bash
~/.venvs/tap-slack/bin/tap-slack --config slack.config.json --catalog catalog.json | ~/.venvs/target-stitch/bin/target-stitch --config stitch.config.json
```

## Replication

The Slack Conversations API does not natively store last updated timestamp information about a Conversation. In addition, Conversation records are mutable. Thus, `tap-slack` requires a `FULL_TABLE` replication strategy to ensure the most up-to-date data in replicated when replicating the following Streams:
 - `Channels` (Conversations)
 - `Channel Members` (Conversation Members)

The `Users` stream _does_ store information about when a User record was last updated, so `tap-slack` uses that timestamp as a bookmark value and prefers using an `INCREMENTAL` replication strategy.

## Table Schemas

### Channels (Conversations)

 - Table Name: `channels`
 - Description:
 - Primary Key Column: `id`
 - Replication Strategy: `FULL_TABLE`
 - API Documentation: [Link](https://api.slack.com/methods/conversations.list)

### Channel Members (Conversation Members)

 - Table Name: `channel_members`
 - Description:
 - Primary Key Columns: `channel_id`, `user_id`
 - Replication Strategy: `FULL_TABLE`
 - API Documentation: [Link](https://api.slack.com/methods/conversations.members)

### Messages (Conversation History)

 - Table Name: `messages`
 - Description:
 - Primary Key Columns: `channel_id`, `ts`
 - Replication Strategy: `INCREMENTAL`
 - API Documentation: [Link](https://api.slack.com/methods/conversations.history)

### Users

 - Table Name: `users`
 - Description:
 - Primary Key Column: `id`
 - Replication Strategy: `INCREMENTAL`
 - API Documentation: [Link](https://api.slack.com/methods/users.list)
 
### Threads (Conversation Replies)

 - Table Name: `threads`
 - Description:
 - Primary Key Columns: `channel_id`, `ts`, `thread_ts`
 - Replication Strategy: `FULL_TABLE` for each parent `message`
 - API Documentation: [Link](https://api.slack.com/methods/conversations.replies)
 
### User Groups 

 - Table Name: `user_groups`
 - Description:
 - Primary Key Column: `id`
 - Replication Strategy: `FULL_TABLE`
 - API Documentation: [Link](https://api.slack.com/methods/usergroups.list)
 
### Files 

 - Table Name: `files`
 - Description:
 - Primary Key Column: `id`
 - Replication Strategy: `INCREMENTAL` query filtered using date windows and lookback window
 - API Documentation: [Link](https://api.slack.com/methods/files.list)
 
### Remote Files 

 - Table Name: `remote_files`
 - Description:
 - Primary Key Column: `id`
 - Replication Strategy: `INCREMENTAL` query filtered using date windows and lookback window
 - API Documentation: [Link](https://api.slack.com/methods/files.remote.list)
 
## Testing the Tap

Install test dependencies
```bash
make venv
```

To run tests:
```bash
make unit_test
```

## Linting

Install test dependencies
```bash
make venv
```

To run linter:
```bash
make pylint
```
