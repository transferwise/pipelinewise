.. _tap-slack:

Slack source
============

``tap-slack`` extracts workspace, channel, message, thread, user, and file
metadata through the Slack API.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Source
     - Status
     - Load path
   * - Slack
     - Experimental
     - Singer only


Authentication
--------------

Create a Slack app and bot token. Grant only scopes required by the selected
streams. Common scopes include channel history/read/join, group read, file read,
reaction read, team read, user-group read, and user read. Add
``users:read.email`` only when email extraction is required. Invite the bot to
private or non-auto-joined channels.


Configuration
-------------

.. code-block:: yaml

   id: "slack"
   name: "Slack"
   type: "tap-slack"
   owner: "data-platform@example.com"
   db_conn:
     token: "{{ env_var['SLACK_BOT_TOKEN'] }}"
     start_date: "2024-01-01"
     channels: ["C01234567"]
     exclude_archived: "true"
     private_channels: "false"
     join_public_channels: "false"
     date_window_size: "5"
     lookback_window: 14
   target: "snowflake"
   batch_size_rows: 20000
   stream_buffer_size: 0
   default_target_schema: "slack"
   schemas:
     - source_schema: "slack"
       target_schema: "slack"
       tables:
         - table_name: "channels"
         - table_name: "users"
         - table_name: "messages"
         - table_name: "threads"

The lookback window repeats recent data so child streams can capture late
changes. Increasing it raises API and target load. Verify channel visibility,
rate-limit recovery, edited/deleted messages, and high-volume channel windows
before production use.
