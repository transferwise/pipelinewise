.. _tap-jira:

Jira source
===========

``tap-jira`` extracts Jira issues and reference data through the Jira API.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Source
     - Status
     - Load path
   * - Jira
     - Experimental
     - Singer only


Authentication
--------------

Configure either username/token authentication with ``base_url``, ``username``,
and ``password``, or OAuth with the client, cloud, access, and refresh token
fields. The principal must be able to browse every selected project and field.


Configuration
-------------

.. code-block:: yaml

   id: "jira"
   name: "Jira"
   type: "tap-jira"
   owner: "data-platform@example.com"
   db_conn:
     base_url: "https://example.atlassian.net"
     username: "pipelinewise@example.com"
     password: "{{ env_var['JIRA_TOKEN'] }}"
     start_date: "2024-01-01"
   target: "snowflake"
   batch_size_rows: 20000
   stream_buffer_size: 0
   default_target_schema: "jira"
   schemas:
     - source_schema: "jira"
       target_schema: "jira"
       tables:
         - table_name: "issues"
         - table_name: "issue_comments"
         - table_name: "worklogs"

Issue, changelog, comment, transition, and worklog streams are incremental.
Reference streams such as projects, users, roles, resolutions, and versions can
require a full-table read on every run. Discover the catalog and estimate their
volume before scheduling.
