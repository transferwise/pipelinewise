.. _tap-github:

GitHub source
=============

``tap-github`` extracts repository metadata through the GitHub API.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Source
     - Status
     - Load path
   * - GitHub
     - Experimental
     - Singer only


Authentication
--------------

Create a token that can read every selected repository. Use the narrowest scopes
that satisfy the chosen streams and keep the token outside source control.


Configuration
-------------

.. code-block:: yaml

   id: "github"
   name: "GitHub"
   type: "tap-github"
   owner: "data-platform@example.com"
   db_conn:
     access_token: "{{ env_var['GITHUB_TOKEN'] }}"
     start_date: "2024-01-01T00:00:00Z"
     organization: "example"
     repos_include: "service-* data-platform"
     repos_exclude: "*-archive"
     include_archived: false
     include_disabled: false
   target: "snowflake"
   batch_size_rows: 20000
   stream_buffer_size: 0
   default_target_schema: "github"
   schemas:
     - source_schema: "github"
       target_schema: "github"
       tables:
         - table_name: "commits"
         - table_name: "pull_requests"
         - table_name: "issues"

``organization`` is required when repository filters use wildcards.
``repos_include`` and ``repos_exclude`` are space-delimited filters.
``repository`` remains a deprecated compatibility setting.

Discover the catalog before finalising streams:

.. code-block:: bash

   pipelinewise discover_tap --tap github --target snowflake

GitHub API rate limits can extend run duration. ``max_rate_limit_wait_seconds``
defaults to 600 seconds and accepts 600–3600. Test large organisations,
permissions, deleted repositories, and retry duplication before production use.
