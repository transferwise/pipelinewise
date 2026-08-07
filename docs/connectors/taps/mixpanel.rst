.. _tap-mixpanel:

Mixpanel source
===============

``tap-mixpanel`` extracts one Mixpanel project through the export APIs.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Source
     - Status
     - Load path
   * - Mixpanel
     - Experimental
     - Singer only


Authentication
--------------

The connector uses the project's ``api_secret`` as a Basic Authentication user
with no password. Each project has a different secret, so configure one tap per
project.


Configuration
-------------

.. code-block:: yaml

   id: "mixpanel"
   name: "Mixpanel"
   type: "tap-mixpanel"
   owner: "data-platform@example.com"
   db_conn:
     api_secret: "{{ env_var['MIXPANEL_API_SECRET'] }}"
     start_date: "2024-01-01"
     date_window_size: 30
     attribution_window: 5
     project_timezone: "Europe/London"
     denest_properties: "false"
   target: "snowflake"
   batch_size_rows: 20000
   stream_buffer_size: 0
   default_target_schema: "mixpanel"
   schemas:
     - source_schema: "mixpanel"
       target_schema: "mixpanel"
       tables:
         - table_name: "export"
         - table_name: "funnels"
         - table_name: "revenue"

``date_window_size`` defaults to 30 days; reduce it for high-volume projects.
``attribution_window`` repeats recent days to capture late attribution and
therefore increases API and target work. Disabling property denesting avoids very
wide tables by retaining nested responses in a JSON value.

Streams such as ``engage``, ``annotations``, ``cohorts``, and
``cohort_members`` can require full-table extraction. Confirm API limits and
expected volume before enabling them.
