
.. _tap-mixpanel:

Tap Mixpanel
------------

Authentication
''''''''''''''

The Mixpanel API uses Basic Authorization with the ``api_secret`` from the tap config in base-64 encoded format.
It is slightly different than normal Basic Authorization with username/password. All requests include this
header with the ``api_secret`` as the username, with no password.

* Authorization: ``Basic <base-64 encoded api_secret>``

More details may be found in the Mixpanel `API Authentication <https://developer.mixpanel.com/docs/data-export-api#section-authentication>`_
instructions.

The API secret can be found in the Mixpanel Console, upper-right Settings (gear icon),
Organization Settings > Projects and in the Access Keys section. For this tap,
only the ``api_secret`` is needed (the ``api_key`` is legacy and the token is used only for uploading data).

.. warning::

    Each Mixpanel project has a different ``api_secret``; therefore each Mixpanel pipeline instance is for a single project.


Configuring what to extract
'''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for the Mixpanel tap can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``tap-mixpanel``:

.. code-block:: bash

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "mixpanel"                         # Unique identifier of the tap
    name: "Mixpanel"                       # Name of the tap
    type: "tap-mixpanel"                   # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"              # Data owner to contact
    #send_alert: False                     # Optional: Disable all configured alerts on this tap


    # ------------------------------------------------------------------------------
    # Source (Tap) - Mixpanel connection details
    # ------------------------------------------------------------------------------
    db_conn:
      api_secret: "<MIXPANEL_API_SECRET>"       # Mixpanel API secret
      start_date: "2020-10-01"                  # The default value to use if no bookmark exists for an endpoint
      date_window_size: 30                      # Number of days for date window looping through transactional endpoints
                                                # with from_date and to_date. Default date_window_size is 30 days.
                                                # Clients with large volumes of events may want to decrease this to 14, 7,
                                                # or even down to 1-2 days.
      attribution_window: 1                     # Latency minimum number of days to look-back to account for delays in
                                                # attributing accurate results. Default attribution window is 5 days.
      project_timezone: "Europe/London"         # Time zone in which integer date times are stored. The project timezone
                                                # may be found in the project settings in the Mixpanel console.
      user_agent: "tap-mixpanel <api@foo.com>"  # Optional: Process and email for API logging purposes.

      #export_events:                           # Optional: List of event names to export
      #  - event_one
      #  - event_two


    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"                       # ID of the target connector where the data will be loaded
    batch_size_rows: 20000                    # Batch size for the stream to optimise load performance
    stream_buffer_size: 0                     # In-memory buffer size (MB) between taps and targets for asynchronous data pipes
    default_target_schema: "mixpanel"         # Target schema where the data will be loaded
    #default_target_schema_select_permission:  # Optional: Grant SELECT on schema and tables that created
    #  - grp_power


    # ------------------------------------------------------------------------------
    # Source to target Schema mapping
    # ------------------------------------------------------------------------------
    schemas:

      - source_schema: "mixpanel"           # This is mandatory, but can be anything in this tap type
        target_schema: "mixpanel"           # Target schema in the destination Data Warehouse
        target_schema_select_permissions:   # Optional: Grant SELECT on schema and tables that created
          - grp_stats

        # List of Mixpanel tables to load into destination Data Warehouse
        # Tap-Mixpanel will use the best incremental strategies automatically to replicate data
        tables:
          # Incrementally loaded tables
          - table_name: "export"
          - table_name: "funnels"
          - table_name: "revenue"

          # Tables that cannot load incrementally and will use FULL_TABLE method
          #- table_name: "engage"
          #- table_name: "annotations"
          #- table_name: "cohorts"
          #- table_name: "cohort_members"

            # OPTIONAL: Load time transformations - you can add it to any table
            #transformations:
            #  - column: "some_column_to_transform" # Column to transform
            #    type: "SET-NULL"                   # Transformation type
