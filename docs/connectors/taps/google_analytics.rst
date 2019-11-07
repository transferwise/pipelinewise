
.. _tap-google-analytics:

Tap Google Analytic
-----------


Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for Google Analytic replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``tap-google-analytics``:

.. code-block:: bash

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "google_analytics_sample"          # Unique identifier of the tap
    name: "Google Analytics"               # Name of the tap
    type: "tap-google-analytics"           # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"              # Data owner to contact


    # ------------------------------------------------------------------------------
    # Source (Tap) - Google Analytics connection details
    # ------------------------------------------------------------------------------
    db_conn:
      # Method: OAuth authentication
      view_id: "<view-id>"
      start_date: "2010-01-01"  # specifies the date at which the tap will begin pulling data

      oauth_credentials:
        client_id: "<client-id>"
        client_secret: "<oauth-client-id>"               # Plain string or vault encrypted
        access_token: "<access-token>"                   # Plain string or vault encrypted
        refresh_token: "<refresh-token>"                 # Plain string or vault encrypted



    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"                        # ID of the target connector where the data will be loaded
    batch_size_rows: 20000                     # Batch size for the stream to optimise load performance
    default_target_schema: "google-analytic"   # Target schema where the data will be loaded
    #default_target_schema_select_permission:  # Optional: Grant SELECT on schema and tables that created
    #  - grp_power


    # ------------------------------------------------------------------------------
    # Source to target Schema mapping
    # ------------------------------------------------------------------------------
    schemas:

      - source_schema: "google-analytics"               # This is mandatory, but can be anything in this tap type
        target_schema: "google-analytics"               # Target schema in the destination Data Warehouse
        #target_schema_select_permissions:   # Optional: Grant SELECT on schema and tables that created
        #  - grp_stats

        # List of Google Analytics tables to replicate into destination Data Warehouse
        # Tap-Google-Analytics will use the best incremental strategies automatically to replicate data
        tables:

          # Tables replicated incrementally
          - table_name: "website_overview"
          - table_name: "traffic_sources"
          - table_name: "monthly_active_users"

            # OPTIONAL: Load time transformations - you can add it to any table
            #transformations:
            #  - column: "some_column_to_transform" # Column to transform
            #    type: "SET-NULL"                   # Transformation type d