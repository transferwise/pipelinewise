
.. _tap-google-analytics:

Tap Google Analytics
--------------------


Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for Google Analytics replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Authorization Methods
'''''''''''''''''''''

``tap-google-analytics`` supports two different ways of authorization:

 - **Service account based authorization**, where an administrator manually creates a service account with the appropriate permissions to view the account, property, and view you wish to fetch data from
 - **OAuth** ``access_token`` **based authorization**, where this tap gets called with a valid ``access_token`` and ``refresh_token`` produced by an OAuth flow conducted in a different system.

If you're setting up ``tap-google-analytics`` for your own organization and only plan to extract from a handful of different views in the same limited set of properties, Service Account based authorization is the simplest. When you create a service account Google gives you a json file with that service account's credentials called the ``client_secrets.json``, and that's all you need to pass to this tap, and you only have to do it once, so this is the recommended way of configuring ``tap-google-analytics``.

If you're building something where a wide variety of users need to be able to give access to their Google Analytics, ``tap-google-analytics`` can use an ``access_token`` granted by those users to authorize it's requests to Google. This ``access_token`` is produced by a normal Google OAuth flow, but this flow is outside the scope of ``tap-google-analytics``. This is useful if you're integrating ``tap-google-analytics`` with another system, like Stitch Data might do to allow users to configure their extracts themselves without manual config setup. This tap expects an ``access_token``, ``refresh_token``, ``client_id`` and ``client_secret`` to be passed to it in order to authenticate as the user who granted the token and then access their data.

.. warning::

  This tap does not currently use any ``STATE`` information for incrementally extracting data. This is currently mitigated by allowing for chunked runs using ``[start_date, end_date]``, but we should definitely add support for using ``STATE`` messages.

  The difficulty on that front is on dynamically deciding which attributes to use for capturing state for ad-hoc reports that do not include the `ga:date` dimension or other combinations of Time Dimensions.

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
      view_id: "<view-id>"
      start_date: "2010-01-01"  # specifies the date at which the tap will begin pulling data

      # OAuth authentication
      oauth_credentials:
        client_id: "<client-id>"
        client_secret: "<oauth-client-id>"               # Plain string or vault encrypted
        access_token: "<access-token>"                   # Plain string or vault encrypted
        refresh_token: "<refresh-token>"                 # Plain string or vault encrypted

      # Service account based authorization
      # key_file_location: "full-path-to-client_secrets.json"


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
        # List of available tables available at https://github.com/transferwise/pipelinewise-tap-google-analytics/blob/master/tap_google_analytics/defaults/default_report_definition.json
        tables:

          # Tables replicated incrementally
          - table_name: "website_overview"
          - table_name: "traffic_sources"
          - table_name: "monthly_active_users"

            # OPTIONAL: Load time transformations - you can add it to any table
            #transformations:
            #  - column: "some_column_to_transform" # Column to transform
            #    type: "SET-NULL"                   # Transformation type
