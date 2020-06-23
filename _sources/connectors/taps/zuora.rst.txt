
.. _tap-zuora:

Tap Zuora
--------------

Connecting to Zuora
''''''''''''''''''''''''


Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for Zuora replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for tap-zuora:

.. code-block:: bash

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "zuora"                       # Unique identifier of the tap
    name: "Sample data on Zuora"      # Name of the tap
    type: "tap-zuora"                 # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"              # Data owner to contact


    # ------------------------------------------------------------------------------
    # Source (Tap) - Zuora connection details
    #
    # The client_id and client_secret keys are your OAuth Salesforce App secrets.
    # The refresh_token is a secret created during the OAuth flow. For more info on
    # the Salesforce OAuth flow, visit the Salesforce documentation at
    # https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_understanding_web_server_oauth_flow.htm
    #
    # api_type to use extracting data from Zuora. This can be AQUA or REST.
    # Further details about API types at https://www.stitchdata.com/docs/integrations/saas/zuora#rest-vs-aqua-api
    # ------------------------------------------------------------------------------
    db_conn:
      username: "<USERNAME>"                  # Zuora username
      password: "<PASSWORD>"                  # Zuora password
      partner_id: "<PARTNER_ID>"              # In case of using the AQUA api, a partner id is required
      start_date: "2019-01-01T00:00:00Z"      # Bound on api queries when searching for records
      api_type: "AQUA"                        # Zuora API Type: AQUA or REST
      sandbox: "true"                         # Determines which api location to call
      european: "true"                        # Determines which api location to call



    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"                       # ID of the target connector where the data will be loaded
    batch_size_rows: 20000                    # Batch size for the stream to optimise load performance
    stream_buffer_size: 0                     # In-memory buffer size (MB) between taps and targets for asynchronous data pipes
    default_target_schema: "zuora"       # Target schema where the data will be loaded
    default_target_schema_select_permission:  # Optional: Grant SELECT on schema and tables that created
      - grp_power


    # ------------------------------------------------------------------------------
    # Source to target Schema mapping
    # ------------------------------------------------------------------------------
    schemas:
      - source_schema: "zuora"           # This is mandatory, but can be anything in this tap type
        target_schema: "zuora"           # Target schema in the destination Data Warehouse
        default_target_schema_select_permission:  # Optional: Grant SELECT on schema and tables that created
        - grp_power


        # List of Zuora tables to replicate into destination Data Warehouse
        # Tap-Zuora will default to FULL_TABLE replication, but supports INCREMENTAL replication, which is recommended
        #
        # The available object types (and their replication keys to use, if supported) which are supported are listed on
        # https://www.stitchdata.com/docs/integrations/saas/zuora#zuora-entity-relationships
        #
        # Unsupported objects to replicate are listed on
        # https://www.stitchdata.com/docs/integrations/saas/zuora#unsupported-objects
        #
        tables:
          - table_name: "Account"
            replication_method: "INCREMENTAL"
            replication_key: "updatedDate"
          - table_name: "BillingRun"
            replication_method: "FULL_TABLE"

            # OPTIONAL: Load time transformations
            #transformations:                    
            #  - column: "last_name"            # Column to transform
            #    type: "SET-NULL"               # Transformation type

