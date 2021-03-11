
.. _tap-salesforce:

Tap Salesforce
--------------

Connecting to Salesforce
''''''''''''''''''''''''

.. warning::

  This section of the documentation is work in progress.


Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for Salesforce replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for tap-salesforce:

.. code-block:: yaml

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "salesforce"                       # Unique identifier of the tap
    name: "Sample data on Salesforce"      # Name of the tap
    type: "tap-salesforce"                 # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"              # Data owner to contact
    #send_alert: False                     # Optional: Disable all configured alerts on this tap


    # ------------------------------------------------------------------------------
    # Source (Tap) - Salesforce connection details
    #
    # The client_id and client_secret keys are your OAuth Salesforce App secrets.
    # The refresh_token is a secret created during the OAuth flow. For more info on
    # the Salesforce OAuth flow, visit the Salesforce documentation at
    # https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_understanding_web_server_oauth_flow.htm
    #
    # api_type to use extracting data from Snowflake. This can be BULK or REST.
    # Further details about API types at https://www.stitchdata.com/docs/integrations/saas/salesforce#bulk-vs-rest-api
    # ------------------------------------------------------------------------------
    db_conn:
      client_id: "<ACCESS_TOKEN>"             # Salesforce Client ID
      client_secret: "<CLIENT_SECRET>"        # Salesforce Client Secret
      refresh_token: "<REFRESH_TOKEN"         # Salesforce Refresh Token
      start_date: "2019-01-01T00:00:00Z"      # Bound on SOQL queries when searching for records
      api_type: "BULK"                        # Salesforce API Type: BULK or REST


    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"                       # ID of the target connector where the data will be loaded
    batch_size_rows: 20000                    # Batch size for the stream to optimise load performance
    stream_buffer_size: 0                     # In-memory buffer size (MB) between taps and targets for asynchronous data pipes
    default_target_schema: "salesforce"       # Target schema where the data will be loaded
    default_target_schema_select_permission:  # Optional: Grant SELECT on schema and tables that created
      - grp_power


    # ------------------------------------------------------------------------------
    # Source to target Schema mapping
    # ------------------------------------------------------------------------------
    schemas:
      - source_schema: "salesforce"           # This is mandatory, but can be anything in this tap type
        target_schema: "salesforce"           # Target schema in the destination Data Warehouse

        # List of Salesforce tables to replicate into destination Data Warehouse
        # Tap-Salesforce will use the best incremental strategies automatically to replicate data
        #
        # Tap-Salesforce currently supports the replication of the majority of Salesforce objects,
        # with the exception of those listed in the Unsupported Objects row of this table at
        # https://www.stitchdata.com/docs/integrations/saas/salesforce#bulk-vs-rest-api
        #
        # 
        # This section will only cover a few of the most popular tables Salesforce integration offers.
        # See the Salesforce Object Reference guide for info on objects not listed here, including the
        # fields available in each object at https://resources.docs.salesforce.com/sfdc/pdf/object_reference.pdf
        tables:
          - table_name: "Account"
          - table_name: "Contact"
          - table_name: "Lead"
          - table_name: "Opportunity"
          - table_name: "User"

            # OPTIONAL: Load time transformations
            #transformations:                    
            #  - column: "last_name"            # Column to transform
            #    type: "SET-NULL"               # Transformation type

