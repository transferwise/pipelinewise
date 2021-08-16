
.. _tap-twilio:

Tap Twilio
-----------


Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for Twilio replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for tap-twilio:

.. code-block:: yaml

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "twilio"                           # Unique identifier of the tap
    name: "Twilio"                         # Name of the tap
    type: "tap-twilio"                     # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"              # Data owner to contact
    #send_alert: False                     # Optional: Disable all configured alerts on this tap


    # ------------------------------------------------------------------------------
    # Source (Tap) - Twilio connection details
    # ------------------------------------------------------------------------------
    db_conn:
      account_sid: <TWILIO_ACCOUNT_SID>         # Twilio Account SID
      auth_token: <TWILIO_AUTH_TOKEN>           # Twilio Auth token
      start_date: "2021-02-01T00:00:00Z"        # The default value to use if no bookmark exists for an endpoint. ISO-8601 datetime formatted string
      user_agent: "someone@transferwise.com"    # Optional: Process and email for API logging purposes.


    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"                        # ID of the target connector where the data will be loaded
    batch_size_rows: 20000                     # Batch size for the stream to optimise load performance
    stream_buffer_size: 0                      # In-memory buffer size (MB) between taps and targets for asynchronous data pipes
    default_target_schema: "twilio"            # Target schema where the data will be loaded
    #default_target_schema_select_permission:  # Optional: Grant SELECT on schema and tables that created
    #  - grp_power
    #batch_wait_limit_seconds: 3600            # Optional: Maximum time to wait for `batch_size_rows`. Available only for snowflake target.

    # Options only for Snowflake target
    #archive_load_files: False                      # Optional: when enabled, the files loaded to Snowflake will also be stored in `archive_load_files_s3_bucket`
    #archive_load_files_s3_prefix: "archive"        # Optional: When `archive_load_files` is enabled, the archived files will be placed in the archive S3 bucket under this prefix.
    #archive_load_files_s3_bucket: "<BUCKET_NAME>"  # Optional: When `archive_load_files` is enabled, the archived files will be placed in this bucket. (Default: the value of `s3_bucket` in target snowflake YAML)


    # ------------------------------------------------------------------------------
    # Source to target Schema mapping
    # ------------------------------------------------------------------------------
    schemas:

      - source_schema: "twilio"             # This is mandatory, but can be anything in this tap type
        target_schema: "twilio"             # Target schema in the destination Data Warehouse
        target_schema_select_permissions:   # Optional: Grant SELECT on schema and tables that created
          - grp_stats

        # List of Twilio tables to load into destination Data Warehouse
        # Tap-Twilio will use the best incremental strategies automatically to replicate data
        tables:
          # Incrementally loaded tables
          # TaskRouter resources
          - table_name: "workspaces"
          - table_name: "activities"
          - table_name: "events"
          - table_name: "tasks"
          - table_name: "task_channels"
          - table_name: "task_queues"
          - table_name: "workers"
          - table_name: "workflows"
          # Programmable Chat resources
          - table_name: "services"
          - table_name: "roles"
          - table_name: "chat_channels"
          - table_name: "users"


          # Tables that cannot load incrementally and will use FULL_TABLE method
          # TaskRouter resources
          - table_name: "cumulative_statistics"
          - table_name: "channels"

          # Programmable Chat resources
          # These 2 resources are using FULL_TABLE method and can pull huge amount of data from the twilio api at every sync.
          # Please use it with caution.
          #- table_name: "members"
          #- table_name: "chat_messages"


            # OPTIONAL: Load time transformations - you can add it to any table
            #transformations:
            #  - column: "some_column_to_transform" # Column to transform
            #    type: "SET-NULL"                   # Transformation type
