
.. _tap-snowflake:

Tap Snowflake
-------------

Connecting to Snowflake
'''''''''''''''''''''''

.. warning::

  This section of the documentation is work in progress.


Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for Snowflake replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for tap-snowflake:

.. code-block:: yaml

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "snowflake_sample"                 # Unique identifier of the tap
    name: "Sample Snowflake Database Tap"  # Name of the tap
    type: "tap-snowflake"                  # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"              # Data owner to contact
    #send_alert: False                     # Optional: Disable all configured alerts on this tap
    #slack_alert_channel: "#tap-channel"   # Optional: Sending a copy of specific tap alerts to this slack channel


    # ------------------------------------------------------------------------------
    # Source (Tap) - Snowflake connection details
    # ------------------------------------------------------------------------------
    db_conn:
      account: "<HOST>"                    # Snowflake host
      dbname: "<DBNANE>"                   # Snowflake database name
      user: "<USER>"                       # Snowflake user
      password: "<PASSWORD>"               # Plain string or vault encrypted
      warehouse: "<WAREHOUSE>"             # Snowflake warehouse


    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"                    # ID of the target connector where the data will be loaded
    batch_size_rows: 20000                 # Batch size for the stream to optimise load performance
    stream_buffer_size: 0                  # In-memory buffer size (MB) between taps and targets for asynchronous data pipes
    #batch_wait_limit_seconds: 3600        # Optional: Maximum time to wait for `batch_size_rows`. Available only for snowflake target.

    # Options only for Snowflake target
    #archive_load_files: False                      # Optional: when enabled, the files loaded to Snowflake will also be stored in `archive_load_files_s3_bucket`
    #archive_load_files_s3_prefix: "archive"        # Optional: When `archive_load_files` is enabled, the archived files will be placed in the archive S3 bucket under this prefix.
    #archive_load_files_s3_bucket: "<BUCKET_NAME>"  # Optional: When `archive_load_files` is enabled, the archived files will be placed in this bucket. (Default: the value of `s3_bucket` in target snowflake YAML)


    # ------------------------------------------------------------------------------
    # Source to target Schema mapping
    # ------------------------------------------------------------------------------
    schemas:

      - source_schema: "SCHEMA_1"          # Source schema (aka. database) in Snowflake with tables
        target_schema: "REPL_SCHEMA_1"     # Target schema in the destination Data Warehouse

        # List of tables to replicate from Snowflake to a destination
        #
        # Please check the Replication Strategies section in the documentation to understand the differences.
        tables:
          - table_name: "TABLE_ONE"
            replication_method: "INCREMENTAL"   # One of INCREMENTAL or FULL_TABLE
            replication_key: "last_update"      # Important: Incremental load always needs replication key

            # OPTIONAL: Load time transformations
            #transformations:                    
            #  - column: "last_name"            # Column to transform
            #    type: "SET-NULL"               # Transformation type

          # You can add as many tables as you need...
          - table_name: "TABLE_TWO"
            replication_method: "FULL_TABLE"

      # You can add as many schemas as you need...
      # Uncomment this if you want replicate tables from multiple schemas
      #- source_schema: "SCHEMA_2" 
      #  target_schema: "REPL_SCHEMA_2"