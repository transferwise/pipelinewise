
.. _tap-s3-csv:

Tap S3 CSV
-----------


Extracting data from S3 in CSV file format is straightforward. You need to have
access to an S3 bucket and the tap will download every file that matches the
configured file pattern. It's tracking the ``Last-Modified`` timestamp on the
S3 objects to incrementally download only the new or updated files.

.. warning::

  **Authentication Methods**

   * **Profile based authentication**: This is the default authentication method. Credentials taken from
     the ``AWS_PROFILE`` environment variable or the ``default`` AWS profile, that's available on the host where
     PipelineWise is running.
     To use another profile set the ``aws_profile`` parameter.
     This method requires the presence of ``~/.aws/credentials`` file on the host.

   * **Credentials based authentication**: To provide fixed credentials set ``aws_access_key_id``,
     ``aws_secret_access_key`` and optionally the ``aws_session_token`` parameters.

     Optionally the credentials can be vault-encrypted in the YAML. Please check :ref:`encrypting_passwords`
     for further details.

   * **IAM role based authentication**: When no credentials and no AWS profile is given nor found on the host,
     PipelineWise will resort to use the IAM role attached to the host.

Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for S3 CSV replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``tap-s3-csv``:

.. code-block:: yaml

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "csv_on_s3"                        # Unique identifier of the tap
    name: "Sample CSV files on S3"          # Name of the tap
    type: "tap-s3-csv"                     # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"              # Data owner to contact
    #send_alert: False                     # Optional: Disable all configured alerts on this tap


    # ------------------------------------------------------------------------------
    # Source (Tap) - S3 connection details
    # ------------------------------------------------------------------------------
    db_conn:

      # Profile based authentication
      aws_profile: "<AWS_PROFILE>"                  # AWS profile name, if not provided, the AWS_PROFILE environment
                                                    # variable or the 'default' profile will be used, if not
                                                    # available, then IAM role attached to the host will be used.

      # Credentials based authentication
      #aws_access_key_id: "<ACCESS_KEY>"            # Plain string or vault encrypted. Required for non-profile based auth. If not provided, AWS_ACCESS_KEY_ID environment variable will be used.
      #aws_secret_access_key: "<SECRET_ACCESS_KEY"  # Plain string or vault encrypted. Required for non-profile based auth. If not provided, AWS_SECRET_ACCESS_KEY environment variable will be used.
      #aws_session_token: "<AWS_SESSION_TOKEN>"     # Optional: Plain string or vault encrypted. If not provided, AWS_SESSION_TOKEN environment variable will be used.

      #aws_endpoint_url: "<FULL_ENDPOINT_URL>"      # Optional: for non AWS S3, for example https://nyc3.digitaloceanspaces.com

      bucket: "my-bucket"                           # S3 Bucket name
      start_date: "2000-01-01"                      # File before this data will be excluded
      fastsync_parallelism: <int>                   # Optional: size of multiprocessing pool used by FastSync
                                                    #           Min: 1
                                                    #           Default: number of CPU cores
    
    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"                       # ID of the target connector where the data will be loaded
    batch_size_rows: 20000                    # Batch size for the stream to optimise load performance
    stream_buffer_size: 0                     # In-memory buffer size (MB) between taps and targets for asynchronous data pipes
    default_target_schema: "s3_feeds"         # Target schema where the data will be loaded 
    default_target_schema_select_permission:  # Optional: Grant SELECT on schema and tables that created
      - grp_power
    # primary_key_required: False             # Optional: in case you want to load tables without key
                                              #            properties, uncomment this. Please note
                                              #            that files without primary keys will not
                                              #            be de-duplicated and could cause
                                              #            duplicates. Always try selecting
                                              #            a reasonable key from the CSV file
    #batch_wait_limit_seconds: 3600           # Optional: Maximum time to wait for `batch_size_rows`. Available only for snowflake target.


    # ------------------------------------------------------------------------------
    # Source to target Schema mapping
    # ------------------------------------------------------------------------------
    schemas:
      - source_schema: "s3_feeds" # This is mandatory, but can be anything in this tap type
        target_schema: "s3_feeds" # Target schema in the destination Data Warehouse
        
        # List of CSV files to destination tables
        tables:

          # Every file in S3 bucket that matches the search pattern will be loaded into this table
          - table_name: "feed_file_one"
            s3_csv_mapping:
              search_pattern: "^feed_file_one_.*.csv$" # Required.
              search_prefix: ""                        # Optional
              key_properties: ["id"]                   # Optional
              delimiter: ","                           # Optional. Default: ','

            # OPTIONAL: Load time transformations
            #transformations:                    
            #  - column: "last_name"            # Column to transform
            #    type: "SET-NULL"               # Transformation type

          # You can add as many tables as you need...
          - table_name: "feed_file_two"
            s3_csv_mapping:
              search_pattern: "^feed_file_tow_.csv$"

