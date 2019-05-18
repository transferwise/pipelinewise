
.. _tap-s3-csv:

Tap S3 CSV
-----------


Connecting to S3
''''''''''''''''

** :: TODO :: **


Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for S3 CSV replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for tap-s3-csv:

.. code-block:: yaml

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "feeds_on_s3"                       # Unique identifier of the tap
    name: "Sampe CSV files on S3"          # Name of the tap
    type: "tap-s3-csv"                     # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"              # Data owner to contact


    # ------------------------------------------------------------------------------
    # Source (Tap) - S3 connection details
    # ------------------------------------------------------------------------------
    db_conn:
      aws_access_key_id: "<ACCESS_KEY_ID>"          # Plain string or vault encrypted
      aws_secret_access_key: "<SECRET_ASCCESS_KEY>" # Plain string or vault encrypted
      bucket: "my-bucket"                           # S3 Bucket name
      start_date: "2000-01-01"                      # File before this data will be excluded

    
    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"                       # ID of the target connector where the data will be loaded
    batch_size_rows: 20000                    # Batch size for the stream to optimise load performance
    default_target_schema: "s3_feeds"         # Target schema where the data will be loaded 
    default_target_schema_select_permission:  # Optional: Grant SELECT on schema and tables that created
      - grp_power
    # primary_key_required: False             # Optional: in case you want to load tables without key
                                              #            properties, uncomment this. Please note
                                              #            that files without primary keys will not
                                              #            be de-duplicated and could cause
                                              #            duplicates. Aloways try selecting
                                              #            a reasonable key from the CSV file


    # ------------------------------------------------------------------------------
    # Source to target Schema mapping
    # ------------------------------------------------------------------------------
    schemas:
      - source_schema: "greenhouse" # This is mandatory, but can be anything in this tap type
        target_schema: "greenhouse" # Target schema in the destination Data Warehouse
        
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

