
.. _tap-adwords:

Tap AdWords
-----------

Documentation
'''''''''''''

[Singer-io tap-adwords](https://github.com/singer-io/tap-adwords/blob/master/README.md)


Configuring what to extract
'''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for AdWords replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``tap-adwords``:

.. code-block:: yaml

    ---
    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "adwords"                           # Unique identifier of the tap
    name: "Google Adwords"                  # Name of the tap
    type: "tap-adwords"                     # !! THIS SHOULD NOT CHANGE !!
    owner: "Foo"                            # Data owner to contact
    #send_alert: False                      # Optional: Disable all configured alerts on this tap

    primary_key_required: False
    db_conn:
      developer_token: <>
      oauth_client_secret: <>
      refresh_token: <>
      oauth_client_id: <>
      start_date: "2021-01-01"
      user_agent: <>
      customer_ids: "ID1,ID2,ID3"

    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"
    batch_size_rows: 20000
    stream_buffer_size: 0                     # In-memory buffer size (MB) between taps and targets for asynchronous data pipes
    #batch_wait_limit_seconds: 3600           # Optional: Maximum time to wait for `batch_size_rows`. Available only for snowflake target.

    # Options only for Snowflake target
    #archive_load_files: False                      # Optional: when enabled, the files loaded to Snowflake will also be stored in `archive_load_files_s3_bucket`
    #archive_load_files_s3_prefix: "archive"        # Optional: When `archive_load_files` is enabled, the archived files will be placed in the archive S3 bucket under this prefix.
    #archive_load_files_s3_bucket: "<BUCKET_NAME>"  # Optional: When `archive_load_files` is enabled, the archived files will be placed in this bucket. (Default: the value of `s3_bucket` in target snowflake YAML)


    # Replication methods are not required to be defined for this tap
    schemas:
      - source_schema: "google_adwords"
        target_schema: "google_adwords"
        target_schema_select_permissions:
          - role_x
        tables:
          - table_name: "campaigns"
          - table_name: "ad_groups"
          - table_name: "ads"
          - table_name: "accounts"
          - table_name: "keywords_performance_report"
          - table_name: "ad_performance_report"
          - table_name: "adgroup_performance_report"
          - table_name: "campaign_performance_report"
          - table_name: "account_performance_report"
          - table_name: "geo_performance_report"
          - table_name: "search_query_performance_report"
          - table_name: "criteria_performance_report"
          - table_name: "click_performance_report"
          - table_name: "display_keyword_performance_report"
          - table_name: "placement_performance_report"
          - table_name: "audience_performance_report"
          - table_name: "display_topics_performance_report"
          - table_name: "final_url_report"
          - table_name: "video_performance_report"
            # OPTIONAL: Load time transformations - you can add it to any table
            #transformations:
            #  - column: "some_column_to_transform" # Column to transform
            #    type: "SET-NULL"                   # Transformation type
