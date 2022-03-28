
.. _tap-github:

Tap Github
----------

Configure your GitHub account
'''''''''''''''''''''''''''''

You need to create a GitHub access token to extract data from the Github API. Login to your
GitHub account, go to the `Personal Access Tokens <https://github.com/settings/tokens>`_
settings page, and generate a new token with at least the `repo` scope. Save this
access token, you'll need it for the next step.


Configuring what to extract
'''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for GitHub replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``tap-github``:

.. code-block:: yaml

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "github"                           # Unique identifier of the tap
    name: "Github"                         # Name of the tap
    type: "tap-github"                     # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"              # Data owner to contact
    sync_period: "*/90 * * * *"            # Period in which the tap will run
    #send_alert: False                     # Optional: Disable all configured alerts on this tap
    #slack_alert_channel: "#tap-channel"   # Optional: Sending a copy of specific tap alerts to this slack channel


    # ------------------------------------------------------------------------------
    # Source (Tap) - Github connection details
    # ------------------------------------------------------------------------------
    db_conn:
      access_token: "<ACCESS_TOKEN>"            # Github access token with at least the repo scope
      organization: "gnome"                     # The organization you want to extract the data from
                                                # Required when repos_include/repository isn't present
                                                # OR
                                                # Required when repos_exclude contains wildcard matchers
                                                # OR
                                                # Required when repos_include/repository contains wildcard matchers
      repos_include: "gnome* polari"            # Allow list strategy to extract selected repos data from organization.
                                                # Each repo path should be space delimited.
                                                # Supports wildcard matching
                                                # Values also valid: singer-io/tap-github another-org/tap-octopus
                                                # Org prefix not allowed when organization is present
      repos_exclude: "*tests* api-docs"         # Deny list to extract all repos from organization except the ones listed.
                                                # Each repo path should be space delimited.
                                                # Supports wildcard matching
                                                # Requires organization
                                                # Org prefix not allowed in repos_exclude
      repository: "gnome/gnome-software"        # (DEPRECATED) Path to one or multiple repositories that you want to extract data from organization (has priority over repos_exclude))
                                                # Each repo path should be space delimited.
                                                # Org prefix not allowed when organization is present
      include_archived: false                   # Optional: true/false to include archived repos. Default false
      include_disabled: false                   # Optional: true/false to include disabled repos. Default false
      max_rate_limit_wait_seconds: 600          # Optional: Max time to wait if you hit the github api limit. Default to 600s


    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"                       # ID of the target connector where the data will be loaded
    batch_size_rows: 20000                    # Batch size for the stream to optimise load performance
    stream_buffer_size: 0                     # In-memory buffer size (MB) between taps and targets for asynchronous data pipes
    default_target_schema: "github"            # Target schema where the data will be loaded
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

      - source_schema: "github"             # This is mandatory, but can be anything in this tap type
        target_schema: "github"             # Target schema in the destination Data Warehouse
        target_schema_select_permissions:   # Optional: Grant SELECT on schema and tables that created
          - grp_stats

        # List of Github tables to load into destination Data Warehouse
        # Tap-Github will use the best incremental strategies automatically to replicate data
        tables:
          # Supported tables
          - table_name: "commits"
          - table_name: "commit_comments"
          - table_name: "pull_requests"
          - table_name: "pull_request_reviews"
          - table_name: "events"
          - table_name: "pr_commits"
          - table_name: "reviews"
          - table_name: "review_comments"
          - table_name: "comments"
          - table_name: "issues"
          - table_name: "issue_labels"
          - table_name: "issue_milestones"
          - table_name: "releases"
          - table_name: "assignees"
          - table_name: "collaborators"
          - table_name: "stargazers"

          # Additional supported tables
          #- table_name: "projects"
          #- table_name: "project_cards"
          #- table_name: "project_columns"
          #- table_name: "teams"
          #- table_name: "team_memberships"
          #- table_name: "team_members"

            # OPTIONAL: Load time transformations - you can add it to any table
            #transformations:
            #  - column: "some_column_to_transform" # Column to transform
            #    type: "SET-NULL"                   # Transformation type
