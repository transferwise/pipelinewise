
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
A sample YAML for Jira replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``tap-github``:

.. code-block:: bash

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "github"                           # Unique identifier of the tap
    name: "Github"                         # Name of the tap
    type: "tap-github"                     # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"              # Data owner to contact
    #send_alert: False                     # Optional: Disable all configured alerts on this tap


    # ------------------------------------------------------------------------------
    # Source (Tap) - Github connection details
    # ------------------------------------------------------------------------------
    db_conn:
      access_token: "<ACCESS_TOKEN>"            # Github access token with at least the repo scope
      repository: "transferwise/pipelinewise"   # Path to one or multiple repositories that you want to extract data from
                                                # Each repo path should be space delimited.

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
