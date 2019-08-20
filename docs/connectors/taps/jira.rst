
.. _tap-jira:

Tap Jira
--------

Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for Jira replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``tap-jira``:

.. code-block:: bash

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "jira"                             # Unique identifier of the tap
    name: "Jira"                           # Name of the tap
    type: "tap-jira"                       # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"              # Data owner to contact


    # ------------------------------------------------------------------------------
    # Source (Tap) - Jira connection details
    # ------------------------------------------------------------------------------
    db_conn:
      # Method 1: Base authentication
      base_url: "https://<your_domain>.atlassian.net"  # the URL where your Jira installation can be found
      username: "<USERNAME>"
      password: "<PASSWORD>"                            # Plain string or vault encrypted

      # Mathod 2: OAuth authentication
      #oauth_client_secret: "<oauth-client-secret>"     # Plain string or vault encrypted
      #oauth_client_id: "<oauth-client-id>"
      #access_token: "<access-token>"                   # Plain string or vault encrypted
      #cloud_id: "<cloud-id>"
      #refresh_token: "<refresh-token>"                 # Plain string or vault encrypted

      start_date: "2010-01-01"  # specifies the date at which the tap will begin pulling data


    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"                       # ID of the target connector where the data will be loaded
    batch_size_rows: 20000                    # Batch size for the stream to optimise load performance
    default_target_schema: "jira"             # Target schema where the data will be loaded 
    default_target_schema_select_permission:  # Optional: Grant SELECT on schema and tables that created
      - grp_power


    # ------------------------------------------------------------------------------
    # Source to target Schema mapping
    # ------------------------------------------------------------------------------
    schemas:
      - source_schema: "jira"               # This is mandatory, but can be anything in this tap type
        target_schema: "jira"               # Target schema in the destination Data Warehouse
        target_schema_select_permissions:   # Optional: Grant SELECT on schema and tables that created
          - grp_stats

        # List of Jira tables to replicate into destination Data Warehouse
        # Tap-Jira will use the best incremental strategies automatically to replicate data
        tables:

          # Tables replicated incrementally
          - table_name: "changelogs"
          - table_name: "issues"
          - table_name: "issue_comments"
          - table_name: "issue_transitions"
          - table_name: "worklogs"

            # OPTIONAL: Load time transformations - you can add it to any table
            #transformations:                    
            #  - column: "some_column_to_transform" # Column to transform
            #    type: "SET-NULL"                   # Transformation type

          # FULL_TABLE replicated tables
          # JIRA Cloud REST API doesn't provide reasonable replication keys for these tables.
          # Replicating these tables can run longer. Please consider this when scheduling the tap.
          - table_name: "projects"
          - table_name: "project_categories"
          - table_name: "project_types"
          - table_name: "resolutions"
          - table_name: "roles"
          - table_name: "users"
          - table_name: "versions"
