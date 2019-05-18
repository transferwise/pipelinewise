
.. _tap-postgres:

Tap PostgreSQL
--------------


Connecting to PostgreSQL
''''''''''''''''''''''''

.. warning::

  :ref:`log_based` for PostgreSQL-based databases requires:

  * **PostgreSQL databases running PostgreSQL versions 9.4.x or greater.** Earlier versions of PostgreSQL do not include logical replication functionality, which is required for Log-based Replication.

  * **A connection to the master instance.** Log-based replication will only work on master instances due to a feature gap in PostgreSQL 10. Based on their forums, PostgreSQL is working on adding support for using logical replication on a read replica to a future version.

    Until this feature is released, you can connect Stitch to the master instance and use Log-based Replication, or connect to a read replica and use Key-based Incremental Replication.


Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for Postgres replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for tap-postgres:

.. code-block:: yaml

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "postgres_sample"                  # Unique identifier of the tap
    name: "Sample Postgres Database"       # Name of the tap
    type: "tap-postgres"                   # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"              # Data owner to contact


    # ------------------------------------------------------------------------------
    # Source (Tap) - PostgreSQL connection details
    # ------------------------------------------------------------------------------
    db_conn:
      host: "<HOST>"                       # PostgreSQL host
      port: 5432                           # PostgreSQL port
      user: "<USER>"                       # PostfreSQL user
      password: "<PASSWORD>"               # Plain string or vault encrypted
      dbname: "<DB_NAME>"                  # PostgreSQL database name
      #filter_schemas: "schema1,schema2"   # Optional: Scan only the required schemas
                                           #           to improve the performance of
                                           #           data extraction


    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"                    # ID of the target connector where the data will be loaded
    batch_size_rows: 20000                 # Batch size for the stream to optimise load performance


    # ------------------------------------------------------------------------------
    # Source to target Schema mapping
    # ------------------------------------------------------------------------------
    schemas:

      - source_schema: "public"            # Source schema in postgres with tables
        target_schema: "repl_pg_public"    # Target schema in the destination Data Warehouse
        target_schema_select_permissions:  # Optional: Grant SELECT on schema and tables that created
          - grp_stats

        # List of tables to replicate from Postgres to destination Data Warehouse
        #
        # Please check the Replication Strategies section in the documentation to understand the differences.
        # For LOG_BASED replication method you might need to adjust the source mysql/ mariadb configuration.
        tables:
          - table_name: "table_one"
            replication_method: "INCREMENTAL"   # One of INCREMENTAL, LOG_BASED and FULL_TABLE
            replication_key: "last_update"      # Important: Incremental load always needs replication key

            # OPTIONAL: Load time transformations
            #transformations:                    
            #  - column: "last_name"            # Column to transform
            #    type: "SET-NULL"               # Transformation type

          # You can add as many tables as you need...
          - table_name: "table_tow"
            replication_method: "INCREMENTAL"   # Important! Log based must be enabled in MySQL

      # You can add as many schemas as you need...
      # Uncommend this if you want replicate tables from multiple schemas
      #- source_schema: "another_schema_in_postgres" 
      #  target_schema: "another
