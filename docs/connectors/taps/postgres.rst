
.. _tap-postgres:

Tap PostgreSQL
--------------


PostgreSQL setup requirements
'''''''''''''''''''''''''''''

*(Section based on Stitch documentation)*

**Step 1: Check if you have all the required credentials for replicating data from PostgreSQL**

* The ``CREATEROLE`` or ``SUPERUSER`` privilege. Either permission is required to create a database user for PipelineWise.

* The ``GRANT OPTION`` privilege in MySQL. The ``GRANT OPTION`` privilege is required to grant the necessary privileges to the PipelineWise database user.

* The ``SUPER`` privilege in MySQL. If using :ref:`log_based`, the ``SUPER`` privilege is required to define the appropriate server settings.

**Step 2. Create a PipelineWise database user**

Next, you’ll create a dedicated database user for PipelineWise. Create a new user and grant the required permissions
on the database, schema and tables that you want to replicate:

    * ``CREATE USER pipelinewise WITH ENCRYPTED PASSWORD '<password>'``
    * ``GRANT CONNECT ON DATABASE <database_name> TO pipelinewise``
    * ``GRANT USAGE ON SCHEMA <schema_name> TO pipelinewise``
    * ``GRANT SELECT ON ALL TABLES IN SCHEMA <schema_name> TO pipelinewise``

**Step 3: Configure Log-based Incremental Replication**

.. note::

  This step is only required if you use :ref:`log_based` replication method.

.. warning::

  :ref:`log_based` for PostgreSQL-based databases requires:

  * **PostgreSQL databases running PostgreSQL versions 9.4.x or greater.** Earlier versions of PostgreSQL do not include logical replication functionality, which is required for Log-based Replication.

  * **A connection to the master instance.** Log-based replication will only work on master instances due to a feature gap in PostgreSQL 10. Based on their forums, PostgreSQL is working on adding support for using logical replication on a read replica to a future version.

    Until this feature is released, you can connect PipelineWise to the master instance and use Log-based Replication, or connect to a read replica and use Key-based Incremental Replication.

**Step 3.1: Install the wal2json plugin**

To use :ref:`log_based` for your PostgreSQL integration, you must install the `wal2json <https://github.com/eulerto/wal2json>`_ plugin. The wal2json plugin outputs JSON objects for logical decoding, which Stitch then uses to perform Log-based Replication.

Steps for installing the plugin vary depending on your operating system. Instructions for each operating system type are in the wal2json’s GitHub repository:

* `Unix-based operating systems <https://github.com/eulerto/wal2json#unix-based-operating-systems>`_

* `Windows <https://github.com/eulerto/wal2json#windows>`_

After you’ve installed the plugin, you can move onto the next step.

**Step 3.2: Edit the database configuration file**

Locate the database configuration file (usually ``postgresql.conf``) and define the parameters as follows:

.. code-block:: bash

    wal_level=logical
    max_replication_slots=5
    max_wal_senders=5

**Note**: For ``max_replication_slots`` and ``max_wal_senders``, we’re defaulting to a value of 5.
This should be sufficient unless you have a large number of read replicas connected to the master instance.

**Step 3.3: Restart the PostgreSQL service**

Restart your PostgreSQL service to ensure the changes take effect.

**Step 3.4: Create a replication slot**

Next, you’ll create a dedicated logical replication slot for Stitch. In PostgreSQL, a logical replication
slot represents a stream of database changes that can then be replayed to a client in the order they were
made on the original server. Each slot streams a sequence of changes from a single database.

**Note**: Replication slots are specific to a given database in a cluster. If you want to connect
multiple databases - whether in one integration or several - you’ll need to create a replication slot
for each database.

1. Log into the master database as a superuser.

2. Using the ``wal2json`` plugin, create a logical replication slot:

.. code-block:: bash

    SELECT *
    FROM pg_create_logical_replication_slot('pipelinewise_<raw_database_name>', 'wal2json');

3. Log in as the PipelineWise user and verify you can read from the replication slot,
replacing ``<replication_slot_name>`` with the name of the replication slot:

.. code-block:: bash

    SELECT COUNT(*)
    FROM pg_logical_slot_peek_changes('<replication_slot_name>', null, null);

**Note**: ``wal2json`` is required to use :ref:`log_based` in Stitch for PostgreSQL-backed databases.


Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for Postgres replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``tap-postgres``:

.. code-block:: bash

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
