
.. _tap-postgres:

Tap PostgreSQL
--------------


PostgreSQL setup requirements
'''''''''''''''''''''''''''''

*(Section based on Stitch documentation)*

**Step 1: Check if you have all the required credentials for replicating data from PostgreSQL**

* ``CREATEROLE`` or ``SUPERUSER`` privilege - Either permission is required to create a database user for PipelineWise.

**Step 2. Create a PipelineWise database user**

Next, you’ll create a dedicated database user for PipelineWise. Create a new user and grant the required permissions
on the database, schema and tables that you want to replicate:

    * ``CREATE USER pipelinewise WITH ENCRYPTED PASSWORD '<password>'``
    * ``GRANT CONNECT ON DATABASE <database_name> TO pipelinewise``
    * ``GRANT USAGE ON SCHEMA <schema_name> TO pipelinewise``
    * ``GRANT SELECT ON ALL TABLES IN SCHEMA <schema_name> TO pipelinewise``


In order for pipelinewise user to automatically be able to access any tables created in the future, we recommend running the following query:

``ALTER DEFAULT PRIVILEGES IN SCHEMA <schema_name> GRANT SELECT ON TABLES TO pipelinewise;``


**Step 3: Configure Log-based Incremental Replication**

.. note::

  This step is only required if you use :ref:`log_based` replication method.

.. warning::

  :ref:`log_based` for PostgreSQL-based databases requires:

  * **PostgreSQL databases running PostgreSQL versions 9.4.x or greater.**
  * **To avoid a critical PostgreSQL bug, use at least one of the following minor versions**

    * PostgreSQL 12.0

    * PostgreSQL 11.2

    * PostgreSQL 10.7

    * PostgreSQL 9.6.12

    * PostgreSQL 9.5.16

    * PostgreSQL 9.4.21

  * **A connection to the master instance.** Log-based replication will only work by connecting to the master instance.

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

**Step 3.4: Replication slot**

In PostgreSQL, a logical replication slot represents a stream of database changes that can then be replayed to a
client in the order they were made on the original server. Each slot streams a sequence of changes from a single
database.

Pipelinewise automatically creates a dedicated logical replication slot for each database and tap.


.. note:: ``wal2json`` is required to use :ref:`log_based` in Pipelinewise for PostgreSQL-backed databases.

.. note:: In case of full resync of a whole tap, Pipelinewise will attempt to drop the slot.


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
    #send_alert: False                     # Optional: Disable all configured alerts on this tap


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
      #max_run_seconds                     # Optional: Stop running the tap after certain
                                           #           number of seconds
                                           #           Default: 43200
      #logical_poll_total_seconds:         # Optional: Stop running the tap when no data
                                           #           received from wal after certain number of seconds
                                           #           Default: 10800
      #break_at_end_lsn:                   # Optional: Stop running the tap if the newly received lsn
                                           #           is after the max lsn that was detected when the tap started
                                           #           Default: true
      #ssl: "true"                         # Optional: Using SSL via postgres sslmode 'require' option.
                                           #           If the server does not accept SSL connections or the client
                                           #           certificate is not recognized the connection will fail


    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"                    # ID of the target connector where the data will be loaded
    batch_size_rows: 20000                 # Batch size for the stream to optimise load performance
    stream_buffer_size: 0                  # In-memory buffer size (MB) between taps and targets for asynchronous data pipes


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
          - table_name: "table_two"
            replication_method: "LOG_BASED"     # Important! Log based must be enabled in MySQL

      # You can add as many schemas as you need...
      # Uncomment this if you want replicate tables from multiple schemas
      #- source_schema: "another_schema_in_postgres" 
      #  target_schema: "another
