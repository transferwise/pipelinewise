
.. _tap-mysql:

Tap MySQL
---------


MySQL setup requirements
''''''''''''''''''''''''

*(Section based on Stitch documentation)*

**Step 1: Check if you have all the required credentials for reproducing data from MySQL**

* ``CREATE USER`` or ``INSERT`` privilege (for the mysql database) - The ``CREATE USER`` privilege is required to create a database user for PipelineWise.

* ``GRANT OPTION`` privilege in MySQL - The ``GRANT OPTION`` privilege is required to grant the necessary privileges to the PipelineWise database user.

* ``SUPER`` privilege in MySQL - If using :ref:`log_based`, the ``SUPER`` privilege is required to define the appropriate server settings.

* The database connection credentials (which you will supply to the connection details in the tap) also need to have access to ``INFORMATION_SCHEMA`` of the MySQL DB in order to get metadata of the tables (like primary key details, column data type details etc)

**Step 2: Configuring database server settings**

.. note::

  This step is only required if you use :ref:`log_based` reproduction method.


.. warning::

  To use binlog reproduction, your MySQL database must be running MySQL version 5.6.2 or greater.

1. Log into your MySQL server.

2. Verify that binlog is enabled by running the following statement. The value returned should be 1:

.. code-block:: bash

    mysql> select @@log_bin;


3. Locate the ``my.cnf file``. It's usually located at ``/etc/my.cnf``. Verify that ``my.cnf`` has the following lines in the mysqld section:

.. code-block:: bash

    mysql> select @@log_bin;

    [mysqld]
    binlog_format=ROW
    binlog_row_image=FULL
    expire_logs_days=7
    binlog_expire_logs_seconds=604800
    log_bin=mysql-binlog
    log_slave_updates=1

A few things to note:

  * ``log_bin`` doesn't have to be ``mysql-binlog`` - this value can be anything. Additionally, if ``log_bin`` already has an entry (which you checked in step one), you don’t need to change it.

  * Use either ``expire_log_days`` or ``binlog_expire_logs_seconds``, not both

  * Setting ``log_slave_updates`` is only required if you are connecting a read replica. This isn’t required for master instances.


4. When finished, restart your MySQL server to ensure the changes take effect.


**Step 3. Create a PipelineWise database user**

Next, you’ll create a dedicated database user for PipelineWise. The user needs to have:

    * ``SELECT`` privileges on the database and every table that you want to reproduce.

    *  If using :ref:`log_based`, you'll also need to grant ``REPRODUCTION CLIENT`` and ``REPRODUCTION SLAVE`` privileges.


Configuring what to reproduce
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for MySQL reproduction can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``tap-mysql``:

.. code-block:: bash

  ---

  # ------------------------------------------------------------------------------
  # General Properties
  # ------------------------------------------------------------------------------
  id: "mysql_sample"                     # Unique identifier of the tap
  name: "Sample MySQL Database"          # Name of the tap
  type: "tap-mysql"                      # !! THIS SHOULD NOT CHANGE !!
  owner: "somebody@foo.com"              # Data owner to contact


  # ------------------------------------------------------------------------------
  # Source (Tap) - MySQL/ MariaDB connection details
  # ------------------------------------------------------------------------------
  db_conn:
    host: "<HOST>"                       # MySQL/ MariaDB host
    port: 3306                           # MySQL/ MariaDB port
    user: "<USER>"                       # MySQL/ MariaDB user
    password: "<PASSWORD>"               # Plain string or vault encrypted
    dbname: "<DB_NAME>"                  # MySQL/ MariaDB database name
    #filter_dbs: "schema1,schema2"       # Optional: Scan only the required schemas
                                         #           to improve the performance of
                                         #           data extraction
    #export_batch_rows                   # Optional: Number of rows to export from MySQL
                                         #           in one batch. Default is 20000.


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

    - source_schema: "my_db"             # Source schema (aka. database) in MySQL/ MariaDB with tables
      target_schema: "repl_my_db"        # Target schema in the destination Data Warehouse
      target_schema_select_permissions:  # Optional: Grant SELECT on schema and tables that created
        - grp_stats

      # List of tables to reproduce from Postgres to destination Data Warehouse
      #
      # Please check the Reproduction Strategies section in the documentation to understand the differences.
      # For LOG_BASED reproduction method you might need to adjust the source mysql/ mariadb configuration.
      tables:
        - table_name: "table_one"
          reproduction_method: "INCREMENTAL"   # One of INCREMENTAL, LOG_BASED and FULL_TABLE
          reproduction_key: "last_update"      # Important: Incremental load always needs reproduction key

          # OPTIONAL: Load time transformations
          #transformations:
          #  - column: "last_name"            # Column to transform
          #    type: "SET-NULL"               # Transformation type

        # You can add as many tables as you need...
        - table_name: "table_two"
          reproduction_method: "LOG_BASED"     # Important! Log based must be enabled in MySQL

    # You can add as many schemas as you need...
    # Uncomment this if you want reproduce tables from multiple schemas
    #- source_schema: "another_schema_in_mysql"
    #  target_schema: "another

