
.. _tap-mysql:

Tap MySQL
---------

This tap is compatible with Mysql and Mariadb servers to some extent.

MySQL setup requirements
''''''''''''''''''''''''

**Step 1: Check if you have all the required credentials for replicating data from MySQL**

* ``CREATE USER`` or ``INSERT`` privilege (for the mysql database) - The ``CREATE USER`` privilege is required to create a database user for PipelineWise.

* ``GRANT OPTION`` privilege in MySQL - The ``GRANT OPTION`` privilege is required to grant the necessary privileges to the PipelineWise database user.

* ``SUPER`` privilege in MySQL - If using :ref:`log_based`, the ``SUPER`` privilege is required to define the appropriate server settings.

* The database connection credentials (which you will supply to the connection details in the tap) also need to have access to ``INFORMATION_SCHEMA`` of the MySQL DB in order to get metadata of the tables (like primary key details, column data type details etc)

**Step 2: Configuring database server settings**

.. note::

  This step is only required if you use :ref:`log_based` replication method.


.. warning::

  To use binlog replication, your MySQL database must be running MySQL version 5.6.2 or greater.

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

    * ``SELECT`` privileges on the database and every table that you want to replicate.

    *  If using :ref:`log_based`, you'll also need to grant ``REPLICATION CLIENT`` and ``REPLICATION SLAVE`` privileges.


Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for MySQL replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

.. warning::

  ``TINYINT`` by convention is used to store 0/1 for mysql booleans. In keeping with the
  upstream singer mysql tap, pipelinewise uses this convention when creating target schemas.
  If your tinyint columns contain values greater than 1, this will lead to errors when
  loading data.

  Read more info about this decision in `this github issue <https://github.com/singer-io/tap-mysql/issues/82>`_
  or browse the codebase `here <https://github.com/transferwise/pipelinewise-tap-mysql/blob/34cbd9b085146c08003bfa460f1550ce78c65e4c/tap_mysql/__init__.py#L73>`_.


.. note::

  This tap supports :ref:`log_based` replication method with GTID position, at the time
  of writing this, only MariaDB GTID is implemented.


Example YAML for ``tap-mysql``:

.. code-block:: yaml

  ---

  # ------------------------------------------------------------------------------
  # General Properties
  # ------------------------------------------------------------------------------
  id: "mysql_sample"                     # Unique identifier of the tap
  name: "Sample MySQL Database"          # Name of the tap
  type: "tap-mysql"                      # !! THIS SHOULD NOT CHANGE !!
  owner: "somebody@foo.com"              # Data owner to contact
  #send_alert: False                     # Optional: Disable all configured alerts on this tap


  # ------------------------------------------------------------------------------
  # Source (Tap) - MySQL/ MariaDB connection details
  # ------------------------------------------------------------------------------
  db_conn:
    host: "<HOST>"                       # MySQL/ MariaDB host
    port: 3306                           # MySQL/ MariaDB port
    user: "<USER>"                       # MySQL/ MariaDB user
    password: "<PASSWORD>"               # Plain string or vault encrypted
    dbname: "<DB_NAME>"                  # MySQL/ MariaDB database name
    use_gtid: <boolean>                  # Flag to enable using GTID as the state bookmark for log based tables
    engine: "mariadb/mysql"              # Flavor of the server, used in conjunction with "use_gtid"
    #filter_dbs: "schema1,schema2"       # Optional: Scan only the required schemas
                                         #           to improve the performance of
                                         #           data extraction
    #export_batch_rows                   # Optional: Number of rows to export from MySQL
                                         #           in one batch. Default is 50000.
    #session_sqls:                       # Optional: Run SQLs to set session variables
    #  - SET @@session.time_zone="+0:00"             # when the connection made
    #  - SET @@session.wait_timeout=28800            # Defaults to the values listed here
    #  - SET @@session.net_read_timeout=3600
    #  - SET @@session.innodb_lock_wait_timeout=3600

    fastsync_parallelism: <int>          # Optional: size of multiprocessing pool used by FastSync
                                         #           Min: 1
                                         #           Default: number of CPU cores

  # ------------------------------------------------------------------------------
  # Destination (Target) - Target properties
  # Connection details should be in the relevant target YAML file
  # ------------------------------------------------------------------------------
  target: "snowflake"                    # ID of the target connector where the data will be loaded
  batch_size_rows: 20000                 # Batch size for the stream to optimise load performance
  stream_buffer_size: 0                  # In-memory buffer size (MB) between taps and targets for asynchronous data pipes
  #batch_wait_limit_seconds: 3600        # Optional: Maximum time to wait for `batch_size_rows`. Available only for snowflake target.

  # Options only for Snowflake target
  #split_large_files: False                       # Optional: split large files to multiple pieces and create multipart zip files. (Default: False)
  #split_file_chunk_size_mb: 1000                 # Optional: File chunk sizes if `split_large_files` enabled. (Default: 1000)
  #split_file_max_chunks: 20                      # Optional: Max number of chunks if `split_large_files` enabled. (Default: 20)
  #archive_load_files: False                      # Optional: when enabled, the files loaded to Snowflake will also be stored in `archive_load_files_s3_bucket`
  #archive_load_files_s3_prefix: "archive"        # Optional: When `archive_load_files` is enabled, the archived files will be placed in the archive S3 bucket under this prefix.
  #archive_load_files_s3_bucket: "<BUCKET_NAME>"  # Optional: When `archive_load_files` is enabled, the archived files will be placed in this bucket. (Default: the value of `s3_bucket` in target snowflake YAML)


  # ------------------------------------------------------------------------------
  # Source to target Schema mapping
  # ------------------------------------------------------------------------------
  schemas:

    - source_schema: "my_db"             # Source schema (aka. database) in MySQL/ MariaDB with tables
      target_schema: "repl_my_db"        # Target schema in the destination Data Warehouse
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
    #- source_schema: "another_schema_in_mysql" 
    #  target_schema: "another

