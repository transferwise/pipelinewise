
.. _tap-oracle:

Tap Oracle
----------

.. warning::

    `Oracle Instant Client <https://www.oracle.com/database/technologies/instant-client.html>`_ is
    required to use Tap Oracle. If PipelineWise is :ref:`running_in_docker` then no further
    action needed because  **PipelineWise Docker Image includes Oracle Instant Client**
    automatically.

    If PipelineWise :ref:`building_from_source` then you have to
    **install Oracle Instant Client manually** to your machine.


Oracle setup requirements
'''''''''''''''''''''''''

**Step 1. Create a PipelineWise database user**

You’ll create a dedicated database user for PipelineWise. Create a new user and grant the required permissions
on the database, schema and tables that you want to replicate:

    * ``CREATE USER pipelinewise IDENTIFIEDBY <password>``
    * ``GRANT CONNECT TO pipelinewise``
    * ``GRANT CREATE SESSION TO pipelinewise``
    * ``GRANT UNLIMITED TABLESPACE TO TO pipelinewise``
    * ``GRANT USAGE ON SCHEMA <schema_name> TO pipelinewise``
    * ``GRANT SELECT ON <schema_name>.<table_name> TO pipelinewise`` (Repeat this grant on every table that you want to replicate)


**Step 2: Check if you have all the required credentials for replicating data from Oracle**

Access to ``V$DATABASE`` and ``V_$THREAD`` performance views.
These are required to verify setting configuration while setting up your Oracle database and to
retrieve the database’s Oracle System ID.


**Step 3: Configure Log-based Incremental Replication with LogMiner**

.. note::

  This step is only required if you use :ref:`log_based` replication method.

**Step 3.1: Verify the database's current archiving mode**

To check the database’s current mode, run:

.. code-block:: bash

    SELECT LOG_MODE FROM V$DATABASE


If the result is ``ARCHIVELOG``, archiving is enabled and no further action is required. Skip to Step 3.3 to configure RMAN backups.


**Step 3.2: Enable ARCHIVELOG mode**

1. Shut down the database instance. The database and any associated instances must be shut down before the database’s archiving mode can be changed.

.. code-block:: bash

    SQL> SHUTDOWN IMMEDIATE
    SQL> STARTUP MOUNT
    SQL> ALTER DATABASE ARCHIVELOG
    SQL> ALTER DATABASE OPEN

**Step 3.3: Set retention period by RMAN**

.. code-block:: bash

    RMAN> CONFIGURE RETENTION POLICY TO RECOVERY WINDOW OF 3 DAYS;


**Note**: To ensure that archive log files don’t consume all of your available disk space,
you should also set the ``DB_RECOVERY_FILE_DEST_SIZE`` parameter to a value that agrees with
your available disk quota. Refer to `Oracle's documentation <https://docs.oracle.com/cd/B28359_01/backup.111/b28270/rcmconfb.htm#BRADV89425>`_
for more info about this parameter.


**Step 3.4: Enable supplemental logging**

.. code-block:: bash

    SQL> ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS


**Note**: Alternatively to enable supplemental logging at the table level, run
``ALTER TABLE <SCHEMA_NAME>.<TABLE_NAME> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS``
for every table you want to replicate.


Verify that supplemental logging was successfully enabled by running the following query:

.. code-block:: bash

    SELECT SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE


If the returned value is ``YES`` or ``IMPLICIT``, supplemental logging is enabled.

.. warning::

    If you want to use Log-based Incremental Replication, you’ll also need to
    **grant additional permissions** to the ``pipelinewise`` user:

    * ``GRANT EXECUTE_CATALOG_ROLE TO PIPELINEWISE``

    * ``GRANT SELECT ANY TRANSACTION TO PIPELINEWISE``
    
    * ``GRANT SELECT ANY DICTIONARY TO PIPELINEWISE``
    
    * ``GRANT EXECUTE ON DBMS_LOGMNR TO PIPELINEWISE``
    
    * ``GRANT EXECUTE ON DBMS_LOGMNR_D TO PIPELINEWISE``
    
    * ``GRANT SELECT ON SYS.V_$DATABASE TO PIPELINEWISE``
    
    * ``GRANT SELECT ON SYS.V_$ARCHIVED_LOG TO PIPELINEWISE``
    
    * ``GRANT SELECT ON SYS.V_$LOGMNR_CONTENTS TO PIPELINEWISE``

    **If you’re using version 12 of Oracle**, you’ll also need to grant the
    ``LOGMINING`` privilege to the PipelineWise user:

    * ``GRANT LOGMINING TO PIPELINEWISE``


Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for Oracle replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``tap-oracle``:

.. code-block:: bash

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "oracle_sample"                    # Unique identifier of the tap
    name: "Sample Oracle Database"         # Name of the tap
    type: "tap-oracle"                     # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"              # Data owner to contact


    # ------------------------------------------------------------------------------
    # Source (Tap) - Oracle connection details
    # ------------------------------------------------------------------------------
    db_conn:
      sid: "<SID>"                        # Oracle SID
      host: "<HOST>"                      # Oracle host
      port: 1521                          # Oracle port
      user: "<USER>"                      # Oracle user
      password: "<PASSWORD>"              # Plain string or vault encrypted
      #filter_schemas: "SCHEMA1,SCHEMA2"  # Optional: Scan only the required schemas
                                          #           to improve the performance of
                                          #           data extraction


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

      - source_schema: "SCHEMA1"           # Source schema in Oracle with tables
        target_schema: "repl_oracle"       # Target schema in the destination Data Warehouse
        target_schema_select_permissions:  # Optional: Grant SELECT on schema and tables that created
          - grp_stats

        # List of tables to replicate from Oracle to destination Data Warehouse
        #
        # Please check the Replication Strategies section in the documentation to understand the differences.
        # For LOG_BASED replication method you might need to adjust the source Oracle database.
        tables:
          - table_name: "TABLE_ONE"
            replication_method: "INCREMENTAL"   # One of INCREMENTAL, LOG_BASED and FULL_TABLE
            replication_key: "LAST_UPDATE"      # Important: Incremental load always needs replication key

            # OPTIONAL: Load time transformations
            #transformations:                    
            #  - column: "last_name"            # Column to transform
            #    type: "SET-NULL"               # Transformation type

          # You can add as many tables as you need...
          - table_name: "TABLE_TWO"
            replication_method: "LOG_BASED"     # Important! Log based must be enabled in Oracle

      # You can add as many schemas as you need...
      # Uncommend this if you want replicate tables from multiple schemas
      #- source_schema: "another_schema_in_oracle"
      #  target_schema: "another
