
.. _creating_pipelines:

Creating Pipelines
==================

Pipelines define how the data should flow from source sytem to target. It defines the data source
credentials, the data that needs to be captured, replication methods, load time transformations,
destination database credentials, source to target mapping, grants etc.

Pipelines are expressed in YAML format and have a minimum of syntax, which intentionally tries
to not be a programming language or script, but rather a model of a configuration or a process.
PipelineWise is using these YAML files as the main input to generate all the required JSON files
for the underlying singer.io components.

Under the hood `Singer.io <https://www.singer.io/>`_  components need several JSON files to
operate properly, but you will never need to edit these JSON config files directly.
PipelineWise will generate it from the YAML files and install into a correct place automatically
whenever it's needed.


.. _generating_pipelines:

Generating Sample Pipelines
---------------------------

The easiest way to understand these pipeline YAML files is to generate the sample set for each
of the supported connectors, which you can then adjust for your own purposes.

Once you completed the :ref:`installation_guide` section you should be able to create a new
project with the PipelineWise  :ref:`cli_init` command:

.. code-block:: bash

    $ pipelinewise init --dir pipelinewise_samples

This will create a ``pipelinewise_samples`` directory with samples for each supported component:

.. code-block:: bash

    └── pipelinewise_samples
        ├── README.md
        ├── tap_mysql_mariadb.yml.sample
        ├── tap_postgres.yml.sample
        ├── tap_s3_csv.yml.sample
        ├── tap_zendesk.yml.sample
        ├── tap_kafka.yml.sample
        └── target_snowflake.yml.sample

Customising the Pipelines
-------------------------

In this example we will replicate three tables from a MySQL database into a Snowflake Data Warehouse,
using a mix of :ref:`full_table`, :ref:`incremental` and :ref:`log_based` replication methods.
We will need the ``tap_mysql_mariadb.yml`` and ``target_snowflake.yml``:

.. code-block:: bash

    $ cd pipelinewise_samples
    $ mv tap_mysql_mariadb.yml.sample tap_my_mysql_db_one.yml
    $ mv target_snowflake.yml.sample  target_snowflake.yml

1.  Edit ``target_snowflake.yml``. This will be the destination of one or more sources.
You can edit it with the text editor of your choice:

.. code-block:: bash

    ---
    id: "snowflake_test"
    name: "Snowflake Test"
    type: "target-snowflake"
    db_conn:
      account: "rtxxxxxx.eu-central-1"
      dbname: "analytics_db_test"
      user: "circleci"
      password: "PASSWORD"                                   # Plain string or Vault Encrypted password
      warehouse: "LOAD_WH"
      s3_bucket: "tw-staging-analyticsdb-etl"
      s3_key_prefix: "snowflake-imports-test/"
      aws_access_key_id: "ACCESS_KEY_ID"                     # Plain string or Vault Encrypted password
      # stage and file_format are pre-created objects in Snowflake that requires to load and
      # merge data correctly from S3 to tables in one step without using temp tables
      #  stage      : External stage object pointing to an S3 bucket
      #  file_format: Named file format object used for bulk loading data from S3 into
      #               snowflake tables.
      stage: "pipelinewise.encrypted_etl_stage_test"
      file_format: "pipelinewise.etl_stage_file_format"
      aws_secret_access_key: "<SECRET_ASCCESS_KEY>"          # Plain string or Vault Encrypted password
      # The same master key has to be added to the external stage object created in snowflake
      client_side_encryption_master_key: "<CSE_MASTER_KEY>"  # Plain string or Vault Encrypted password


.. note::

  **PipelineWise can encrypt sensitive data** in the YAML files (like database password or other credentials)
  making them safe to distribute or place in source control. For further details, please check the
  :ref:`encrypting_passwords` section.


2) Edit ``tap_mysql_mariadb.yml``:

.. code-block:: bash

    ---
    id: "fx"
    name: "FX (Monolith)"
    type: "tap-mysql"
    owner: "somebody@transferwise.com"
    sync_period: "*/15 * * * *"

    # Source connection details
    db_conn:
      host: "localhost"
      port: 10602
      user: "pgninja_replica"
      password: "<PASSWORD>"                  # Plain string or Vault Encrypted password
      
    target: "snowflake_test"                  # Target ID, should match the id from target_snowflake.yml
    batch_size_rows: 100000                   # Batch size for the stream to optimise load performance

    # Source to Destination Schema mapping
    schemas:
      - source_schema: "fx"                   # You can replicate from multiple schemas
          target_schema: "fx_clear"           # Target schema in snowflake
          target_schema_select_permissions:   # Grant permission once the table created
            - grp_power
          tables:                             # List Tables to replicate
            - table_name: "table_one"
              replication_method: FULL_TABLE  # 1) FULL_TABLE replication
            - table_name: "table_two"         #
              replication_method: LOG_BASED   # 2) LOG_BASED replication
            - table_name: "table_three"       #
              replication_method: INCREMENTAL # 3) INCREMENTAL replication
              replication_key: "updated_at"   #    Incremental load needs replication key


Activating the Pipelines from the YAML files
--------------------------------------------

When you are happy with the configuration you need to import it with the :ref:`cli_import_config` command:

.. code-block:: bash

    $ pipelinewise import_config --dir pipelinewise_samples

At this point PipelineWise will generate the required JSON files for the singer taps and
targets into ``~/.pipelinewise``. PipelineWise will use this directory internally to keep
tracking the state files for :ref:`incremental` and :ref:`log_based` replications
(aka. bookmarks) and this will be the directory where the log files will be created.
Normally you will need to go into ``~/.pipelinewise`` only when you want to access the
log files.

Once the config YAML files are imported, you can see the new pipelines with the :ref:`cli_status` command:

.. code-block:: bash

    $ pipelinewise status
    Tap ID        Tap Type    Target ID    Target Type       Enabled    Status    Last Sync    Last Sync Result
    ------------  ----------  -----------  ----------------  ---------  --------  -----------  ------------------
    mysql_sample  tap-mysql   snowflake    target-snowflake  True       ready                  unknown
    1 pipeline(s)


Congratulations! At this point you have successfully çreated your first pipeline in PipelineWise and it's now
ready to run. You may want you can create a new git repository and push the ``pipelinewise_samples``
directory into it to keep everything under version control.

Now you can head to the :ref:`running_pipelines` section to run the pipelines and to start replicating data.
