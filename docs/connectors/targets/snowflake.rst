
.. _target-snowflake:

Target Snowflake
----------------


Snowflake setup requirements
''''''''''''''''''''''''''''

.. warning::

  You need to create a few objects in a Snowflake schema before start replicating data to Snowflake:
   * **Named External Stage**: to upload the CSV files to S3 and to MERGE data into snowflake tables.
   * **Named File Format**: to run MERGE/COPY commands and to parse the CSV files correctly
   * **A Role**: to grant all the required permissions
   * **A User**: to run PipelineWise

1. Create a named external stage object on S3:

.. code-block:: bash

    CREATE STAGE {database}.{schema}.{stage_name}
    url='s3://{s3_bucket}'
    credentials=(AWS_KEY_ID='{aws_key_id}' AWS_SECRET_KEY='{aws_secret_key}')
    encryption=(MASTER_KEY='{client_side_encryption_master_key}');

**Note**:
 * The ``{schema}`` and ``{stage_name}`` can be any name that Snowflake accepts.
 * The encryption option is optional and used for client side encryption.
   If you want client side encryption  you'll need to define the same master
   key in the ``target-snowflake`` YAML. See the example below.
 * For server side encryption use a different stage definition and grants.
   See :ref:`target_snowflake_sse_kms`.

2. Create a named file format:

.. code-block:: bash

    CREATE FILE FORMAT {database}.{schema}.{file_format_name}
    TYPE = 'CSV' ESCAPE='\\' FIELD_OPTIONALLY_ENCLOSED_BY='"';

3. Create a Role with all the required permissions:

.. code-block:: bash

    CREATE OR REPLACE ROLE ppw_target_snowflake;
    GRANT USAGE ON DATABASE {database} TO ROLE ppw_target_snowflake;
    GRANT CREATE SCHEMA ON DATABASE {database} TO ROLE ppw_target_snowflake;

    GRANT USAGE ON SCHEMA {database}.{schema} TO role ppw_target_snowflake;
    GRANT USAGE ON STAGE {database}.{schema}.{stage_name} TO ROLE ppw_target_snowflake;
    GRANT USAGE ON FILE FORMAT {database}.{schema}.{file_format_name} TO ROLE ppw_target_snowflake;
    GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE ppw_target_snowflake;

Replace ``database``, ``schema``, ``warehouse``, ``stage_name`` and ``file_format_name``
between ``{`` and ``}`` characters to the actual values from point 1 and 2.


4. Create a user and grant permission to the role:

.. code-block:: bash

    CREATE OR REPLACE USER {user}
    PASSWORD = '{password}'
    DEFAULT_ROLE = ppw_target_snowflake
    DEFAULT_WAREHOUSE = '{warehouse}'
    MUST_CHANGE_PASSWORD = FALSE;

    GRANT ROLE ppw_target_snowflake TO USER {user};

Replace ``warehouse`` between ``{`` and ``}`` characters to the actual values from point 3.

Configuring where to replicate data
'''''''''''''''''''''''''''''''''''

PipelineWise configures every target with a common structured YAML file format.
A sample YAML for Snowflake target can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for target-snowflake:

.. code-block:: yaml

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "snowflake"                        # Unique identifier of the target
    name: "Snowflake"                      # Name of the target
    type: "target-snowflake"               # !! THIS SHOULD NOT CHANGE !!


    # ------------------------------------------------------------------------------
    # Target - Data Warehouse connection details
    # ------------------------------------------------------------------------------
    db_conn:
      account: "rtxxxxx.eu-central-1"               # Snowflake account
      dbname: "<DB_NAME>"                           # Snowflake database name
      user: "<USER>"                                # Snowflake user
      private_key: "<private_key_path>"             # File contains PEM format for connecting to Snowflake
      warehouse: "<WAREHOUSE>"                      # Snowflake virtual warehouse
      iceberg_create: false                         # Create new tables as Iceberg tables (only available for pure Singer replications)

      # We use an external stage on S3 to load data into Snowflake
      # S3 Profile based authentication
      aws_profile: "<AWS_PROFILE>"                  # AWS profile name, if not provided, the AWS_PROFILE environment
                                                    # variable or the 'default' profile will be used, if not
                                                    # available, then IAM role attached to the host will be used.

      # S3 Credentials based authentication
      #aws_access_key_id: "<ACCESS_KEY>"            # Plain string or vault encrypted. Required for non-profile based auth. If not provided, AWS_ACCESS_KEY_ID environment variable will be used.
      #aws_secret_access_key: "<SECRET_ACCESS_KEY"  # Plain string or vault encrypted. Required for non-profile based auth. If not provided, AWS_SECRET_ACCESS_KEY environment variable will be used.
      #aws_session_token: "<AWS_SESSION_TOKEN>"     # Optional: Plain string or vault encrypted. If not provided, AWS_SESSION_TOKEN environment variable will be used.

      #aws_endpoint_url: "<FULL_ENDPOINT_URL>"      # Optional: for non AWS S3, for example https://nyc3.digitaloceanspaces.com

      s3_bucket: "<BUCKET_NAME>"                    # S3 external stage bucket name
      s3_key_prefix: "snowflake-imports/"           # Optional: S3 key prefix
      #s3_acl: "<S3_OBJECT_ACL>"                    # Optional: Assign the canned ACL to the uploaded file on S3

      # stage and file_format are pre-created objects in Snowflake that requires to load and
      # merge data correctly from S3 to tables in one step without using temp tables
      #  stage      : External stage object pointing to an S3 bucket
      #  file_format: Named file format object used for bulk loading data from S3 into
      #               snowflake tables.
      stage: "<SCHEMA>.<STAGE_OBJECT_NAME>"
      file_format: "<SCHEMA>.<FILE_FORMAT_OBJECT_NAME>"

      # Optional: Client Side Encryption
      # The same master key has to be added to the external stage object created in snowflake
      #client_side_encryption_master_key: "<MASTER_KEY>" # Plain string or vault encrypted

      # Optional: Server Side Encryption (SSE-KMS)
      # Requires the stage and KMS prerequisites in the section below. Applies to load file
      # uploads and to archived load file copies. Takes precedence over
      # client_side_encryption_master_key: when both are set, files are not client side
      # encrypted, so the stage must not declare AWS_CSE encryption.
      #encryption_type: "KMS"                       # (Default: None) The type of encryption to use. Current supported options are: 'none' and 'KMS'.
      #encryption_key: "<ENCRYPTION_KEY_ID>"        # Optional: The KMS encryption key ID (e.g. '1234abcd-1234-1234-1234-1234abcd1234') or ARN.
                                                    # If omitted, the S3 bucket default KMS key is used.
                                                    # This field is ignored if 'encryption_type' is none or blank.


.. _target_snowflake_sse_kms:

Server side encryption with SSE-KMS
'''''''''''''''''''''''''''''''''''

Setting ``encryption_type: "KMS"`` makes PipelineWise request SSE-KMS on every object it
writes to the S3 external stage bucket: load files uploaded by the Singer target and by
FastSync/PartialSync, and the archived copies created when ``archive_load_files`` is
enabled. It does not change how Snowflake reads those files, so the stage and the KMS key
policy must be prepared before enabling it.

.. warning::

  SSE-KMS and client side encryption are mutually exclusive in practice. When both
  ``encryption_type`` and ``client_side_encryption_master_key`` are set, PipelineWise logs a
  warning, skips client side encryption and uploads the plaintext file with SSE-KMS headers.
  A stage declaring ``AWS_CSE`` then fails to load it. Remove the master key from the YAML
  and recreate the stage as below.

Create the stage with ``AWS_SSE_KMS`` instead of a ``MASTER_KEY``. Using a storage
integration avoids embedding static credentials in the stage definition:

.. code-block:: bash

    CREATE STORAGE INTEGRATION {integration_name}
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    STORAGE_AWS_ROLE_ARN = '{snowflake_iam_role_arn}'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://{s3_bucket}/{s3_key_prefix}');

    GRANT USAGE ON INTEGRATION {integration_name} TO ROLE ppw_target_snowflake;

    CREATE STAGE {database}.{schema}.{stage_name}
    URL = 's3://{s3_bucket}/{s3_key_prefix}'
    STORAGE_INTEGRATION = {integration_name}
    ENCRYPTION = (TYPE = 'AWS_SSE_KMS' KMS_KEY_ID = '{encryption_key}');

    GRANT USAGE ON STAGE {database}.{schema}.{stage_name} TO ROLE ppw_target_snowflake;

Run ``DESC STORAGE INTEGRATION {integration_name}`` to read the
``STORAGE_AWS_IAM_USER_ARN`` and ``STORAGE_AWS_EXTERNAL_ID`` that the integration's IAM
trust policy must allow.

**Note**:
 * ``KMS_KEY_ID`` is only used when unloading; Snowflake ignores it on load and decrypts
   using whichever key each object was written with. Set it so it matches ``encryption_key``
   rather than relying on it for loads.
 * The stage must point at the same bucket and prefix as ``s3_bucket`` and ``s3_key_prefix``.

Grant KMS permissions on the key named in ``encryption_key``, or on the bucket default key
when ``encryption_key`` is omitted:

.. list-table::
   :header-rows: 1
   :widths: 30 30 40

   * - Identity
     - KMS actions
     - Why
   * - The identity PipelineWise uploads with (``aws_profile``, ``aws_access_key_id`` or the
       attached instance role)
     - ``kms:GenerateDataKey``, ``kms:Decrypt``
     - ``GenerateDataKey`` for ``PutObject``; ``Decrypt`` additionally for multipart uploads
       of large load files
   * - The same upload identity, only when ``archive_load_files`` is enabled
     - ``kms:Decrypt`` on the source key, ``kms:GenerateDataKey`` on the destination key
     - ``CopyObject`` decrypts the load file and re-encrypts the archived copy
   * - The storage integration's IAM role, or the stage credentials
     - ``kms:Decrypt``
     - Snowflake decrypts the load files during ``COPY INTO``

.. note::

  PipelineWise sends the same ``encryption_key`` for the archived copy regardless of
  destination, so when ``archive_load_files_s3_bucket`` names a different bucket that key
  must be usable for it. Cross-Region buckets need their own key: an S3 KMS key must be in
  the bucket's Region.

Missing permissions surface as an ``AccessDenied`` on upload, or, when a bucket policy
enforces ``aws:kms``, on the archive copy after the data has already been loaded.


Snowflake Iceberg tables
''''''''''''''''''''''''
Iceberg support needs to be setup in Snowflake
Useful tutorial : https://docs.snowflake.com/en/user-guide/tutorials/create-your-first-iceberg-table

PipelineWise expects the target database to already have default Iceberg settings

.. code-block:: text

    CREATE OR REPLACE EXTERNAL VOLUME ACCOUNT_ICEBERG_VOLUME ... ;
    ALTER DATABASE {target-database} SET CATALOG='snowflake';
    ALTER DATABASE {target-database} SET EXTERNAL_VOLUME = ACCOUNT_ICEBERG_VOLUME;

To create "**new**" tables as Iceberg tables, update target-snowflake yaml to include ``iceberg_create: true``

.. code-block:: yaml

    db_conn:
      account: "rtxxxxx.eu-central-1"               # Snowflake account
      dbname: "<DB_NAME>"                           # Snowflake database name
      user: "<USER>"                                # Snowflake user
      private_key: "<private_key_path>"             # File contains PEM format for connecting to Snowflake
      warehouse: "<WAREHOUSE>"                      # Snowflake virtual warehouse
      iceberg_create: true                          # Create new tables as Iceberg tables (only available for pure Singer replications)

target-snowflake has a utility that can be used to convert an *existing* Native table into an Iceberg table in a PipelineWise compatible manner

.. code-block:: bash

    usage: copy-native-to-iceberg [-h] [-c CONFIG] [-t FQTN] [-e EVENTUAL]

    options:
    -h, --help            show this help message and exit
    -c CONFIG, --config CONFIG
                            target-snowflake config file
    -t FQTN, --fqtn FQTN  Snowflake fully qualified table name (fqtn) in format database.schema.table
    -e EVENTUAL, --eventual EVENTUAL
                            EVENTUAL type of fqtn : NATIVE (Default) or ICEBERG. The other table type will still exist as a copy

Limitations
^^^^^^^^^^^
* Only target-snowflake using the standard Singer replication path (i.e. not :ref:`fast_sync_main`)
  is able to create a new Iceberg table. FastSync and PartialSync do not support Iceberg.
* PipelineWise ``fast_sync`` and ``partial_sync_table`` commands will fail with
  ``(42710): SQL compilation error: table already exists as ICEBERG_TABLE``
