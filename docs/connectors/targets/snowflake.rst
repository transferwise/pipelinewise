
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
      private_key: "<private_key_path>"             # File contains PEM forrmat for connecting to Snowflake
      warehouse: "<WAREHOUSE>"                      # Snowflake virtual warehouse

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

      s3_bucket: "<BUCKET_NAME>"                    # S3 external stbucket name
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
      #client_side_encryption_master_key: "<MASTER_KEY"> # Plain string or vault encrypted
