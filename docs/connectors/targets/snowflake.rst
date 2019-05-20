
.. _target-snowflake:

Target Snowflake
----------------


Snowflake setup requirements
''''''''''''''''''''''''''''

You need to create two objects in Snowflake in a schema before start replicating data:

1. A named external stage object on S3. This will be used to upload the CSV files to S3 and to MERGE data into snowflake tables.

.. code-block:: bash

    CREATE STAGE {schema}.{stage_name}
    url='s3://{s3_bucket}'
    credentials=(AWS_KEY_ID='{aws_key_id}' AWS_SECRET_KEY='{aws_secret_key}')
    encryption=(MASTER_KEY='{client_side_encryption_master_key}');


The encryption option is optional and used for client side encryption.
If you want client side encryption  you'll need to define the same master key in the `target-snowflake` YAML. See the example below.

2. A named file format. This will be used by the MERGE/COPY commands to parse the CSV files correctly from S3:

.. code-block:: bash

    CREATE file format IF NOT EXISTS {schema}.{file_format_name}
    type = 'CSV' escape='\\' field_optionally_enclosed_by='"';


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
      password: "<PASSWORD>"                        # Plain string or vault encrypted
      warehouse: "<WAREHOUSE>"                      # Snowflake virtual warehouse

      # We use an intermediate external stage on S3 to load data into Snowflake
      aws_access_key_id: "<ACCESS_KEY>"             # S3 - Plain string or vault encrypted
      aws_secret_access_key: "<SECRET_ACCESS_KEY>"  # S3 - Plain string or vault encrypted
      s3_bucket: "<BUCKET_NAME>"                    # S3 external stbucket name
      s3_key_prefix: "snowflake-imports/"           # Optional: S3 key prefix

      # stage and file_format are pre-created objects in Snowflake that requires to load and
      # merge data correctly from S3 to tables in one step without using temp tables
      #  stage      : External stage object pointing to an S3 bucket
      #  file_format: Named file format object used for bulk loading data from S3 into
      #               snowflake tables.
      stage: "pipelinewise.encrypted_etl_stage"
      file_format: "pipelinewise.etl_stage_file_format"
      
      # Optional: Client Side Encryption
      # The same master key has to be added to the external stage object created in snowflake
      #client_side_encryption_master_key: "<MASTER_KEY"> # Plain string or vault encrypted
