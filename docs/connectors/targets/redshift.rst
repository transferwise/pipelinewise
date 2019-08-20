
.. _target-redshift:

Target Redshift
---------------

Amazon Redshift setup requirements
''''''''''''''''''''''''''''''''''

Configuring PostgreSQL as a replication target is straightforward.
You need to have a user with permissions to create new schemas and
tables in an Redshift database and you can replicate data from all the
supported :ref:`taps_list`.

Configuring where to replicate data
'''''''''''''''''''''''''''''''''''

PipelineWise configures every target with a common structured YAML file format.
A sample YAML for Redshift target can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for target-redshift:

.. code-block:: bash

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "redshift"                        # Unique identifier of the target
    name: "Amazon Redshift"               # Name of the target
    type: "target-redshift"               # !! THIS SHOULD NOT CHANGE !!


    # ------------------------------------------------------------------------------
    # Target - Data Warehouse connection details
    # ------------------------------------------------------------------------------
    db_conn:
      host: "xxxxx.redshift.amazonaws.com"          # Redshift host
      port: 5439                                    # Redshift port
      user: "<USER>"                                # Redshift user
      password: "<PASSWORD>"                        # Plain string or vault encrypted
      dbname: "<DB_NAME>"                           # Redshift database name

      # We use an intermediate S3 to load data into Redshift
      aws_access_key_id: "<ACCESS_KEY>"             # S3 - Plain string or vault encrypted
      aws_secret_access_key: "<SECRET_ACCESS_KEY>"  # S3 - Plain string or vault encrypted
      s3_bucket: "<BUCKET_NAME>"                    # S3 external bucket name
      s3_key_prefix: "redshift-imports/"            # Optional: S3 key prefix
