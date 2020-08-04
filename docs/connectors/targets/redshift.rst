
.. _target-redshift:

Target Redshift
---------------


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
      # S3 Profile based authentication
      aws_profile: "<AWS_PROFILE>"                  # AWS profile name, if not provided, the AWS_PROFILE environment variable or the 'default' profile will be used

      # S3 Non-profile based authentication
      #aws_access_key_id: "<ACCESS_KEY>"            # Plain string or vault encrypted. Required for non-profile based auth. If not provided, AWS_ACCESS_KEY_ID environment variable will be used.
      #aws_secret_access_key: "<SECRET_ACCESS_KEY"  # Plain string or vault encrypted. Required for non-profile based auth. If not provided, AWS_SECRET_ACCESS_KEY environment variable will be used.
      #aws_session_token: "<AWS_SESSION_TOKEN>"     # Optional: Plain string or vault encrypted. If not provided, AWS_SESSION_TOKEN environment variable will be used.
      #aws_redshift_copy_role_arn: "<ROLE_ARN>"     # Optional: AWS Role ARN to be used for the Redshift COPY operation.
                                                    #           Allow the user to use environment credentials and delegate the COPY command to a role
                                                    #           Used instead of the given AWS keys for the COPY operation if provided

      s3_bucket: "<BUCKET_NAME>"                    # S3 external bucket name
      s3_key_prefix: "redshift-imports/"            # Optional: S3 key prefix
      #s3_acl: "<S3_OBJECT_ACL>"                    # Optional: Assign the canned ACL to the uploaded file on S3

      # Optional: Overrides the default COPY options to load data into Redshift
      #           The values below are the defaults and fit for purpose for most cases.
      #           Some basic file formatting parameters are fixed values and not
      #           recommended overriding by custom ones.
      #           They are like: CSV GZIP DELIMITER ',' REMOVEQUOTES ESCAPE
      #copy_options: "
      #  EMPTYASNULL BLANKSASNULL TRIMBLANKS TRUNCATECOLUMNS
      #  TIMEFORMAT 'auto'"
