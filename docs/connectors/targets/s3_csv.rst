
.. _target-s3-csv:

Target S3 CSV
----------------


Configuring
'''''''''''

PipelineWise configures every target with a common structured YAML file format.
A sample YAML for Snowflake target can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for target-s3-csv:

.. code-block:: yaml

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "csv_to_s3"                        # Unique identifier of the target
    name: "CSV exports to S3"              # Name of the target
    type: "target-s3-csv"                  # !! THIS SHOULD NOT CHANGE !!


    # ------------------------------------------------------------------------------
    # Target - S3 details
    # ------------------------------------------------------------------------------
    db_conn:
      aws_access_key_id: "<ACCESS_KEY>"             # S3 - Plain string or vault encrypted
      aws_secret_access_key: "<SECRET_ACCESS_KEY>"  # S3 - Plain string or vault encrypted
      s3_bucket: "<BUCKET_NAME>"                    # S3 external stbucket name

      s3_key_prefix: "pipelinewise-exports/"        # Optional S3 key prefix. Defaults to None
      delimiter: ","                                # Optional. Defaults to ','
      quotechar: "\""                               # Optional. Defaults to '"'
