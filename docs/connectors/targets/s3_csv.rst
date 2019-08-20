
.. _target-s3-csv:

Target S3 CSV
----------------


AWS S3 setup requirements
'''''''''''''''''''''''''

Loading data to S3 in CSV file format is straightforward. You need to have
access to an S3 bucket and you can generate data files on S3 from all the
supported :ref:`taps_list`.

Configuring where to replicate data
'''''''''''''''''''''''''''''''''''

PipelineWise configures every target with a common structured YAML file format.
A sample YAML for S3 CSV target can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``target-s3-csv``:

.. code-block:: bash

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "s3"                               # Unique identifier of the target
    name: "S3 Target connector"            # Name of the target
    type: "target-s3-csv"                  # !! THIS SHOULD NOT CHANGE !!


    # ------------------------------------------------------------------------------
    # Target - S3 details
    # ------------------------------------------------------------------------------
    db_conn:
      aws_access_key_id: "<ACCESS_KEY>"             # S3 - Plain string or vault encrypted
      aws_secret_access_key: "<SECRET_ACCESS_KEY>"  # S3 - Plain string or vault encrypted
      s3_bucket: "<BUCKET_NAME>"                    # S3 external stbucket name

      s3_key_prefix: "pipelinewise-exports/"        # (Default: None) A static prefix before the generated S3 key names
      delimiter: ","                                # (Default: ',') A one-character string used to separate fields.
      quotechar: "\""                               # Default: '\"') A one-character string used to quote fields containing
                                                      special characters, such as the delimiter or quotechar, or which contain
                                                      new-line characters.
