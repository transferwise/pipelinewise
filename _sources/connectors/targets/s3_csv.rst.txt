
.. _target-s3-csv:

Target S3 CSV
----------------


Loading data to S3 in CSV file format is straightforward. You need to have
access to an S3 bucket and you can generate data files on S3 from all the
supported :ref:`taps_list`.


.. warning::

  **Authentication Methods**

   * **Profile based authentication**: This is the default authentication method. Credentials taken from
     the ``default`` AWS profile, that's available on the host where PipelineWise is running.
     To use anoter profile set the ``aws_profile`` parameter.
   * **Non-profile based authentication**: To provide fixed credentials set ``aws_access_key_id``,
     ``aws_secret_access_key`` and optionally the ``aws_session_token`` parameters.

     Optionally the credentials can be vault-encrypted in the YAML. Please check :ref:`encrypting_passwords`
     for further details.

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
      # Profile based authentication
      aws_profile: "<AWS_PROFILE>"                  # AWS profile name, if not provided, the AWS_PROFILE environment variable or the 'default' profile will be used

      # Non-profile based authentication
      #aws_access_key_id: "<ACCESS_KEY>"            # Plain string or vault encrypted. Required for non-profile based auth. If not provided, AWS_ACCESS_KEY_ID environment variable will be used.
      #aws_secret_access_key: "<SECRET_ACCESS_KEY"  # Plain string or vault encrypted. Required for non-profile based auth. If not provided, AWS_SECRET_ACCESS_KEY environment variable will be used.
      #aws_session_token: "<AWS_SESSION_TOKEN>"     # Optional: Plain string or vault encrypted. If not provided, AWS_SESSION_TOKEN environment variable will be used.

      s3_bucket: "<BUCKET_NAME>"                     # S3 bucket name

      s3_key_prefix: "pipelinewise-exports/"         # (Default: None) A static prefix before the generated S3 key names
      delimiter: ","                                 # (Default: ',') A one-character string used to separate fields.
      quotechar: "\""                                # Default: '\"') A one-character string used to quote fields containing
                                                       special characters, such as the delimiter or quotechar, or which contain
                                                       new-line characters.
