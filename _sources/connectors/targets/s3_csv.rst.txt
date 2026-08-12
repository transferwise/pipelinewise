.. _target-s3-csv:

S3 CSV target
=============

``target-s3-csv`` writes Singer streams as CSV objects in S3.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Target
     - Status
     - Load path
   * - S3 CSV
     - Experimental
     - Singer only; no FastSync or PartialSync


Authentication
--------------

Credential resolution uses an explicit profile, environment credentials, then
the host's IAM role. Prefer temporary role credentials. If static credentials
are unavoidable, encrypt them and rotate them independently of pipeline state.

``aws_profile`` falls back to ``AWS_PROFILE``. ``aws_access_key_id``,
``aws_secret_access_key``, and ``aws_session_token`` fall back to
``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, and ``AWS_SESSION_TOKEN``.
Configure the access-key ID and secret together; the session token is required
only for temporary static credentials. With none configured, Boto3 uses its
default credential chain.


Configuration
-------------

.. code-block:: yaml

   id: "s3_exports"
   name: "S3 CSV exports"
   type: "target-s3-csv"
   db_conn:
     s3_bucket: "analytics-exports"
     s3_key_prefix: "pipelinewise/"
     delimiter: ","
     quotechar: '"'
     encryption_type: "KMS"
     encryption_key: "<KMS_KEY_ID>"

.. list-table:: Connector-specific settings
   :header-rows: 1
   :widths: 28 18 18 36
   :width: 100%

   * - Setting
     - Required
     - Default
     - Effect
   * - ``s3_bucket``
     - Yes
     - —
     - Destination bucket.
   * - ``s3_key_prefix``
     - No
     - None
     - Static prefix for generated object keys.
   * - ``aws_profile``
     - No
     - ``AWS_PROFILE``
     - Selects a named profile when no static key pair is configured.
   * - ``aws_access_key_id`` / ``aws_secret_access_key``
     - No
     - AWS environment
     - Supplies a static credential pair; encrypt both YAML values.
   * - ``aws_session_token``
     - With temporary keys
     - ``AWS_SESSION_TOKEN``
     - Completes temporary static credentials.
   * - ``s3_acl``
     - No
     - None
     - Accepted by the PipelineWise schema but currently ignored by
       ``target-s3-csv``; use bucket ownership and policy instead.
   * - ``delimiter``
     - No
     - ``,``
     - One-character field separator.
   * - ``quotechar``
     - No
     - ``"``
     - Quotes fields containing delimiters, quotes, or newlines.
   * - ``encryption_type``
     - No
     - None
     - Selects no encryption or KMS encryption.
   * - ``encryption_key``
     - With KMS
     - —
     - KMS key identifier.

Before production use, verify object naming, retry duplication, CSV escaping,
encryption, lifecycle retention, and downstream schema handling.
