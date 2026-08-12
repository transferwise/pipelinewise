.. _tap-s3-csv:

S3 CSV source
=============

``tap-s3-csv`` discovers CSV objects in S3 and maps matching object keys to
target tables.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Source
     - Status
     - Important limitation
   * - S3 CSV
     - Experimental
     - Every CSV field is emitted as a string.


Authentication
--------------

Credential resolution follows the AWS profile/environment/instance-role chain.
Prefer an instance role or temporary profile. Static access keys can be supplied
and vault-encrypted but increase secret-management risk.

.. list-table:: AWS credential settings
   :header-rows: 1
   :widths: 34 22 44
   :width: 100%

   * - Setting
     - Environment fallback
     - Behaviour
   * - ``aws_profile``
     - ``AWS_PROFILE``
     - Selects a named profile when no static key pair is configured.
   * - ``aws_access_key_id``
     - ``AWS_ACCESS_KEY_ID``
     - Static access-key ID; configure it together with
       ``aws_secret_access_key``.
   * - ``aws_secret_access_key``
     - ``AWS_SECRET_ACCESS_KEY``
     - Static secret; encrypt it and never commit the clear-text value.
   * - ``aws_session_token``
     - ``AWS_SESSION_TOKEN``
     - Session token required with temporary static credentials.

When none of these settings or environment variables is supplied, Boto3 uses
its default credential chain, including workload and instance roles.


Configuration
-------------

.. code-block:: yaml

   id: "csv_orders"
   name: "Orders CSV feed"
   type: "tap-s3-csv"
   owner: "data-platform@example.com"
   db_conn:
     bucket: "orders-feed"
     start_date: "2024-01-01"
     aws_profile: "pipelinewise"
   target: "snowflake"
   batch_size_rows: 20000
   stream_buffer_size: 0
   schemas:
     - source_schema: "s3_feeds"
       target_schema: "s3_feeds"
       tables:
         - table_name: "orders"
           s3_csv_mapping:
             search_prefix: "orders/"
             search_pattern: "^orders_.*[.]csv$"
             key_properties: ["id"]
             delimiter: ","

.. list-table:: Mapping settings
   :header-rows: 1
   :widths: 28 20 20 32
   :width: 100%

   * - Setting
     - Required
     - Default
     - Effect
   * - ``search_pattern``
     - Yes
     - —
     - Regular expression matched against candidate object keys.
   * - ``search_prefix``
     - No
     - Empty
     - Limits the S3 listing before pattern matching.
   * - ``key_properties``
     - No
     - None
     - Defines fields used to deduplicate records.
   * - ``delimiter``
     - No
     - ``,``
     - Selects the one-character CSV delimiter.

Without key properties, repeated or overlapping files can create duplicates.
Validate object naming, headers, quoting, and malformed-row handling before
enabling a schedule.
