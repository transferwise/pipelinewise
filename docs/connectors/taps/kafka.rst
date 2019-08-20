
.. _tap-kafka:

Tap Kafka
---------


Connecting to Kafka
'''''''''''''''''''

.. warning::

  This section of the documentation is work in progress.


Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for Kafka replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``tap-kafka``:

.. code-block:: bash

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "kafka"                            # Unique identifier of the tap
    name: "Kafka Topic with sample data"   # Name of the tap
    type: "tap-kafka"                      # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"              # Data owner to contact


    # ------------------------------------------------------------------------------
    # Source (Tap) - Kafka connection details
    # ------------------------------------------------------------------------------
    db_conn:
      group_id: "myGroupId"
      bootstrap_servers: "kafka1.foo.com:9092,kafka2.foo.com:9092,kafka3.foo.com:9092"
      topic: "myTopic"

      # --------------------------------------------------------------------------
      # SCHEMA is a standard JSON Schema document that used for multiple purposes:
      #
      # 1. Validating kafka messages read from the stream
      # 2. Creating destination table in Snowflake:
      #        - Column names are flattened, using the '__' characters to
      #          separate multi-level objects
      #        - Snowflake column types will be generated from JSON schema types
      #
      #
      # Sample Kafka message extracted from profileBehaviourStats topic:
      # (The JSON Schema needs to be tailored for this sample JSON message)
      #
      # {
      #   "eventTime":1550564993738,
      #   "startingState":"NEW",
      #   "ltvValueInGbp":1701.36,
      #   "ltvValueInEur":1919.7188620000002,
      #   "transferMetadata":{
      #     "transferId":63432435,
      #     "profileId":7623199,
      #     "userId":10523356,
      #     "currentState":"NEW",
      #     "sourceCurrency":"EUR",
      #     "targetCurrency":"EUR",
      #     "invoiceValue":50.0,
      #     "invoiceValueInGbp":43.73,
      #     "invoiceValueInEur":50.0,
      #     "submitTime":1550564993000
      #   }
      #  }
      # --------------------------------------------------------------------------
      schema: '
          {
            "properties": {
              "eventTime": {"type": ["number", "null"]},
              "startingState": {"type": ["string", "null"]},
              "ltvValueInGbp": {"type": ["number", "null"]},
              "ltvValueInEur": {"type": ["number", "null"]},
              "transferMetadata": {
                "type": "object",
                "properties": {
                  "transferId": {"type": "integer"},
                  "profileId": {"type": ["integer", "null"]},
                  "userId": {"type": ["integer", "null"]},
                  "currentState": {"type": ["string", "null"]},
                  "sourceCurrency": {"type": ["string", "null"]},
                  "targetCurrency": {"type": ["string", "null"]},
                  "invoiceValue": {"type": ["number", "null"]},
                  "invoiceValueInGbp": {"type": ["number", "null"]},
                  "invoiceValueInEur": {"type": ["number", "null"]},
                  "submitTime": {"type": ["integer", "null"]}
                }
              }
            }
          }'

      # --------------------------------------------------------------------------
      # One field from the kafka message will be the Primary Key of the target
      # table. Selecting primary key is mandatory
      # --------------------------------------------------------------------------
      primary_keys: '["transferMetadata__transferId"]'

      consumer_timeout_ms: 5000


    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"                       # ID of the target connector where the data will be loaded
    batch_size_rows: 20000                    # Batch size for the stream to optimise load performance
    default_target_schema: "kafka"            # Target schema where the data will be loaded 
    default_target_schema_select_permission:  # Optional: Grant SELECT on schema and tables that created
      - grp_stats


    # ------------------------------------------------------------------------------
    # Source to target Schema mapping
    # ------------------------------------------------------------------------------
    schemas:
      - source_schema: "kafka"             # This is mandatory, but can be anything in this tap type
        target_schema: "kafka"             # Target schema in the destination Data Warehouse

        # Kafka topic to replicate into destination Data Warehouse
        # You can load data only from one kafka topic in one YAML file.
        # If you want load from multiple kafka topics, create another tap YAML similar to this file
        tables:
          - table_name: "kafka_topic"

            # OPTIONAL: Load time transformations
            #transformations:                    
            #  - column: "last_name"            # Column to transform
            #    type: "SET-NULL"               # Transformation type