
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
      topic: "myKafkaTopic"


      # --------------------------------------------------------------------------
      # Optionally you can define primary key(s) from the kafka JSON messages.
      # If primary keys defined then extra column(s) will be added to the output
      # singer stream with the extracted values by JSONPath selectors.
      # --------------------------------------------------------------------------
      primary_keys:
         transfer_id: "$.transferMetadata.transferId"

      # --------------------------------------------------------------------------
      # Stop listening the topic if no message after certain milliseconds.
      # Default is 10 seconds
      # --------------------------------------------------------------------------
      #consumer_timeout_ms: 10000


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
          - table_name: "my_kafka_topic"   # target table name needs to match to the topic name in snake case format

            # OPTIONAL: Load time transformations
            #transformations:                    
            #  - column: "last_name"            # Column to transform
            #    type: "SET-NULL"               # Transformation type