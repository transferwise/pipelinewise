
.. _tap-kafka:

Tap Kafka
---------

Messages from kafka topics are extracted into the following fields:

* ``MESSAGE_TIMESTAMP``: Timestamp extracted from the kafka metadata
* ``MESSAGE_OFFSET``: Offset extracted from the kafka metadata
* ``MESSAGE_PARTITION``: Partition extracted from the kafka metadata
* ``MESSAGE``: The original and full kafka message
* `Dynamic primary key columns`: (Optional) Fields extracted from the Kafka JSON messages by JSONPath selector(s).

Consuming Kafka messages
''''''''''''''''''''''''

Tap Kafka saves consumed messages into a local disk storage and sends commit messages to Kafka after every
consumed message. A batching mechanism keeps maintaining of deleting and flushing messages from the local storage
and sends singer compatible messages in small batches to standard output.


Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for Kafka replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``tap-kafka``:

.. code-block:: yaml

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "kafka"                            # Unique identifier of the tap
    name: "Kafka Topic with sample data"   # Name of the tap
    type: "tap-kafka"                      # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"              # Data owner to contact
    #send_alert: False                     # Optional: Disable all configured alerts on this tap


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
      # singer stream with the extracted values by /slashed/paths ala XPath selectors.
      # --------------------------------------------------------------------------
      primary_keys:
         transfer_id: "/transferMetadata/transferId"

      # --------------------------------------------------------------------------
      # Kafka Consumer optional parameters
      # --------------------------------------------------------------------------
      #max_runtime_ms: 300000                   # The maximum time for the tap to collect new messages from Kafka topic.
      #consumer_timeout_ms: 10000               # KafkaConsumer setting. Number of milliseconds to block during message iteration before raising StopIteration
      #session_timeout_ms: 30000                # KafkaConsumer setting. The timeout used to detect failures when using Kafka’s group management facilities.
      #heartbeat_interval_ms: 10000             # KafkaConsumer setting. The expected time in milliseconds between heartbeats to the consumer coordinator when using Kafka’s group management facilities.
      #max_poll_interval_ms: 300000             # KafkaConsumer setting. The maximum delay between invocations of poll() when using consumer group management.
      #max_poll_records: 500                    # KafkaConsumer setting. The maximum number of records returned in a single call to poll().

      #commit_interval_ms: 5000                 # Number of milliseconds between two commits. This is different than the kafka auto commit feature. Tap-kafka sends commit messages automatically but only when the data consumed successfully and persisted to local store.
      #local_store_dir: ./tap-kafka-local-store # Path to the local store with consumed kafka messages
      #local_store_batch_size_rows: 1000        # Number of messages to write to disk in one go. This can avoid high I/O issues when messages written to local store disk too frequently.


    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"                       # ID of the target connector where the data will be loaded
    batch_size_rows: 20000                    # Batch size for the stream to optimise load performance
    stream_buffer_size: 0                     # In-memory buffer size (MB) between taps and targets for asynchronous data pipes
    default_target_schema: "kafka"            # Target schema where the data will be loaded
    default_target_schema_select_permission:  # Optional: Grant SELECT on schema and tables that created
      - grp_stats
    #batch_wait_limit_seconds: 3600           # Optional: Maximum time to wait for `batch_size_rows`. Available only for snowflake target.


    # ------------------------------------------------------------------------------
    # Source to target Schema mapping
    # ------------------------------------------------------------------------------
    schemas:
      - source_schema: "kafka"             # This is mandatory, but can be anything in this tap type
        target_schema: "kafka"             # Target schema in the destination Data Warehouse

        # Kafka topic to replicate into destination Data Warehouse
        # You can load data only from one kafka topic in one YAML file.
        # If you want load from multiple kafka topics, create another tap YAML similar to this file
        tables:
          - table_name: "my_kafka_topic"   # target table name needs to match to the topic name in snake case format

            # OPTIONAL: Load time transformations
            #transformations:
            #  - column: "last_name"            # Column to transform
            #    type: "SET-NULL"               # Transformation type