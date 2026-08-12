.. _tap-kafka:

Kafka source
============

``tap-kafka`` consumes one Kafka topic per tap and emits its messages as a Singer
stream.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Source
     - Status
     - Formats
   * - Kafka
     - Experimental
     - JSON; Protobuf support is also experimental


Configuration
-------------

.. code-block:: yaml

   id: "order_events"
   name: "Order events"
   type: "tap-kafka"
   owner: "data-platform@example.com"
   db_conn:
     group_id: "pipelinewise-orders"
     bootstrap_servers: "kafka1.example.com:9092,kafka2.example.com:9092"
     topic: "order-events"
     initial_start_time: "latest"
     primary_keys:
       order_id: "/order/id"
   target: "snowflake"
   batch_size_rows: 20000
   stream_buffer_size: 0
   default_target_schema: "kafka"
   schemas:
     - source_schema: "kafka"
       target_schema: "kafka"
       tables:
         - table_name: "order_events"

.. list-table:: Connector-specific settings
   :header-rows: 1
   :widths: 28 20 20 32
   :width: 100%

   * - Setting
     - Required
     - Default
     - Effect
   * - ``group_id``
     - Yes
     - —
     - Kafka consumer group identifier.
   * - ``initial_start_time``
     - No
     - ``latest``
     - Initial offset position: ``latest``, ``earliest``, or an ISO-8601 time.
   * - ``primary_keys``
     - No
     - None
     - Maps output key columns to JSON paths.
   * - ``use_message_key``
     - No
     - ``true``
     - Uses the UTF-8 message key when custom primary keys are absent.
   * - ``max_runtime_ms``
     - No
     - ``300000``
     - Bounds one tap invocation.
   * - ``commit_interval_ms``
     - No
     - ``5000``
     - Controls the tap's offset commit cadence.

Test restart behaviour, duplicate handling, consumer-group ownership, schema
changes, and poison messages before production use. Use one tap YAML per topic.
