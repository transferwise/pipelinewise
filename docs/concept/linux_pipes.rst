.. _linux_pipes:
.. _stream_buffering:

Stream buffering and acknowledgement
====================================

Singer replication connects the tap, optional transform, optional ``mbuffer``,
and target as separate processes:

.. code-block:: text

   tap | transform-field | mbuffer | target

Without ``mbuffer``, operating-system pipe capacity applies backpressure to the
tap. ``stream_buffer_size`` inserts a larger in-memory queue so the tap can read
ahead while the target is temporarily slower.


Configuration
-------------

Set the buffer size in megabytes in the tap YAML:

.. code-block:: yaml

   batch_size_rows: 20000
   stream_buffer_size: 256

``0`` disables ``mbuffer``. Valid configured values are 0–2500 MB; PipelineWise
rounds every non-zero value below 10 up to 10 MB. Reserve the effective memory
in addition to tap, transform, target, compression, and database-client memory.
A larger buffer absorbs a longer slowdown but does not increase target
throughput.


Acknowledgement boundary
------------------------

The tap can consume records ahead of the target. Data in ``mbuffer`` or target
memory is not durable target state. PipelineWise therefore persists the state
emitted by the target, not the latest position merely read by the tap.

For PostgreSQL LOG_BASED replication:

.. code-block:: text

   consumed LSN >= safe target-acknowledged LSN
   slot confirmed_flush_lsn <= safe target-acknowledged LSN

Feedback may advance to the minimum acknowledged LSN across logical streams, but
never directly to the latest consumed LSN. Missing, temporarily unreadable,
truncated, invalid, or regressing state retains the previous monotonic safe LSN
rather than trusting the current file contents.


Termination and restart
-----------------------

``pipelinewise stop_tap`` sends termination through the managed process tree.
The tap stops producing records and the target is given an opportunity to finish
records already received. A forced kill, host loss, or out-of-memory termination
can discard buffered records.

After an unexpected termination:

1. do not edit or advance ``state.json``;
2. confirm the PostgreSQL replication slot still exists;
3. restart the same tap-target pair; and
4. verify exact target keys or a deterministic reconciliation, not row count
   alone.

Unacknowledged WAL is replayed while the slot and required WAL remain available.
Targets can receive duplicates around the acknowledgement boundary and must keep
their primary-key merge semantics.


Operational guidance
---------------------

- Increase the buffer only after identifying target backpressure in logs and
  database telemetry.
- Monitor process memory, target latency, retained PostgreSQL WAL, and end-to-end
  lag together.
- Treat a continuously full buffer as a sustained throughput deficit, not a
  sizing problem.
- Keep source-log retention longer than the maximum detection and recovery time.
- Use :ref:`data_diff` or an exact key/checksum comparison after interruption.
