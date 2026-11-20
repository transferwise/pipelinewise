.. _logging:

Logging
========

Each replication run writes a connector log below:

.. code-block:: text

   ~/.pipelinewise/<target_id>/<tap_id>/log/

The filename is
``<target>-<tap>-<YYYYMMDD>_<HHMMSS>.<engine>.log.<status>``.


Filename fields
---------------

.. list-table::
   :header-rows: 1
   :widths: 24 76
   :width: 100%

   * - Field
     - Values
   * - ``engine``
     - ``singer``, ``fastsync``, or ``conversion``.
   * - ``status``
     - ``running``, ``success``, ``failed``, or ``terminated``.
   * - Timestamp
     - UTC run start time as ``YYYYMMDD_HHMMSS``.

FastSync and Singer portions of one ``run_tap`` can write separate files. Keep
both when diagnosing an initial-load failure.

When automatic native-to-Iceberg conversion is enabled, ``run_tap`` first writes
a separate ``.conversion.log.running`` file in the same directory. It records
conversion checks, each table's conversion or resumption, elapsed time, and
failures. Unexpected failures include tracebacks with credentials redacted;
expected check refusals show their reason without a traceback.
The suffix changes to ``success``, ``failed``, or ``terminated`` when
that step ends. These messages are saved even when the scheduler discards
command output. A successful conversion log covers that step; check the later
FastSync or Singer log for the replication result.


Observe a run
-------------

Find and follow the active log:

.. code-block:: bash

   find ~/.pipelinewise/<target_id>/<tap_id>/log -name '*.running' -print
   tail -f ~/.pipelinewise/<target_id>/<tap_id>/log/*.running

Use ``--extra_log`` to mirror connector output to the invoking terminal:

.. code-block:: bash

   pipelinewise run_tap \
     --tap <tap_id> \
     --target <target_id> \
     --extra_log


Diagnose failure
----------------

Collect these together:

1. the complete ``.failed`` or ``.terminated`` log;
2. ``pipelinewise status`` output;
3. the tap and target IDs and connector versions;
4. the last successful log;
5. source and target database errors at the same UTC time; and
6. whether ``state.json`` changed and which bookmark it contains.

Do not remove a ``.running`` file or PID file to make a live pipeline appear
stopped. Use :ref:`cli_stop_tap`, then confirm the process tree has exited.


Retention
---------

PipelineWise does not provide a log-retention policy. Apply filesystem or
platform retention that preserves enough successful and failed history to cover
the maximum investigation and recovery period. Never delete runtime state while
rotating logs.

See :ref:`troubleshooting` for known errors and :ref:`stream_buffering` for
interrupted-run recovery.
