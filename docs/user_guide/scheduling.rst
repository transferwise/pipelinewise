.. _scheduling:

Scheduling
==========

PipelineWise does not include a scheduler. Any scheduler that can run a command,
preserve its exit status, and keep the runtime directory available can trigger a
pipeline.


Requirements
------------

The scheduler must:

- run with the same ``PIPELINEWISE_HOME`` and
  ``PIPELINEWISE_CONFIG_DIRECTORY`` used during import;
- provide source, target, secret, and cloud credentials;
- prevent concurrent execution of the same tap-target pair;
- retain standard output, standard error, and the PipelineWise run log;
- treat a non-zero exit as failure; and
- allow a run to finish or terminate it through ``pipelinewise stop_tap``.

PipelineWise also uses a per-pipeline PID file and refuses a second local
instance. Do not rely on that as a distributed scheduler lock without validating
the shared filesystem's locking semantics.


Example
-------

Run separate pipelines independently:

.. code-block:: text

   */5 * * * * pipelinewise run_tap --tap orders --target snowflake
   15 * * * * pipelinewise run_tap --tap ledger --target snowflake

Redirecting output is optional when the scheduler captures it. PipelineWise still
writes its connector logs under ``~/.pipelinewise``.


Cadence and overlap
-------------------

Choose a cadence longer than normal run duration or configure the scheduler to
skip overlap. A backlog that makes every run reach the next scheduled start is a
throughput or source/target availability problem; increasing frequency makes it
worse.

For LOG_BASED sources, source-log retention must exceed the maximum time between
the last target acknowledgement and successful recovery. For data-diff, schedule
``run_data_diff_checks`` independently from replication and leave enough
``window_end`` lag for the target to settle.


Retries and recovery
--------------------

Retry the same command with unchanged state after a transient failure. Do not
advance bookmarks as a retry mechanism. Limit automatic retry frequency so a
credential, schema, or database outage does not create an alert or connection
storm.

After repeated failure, stop retries, retain evidence, correct the cause, and
verify target contents after the next success. See :ref:`troubleshooting`.
