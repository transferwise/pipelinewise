.. _running_pipelines:

Run a pipeline
==============

Run one imported tap-target pair at a time. PipelineWise selects FastSync for an
eligible initial load and the Singer path for ongoing incremental or log-based
replication.


Preflight
---------

Check configuration, connectivity, and imported status before starting a load:

.. code-block:: bash

    pipelinewise validate --dir ./pipelinewise_samples
    pipelinewise test_tap_connection --tap orders --target snowflake
    pipelinewise status

Re-run ``import_config`` after changing project YAML. Validation alone does not
update generated runtime configuration.


Start and observe
-----------------

.. code-block:: bash

    pipelinewise run_tap --tap orders --target snowflake

During the run, PipelineWise:

1. locks the tap-target pair to prevent a concurrent run;
2. selects FastSync-eligible initial tables and Singer tables;
3. streams or stages records into the target;
4. persists only target-acknowledged state; and
5. marks the log ``success`` or ``failed``.

Follow the active log from another terminal:

.. code-block:: bash

    tail -f ~/.pipelinewise/snowflake/orders/log/*.running

See :ref:`logging` for filenames and diagnostic collection.


Stop safely
-----------

Use the CLI rather than killing an arbitrary child process:

.. code-block:: bash

    pipelinewise stop_tap --tap orders --target snowflake

The tap stops producing records and the target is allowed to finish data it has
already received. For PostgreSQL LOG_BASED replication, WAL feedback remains
bounded by target-acknowledged state. An unexpected termination can replay
records after restart; targets must therefore retain their normal primary-key
merge semantics.


Confirm success
---------------

After the command exits successfully:

.. code-block:: bash

    pipelinewise status

Confirm the target data as well as the PipelineWise status. For critical tables,
configure :ref:`data_diff` rather than treating a successful process exit as
proof of source-to-target equality.


Recover from failure
--------------------

Do not advance or edit state merely to make a failed run start. Instead:

1. retain the failed log and exact error;
2. correct the source, target, credential, or configuration problem;
3. restart the same ``run_tap`` command; and
4. verify target contents and the new acknowledged state.

Use :ref:`troubleshooting` for known errors. Use :ref:`resync` only when the
required source log or replication slot is no longer available, or when target
data must be rebuilt deliberately.

Schedule the command only after an interactive run succeeds; see
:ref:`scheduling`.
