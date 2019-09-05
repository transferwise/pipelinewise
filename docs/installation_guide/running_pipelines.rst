
.. _running_pipelines:

Running Pipelines
=================

First get a list of the available pipelines by running ``pipelinewise status`` command. If you followed the steps at
:ref:`example_replication_mysql_to_snowflake` then you should see this output:

.. code-block:: bash

    $ pipelinewise status
    Tap ID        Tap Type    Target ID    Target Type       Enabled    Status    Last Sync    Last Sync Result
    ------------  ----------  -----------  ----------------  ---------  --------  -----------  ------------------
    mysql_sample  tap-mysql   snowflake    target-snowflake  True       ready                  unknown
    1 pipeline(s)


To run a pipeline use the ``run_tap`` command with ``--tap`` and ``--target`` arguments to specify which pipeline
to run by IDs. In the above example we need to run ``pipelinewise run_tap --tap mysql_sample --target snowflake``:

.. code-block:: bash

    $ pipelinewise run_tap --tap mysql_sample --target snowflake

    2019-08-19 16:52:07 INFO: Running mysql_sample tap in snowflake target
    2019-08-19 16:52:08 INFO: Table(s) selected to sync by fastsync: ['table_one']
    2019-08-19 16:52:08 INFO: Table(s) selected to sync by singer: ['table_two', 'table_three']
    2019-08-19 16:52:08 INFO: Writing output into /app/.pipelinewise/postgres_dwh/mysql_fx/log/postgres_dwh-mysql_fx-20190819_165207.singer.log


The pipeline should start running, it will detect automatically if initial sync or incremental load
is required, when was the last time when it was running and will replicate every change since the last run.
Once it's successfully finished the data is available in the target database and PipelineWise will update
the internal state file with the bookmark for the next run.

.. warning::

  If you :ref:`running_from_docker` then the full path to the log files in the output is
  maybe not correct. Everything that referring to ``/app/.pipelinewise/...`` in the output
  is available on your Docker host at ``${HOME}/.pipelinewise`` directory.

  Read the :ref:`logging` section for further details about logging.

Typically you need to run the above command automatically, every midnight, every hour, every 5 minutes, etc.
Now you can head to the :ref:`scheduling` section that will give you some idea about scheduling.

Alternatively you can learn more about PipelineWise how it automatically performs
schema changes, how it does logging or load time transformations etc. in the recommended sections below:

* :ref:`schema_changes`

* :ref:`encrypting_passwords`

* :ref:`transformations`

* :ref:`logging`

* :ref:`resync`

