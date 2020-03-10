
.. _logging:

Logging
-------

PipelineWise is generating log files at ``~/.pipelinewise/<TARGET_ID>/<TAP_ID>/log``
from every run started by the ``run_tap`` command. Please check :ref:`running_pipelines`
section for further details how to run tap when you do a data sync.

Every log file will match the following pattern: ``<TARGET_ID>-<TAP_ID>-<DATE>_<TIME>.<SYNC_ENGINE>.log.<STATUS>``

Variables:

  * **TARGET_ID**: Unique identifier of the target. This is the ``id`` property from a :ref:`targets_list` YAML.
  * **TAP_ID**: Unique identifier of the tap. This is the ``id`` property from a :ref:`taps_list` YAML.
  * **DATE**: Date when the tap started in ``YYYYMMDD`` format
  * **TIME**: Time when the tap started in ``HH24MMSS`` format
  * **SYNC_ENGINE**: One of ``singer`` or ``fastsync``. Check :ref:`reproduction_methods` and :ref:`fast_sync_main` section for further details.
  * **STATUS**: One of ``running``, ``failed`` or ``success``


Example:

.. code-block:: bash

    $ ls -lah ~/.pipelinewise/snowflake/fx/log/
    -rw-rw-r-- 1 pipelinewise pipelinewise  13K May  8 00:02 snowflake-fx-20190508_000038.singer.log.success
    -rw-rw-r-- 1 pipelinewise pipelinewise  13K May  8 00:31 snowflake-fx-20190508_003014.singer.log.success
    -rw-rw-r-- 1 pipelinewise pipelinewise  13K May  8 01:04 snowflake-fx-20190508_010031.singer.log.success
    -rw-rw-r-- 1 pipelinewise pipelinewise  13K May  8 01:33 snowflake-fx-20190508_013036.singer.log.success
    -rw-rw-r-- 1 pipelinewise pipelinewise  13K May  8 02:02 snowflake-fx-20190508_020037.singer.log.success
    -rw-rw-r-- 1 pipelinewise pipelinewise  13K May  8 02:32 snowflake-fx-20190508_023029.singer.log.success
    -rw-rw-r-- 1 pipelinewise pipelinewise  13K May  8 03:02 snowflake-fx-20190508_030032.singer.log.success
    -rw-rw-r-- 1 pipelinewise pipelinewise  13K May  8 03:31 snowflake-fx-20190508_033031.singer.log.success
    -rw-rw-r-- 1 pipelinewise pipelinewise  13K May  8 04:02 snowflake-fx-20190508_040033.fastsync.log.success
    -rw-rw-r-- 1 pipelinewise pipelinewise  13K May  8 04:04 snowflake-fx-20190508_040222.singer.log.success
    -rw-rw-r-- 1 pipelinewise pipelinewise  13K May  8 04:30 snowflake-fx-20190508_043015.singer.log.success
    -rw-rw-r-- 1 pipelinewise pipelinewise  13K May  8 05:01 snowflake-fx-20190508_050035.singer.log.success
    -rw-rw-r-- 1 pipelinewise pipelinewise  13K May  8 05:31 snowflake-fx-20190508_053030.singer.log.failed
    -rw-rw-r-- 1 pipelinewise pipelinewise  13K May  8 06:02 snowflake-fx-20190508_060037.singer.log.success
    -rw-rw-r-- 1 pipelinewise pipelinewise  13K May  8 06:33 snowflake-fx-20190508_063032.singer.log.success
    -rw-rw-r-- 1 pipelinewise pipelinewise   8K May  8 07:03 snowflake-fx-20190508_070036.singer.log.running


