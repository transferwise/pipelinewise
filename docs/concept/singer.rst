.. _singer:

Singer data flow
================

PipelineWise uses the `Singer specification
<https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md>`_ as the
record protocol between source taps and targets:

.. code-block:: text

   tap | optional transform-field | optional mbuffer | target

FastSync bypasses this protocol for supported bulk-transfer routes. See
:ref:`fast_sync_main`.


Messages
--------

.. list-table::
   :header-rows: 1
   :widths: 24 38 38
   :width: 100%

   * - Message
     - Producer
     - Purpose
   * - ``SCHEMA``
     - Tap
     - Describes stream fields, types, keys, and schema changes.
   * - ``RECORD``
     - Tap
     - Carries one source record or delete event.
   * - ``STATE``
     - Tap and target
     - Carries replication bookmarks and target acknowledgement.


.. _taps:

Taps
----

A tap reads connector configuration, a selected catalog, and optional prior
state. It writes ordered Singer messages to standard output. PipelineWise owns
the generated JSON files; operators configure the source through YAML and should
not edit generated catalog or state while a tap is running.

See :ref:`taps_list` for source status and connector-specific requirements.


.. _targets:

Targets
-------

A target consumes Singer messages, creates or evolves destination tables, loads
records, and emits acknowledged state. PipelineWise persists target-emitted
state only after the target process has accepted the corresponding data.

``target-snowflake`` can create and load managed Iceberg v3 through this Singer
path for any compatible tap. That capability does not add a FastSync component;
managed-v3 FastSync FullSync and PartialSync remain limited to MariaDB/MySQL
and PostgreSQL sources.

For both native and managed Iceberg v3 tables, ``target-snowflake`` declares
every newly created or added string column as ``VARCHAR(134217728)``. It leaves
a compatible existing native string column at its current width, but requires
an existing managed-v3 string column to have the exact maximum width before
writing. See :ref:`target-snowflake` for target behavior and
:ref:`snowflake_iceberg` for the managed-v3 compatibility contract.

Singer interoperability does not guarantee that every packaged tap-target pair
has production support. Use :ref:`connector_support` for endpoint and route
status.


Acknowledgement boundary
------------------------

The source's latest consumed position can be ahead of the state durably accepted
by the target. PipelineWise treats the target-emitted state as the recovery
boundary. Buffered or in-memory records beyond that boundary must remain
replayable after interruption.

For PostgreSQL LOG_BASED replication, the logical slot's flush feedback is
bounded by the minimum acknowledged LSN in ``state.json``. See
:ref:`stream_buffering` for termination and replay behaviour.
