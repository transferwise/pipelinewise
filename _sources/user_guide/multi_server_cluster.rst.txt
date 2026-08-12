.. _multi_server_cluster:

Multi-server operation
======================

PipelineWise can run commands from multiple hosts only when every host sees the
same runtime configuration, state, logs, and lock files. PipelineWise does not
provide a distributed scheduler, leader election, or state database for
replication orchestration.


Prerequisites
-------------

Every host needs:

- the same PipelineWise and connector versions;
- identical ``PIPELINEWISE_HOME`` configuration;
- a shared ``PIPELINEWISE_CONFIG_DIRECTORY`` mounted at the same path;
- consistent secret and cloud identity;
- synchronized clocks; and
- network access to all configured sources and targets.

The shared filesystem must provide reliable read-after-write visibility, atomic
rename, and lock/PID-file behaviour. NFS, EFS, and Filestore configurations vary;
validate these semantics with the selected mount options before production use.


Scheduling boundary
-------------------

Use an external scheduler to assign one tap-target pair to one host at a time.
Never start the same pipeline concurrently on two hosts. A PID file prevents a
normal duplicate only when both hosts observe the same lock reliably.

If a host disappears, first prove that its process cannot still write, then
inspect the PID file, active log, and state before rescheduling elsewhere. Do not
delete a lock solely because its originating host is unreachable.


Failure recovery
----------------

After host loss:

1. fence or terminate the old host;
2. confirm shared runtime files are readable and complete;
3. retain the interrupted log and state backup;
4. restart the same tap-target pair on one healthy host; and
5. verify target data and acknowledgement progress.

For PostgreSQL LOG_BASED pipelines, also confirm the replication slot survives
and retained WAL covers the acknowledged LSN. See :ref:`stream_buffering`.
