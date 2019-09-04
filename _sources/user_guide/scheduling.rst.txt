
.. _scheduling:

Scheduling
----------

Scheduling and running PipelineWise tasks automatically is not part of the PipelineWise
package but any task scheduler that can run Unix CLI commands can trigger PipelineWise
jobs to run. Both Single Server and :ref:`multi_server_cluster` installations are achievable.


Let's say you have 5 microservice databases that you want to replicate to Amazon Redshift
and ``pipelinewise status`` output looks like this:

.. code-block:: bash

    $ pipelinewise status

    Tap ID        Tap Type     Target ID   Target Type       Enabled    Status    Last Sync    Last Sync Result
    ------------  ----------   ----------  ----------------  ---------  --------  -----------  ------------------
    microserv_1   tap-mysql    redshift    target-redshift   True       ready                  unknown
    microserv_2   tap-mysql    redshift    target-redshift   True       ready                  unknown
    microserv_3   tap-postgres redshift    target-redshift   True       ready                  unknown
    microserv_4   tap-postgres redshift    target-redshift   True       ready                  unknown
    microserv_5   tap-postgres redshift    target-redshift   True       ready                  unknown
    5 pipeline(s)


Since every pipeline runs, logs and manages state files independently, you'll need to run
5 commands independently. For example if using
`Unix Cron <https://en.wikipedia.org/wiki/Cron/>`_ you can create the following crontab:

.. code-block:: bash

   */5 *   * * * pipelinewise run_tap --tap microserv_1 --target redshift # Sync every 5 minutes
     0 *   * * * pipelinewise run_tap --tap microserv_2 --target redshift # Sync ever hour
     0 */3 * * * pipelinewise run_tap --tap microserv_3 --target redshift # Sync every three hours
     0 0   * * * pipelinewise run_tap --tap microserv_4 --target redshift # Sync every midnight
     0 0   * * 6 pipelinewise run_tap --tap microserv_5 --target redshift # Sync every Saturday


PipelineWise is tested and can run with at least the following
schedulers:

* `Unix Cron <https://en.wikipedia.org/wiki/Cron/>`_ Unix Cron - This is the simplest option
  for a single server installation. 

* `Cronicle <https://github.com/jhuckaby/Cronicle/>`_ - Cronicle is a reasonably good and
  relatively simple tool to schedule PipelineWise jobs in both Single Server and Multi-Server
  cluster installations.
  
* `Apache Airflow <https://airflow.apache.org/>`_ - Airflow is a robust and mature tool to
  schedule and monitor workflows.

.. _multi_server_cluster:

Multi-Server Cluster
--------------------

Running Multi-Server Cluster requires a `Network File System <https://en.wikipedia.org/wiki/Network_File_System>`_
that is accessible from every host in the PipelineWise cluster.
(`Amazon EFS <https://aws.amazon.com/efs/>`_, `Google FileStore <https://cloud.google.com/filestore/>`_ or similar)

Network File System is required because PipelineWise keeps runtime configuration files in
a common place on the host machine at ``${HOME}/.pipelinewise`` directory. If you run
PipelineWise commands on multiple nodes that operate on the same project, then
every node has to read/write into the same directory, doesn't matter where the nodes are
located. This is typically done by mounting ``${HOME}/.pipelinewise`` on every node to
a shared directory on NFS/EFS.

.. warning::

  There are plans to store ``${HOME}/.pipelinewise`` runtime configuration files
  optionally on different data stores, like S3, RDBMs or on document stores like
  Couchbase or MongoDB. Once it is implemented, Multi-Server Cluster installation
  will not require NFS/EFS.

