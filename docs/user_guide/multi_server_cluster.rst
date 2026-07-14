
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
