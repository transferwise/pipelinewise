
.. _tap-mongodb:

Tap MongoDB
-----------


MongoDB setup requirements
''''''''''''''''''''''''''

**Step 1: Check if you have all the required credentials for replicating data from MongoDB**

* The user must have one of the following roles: ``read``, ``readWrite``, ``readAnyDatabase``, ``readWriteAnyDatabase``, ``dbOwner``, ``backup``, ``root``. These roles allow PipelineWise to see and read from the dbs to sync from.

* If privileges are set, the user must have at least these two actions: ``find`` and ``changeStream``. These actions are necessary because they're the actions that PipelineWise performs while syncing.


**Step 2: Required database server settings**

.. note::

  This step is only required if you use :ref:`log_based` replication method.


.. warning::

  To use log_based replication, your MongoDB server must be running MongoDB version 3.6 or greater, is either a replica set or sharded cluster and majority read concern is enabled.

  The ``log_based`` replication makes use of ChangeStreams that were introduced in version 3.6, for more info on ChangeStreams, head over to `the official documentation <https://docs.mongodb.com/manual/changeStreams/>`_.


**Step 3. Create a PipelineWise database user**

Next, you’ll create a dedicated user for PipelineWise. The user needs to have:

* One of the roles ``read``, ``readWrite``, ``readAnyDatabase``, ``readWriteAnyDatabase``, ``dbOwner``, ``backup``, ``root`` on the database that you want to replicate
* ``find`` & ``changeStream`` privileges on the every collection that you want to replicate.

Example:


.. code-block:: js

	db.createRole({

		"role" : "PipelineWiseRole",
		"privileges" : [{

			"resource" :{

				"db" : "my_db",
				"collection" : "my_collection"

			},
			"actions" : ["find", "changeStream"]

		}],
		"roles" : [{"role": "read", "db": "my_db"}]

	});

	db.createUser({

		"user" : "PipelineWiseUser",
		"pwd": "mY_VerY_StRonG_PaSSwoRd",
		"roles" : ["PipelineWiseRole"]

	});


Configuring what to replicate
'''''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for MongoDB replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``tap-mongodb``:

.. code-block:: yaml

    ---
    id: "tap_mongo"                    # Unique identifier of the tap
    name: "MongoDB tap"                 # Human-readable name of the tap
    type: "tap-mongodb"                 # Connector type; do not change
    owner: "foo@bar.com"                # Data owner to contact
    #send_alert: false                   # Optional: Disable configured alerts for this tap
    #slack_alert_channel: "#tap-channel" # Optional: Send a copy of alerts to this Slack channel

    db_conn:
      host: "mongodb_host1,mongodb_host2,mongodb_host3" # Comma-separated MongoDB hosts
      port: 27017                       # MongoDB port
      srv: "false"                       # Use "true" for MongoDB Atlas; port is then ignored
      user: "PipelineWiseUser"           # User with permission to read the source collections
      password: "<PASSWORD>"             # Plain string or Vault encrypted
      auth_database: "admin"              # Database against which the user authenticates
      dbname: "my_db"                     # MongoDB database to replicate
      replica_set: "my_replica_set"       # Optional: Replica set name; default is null
      #write_batch_rows: 50000             # Optional: Rows written to a CSV batch; default 50000
      #update_buffer_size: 1               # Optional, LOG_BASED: Buffered update operations; default 1
      #await_time_ms: 1000                 # Optional, LOG_BASED: Wait for changes before exit; default 1000 ms
      #fastsync_parallelism: 4             # Optional: FastSync process pool size; defaults to CPU count

    target: "my_target"                   # ID of the target connector
    batch_size_rows: 1000                 # Rows sent to the target in each batch
    stream_buffer_size: 0                 # In-memory tap-to-target buffer size in MB
    default_target_schema: "my_db"         # Optional: Default destination schema
    #default_target_schema_select_permissions: # Optional: Groups granted SELECT access
    #  - grp_power
    #batch_wait_limit_seconds: 3600        # Optional, Snowflake: Flush a partial batch after this time

    # Options only for the Snowflake target
    #split_large_files: false                       # Split large files into multipart ZIP files
    #split_file_chunk_size_mb: 1000                 # Chunk size when split_large_files is enabled
    #split_file_max_chunks: 20                      # Maximum chunks when split_large_files is enabled
    #archive_load_files: false                      # Store loaded files in an archive S3 bucket
    #archive_load_files_s3_prefix: "archive"        # Prefix within the archive bucket
    #archive_load_files_s3_bucket: "<BUCKET_NAME>"  # Archive bucket; defaults to the target S3 bucket

    schemas:
      - source_schema: "my_db"           # Must match dbname
        target_schema: "repl_my_db"       # Destination schema
        target_schema_select_permissions: # Optional: Groups granted SELECT access
          - grp_stats

        # MongoDB supports FULL_TABLE and LOG_BASED; LOG_BASED is the default.
        tables:
          - table_name: "my_collection"
            replication_method: "FULL_TABLE"

            # Optional load-time transformations
            #transformations:
            #  - column: "last_name"
            #    type: "SET-NULL"

          - table_name: "my_other_collection"
            replication_method: "LOG_BASED"


Example connection to MongoDB Atlas
"""""""""""""""""""""""""""""""""""

.. code-block:: yaml

    db_conn:
      srv: "true"
      host: "xxxxxxxxx.xxxxx.mongodb.net"
      auth_database: "admin"             # Database used for authentication
      dbname: "db-name"                  # Database to replicate
      user: "user-name"
      password: "<PASSWORD>"             # Plain string or Vault encrypted
