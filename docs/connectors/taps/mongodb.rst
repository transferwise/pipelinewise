
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

.. code-block:: bash

	---

	# ------------------------------------------------------------------------------
	# General Properties
	# ------------------------------------------------------------------------------
	id: "tap_mongo"
	name: "MongoDB tap"
	type: "tap-mongodb"
	owner: "foo@bar.com"
	#send_alert: False                     # Optional: Disable all configured alerts on this tap


	# ------------------------------------------------------------------------------
	# Source (Tap) - Mongo connection details
	# ------------------------------------------------------------------------------
	db_conn:
		host: "mongodb_host1,mongodb_host2,mongodb_host3" 	# Mongodb host(s)
		port: 27017                           				# Mongodb port
		user: "PipelineWiseUser"                  			# Mongodb user
		password: "mY_VerY_StRonG_PaSSwoRd"                 # Mongodb plain string or vault encrypted
		auth_database: "admin"            					# Mongodb database to authenticate on
		dbname: "my_db"           							# Mongodb database name to sync from
		replica_set: "my_replica_set"        				# Optional, Mongodb replica set name, default null
  		write_batch_rows: <int>								# Optional: Number of rows to write to csv file
                                       						#           in one batch. Default is 50000.
        update_buffer_size: <int> 						    # Optional: [LOG_BASED] The size of the buffer that holds detected update
                                                            #           operations in memory, the buffer is flushed once the size is reached. Default is 1.
        await_time_ms: <int>								# Optional: [LOG_BASED] The maximum amount of time in milliseconds
                                                            #           the loge_base method waits for new data changes before exiting. Default is 1000 ms.
        fastsync_parallelism: <int>                         # Optional: size of multiprocessing pool used by FastSync
                                                            #           Min: 1
                                                            #           Default: number of CPU cores
	# ------------------------------------------------------------------------------
	# Destination (Target) - Target properties
	# Connection details should be in the relevant target YAML file
	# ------------------------------------------------------------------------------
	target: "my_target"                   			# ID of the target connector where the data will be loaded
	batch_size_rows: 1000                  			# Batch size for the stream to optimise load performance
	stream_buffer_size: 0                           # In-memory buffer size (MB) between taps and targets for asynchronous data pipes
	#batch_wait_limit_seconds: 3600                 # Optional: Maximum time to wait for `batch_size_rows`. Available only for snowflake target.

    # Options only for Snowflake target
    #split_large_files: False                       # Optional: split large files to multiple pieces and create multipart zip files. (Default: False)
    #split_file_chunk_size_mb: 1000                 # Optional: File chunk sizes if `split_large_files` enabled. (Default: 1000)
    #split_file_max_chunks: 20                      # Optional: Max number of chunks if `split_large_files` enabled. (Default: 20)
    #archive_load_files: False                      # Optional: when enabled, the files loaded to Snowflake will also be stored in `archive_load_files_s3_bucket`
    #archive_load_files_s3_prefix: "archive"        # Optional: When `archive_load_files` is enabled, the archived files will be placed in the archive S3 bucket under this prefix.
    #archive_load_files_s3_bucket: "<BUCKET_NAME>"  # Optional: When `archive_load_files` is enabled, the archived files will be placed in this bucket. (Default: the value of `s3_bucket` in target snowflake YAML)


	# ------------------------------------------------------------------------------
	# Source to target Schema mapping
	# ------------------------------------------------------------------------------
	schemas:
	  	- source_schema: "my_db"					# Same name as dbname
		  target_schema: "ppw_e2e_tap_mongodb"		# Name of target schema to load to

		  # List of collections to sync
		  tables:
			- table_name: "my_collection"
			  replication_method: "FULL_TABLE"

		  	# default replication method is LOG_BASED
		  	- table_name: "my_other_collection"
