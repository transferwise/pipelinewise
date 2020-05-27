
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
.. code-block:: javascript

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


	# ------------------------------------------------------------------------------
	# Source (Tap) - Mongo connection details
	# ------------------------------------------------------------------------------
	db_conn:
		host: "mongodb_source_host1,mongodb_source_host2,mongodb_source_host3"    # Mongodb host(s)
		port: 27017                           									# Mongodb port
		user: "PipelineWiseUser"                  								# Mongodb user
		password: "mY_VerY_StRonG_PaSSwoRd"                 						# Mongodb plain string or vault encrypted
		auth_database: "admin"            										# Mongodb database to authenticate on
		dbname: "my_db"           												# Mongodb database name to sync from
		replica_set: "my_replica_set"        										# Optional, Mongodb replica set name, default null

	# ------------------------------------------------------------------------------
	# Destination (Target) - Target properties
	# Connection details should be in the relevant target YAML file
	# ------------------------------------------------------------------------------
	target: "my_target"                   			# ID of the target connector where the data will be loaded
	batch_size_rows: 1000                  			# Batch size for the stream to optimise load performance

	# ------------------------------------------------------------------------------
	# Source to target Schema mapping
	# ------------------------------------------------------------------------------
	schemas:
	  	- source_schema: "my_db"						# Same name as dbname
		  target_schema: "ppw_e2e_tap_mongodb"		# Name of target schema to load to

		  # List of collections to sync
		  tables:
			- table_name: "my_collection"
			  replication_method: "FULL_TABLE"

		  	# default replication method is LOG_BASED
		  	- table_name: "my_other_collection"
