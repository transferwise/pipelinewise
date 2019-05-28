
.. _target-postgres:

Target Postgres
----------------


Postgres setup requirements
''''''''''''''''''''''''''''

.. warning::

  This section of the documentation is work in progress.


Configuring where to replicate data
'''''''''''''''''''''''''''''''''''

PipelineWise configures every target with a common structured YAML file format.
A sample YAML for Postgres target can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for target-postgres:

.. code-block:: bash

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "postgres"                         # Unique identifier of the target
    name: "Postgres"                       # Name of the target
    type: "target-postgres"                # !! THIS SHOULD NOT CHANGE !!


    # ------------------------------------------------------------------------------
    # Target - Data Warehouse connection details
    # ------------------------------------------------------------------------------
    db_conn:
      host: "<HOST>"                       # PostgreSQL host
      port: 5432                           # PostgreSQL port
      user: "<USER>"                       # PostfreSQL user
      password: "<PASSWORD>"               # Plain string or vault encrypted
      dbname: "<DB_NAME>"                  # PostgreSQL database name

