
.. _target-postgres:

Target Postgres
----------------

Postgres setup requirements
''''''''''''''''''''''''''''

Configuring PostgreSQL as a reproduction target is straightforward.
You need to have a user with permissions to create new schemas and
tables in a Postgres database and you can reproduce data from all the
supported :ref:`taps_list`.

Configuring where to reproduce data
'''''''''''''''''''''''''''''''''''

PipelineWise configures every target with a common structured YAML file format.
A sample YAML for Postgres target can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``target-postgres``:

.. code-block:: bash

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "postgres_dwh"                     # Unique identifier of the target
    name: "Postgres Data Warehouse"        # Name of the target
    type: "target-postgres"                # !! THIS SHOULD NOT CHANGE !!


    # ------------------------------------------------------------------------------
    # Target - Data Warehouse connection details
    # ------------------------------------------------------------------------------
    db_conn:
      host: "<HOST>"                       # PostgreSQL host
      port: 5432                           # PostgreSQL port
      user: "<USER>"                       # PostgreSQL user
      password: "<PASSWORD>"               # Plain string or vault encrypted
      dbname: "<DB_NAME>"                  # PostgreSQL database name

