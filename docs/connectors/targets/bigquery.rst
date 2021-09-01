
.. _target-bigquery:

Target Bigquery
----------------


Bigquery setup requirements
''''''''''''''''''''''''''''

.. warning::

  You need to create a few objects in a Bigquery schema before start replicating data to Bigquery:
   * **Existing Google Cloud Platform project**: It needs to have billing enabled and a Bigquery project
   * **Admin permissions**: Ability to create Identity Access Management service accounts

Configuring BigQuery as a replication target is straightforward.
Once you have a user with the permissions to create new tables and schemas
(or the schema was already created). You can start replicating data from
all the supported :ref:`taps_list`.


Configuring where to replicate data
'''''''''''''''''''''''''''''''''''

PipelineWise configures every target with a common structured YAML file format.
A sample YAML for Bigquery target can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for target-bigquery:

.. code-block:: yaml

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "bigquery"                        # Unique identifier of the target
    name: "Bigquery"                      # Name of the target
    type: "target-bigquery"               # !! THIS SHOULD NOT CHANGE !!


    # ------------------------------------------------------------------------------
    # Target - Data Warehouse connection details
    # ------------------------------------------------------------------------------
    db_conn:
      project_id: "<PROJECT_NAME>"                 # Bigquery project name
      dataset_id: "<DATASET_NAME>"                 # Bigquery dataset name
      # Optional: Location/region of your dataset
      location: "<LOCATION_NAME>"                  # Bigquery location of the dataset
