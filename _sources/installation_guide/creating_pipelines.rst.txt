.. _creating_pipelines:

Create a pipeline
=================

A PipelineWise project contains global configuration plus one YAML file per
source and target. ``import_config`` validates these files, discovers source
schemas, and generates the JSON, catalog, and state files used at runtime.

This walkthrough configures the available PostgreSQL-to-Snowflake route with
``INCREMENTAL`` replication. See :ref:`tap-postgres` and
:ref:`target-snowflake` for production prerequisites.


.. _generating_pipelines:

Generate a project
------------------

.. code-block:: bash

    pipelinewise init --name pipelinewise_samples
    cd pipelinewise_samples
    mv tap_postgres.yml.sample tap_postgres.yml
    mv target_snowflake.yml.sample target_snowflake.yml

``pipelinewise init`` also writes templates for experimental and legacy
connectors. A template is not evidence that its connector is packaged or
available; check :ref:`connector_support`.


Configure Snowflake
-------------------

Keep only the required target settings in ``target_snowflake.yml``:

.. code-block:: yaml

    id: "snowflake"
    name: "Analytics Snowflake"
    type: "target-snowflake"
    db_conn:
      account: "<ACCOUNT>"
      dbname: "<DATABASE>"
      user: "<USER>"
      private_key: "/run/secrets/snowflake-key.pem"
      warehouse: "<WAREHOUSE>"
      s3_bucket: "<STAGING_BUCKET>"
      s3_key_prefix: "pipelinewise/"
      stage: "<SCHEMA>.<STAGE>"
      file_format: "<SCHEMA>.<FILE_FORMAT>"

The stage, file format, role, and user must exist before the first run. Use an
instance role or AWS profile where possible; avoid committing static credentials.


Configure PostgreSQL
--------------------

Edit ``tap_postgres.yml``. The source user needs metadata access and ``SELECT``
on the configured table. ``INCREMENTAL`` requires a stable replication key.

.. code-block:: yaml

    id: "orders"
    name: "Orders PostgreSQL"
    type: "tap-postgres"
    owner: "data-platform@example.com"
    db_conn:
      host: "<HOST>"
      port: 5432
      user: "<USER>"
      password: "{{ env_var['POSTGRES_PASSWORD'] }}"
      dbname: "<DATABASE>"
    target: "snowflake"
    batch_size_rows: 20000
    stream_buffer_size: 0
    schemas:
      - source_schema: "public"
        target_schema: "repl_orders"
        tables:
          - table_name: "orders"
            replication_method: "INCREMENTAL"
            replication_key: "updated_at"

The ``target`` value must equal the target YAML ``id``. For LOG_BASED
replication, configure logical decoding before importing; see
:ref:`tap-postgres`.


.. _passing_environment_variables_via_jinja:

Provide secrets
---------------

Jinja expressions read environment variables when PipelineWise loads YAML:

.. code-block:: yaml

    password: "{{ env_var['POSTGRES_PASSWORD'] }}"

Alternatively, encrypt values with Ansible Vault as described in
:ref:`encrypting_passwords`. Keep plaintext secrets, vault password files, and
private keys outside source control.


.. _import_project_from_yaml:

Validate and import
-------------------

Run validation before PipelineWise connects to the source:

.. code-block:: bash

    pipelinewise validate --dir .
    pipelinewise import_config --dir .
    pipelinewise status

``validate`` checks YAML structure and references but does not test database
connectivity. ``import_config`` performs discovery and writes runtime files below
``~/.pipelinewise/<target_id>/<tap_id>/``. Commit the project YAML, not the
generated runtime directory.

Continue with :ref:`running_pipelines` after the imported pipeline reports
``ready``.
