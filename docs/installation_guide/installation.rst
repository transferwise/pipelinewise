.. _installation_guide:
.. _intro_installation_guide:

Installation
============

PipelineWise requires Linux-compatible connector tooling and Python 3.12.
Running the published application image is the recommended installation method;
it isolates connector dependencies in the same environment used at runtime.


Choose an installation method
-----------------------------

.. list-table::
   :header-rows: 1
   :widths: 24 38 38
   :width: 100%

   * - Method
     - Use when
     - Operational consequence
   * - Docker
     - The host can run Docker.
     - Connector and system dependencies are isolated from the host.
   * - Source installation
     - Docker is unavailable or the image must be customised.
     - The operator owns Python, system-library, and connector dependency
       compatibility.


.. _running_in_docker:

Docker installation
-------------------

Install `Docker Engine <https://docs.docker.com/engine/install/>`_ or
`Docker Desktop <https://docs.docker.com/desktop/>`_, then pull the published
full image. Pin a release tag instead of ``latest`` in production.

.. code-block:: bash

    git clone https://github.com/transferwise/pipelinewise.git
    cd pipelinewise
    docker pull transferwiseworkspace/pipelinewise:latest
    docker tag transferwiseworkspace/pipelinewise:latest pipelinewise:latest

The local tag is required because ``bin/pipelinewise-docker`` runs
``pipelinewise:latest``. Published variants are:

.. list-table:: Published images
   :header-rows: 1
   :widths: 48 52
   :width: 100%

   * - Image
     - Contents
   * - ``transferwiseworkspace/pipelinewise:<version>``
     - PipelineWise with every packaged connector.
   * - ``transferwiseworkspace/pipelinewise:<version>-default``
     - PipelineWise with only the default connector subset.
   * - ``transferwiseworkspace/pipelinewise-barebone:<version>``
     - PipelineWise without pre-installed connector environments.

Packaging experimental connectors does not change their support level; see
:ref:`connector_support`.

Use the wrapper to mount the current project and ``~/.pipelinewise`` into each
short-lived container:

.. code-block:: bash

    alias pipelinewise="$(pwd)/bin/pipelinewise-docker"
    pipelinewise status

A successful empty installation prints ``0 pipeline(s)``. The wrapper persists
generated configuration, state, and logs below ``~/.pipelinewise`` on the host.

To customise system packages or connector selection, build a local image
instead of pulling one:

.. code-block:: bash

    docker build -t pipelinewise:latest .


.. _building_from_source:

Source installation
-------------------

Install Python 3.12, ``venv``, ``gettext``/``envsubst``, and the operating-system
libraries required by the selected connectors. Then install the CLI and only the
connectors needed by the pipeline:

.. code-block:: bash

    git clone https://github.com/transferwise/pipelinewise.git
    cd pipelinewise
    make pipelinewise
    make connectors -e pw_connector=tap-postgres,target-snowflake

PipelineWise creates an isolated virtual environment per component below
``${PIPELINEWISE_HOME}/.virtualenvs``. Activate the CLI environment before use:

.. code-block:: bash

    export PIPELINEWISE_HOME="$(pwd)"
    source .virtualenvs/pipelinewise/bin/activate
    pipelinewise status

Do not install the root project with ``pip`` as a replacement for the Makefile
workflow; that does not create connector environments.


.. _selecting_singer_connectors:

Packaged connectors
-------------------

``make all_connectors`` and the default Docker build package the following
components. Install a subset with a comma-separated ``pw_connector`` value.

.. list-table:: Sources
   :header-rows: 1
   :widths: 38 24 38
   :width: 100%

   * - Connector
     - Status
     - Source
   * - ``tap-mysql``
     - Available / Experimental
     - MariaDB / MySQL
   * - ``tap-postgres``
     - Available
     - PostgreSQL
   * - ``tap-snowflake``
     - Experimental
     - Snowflake
   * - ``tap-github``
     - Experimental
     - GitHub
   * - ``tap-jira``
     - Experimental
     - Jira
   * - ``tap-kafka``
     - Experimental
     - Kafka
   * - ``tap-mixpanel``
     - Experimental
     - Mixpanel
   * - ``tap-mongodb``
     - Experimental
     - MongoDB
   * - ``tap-s3-csv``
     - Experimental
     - S3 CSV
   * - ``tap-salesforce``
     - Experimental
     - Salesforce
   * - ``tap-slack``
     - Experimental
     - Slack
   * - ``tap-twilio``
     - Experimental
     - Twilio
   * - ``tap-yugabyte``
     - Experimental
     - YugabyteDB
   * - ``tap-zendesk``
     - Experimental
     - Zendesk

.. list-table:: Targets and transformations
   :header-rows: 1
   :widths: 38 24 38
   :width: 100%

   * - Component
     - Status
     - Purpose
   * - ``target-postgres``
     - Available
     - Load Singer records into PostgreSQL.
   * - ``target-snowflake``
     - Available
     - Load Singer records into Snowflake.
   * - ``target-s3-csv``
     - Experimental
     - Write Singer records as CSV files in S3.
   * - ``transform-field``
     - Available
     - Apply configured load-time transformations.

.. warning::

   Connector licenses can differ from the PipelineWise license. Review
   :ref:`licenses` before distributing an image or installing a connector.

Continue with :ref:`creating_pipelines` after ``pipelinewise status`` succeeds.
