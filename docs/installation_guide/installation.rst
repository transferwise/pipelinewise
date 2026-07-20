
.. _installation_guide:
.. _intro_installation_guide:

Installation
============

Getting PipelineWise
--------------------

PipelineWise source code is available on GitHub at https://github.com/transferwise/pipelinewise
and can be installed in the following methods:

* :ref:`running_in_docker`: Running a containerised docker executable image
  that is isolated from your host system.

* :ref:`building_from_source` Building from source code directly to your host system.

.. warning::

    PipelineWise is a collection of pre-selected and customised :ref:`singer` components
    with a config management and runtime framework on top of it. When installing PipelineWise
    you will also install a bunch of Singer components into a well defined location.

    PipelineWise installs `Singer.io <https://www.singer.io/>`_  :ref:`taps` and :ref:`targets`
    components into multiple virtual environments to avoid conflicting dependencies between
    any of the components. It is common that multiple Singer components use the same python
    library with different versions.

    This also means **PipelineWise is not distributed on** `PyPI <https://pypi.org//>`_ as a Python package
    and cannot be installed by ``pip``. PipelineWise **can run from Docker** or can be
    **installed from source**. In both cases the build and install scripts will create all the
    virtual environments at the right place in the right format and will take care of finding them
    at runtime.


.. _running_in_docker:

Running in Docker
-----------------

Running PipelineWise from docker is usually the easiest and the recommended method. We will
build an executable docker image that has every required dependency and is isolated from
your host system. First you need to install docker on your computer:

* `Install Docker on Linux <https://docs.docker.com/engine/install/>`_

* `Install Docker on MacOS <https://docs.docker.com/desktop/setup/install/mac-install/>`_

Once Docker is installed you need to clone the PipelineWise git repository and build the
executable Docker image:

.. code-block:: bash

    $ git clone https://github.com/transferwise/pipelinewise.git
    $ cd ./pipelinewise
    $ docker build -t pipelinewise:latest .


Building the image may take 5-10 minutes depending on your network connection. The output image will
contain every supported Singer connector. Alternatively, see `Official Docker Images <https://github.com/transferwise/pipelinewise?tab=readme-ov-file#official-docker-images>`_
to pull a pre-built image. 

Once the image is ready, create an alias to the Docker wrapper script so you can
use the ``pipelinewise`` executable commands everywhere on your system:

.. code-block:: bash

    $ alias pipelinewise="$(pwd)/bin/pipelinewise-docker"


Check if the installation was successful by running the ``pipelinewise status`` command:

.. code-block:: bash

    $ pipelinewise status

    Tap ID    Tap Type      Target ID     Target Type      Enabled    Status    Last Sync    Last Sync Result
    --------  ------------  ------------  ---------------  ---------  --------  -----------  ------------------
    0 pipeline(s)

From this point, you can go to the :ref:`creating_pipelines` section to create pipelines and to start replicating data.


.. _building_from_source:

Installing from source
----------------------

PipelineWise requires Python 3.12. Before building from source, install Python 3.12
and the operating-system dependencies used by the connectors you select. For example:

* Ubuntu: ``apt-get install python3.12-dev python3-pip python3.12-venv``

* macOS: ``brew install python@3.12``

Clone the PipelineWise git repository and run the install script that installs the
PipelineWise CLI and every supported singer connectors into separated virtual environments:

.. code-block:: bash

    $ git clone https://github.com/transferwise/pipelinewise.git
    $ cd ./pipelinewise
    $ make pipelinewise all_connectors

Press ``Y`` to accept the license agreement of the required singer components.

.. code-block:: bash

    $ make pipelinewise all_connectors

    (...installation usually takes 5-10 minutes...)

    --------------------------------------------------------------------------
    PipelineWise installed successfully
    --------------------------------------------------------------------------

    To start CLI:
      $ source /Users/jack/pipelinewise/.virtualenvs/pipelinewise/bin/activate
      $ export PIPELINEWISE_HOME=/Users/jack/pipelinewise
      $ pipelinewise status

    --------------------------------------------------------------------------

.. _selecting_singer_connectors:

Selecting singer connectors
'''''''''''''''''''''''''''

You can install only the connectors you need by setting the ``pw_connector``
Make variable. For example, to replicate data from MySQL and PostgreSQL into
Snowflake, run:

.. code-block:: bash

    $ make pipelinewise connectors -e pw_connector=tap-mysql,tap-postgres,target-snowflake

.. warning::

    Adding components may overwrite the default Apache2 License Version 2.0 terms and conditions.
    Please always double check license compatibilities and terms of the installed components.
    More info in the :ref:`licenses` section.


The current ``all_connectors`` set is listed below. Install one connector with
``make pipelinewise connectors -e pw_connector=<connector>`` or pass a
comma-separated list as shown above.

.. list-table:: Connectors installed by ``all_connectors``
   :header-rows: 1

   * - Sources (taps)
     - Destinations and transformations
   * - ``tap-github``
     - ``target-postgres``
   * - ``tap-jira``
     - ``target-s3-csv``
   * - ``tap-kafka``
     - ``target-snowflake``
   * - ``tap-mixpanel``
     - ``transform-field``
   * - ``tap-mongodb``
     -
   * - ``tap-mysql``
     -
   * - ``tap-postgres``
     -
   * - ``tap-s3-csv``
     -
   * - ``tap-salesforce``
     -
   * - ``tap-slack``
     -
   * - ``tap-snowflake``
     -
   * - ``tap-twilio``
     -
   * - ``tap-zendesk``
     -

Once the install script finished, you will need to activate the virtual environment
with the Command Line Tools and set the ``PIPELINEWISE_HOME`` environment variable
as it is displayed above at the end of the install script:

.. code-block:: bash

    $ source /Users/jack/pipelinewise/.virtualenvs/pipelinewise/bin/activate
    $ export PIPELINEWISE_HOME=/Users/jack/pipelinewise
    $ pipelinewise status

    Tap ID    Tap Type    Target ID    Target Type    Enabled    Status    Last Sync    Last Sync Result
    --------  ----------  -----------  -------------  ---------  --------  -----------  ------------------
    0 pipeline(s)

If you see that above output saying that you have 0 pipeline in the system then the Installation
was successful.

Cool, what's Next?
------------------

From this point, you can go to the :ref:`creating_pipelines` section to create pipelines and to start replicating data.
