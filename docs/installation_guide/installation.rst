
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
build an executable docker images that has every required dependency and it's isolated from
your host system. First you need to install docker on your computer:

* `Install Docker on Linux <https://runnable.com/docker/install-docker-on-linux>`_

* `Install Docker on MacOS <https://runnable.com/docker/install-docker-on-macos>`_

Once Docker is installed you need to clone the PipelineWise git repository and build the
executable docker image:

.. code-block:: bash

    $ git clone https://github.com/transferwise/pipelinewise.git
    $ cd ./pipelinewise
    $ docker build -t pipelinewise:latest .


Building the image may take 5-10 minutes depending on your network connection. The output image will
contain every supporter singer connectors. At the moment there is no official, pre-built image available
to download on DockerHub. Once the image is ready, create an alias to the docker wrapper script so you can
use the ``pipelinewise`` executable commands everywhere on your system:

.. code-block:: bash

    $ alias pipelinewise="$(pwd)/bin/pipelinewise-docker"


Check if the installation was successfully by running the ``pipelinewise status`` command:

.. code-block:: bash

    $ pipelinewise status

    Tap ID    Tap Type      Target ID     Target Type      Enabled    Status    Last Sync    Last Sync Result
    --------  ------------  ------------  ---------------  ---------  --------  -----------  ------------------
    0 pipeline(s)

From this point, you can go to the :ref:`creating_pipelines` section to create pipelines and to start replicating data.


.. _building_from_source:

Installing from source
----------------------

When building PipelineWise from source make sure that every OS dependencies are installed:

* Ubuntu: ``apt-get install python3-dev python3-pip python3-venv``

* MacOS: ``brew install python``

Clone the PipelineWise git repository and run the install script that installs the
PipelineWise CLI and every supported singer connectors into separated virtual environments:

.. code-block:: bash

    $ git clone https://github.com/transferwise/pipelinewise.git
    $ cd ./pipelinewise
    $ ./install.sh --connectors=all

Press ``Y`` to accept the license agreement of the required singer components. To automate
the installation and accept every license agreement run ``./install --acceptlicenses``.

.. code-block:: bash

    $ ./install.sh --connectors=all

    (...installation usually takes 5-10 minutes...)

    --------------------------------------------------------------------------
    PipelineWise installed successfully
    --------------------------------------------------------------------------

    To start CLI:
      $ source /Users/jack/pipelinewise/.virtualenvs/cli/bin/activate
      $ export PIPELINEWISE_HOME=/Users/jack/pipelinewise/.virtualenvs
      $ pipelinewise status

    --------------------------------------------------------------------------

Selecting singer connectors
'''''''''''''''''''''''''''

You can install PipelineWise only with required connectors by using the `--connectors=` argument. For example if you
need to replicate data only from MySQL and PostgreSQL into a Snowflake database you can install PipelineWise by
running:

.. code-block:: bash

    $ ./install --connectors=tap-mysql,tap-postgres,target-snowflake

Here’s the list of the singer connectors and if they are installed by default or not:

+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| **Connector**              | **Install Command**                     | **Included in default install?** | **Note**                              |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| all                        | ./install --connectors=all              |                                  | Installs every supported connector    |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| tap-adwords                | ./install --connectors=tap-adwords      |                                  |                                       |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| tap-jira                   | ./install --connectors=tap-jira         | YES                              |                                       |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| tap-kafka                  | ./install --connectors=tap-kafka        | YES                              |                                       |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| tap-mysql                  | ./install --connectors=tap-mysql        | YES                              |                                       |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| tap-oracle                 | ./install --connectors=tap-oracle       |                                  |                                       |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| tap-postgres               | ./install --connectors=tap-postgres     | YES                              |                                       |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| tap-s3-csv                 | ./install --connectors=tap-s3-csv       | YES                              |                                       |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| tap-salesforce             | ./install --connectors=tap-salesforce   | YES                              |                                       |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| tap-snowflake              | ./install --connectors=tap-snowflake    | YES                              |                                       |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| tap-zendesk                | ./install --connectors=tap-zendesk      | YES                              |                                       |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| target-postgres            | ./install --connectors=target-postgres  |                                  |                                       |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| target-s3-csv              | ./install --connectors=target-s3-csv    | YES                              |                                       |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| target-redshift            | ./install --connectors=target-redshift  | YES                              |                                       |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| target-snowflake           | ./install --connectors=target-snowflake | YES                              |                                       |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+
| transform-field            | ./install --connectors=transform-field  | YES                              |                                       |
+----------------------------+-----------------------------------------+----------------------------------+---------------------------------------+


.. warning::

    When `--connectors=` argument is not specified then only the default connectors will be installed.

Once the install script finished, you will need to activate the virtual environment
with the Command Line Tools and set the ``PIPELINEWISE_HOME`` environment variable
as it is displayed above at the end of the install script:

.. code-block:: bash

    $ source /Users/jack/pipelinewise/.virtualenvs/cli/bin/activate
    $ export PIPELINEWISE_HOME=/Users/jack/pipelinewise/.virtualenvs
    $ pipelinewise status

    Tap ID    Tap Type    Target ID    Target Type    Enabled    Status    Last Sync    Last Sync Result
    --------  ----------  -----------  -------------  ---------  --------  -----------  ------------------
    0 pipeline(s)

If you see that above output saying that you have 0 pipeline in the system then the Installation
was successful.

Cool, what's Next?
------------------

From this point, you can go to the :ref:`creating_pipelines` section to create pipelines and to start replicating data.
