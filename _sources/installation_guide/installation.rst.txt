
.. _installation_guide:
.. _intro_installation_guide:

Installation
============

Getting PipelineWise
--------------------

PipelineWise installs `Singer.io <https://www.singer.io/>`_  :ref:`taps` and :ref:`targets`
components into separate and multiple separate virtual environments to avoid conflicting
dependencies between any of the components. It is common that multiple Singer components
use the same python library with different versions.

This also means PipelineWise is not distributed on `PyPI <https://pypi.org//>`_ as a pyton package
and cannot be installed by ``pip``. PipelineWise has to be installed from source and the built-in
install script will create every virtual environment required at the right place in the right format
and will take care of finding the required components at run time.

.. _source:

Installing from source
----------------------

.. warning::

  **PipelineWise is not publicly available as of May 2019.**
  TransferWise has not open sourced the project by that time and
  it's not available as a public GitHub repository.

  **Required OS Packages**:
    * Ubuntu: ``apt-get install python3-dev python3-pip python3-venv libpq-dev libsnappy-dev``

    * Mac: ``brew install python``

PipelineWise is easy to run from a checkout - root permissions are not required to use
it and there is no software to actually install. No daemons or database setup are required.

To install from source, clone the PipelineWise git repository:

.. code-block:: bash

    $ git clone https://github.com/transferwise/pipelinewise.git
    $ cd ./pipelinewise

Once git has cloned the PipelineWise repository, run ``install.sh`` to setup the PipelineWise environment.
PipelineWise components are running in multiple python virtual environments and each of them requires different
dependencies. The ``install.sh`` creates the required virtual environments, installs python dependencies
and makes everything ready to use.

.. code-block:: bash

    $ ./install.sh

    (...installing usually takes a couple of minutes...)

    --------------------------------------------------------------------------
    PipelineWise installed successfully
    --------------------------------------------------------------------------

    To start CLI:
      $ source /Users/jack/pipelinewise/.virtualenvs/cli/bin/activate
      $ export PIPELINEWISE_HOME=/Users/jack/pipelinewise/.virtualenvs
      $ pipelinewise status

    --------------------------------------------------------------------------

Once the install script finished, you will need to activate the virtual environment
with the Command Line Tools and set the ``PIPELINEWISE_HOME`` environment variable
as it is displayed above at the end of the install script:

.. code-block:: bash

    $ source /Users/jack/pipelinewise/.virtualenvs/cli/bin/activate
    $ export PIPELINEWISE_HOME=/Users/jack/pipelinewise/.virtualenvs
    $ pipelinewise status

    Warehouse ID    Source ID    Enabled    Type          Status    Last Sync    Last Sync Result
    --------------  -----------  ---------  ------------  --------  -----------  ------------------
    0 pipeline(s)

If you see that above output saying that you have 0 pipeline in the system then the Installation
was successful.

Cool, what's Next?
------------------

From this point, you can go to the :ref:`creating_pipelines` section to create pipelines and to start replicating data.

**Note**: Later if you want to update PipelineWise checkouts to get the latest version from GitHub, use pull-with-rebase
so any local changes are replayed and rerun the install script.

.. code-block:: bash

    $ git pull --rebase
    $ ./install.sh
