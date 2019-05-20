
.. _installation_guide:
.. _intro_installation_guide:

Installation
============

Getting PipelineWise
--------------------

The easiest way to install the latest stable version of PipelineWise is with ``pip``.
If ``pip`` isn’t already available in your version of Python, you can get pip by:

.. warning::

  **Installing with pip is not available as of May 2019.**
  TransferWise has not open sourced the project by that time and
  it's not available as a public package on PyPI.
   
  For the time being you need to install PipelineWise from source.
  Check :ref:`source` section for further details.

.. code-block:: bash

    $ sudo easy_install pip

Then install PipelineWise with

.. code-block:: bash

    $ sudo pip install pipelinewise

Once PipelineWise installed test things with a status command:

.. code-block:: bash

    $ pipelinewise status

    Warehouse ID    Source ID    Enabled    Type          Status    Last Sync    Last Sync Result
    --------------  -----------  ---------  ------------  --------  -----------  ------------------
    0 pipeline(s)

Cool, from this point, you can head to the :ref:`creating_pipelines` section to create pipelines and to start replicating data.

.. _source:

Running from source
-------------------

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

Later if you want to update PipelineWise checkouts to get the latest version from GitHub, use pull-with-rebase
so any local changes are replayed and rerun the install script.

.. code-block:: bash

    $ git pull --rebase
    $ ./install.sh

Now let’s activate the virtual environment with the Command Line Tools and test things with a status command:

.. code-block:: bash

    $ . .virtualenvs/cli/bin/activate
    $ pipelinewise status

    Warehouse ID    Source ID    Enabled    Type          Status    Last Sync    Last Sync Result
    --------------  -----------  ---------  ------------  --------  -----------  ------------------
    0 pipeline(s)


Cool, what's Next?
------------------

From this point, you can head to the :ref:`creating_pipelines` section to create pipelines and to start replicating data.
