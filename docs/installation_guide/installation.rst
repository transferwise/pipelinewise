
.. _installation_guide:
.. _intro_installation_guide:

Installation
============

Getting PipelineWise
''''''''''''''''''''

The easiest way to install the latest stable version of PipelineWise is with ``pip``:

.. code-block:: bash

    pip install pipelinewise

.. warning::

  **Installing with pip is not available as of May 2019.**
  TransferWise has not open sourced the project by that time and
  it's not available as a public package on PyPI.
   
  For the time being you need to install PipelineWise from source.
  Check :ref:`source` section for further details.

.. _source:

Installing from source
''''''''''''''''''''''

Clone the git repository:

.. code-block:: bash

    git clone git@github.com:transferwise/pipelinewise.git


Run the install script.
PipelineWise is creating virtual environments for every components and running them independently.

.. code-block:: bash

    ./install.sh