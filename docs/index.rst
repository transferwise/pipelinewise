Notice
======
To better serve Wise business and customer needs, the PipelineWise codebase needs to shrink.
We have made the difficult decision that, going forward many components of PipelineWise will be removed or incorporated in the main repo.
The last version before this decision is `v0.64.1 <https://github.com/transferwise/pipelinewise/tree/v0.64.1>`_

We thank all in the open-source community, that over the past 6 years, have helped to make PipelineWise a robust product for heterogeneous replication of many many Terabytes, daily

.. image:: img/pipelinewise-with-text.png
    :width: 300

Documentation
=============

PipelineWise is a Data Pipeline Framework using the `Singer.io <https://www.singer.io/>`_
specification to ingest and replicate data from various sources to various destinations.

.. image:: img/pipelinewise-diagram-circle-bold.png

------------

Features
--------

- **Built with ELT in mind**: PipelineWise fits into the ELT landscape and is not a traditional ETL tool. PipelineWise aims to reproduce the data from the source to an Analytics-Data-Store in as close to the original format as possible. Some minor load time transformations are supported but complex mapping and joins have to be done in the Analytics-Data-Store to extract meaning.
- **Lightweight**: No daemons or database setup are required
- **Replication Methods**: Log-Based (CDC), Key-Based Incremental and Full Table snapshots
- **Managed Schema Changes**: When source data changes, PipelineWise detects the change and alters the schema in your Analytics-Data-Store automatically
- **Load time transformations**: Ideal place to obfuscate, mask or filter sensitive data that should never be replicated in the Data Warehouse
- **YAML based configuration**: Data pipelines are defined as YAML files, ensuring that the entire configuration is kept under version control
- **Extensible**: PipelineWise is using `Singer.io <https://www.singer.io/>`_  compatible taps and target connectors. New connectors can be added to PipelineWise with relatively small effort


Beyond the Horizon
------------------

PipelineWise is built on top of several `Singer.io <https://www.singer.io/>`_ components. Singer.io components
are responsible for certain tasks like extracting data from a specific data source or loading data into a
specific destination, however to replicate data end to end you'll need an extra layer on top of these components to
run the jobs, create configurations, select streams to replicate, do logging and more.

This is where PipelineWise comes in place. PipelineWise is a collection of pre-selected singer taps and
targets to add the required functionalities to create, run and maintain data pipelines in a production Data Warehouse
environment without the extra hassle.


Taps (Data Source Connectors)
-----------------------------

:ref:`taps_list`


Target (Destination Connectors)
-------------------------------

:ref:`targets_list`


Transformation at load time
-------------------------------

:ref:`transformations`


Content
-------
.. toctree::
   :maxdepth: 2
   :caption: Installation

   installation_guide/installation
   installation_guide/creating_pipelines
   installation_guide/running_pipelines

.. toctree::
   :maxdepth: 2
   :caption: Concept

   concept/singer
   concept/replication_methods
   concept/fastsync
   concept/linux_pipes

.. toctree::
   :maxdepth: 2
   :caption: Using PipelineWise

   user_guide/yaml_config
   user_guide/encrypting_passwords
   user_guide/cli
   user_guide/scheduling
   user_guide/metadata_columns
   user_guide/schema_changes
   user_guide/transformations
   user_guide/logging
   user_guide/alerts
   user_guide/resync
   user_guide/partial_sync

.. toctree::
   :maxdepth: 2
   :caption: Connectors

   connectors/taps
   connectors/targets

.. toctree::
   :maxdepth: 2
   :caption: Project

   project/contribution
   project/about
   project/licenses

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
