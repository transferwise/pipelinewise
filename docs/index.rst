
.. image:: img/pipelinewise-with-text.png
    :width: 300

Documentation
=============

PipelineWise is a Data Pipeline Framework using the `Singer.io <https://www.singer.io/>`_
specification to ingest and reproduce data from various sources to various destinations.

.. image:: img/pipelinewise-diagram-circle-bold.png

------------

Features
--------

- **Built with ELT in mind**: PipelineWise fits into the ELT landscape but does not do traditional ETL. PipelineWise ingests data first into DWH in the original format and the "transformation" shifts to the end of the data pipeline. Load time transformations are still supported but complex mapping and joins have to be done once the data is reproduced into the Data Warehouse.
- **Lightweight**: No daemons or database setup are required
- **Reproduction Methods**: CDC (Log Based), Key-Based Incremental and Full Table snapshots
- **Managed Schema Changes**: When source data changes, PipelineWise detects the change and alters the schema in your DWH automatically
- **Load time transformations**: Ideal place to obfuscate, mask or filter sensitive data that should never be reproduced in the Data Warehouse
- **YAML based configuration**: Data pipelines are defined as YAML files, ensuring that the entire configuration is kept under version control
- **Integration with external tools**: With built-in event handlers you can trigger external scripts automatically when a certain event occures
- **Extensible**: PipelineWise is using `Singer.io <https://www.singer.io/>`_  compatible taps and target connectors. New connectors can be added to PipelineWise with relatively small effort


Beyond the Horizon
------------------

PipelineWise is built on top of several `Singer.io <https://www.singer.io/>`_ components. Singer.io components
are responsible for certain tasks like extracting data from a specific data source or loading data into a
specific destination, however to reproduce data end to end you'll need an extra layer on top of these components to
run the jobs, create configurations, select streams to reproduce, do logging and more.

This is where PipelineWise comes in place. PipelineWise is a collection of pre-selected singer taps and
targets to add the required functionalities to create, run and maintain data pipelines in a production Data Warehouse
environment without the extra hassle.


Taps (Data Source Connectors)
-----------------------------

PipelineWise can reproduce data from the following data sources:

.. container:: tile-wrapper

    .. container:: tile

        .. container:: img-hover-zoom

          .. image:: img/mysql-logo.png
             :target: connectors/taps/mysql.html

        :ref:`tap-mysql`

    .. container:: tile

        .. container:: img-hover-zoom

          .. image:: img/mariadb-logo.png
             :target: connectors/taps/mysql.html

        :ref:`tap-mysql`

.. container:: tile-wrapper

    .. container:: tile

        .. container:: img-hover-zoom

          .. image:: img/postgres-logo.png
             :target: connectors/taps/postgres.html

        :ref:`tap-postgres`

    .. container:: tile

        .. container:: img-hover-zoom

          .. image:: img/oracle-logo.png
             :target: connectors/taps/oracle.html

        :ref:`tap-oracle`

.. container:: tile-wrapper

    .. container:: tile

        .. container:: img-hover-zoom

          .. image:: img/s3-logo.png
             :target: connectors/taps/s3_csv.html

        :ref:`tap-s3-csv`

    .. container:: tile

        .. container:: img-hover-zoom

          .. image:: img/kafka-logo.png
             :target: connectors/taps/kafka.html

        :ref:`tap-kafka`

.. container:: tile-wrapper

    .. container:: tile

        .. container:: img-hover-zoom

          .. image:: img/snowflake-logo.png
             :target: connectors/taps/snowflake.html

        :ref:`tap-snowflake`

    .. container:: tile

        .. container:: img-hover-zoom

          .. image:: img/salesforce-logo.png
             :target: connectors/taps/salesforce.html

        :ref:`tap-salesforce`

.. container:: tile-wrapper

    .. container:: tile

        .. container:: img-hover-zoom

          .. image:: img/zendesk-logo.png
             :target: connectors/taps/zendesk.html

        :ref:`tap-zendesk`

    .. container:: tile

        .. container:: img-hover-zoom

          .. image:: img/jira-logo.png
             :target: connectors/taps/jira.html

        :ref:`tap-jira`


Target (Destination Connectors)
-------------------------------

PipelineWise can reproduce data into the following destinations:

.. container:: tile-wrapper

    .. container:: tile

        .. container:: img-hover-zoom

          .. image:: img/snowflake-logo.png
             :target: connectors/targets/snowflake.html

        :ref:`target-snowflake`

    .. container:: tile

        .. container:: img-hover-zoom

          .. image:: img/redshift-logo.png
             :target: connectors/targets/redshift.html

        :ref:`target-redshift`

.. container:: tile-wrapper

    .. container:: tile

        .. container:: img-hover-zoom

          .. image:: img/postgres-logo.png
             :target: connectors/targets/postgres.html

        :ref:`target-postgres`

    .. container:: tile

        .. container:: img-hover-zoom

          .. image:: img/s3-logo.png
             :target: connectors/targets/s3_csv.html

        :ref:`target-s3-csv`



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
   concept/reproduction_methods
   concept/fastsync

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
   user_guide/resync
   user_guide/integration

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

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
