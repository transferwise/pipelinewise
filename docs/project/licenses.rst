.. _licenses:

Licenses
========

PipelineWise core is Apache License 2.0. Packaged connectors can use different
licenses, so the obligations of a distributed image depend on every component it
contains. Review the actual connector source and dependency licenses before
building or distributing a customised image.

.. warning::

   Including an AGPL component can add AGPL obligations to the distributed
   combined build. Obtain legal guidance for the intended use and distribution;
   this page is an inventory, not legal advice.


Packaged components
-------------------

.. list-table::
   :header-rows: 1
   :widths: 58 42
   :width: 100%

   * - Component
     - Standalone license
   * - PipelineWise core and ``transform-field``
     - Apache License 2.0
   * - ``tap-github``
     - AGPL 3.0
   * - ``tap-jira``
     - AGPL 3.0
   * - ``tap-kafka``
     - AGPL 3.0
   * - ``tap-mixpanel``
     - AGPL 3.0
   * - ``tap-mongodb``
     - AGPL 3.0
   * - ``tap-mysql``
     - AGPL 3.0
   * - ``tap-postgres``
     - AGPL 3.0
   * - ``tap-s3-csv``
     - AGPL 3.0
   * - ``tap-salesforce``
     - AGPL 3.0
   * - ``tap-slack``
     - AGPL 3.0
   * - ``tap-snowflake``
     - Apache License 2.0
   * - ``tap-twilio``
     - AGPL 3.0
   * - ``tap-yugabyte``
     - AGPL 3.0
   * - ``tap-zendesk``
     - AGPL 3.0
   * - ``target-postgres``
     - Apache License 2.0
   * - ``target-s3-csv``
     - Apache License 2.0
   * - ``target-snowflake``
     - Apache License 2.0

See :ref:`selecting_singer_connectors` to build only the required components.
