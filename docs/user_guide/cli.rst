
.. _command_line_interface:

Command Line Interface
======================

PipelineWise's command line interface allows for a number of operation types on a pipeline.


.. code-block:: bash

    usage: pipelinewise [-h]
                        {init,run_tap,discover_tap,status,test_tap_connection,sync_tables,import_config,validate}


Positional Arguments
--------------------

:subcommand: Possible choices: init, run_tap, discover_tap, status, test_tap_connection, sync_tables, import_config, validate


Sub-commands:
-------------

.. _cli_init:

init
""""

Initialise and create a sample project. The project will contain sample YAML
configuration for every supported tap and target connects.

Positional Arguments
''''''''''''''''''''

:--name: name of the project



.. _cli_run_tap:

run_tap
"""""""

Run a specific pipeline

:--target: Target connector id

:--tap: Tap connector id

:--extra_log: Optional: Copy logging into PipelineWise logger to see tap run logs at runtime in STDOUT

:--enable_fastsync: Optional: Use FastSync when possible. Enabled by default.


.. _cli_stop_tap:

stop_tap
""""""""

Stop running a specific pipeline

:--target: Target connector id

:--tap: Tap connector id


.. _cli_discover_tap:

discover_tap
""""""""""""

Run a specific tap in discovery mode. Discovery mode is connecting to the data source
and collecting information that is required for running the tap.

:--target: Target connector id

:--tap: Tap connector id


.. _cli_status:

status
""""""

Prints a status summary table of every imported pipeline with their tap and target.


.. _cli_test_tap_connection:

test_tap_connection
"""""""""""""""""""

Test the tap connection. It will connect to the data source that is defined in the tap
and will return success if it's available.

:--target: Target connector id

:--tap: Tap connector id


.. _cli_sync_tables:

sync_tables
"""""""""""

Sync or resync one or more tables from a specific datasource. It performs an initial
sync and resets the table bookmarks to their new location.

:--target: Target connector id

:--tap: Tap connector id

:--tables: Optional: Comma separated list of tables to sync from the data source.

:--enable_fastsync: Optional: Use FastSync when possible. Enabled by default.


.. _cli_import:

import
""""""

Import a project directory into PipelineWise. It will create every JSON file required for
the tap and target connectors into ``~/.pipelinewise``.

:--dir: relative path to the project directory to import

.. _cli_validate:

validate
""""""""

Validates a project directory with YAML tap and target files.

:--dir: relative path to the project directory with YAML taps and targets.
