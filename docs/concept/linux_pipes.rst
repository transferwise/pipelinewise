.. _linux_pipes:

Linux Pipes in PipelineWise
===========================

A pipe is unidirectional interprocess communication channel. The term was coined by
`Douglas McIlroy <https://en.wikipedia.org/wiki/Douglas_McIlroy>`_ for Unix shell and
named by analogy with the pipeline.

Pipes are most often used in shell-scripts to connect multiple commands by redirecting the output of
one command (stdout) to the input (stdin) followed by using a pipe symbol '`|`'. :ref:`singer` specification,
hence PipelineWise is also using linux pipes to connect :ref:`taps_list` and :ref:`targets_list` connectors.

For example in the following command ``tap-postgres`` Extracts data from a postgres database and the
extracted data sent to ``target-snowflake`` to Load it into a Snowflake database:

.. code-block:: bash

    tap-postgres | target-snowflake

Logic
-----

Pipes provide asynchronous execution of commands using buffered I/O routines. Thus, all the commands
in the pipeline operate in parallel, each in its own process.

The size of the buffer since kernel version 2.6.11 is 65536 bytes (64K) and is equal to the page memory
in older kernels. When attempting to read from an empty buffer, the read process is blocked until data
appears. Similarly, if you attempt to write to a full buffer, the recording process will be blocked until
the necessary amount of space is available.

It is important to note, that despite the fact that pipes operates using file descriptor I/O streams,
operations are performed in memory without loading to/from the disc.

All the information given below is for bash shell 4.2 and kernel 3.10.10. Further details in the
original `Linux Pipes Tips & Tricks <https://blog.dataart.com/linux-pipes-tips-tricks>`_ post.

Increasing buffer size
----------------------

Sometimes the default 64K buffer size that provided by the Linux kernel is too small. For example in the
example above when you extracting data from a busy postgres database and loading into a busy Snowflake
database sometimes you will find that that ``tap-postgres`` is blocked by ``target-snowflake``.

This happens when the target cannot load the data fast enough. For example if you have lot of concurrent
queries in the target database the database can queue up new queries (at least in case of a Snowflake database)
and this is blocking the tap to extract more data. This scenario can cause unexpected timeout in
``tap-postgres`` and other tap connectors. To avoid this scenario you can consider to increase the buffer size
between the tap and target.


.. warning::

  PipelineWise doesn't modify the kernel buffer size. When you need more buffer than
  the defualt 64K that's provided by the kernel, PipelineWise will use its own
  buffering mechanism between taps and targets.

  PipelineWise is using `mbuffer <https://www.maier-komor.de/mbuffer.html>`_ to
  create custom sized buffer between taps and targets.

You can set custom buffer sizes in the tap YAML files by setting the ``stream_buffer_size``
value. If ``stream_buffer_size`` is greater than 0 then the following piped command will be
generated to create larger buffer between taps and targets than the default
buffer that's provided by the Linux kernel:

.. code-block:: bash

    tap-postgres | mbuffer -m 10M | target-snowflake
