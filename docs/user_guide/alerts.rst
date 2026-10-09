.. _alerts:

Alerts
======

PipelineWise sends failure alerts through handlers configured in ``config.yml``.
Replication and data-diff share the same routing and per-tap suppression.
Data-diff can use a separate global Slack channel.


Handlers
--------

.. list-table:: Available
   :header-rows: 1
   :widths: 28 72
   :width: 100%

   * - Handler
     - Behaviour
   * - Slack
     - Sends to the global channel and optional tap-specific channel.
       Data-diff can replace the global destination with its own channel.

.. list-table:: Experimental
   :header-rows: 1
   :widths: 28 72
   :width: 100%

   * - Handler
     - Limitation
   * - VictorOps
     - Mock-endpoint coverage only; all taps use one routing key.


.. _slack_alert_handler:

Slack
-----

Create a Slack app with ``chat:write``, install it to the workspace, and invite
the bot to each destination channel.

.. code-block:: yaml

   alert_handlers:
     slack:
       token: "{{ env_var['SLACK_BOT_TOKEN'] }}"
       channel: "#pipeline-alerts"
       data_diff_channel: "#data-diff-alerts"  # Optional

``channel`` receives replication alerts. If ``data_diff_channel`` is set,
data-diff uses it instead of ``channel``. Otherwise, both use ``channel``.
``data_diff_channel`` must be a nonempty string when supplied.

Add a second destination for one tap:

.. code-block:: yaml

   slack_alert_channel: "#orders-alerts"

The tap-specific channel receives a copy; it does not replace the global channel.
When the tap-specific and global destination are the same, Slack receives one
message. See :ref:`data_diff_alerts` for the data-diff message layout.


.. _victorops_alert_handler:

VictorOps
---------

.. attention::

   Verify this handler against the real integration before relying on it for
   on-call response. It has only been exercised against a mocked endpoint.

.. code-block:: yaml

   alert_handlers:
     victorops:
       base_url: "https://alert.victorops.com/integrations/generic/.../alert"
       routing_key: "pipelinewise"

``base_url`` must exclude the routing key. Requests time out after 10 seconds.


Routing and suppression
-----------------------

``send_alert: false`` in a tap YAML suppresses its replication and data-diff
alerts. It does not change command exit status or persisted run results.

Test every handler after configuration and monitor the handler itself. An alert
channel is not a substitute for scheduler exit-status monitoring or target-data
verification.
