
.. _alerts:

Alerts
------

PipelineWise can send alerts to external systems on run failures by configuring
alert handlers in the main ``config.yml``. This ``config.yml`` is created
automatically when :ref:`generating_pipelines`. Alerts triggered on
:ref:`cli_run_tap` or :ref:`cli_sync_tables` CLI command failures. The triggered
alert provides the id of the failed tap and a description about the failure
to the alert handler.

.. warning::

  You can optionally disable alerts on certain taps by adding ``send_alert: False``
  optional entry to any tap :ref:`yaml_configuration` file.


Currently available alert handlers:
 * :ref:`slack_alert_handler`
 * :ref:`victorops_alert_handler`


.. _slack_alert_handler:

Slack Alert Handler
'''''''''''''''''''

To send alerts to a Slack channel on failed tap runs:

1. Follow the instructions at `Create a new Slack app <https://api.slack.com/authentication/basics>`_ and get a `Bot user token <https://api.slack.com/authentication/token-types#bot>`_.

2. Add the ``chat:write`` OAuth Scope to the app.

3. Invite the bot to the channel by the ``/invite <bot_name>`` slack command.

4. Configure the main ``config.yml``

   **Config parameters**:

   ``token``: Slack bot user token

   ``channel``: Slack channel where the alerts will be sent

.. code-block:: bash

    ---

    alert_handlers:
      slack:
        token: "slack-token"
        channel: "#slack-channel"



.. _victorops_alert_handler:

VictorOps Alert Handler
'''''''''''''''''''''''

To send alerts and open an incident on VictorOps:

1. Follow the instructions at `Enable the VictorOps REST Endpoint <https://help.victorops.com/knowledge-base/rest-endpoint-integration-guide/>`_ and get the long notify URL.

2. Find your routing key in VictorOps settings page

3. Configure the main ``config.yml``:

   **Config parameters**:

   ``base_url``: The VictorOps notify URL **without** the routing key

   ``routing_key``: VictorOps routing key

.. code-block:: bash

    ---

    alert_handlers:
      victorops:
        base_url: "https://alert.victorops.com/integrations/generic/.../alert/.../..."
        routing_key: "victorops-routing-key"

.. warning::

  Make sure the VictorOps ``base_url`` **does not include** the ``routing_key``.

