
.. _alerts:

Alerts
------

PipelineWise can send alerts to external systems on run failures by configuring
alert handlers in the main ``config.yml``. This file is created automatically
when :ref:`generating_pipelines`. Alerts are triggered when :ref:`cli_run_tap`
or :ref:`cli_fast_sync` fails. The alert provides the ID of the failed tap and a description of the failure
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

3. Invite the bot to the channel with the ``/invite <bot_name>`` Slack command.

4. Configure the main ``config.yml``

   **Config parameters**:

   ``token``: Slack bot user token

   ``channel``: Slack channel where the alerts will be sent

.. code-block:: yaml

    ---

    alert_handlers:
      slack:
        token: "slack-token"
        channel: "#slack-channel"


To send a copy of a tap's alerts to a different channel, add the following setting
to that tap's YAML file in addition to the handler configuration above:

.. code-block:: yaml

    ---

    slack_alert_channel: "#specific-channel-for-this-tap"



.. _victorops_alert_handler:

VictorOps Alert Handler
'''''''''''''''''''''''

To send alerts and open an incident on VictorOps:

1. Follow the instructions at `Enable the VictorOps REST Endpoint <https://help.victorops.com/knowledge-base/rest-endpoint-integration-guide/>`_ and get the long notify URL.

2. Find the routing key on the VictorOps settings page.

3. Configure the main ``config.yml``:

   **Config parameters**:

   ``base_url``: The VictorOps notify URL **without** the routing key

   ``routing_key``: VictorOps routing key

.. code-block:: yaml

    ---

    alert_handlers:
      victorops:
        base_url: "https://alert.victorops.com/integrations/generic/.../alert/.../..."
        routing_key: "victorops-routing-key"

.. warning::

  Make sure the VictorOps ``base_url`` **does not include** the ``routing_key``.
