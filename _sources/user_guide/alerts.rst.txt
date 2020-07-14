
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


.. _slack_alert_handler:

Slack Alert Handler
-------------------

To send alerts to a Slack channel on failed tap runs:

1. Follow the instructions at `Create a new Slack app <https://api.slack.com/authentication/basics>`_ and get a `Bot user token <https://api.slack.com/authentication/token-types#bot>`_.

2. Add the ``chat:write`` OAuth Scope to the app.

3. Invite the bot to the channel by the ``/invite <bot_name>`` slack command.

4. Configure main ``config.yml``

.. code-block:: bash

    ---

    alert_handlers:
      slack:
        token: "slack-token"
        channel: "#slack-channel"
