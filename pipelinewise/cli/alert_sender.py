"""
PipelineWise CLI - Alert sender class
"""
import logging
from typing import Dict
from collections import namedtuple

from .alert_handlers.base_alert_handler import BaseAlertHandler
from .alert_handlers.slack_alert_handler import SlackAlertHandler
from .alert_handlers.victorops_alert_handler import VictoropsAlertHandler

from .alert_handlers.errors import InvalidAlertHandlerException
from .alert_handlers.errors import NotImplementedAlertHandlerException
from .alert_handlers.errors import NotConfiguredAlertHandlerException

LOGGER = logging.getLogger(__name__)

# Alert handler entries from PPW config.yml transformed to Tuples
AlertHandler = namedtuple('AlertHandler', ['type', 'config'])

# Register new alert handlers class here
# The key is the alert handler name from the PPW config.yml
# Every alert handler class needs to implement the BaseAlertHandler base class
ALERT_HANDLER_TYPES_TO_CLASS = {
    'slack': SlackAlertHandler,
    'victorops': VictoropsAlertHandler,
}


class AlertSender:
    """
    AlertDispatcher class

    Takes a list of alert handlers that is normally defined
    in the PPW config.yml and dispatching alert messages to
    alert handler implementations
    """

    def __init__(self, alert_handlers: Dict = None) -> None:
        # Initialise alert_handlers as empty dictionary if None provided
        if not alert_handlers:
            self.alert_handlers = {}
        else:
            self.alert_handlers = alert_handlers

        # Raise an exception if alert_handlers is not a dictionary
        if not isinstance(self.alert_handlers, dict):
            raise InvalidAlertHandlerException(
                'alert_handlers needs to be a dictionary'
            )

    @staticmethod
    def __init_handler_class(alert_handler: AlertHandler) -> BaseAlertHandler:
        """
        Takes an alert handler specification and initialises an alert handler class

        Args:
            alert_handler: an AlertHandler typed Tuple

        Returns:
            Initialised alert handler object
        """
        try:
            # Get and initialise the correct alert handler class
            alert_handler_class = ALERT_HANDLER_TYPES_TO_CLASS[alert_handler.type]
            handler = alert_handler_class(alert_handler.config)
        except KeyError as key_error:
            raise NotImplementedAlertHandlerException(
                f'Alert handler type not implemented: {alert_handler.type}'
            ) from key_error

        return handler

    def __get_alert_handler(self, alert_handler_type: str) -> AlertHandler:
        """
        Get an alert handler from the alert handlers dict

        Args:
            alert_handler_type: type of the alert handler (slack, rollbar, etc.)

        Returns:
            AlertHandler tuple
        """
        if alert_handler_type in self.alert_handlers:
            alert_handler_config = self.alert_handlers[alert_handler_type]
            alert_handler = AlertHandler(
                type=alert_handler_type, config=alert_handler_config
            )
            return alert_handler

        raise NotConfiguredAlertHandlerException(
            f'Alert handler type not configured: {alert_handler_type}'
        )

    def send_to_handler(
        self,
        alert_handler_type: str,
        message: str,
        level: str = BaseAlertHandler.ERROR,
        exc: Exception = None,
        tap_slack_channel: str = None,
        details: str = None,
        next_action: str = None,
        data_diff: bool = False,
        best_effort: bool = False,
    ) -> bool:
        """
        Sends an alert message to a specific alert handler type

        Args:
            alert_handler_type: type of the alert handler (slack, rollbar, etc.)
            message: alert text message to send
            level: alert level
            exc: optional exception that triggered the alert
            tap_slack_channel: optional specific tap slack channel
            details: optional plain-text diagnostic body
            next_action: optional instruction displayed after the body
            data_diff: use the configured data-diff Slack channel
            best_effort: retain partial Slack delivery and log failed destinations

        Returns:
            True if alert sent successfully
        """
        # Get the alert handler class
        alert_handler = self.__get_alert_handler(alert_handler_type)

        # Initialise and create an alert handler object from the the alert handler spec
        handler = self.__init_handler_class(alert_handler)

        if alert_handler_type == 'slack':
            extra = {}
            if details is not None:
                extra.update(details=details, next_action=next_action)
            if data_diff:
                extra['data_diff'] = True
            if best_effort:
                extra['best_effort'] = True
            sent = handler.send(message=message, level=level, exc=exc, tap_slack_channel=tap_slack_channel, **extra)
            return sent > 0 if best_effort else True
        else:
            if details is not None:
                message += f'\n{details}'
            if next_action:
                message += f'\n\nNext action:\n{next_action}'
            handler.send(message=message, level=level, exc=exc)

        # Alert sent successfully
        return True

    def send_to_all_handlers(
        self, message: str, level: str = BaseAlertHandler.ERROR, exc: Exception = None,
        tap_slack_channel: str = None, details: str = None, next_action: str = None, data_diff: bool = False,
        best_effort: bool = False,
    ) -> dict:
        """
        Get all the configured alert handlers and send alert
        message to all of them

        Args:
            message: alert text message
            level: alert level
            exc: optional exception that triggered the alert
            tap_slack_channel: optional specific tap slack channel
            details: optional plain-text diagnostic body
            next_action: optional instruction displayed after the body
            data_diff: use the configured data-diff Slack channel
            best_effort: attempt every handler and log failures instead of raising

        Returns:
            Dictionary with number of handlers that delivered to at least one destination
        """
        sent = 0
        extra = {'best_effort': True} if best_effort else {}
        for handler_type in self.alert_handlers:
            try:
                sent += self.send_to_handler(
                    handler_type, message, level, exc, tap_slack_channel,
                    details=details, next_action=next_action, data_diff=data_diff, **extra,
                )
            except Exception as delivery_error:
                if not best_effort:
                    raise
                LOGGER.warning('Cannot send alert using %s handler: %s', handler_type, delivery_error)
        return {'sent': sent}
