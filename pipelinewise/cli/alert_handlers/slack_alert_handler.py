"""
PipelineWise CLI - Slack alert handler
"""
from slack import WebClient

from .errors import InvalidAlertHandlerException
from .base_alert_handler import BaseAlertHandler

# Map alert levels to slack compatible color names
ALERT_LEVEL_SLACK_COLORS = {
    BaseAlertHandler.LOG: '36C5F0',
    BaseAlertHandler.INFO: 'good',
    BaseAlertHandler.WARNING: 'warning',
    BaseAlertHandler.ERROR: 'danger'
}


# pylint: disable=too-few-public-methods
class SlackAlertHandler(BaseAlertHandler):
    """
    Slack Alert Handler class
    """
    def __init__(self, config: dict) -> None:
        if config is not None:
            if 'token' not in config:
                raise InvalidAlertHandlerException('Missing token in Slack connection')
            self.token = config['token']

            if 'channel' not in config:
                raise InvalidAlertHandlerException('Missing channel in Slack connection')
            self.channel = config['channel']

        else:
            raise InvalidAlertHandlerException('No valid Slack config supplied.')

        self.client = WebClient(self.token)

    def send(self, message: str, level: str = BaseAlertHandler.ERROR, exc: Exception = None) -> None:
        """
        Send alert

        Args:
            message: the alert message
            level: alert level
            exc: optional exception that triggered the alert

        Returns:
            Initialised alert handler object
        """
        self.client.chat_postMessage(channel=self.channel,
                                     text=f'```{exc}```' if exc else None,
                                     attachments=[{
                                         'color': ALERT_LEVEL_SLACK_COLORS.get(level, BaseAlertHandler.ERROR),
                                         'title': message
                                     }])
