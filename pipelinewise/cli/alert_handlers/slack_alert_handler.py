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
    BaseAlertHandler.ERROR: 'danger',
}


def _markdown_text(value: str) -> str:
    """Escape Slack controls and prevent diagnostic text from closing its fence."""
    return value.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('```', "'''")


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
                raise InvalidAlertHandlerException(
                    'Missing channel in Slack connection'
                )
            self.channel = config['channel']
            self.data_diff_channel = config.get('data_diff_channel')

        else:
            raise InvalidAlertHandlerException('No valid Slack config supplied.')

        self.client = WebClient(self.token)

    def send(
        self, message: str, level: str = BaseAlertHandler.ERROR, exc: Exception = None,
        tap_slack_channel: str = None, details: str = None, next_action: str = None, data_diff: bool = False,
    ) -> None:
        """
        Send alert

        Args:
            message: the alert message
            level: alert level
            exc: optional exception that triggered the alert
            tap_slack_channel: optional specific tap slack channel
            details: optional plain-text diagnostic body
            next_action: optional instruction displayed after the body
            data_diff: use the configured data-diff channel instead of the default

        Returns:
            Initialised alert handler object
        """
        channel = (self.data_diff_channel or self.channel) if data_diff else self.channel
        channels = [channel]
        if tap_slack_channel and tap_slack_channel != channel:
            channels.append(tap_slack_channel)

        attachment = {'color': ALERT_LEVEL_SLACK_COLORS.get(level, BaseAlertHandler.ERROR)}
        text = f'```{exc}```' if exc else None
        if details is None:
            attachment['title'] = message
        else:
            text = f':exclamation: *{_markdown_text(message)}*'
            body = f'```{_markdown_text(details)}```'
            if next_action:
                body += f'\n*Next action:* {_markdown_text(next_action)}'
            attachment.update(text=body, mrkdwn_in=['text'], fallback=message)

        for channel in channels:
            self.client.chat_postMessage(
                channel=channel,
                text=text,
                attachments=[attachment],
            )
