"""
PipelineWise CLI - Slack alert handler
"""
import logging

from slack import WebClient

from .errors import InvalidAlertHandlerException
from .base_alert_handler import BaseAlertHandler

LOGGER = logging.getLogger(__name__)

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
        best_effort: bool = False,
    ) -> int | None:
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
            best_effort: attempt every channel and log delivery failures instead of raising

        Returns:
            Successful destination count in best-effort mode, otherwise None
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
                body += f'\n\n*Next action:*\n{_markdown_text(next_action)}'
            attachment.update(text=body, mrkdwn_in=['text'], fallback=message)

        sent = 0
        for channel in channels:
            try:
                self.client.chat_postMessage(
                    channel=channel,
                    text=text,
                    attachments=[attachment],
                )
                sent += 1
            except Exception as delivery_error:
                if not best_effort:
                    raise
                LOGGER.warning('Cannot send Slack alert to %s: %s', channel, delivery_error)

        if best_effort:
            return sent
        return None
