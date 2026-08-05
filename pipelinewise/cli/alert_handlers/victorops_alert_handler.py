"""
PipelineWise CLI - VictorOps alert handler
"""

import json
import requests

from .errors import InvalidAlertHandlerException
from .base_alert_handler import BaseAlertHandler

# An unresponsive endpoint must not stall the run that is trying to report a failure.
REQUEST_TIMEOUT_SECONDS = 10

# Map alert levels to victorops compatible message types
ALERT_LEVEL_MESSAGE_TYPES = {
    BaseAlertHandler.LOG: "INFO",
    BaseAlertHandler.INFO: "INFO",
    BaseAlertHandler.WARNING: "WARNING",
    BaseAlertHandler.ERROR: "CRITICAL",
}


class VictoropsAlertHandler(BaseAlertHandler):
    """
    VictorOps Alert Handler class
    """

    def __init__(self, config: dict) -> None:
        if config is not None:
            if "base_url" not in config:
                raise InvalidAlertHandlerException("Missing REST Endpoint URL in VictorOps connection")
            self.base_url = config["base_url"]

            if "routing_key" not in config:
                raise InvalidAlertHandlerException("Missing routing key in VictorOps connection")
            self.routing_key = config["routing_key"]

        else:
            raise InvalidAlertHandlerException("No valid VictorOps config supplied.")

    def send(self, message: str, level: str = BaseAlertHandler.ERROR, exc: Exception = None, **kwargs) -> None:
        """
        Send alert

        Args:
            message: the alert message
            level: alert level
            exc: optional exception that triggered the alert

        Returns:
            Initialised alert handler object
        """
        # Send alert to VictorOps REST Endpoint as a HTTP post request
        response = requests.post(
            f"{self.base_url}/{self.routing_key}",
            data=json.dumps(
                {
                    "message_type": ALERT_LEVEL_MESSAGE_TYPES.get(level, BaseAlertHandler.ERROR),
                    "entity_display_name": message,
                    # str(): an exception object is not JSON serializable
                    "state_message": str(exc) if exc is not None else None,
                }
            ),
            headers={"Content-Type": "application/json"},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        # Success victorops message should return 200
        if response.status_code != 200:
            raise ValueError(
                "Request to victorops returned an error {}. {}".format(response.status_code, response.text)
            )
