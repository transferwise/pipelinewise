"""
PipelineWise CLI - Base class of alert handlers
"""


# pylint: disable=too-few-public-methods
class BaseAlertHandler:
    """
    Abstract base class for alert handlers
    """
    LOG = 'log'
    INFO = 'info'
    WARNING = 'warning'
    ERROR = 'error'

    def send(self, message: str, level: str = ERROR, exc: Exception = None) -> None:
        """
        Send alert

        Args:
            message: the alert message
            level: alert level
            exc: optional exception that triggered the alert

        Returns:
            Initialised alert handler object
        """
        raise NotImplementedError()
