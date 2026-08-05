"""
PipelineWise CLI - Base class of alert handlers
"""

from abc import ABC, abstractmethod


class BaseAlertHandler(ABC):
    """
    Abstract base class for alert handlers
    """

    LOG = "log"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"

    @abstractmethod
    def send(self, message: str, level: str = ERROR, exc: Exception = None, **kwargs) -> None:
        """
        Send alert

        Args:
            message: the alert message
            level: alert level
            exc: optional exception that triggered the alert

        Returns:
            Initialised alert handler object
        """
        pass
