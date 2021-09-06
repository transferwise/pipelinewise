import os
import logging

from logging.config import fileConfig
from pathlib import Path


# pylint: disable=too-few-public-methods
class Logger:
    """PipelineWise logger class"""

    def __init__(self, debug: bool = False) -> None:
        # Use custom logging configuration if LOGGING_CONF_FILE env var defined
        if 'LOGGING_CONF_FILE' in os.environ and os.environ['LOGGING_CONF_FILE']:
            path = os.environ['LOGGING_CONF_FILE']
        # Use the the embedded logging_debug.conf config if PipelineWise started in debug mode
        elif debug:
            path = os.path.join(Path(__file__).parent, 'logging_debug.conf')
            os.environ['LOGGING_CONF_FILE'] = path
        # Use the default logging.conf otherwise
        else:
            # Get path to logging config file and set the LOGGING_CONF_FILE env variable
            path = os.path.join(Path(__file__).parent, 'logging.conf')
            os.environ['LOGGING_CONF_FILE'] = path

        # Use the path to define the logging config, and don't disable any pre-existing loggers
        fileConfig(path, disable_existing_loggers=False)

    @staticmethod
    def get_logger(name: str):
        """Get the configured python logger class"""
        return logging.getLogger(name)
