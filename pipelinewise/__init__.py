import os

from logging.config import fileConfig
from pathlib import Path

if 'LOGGING_CONF_FILE' in os.environ and os.environ['LOGGING_CONF_FILE']:
    PATH = os.environ['LOGGING_CONF_FILE']
else:
    # Get path to logging config file and set the LOGGING_CONF_FILE env variable
    PATH = os.path.join(Path(__file__).parent, 'logging.conf')
    os.environ.putenv('LOGGING_CONF_FILE', PATH)

# Use the path to define the logging config, and don't disable any pre-existing loggers
fileConfig(PATH, disable_existing_loggers=False)
