import logging
import logging.config
import os


def get_logger(name='singer'):
    """Return a Logger instance to use in singer."""
    # Use custom logging config provided by environment variable
    if 'LOGGING_CONF_FILE' in os.environ and os.environ['LOGGING_CONF_FILE']:
        path = os.environ['LOGGING_CONF_FILE']
        logging.config.fileConfig(path, disable_existing_loggers=False)
    # Use the default logging conf that meets the singer specs criteria
    else:
        this_dir, _ = os.path.split(__file__)
        path = os.path.join(this_dir, 'logging.conf')
        logging.config.fileConfig(path, disable_existing_loggers=False)

    return logging.getLogger(name)
